// paper in /paper/bqueue.pdf
use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[repr(C)]
pub struct BQueue<T: Send + 'static> {
    buf: *mut MaybeUninit<T>,
    valid: *mut AtomicBool, // IPC adaptation: replaces NULL checks with validity flags
    cap: usize,
    mask: usize,
    head: AtomicUsize,             // Line Q01: local to producer
    batch_head: UnsafeCell<usize>, // Line Q01: local to producer
    tail: AtomicUsize,             // Line Q01: local to consumer
    batch_tail: UnsafeCell<usize>, // Line Q01: local to consumer
    batch_history: AtomicUsize,    // Line N01: for adaptive backtracking
}

const BATCH_SIZE: usize = 32; // Reduced from 256 for Miri tests
const INCREMENT: usize = 8; // Line N05: cache line worth of increments

unsafe impl<T: Send + 'static> Sync for BQueue<T> {}
unsafe impl<T: Send + 'static> Send for BQueue<T> {}

impl<T: Send + 'static> BQueue<T> {
    pub const fn shared_size(capacity: usize) -> usize {
        mem::size_of::<Self>()
            + capacity * mem::size_of::<MaybeUninit<T>>()
            + capacity * mem::size_of::<AtomicBool>() // IPC: extra space for validity flags
    }

    pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        assert!(
            capacity > BATCH_SIZE * 2,
            "capacity must be greater than 2 * BATCH_SIZE"
        );

        let header_ptr = mem as *mut Self;
        let buf_ptr = mem.add(mem::size_of::<Self>()) as *mut MaybeUninit<T>;
        let valid_ptr = mem
            .add(mem::size_of::<Self>() + capacity * mem::size_of::<MaybeUninit<T>>())
            as *mut AtomicBool;

        // Initialize buffer and validity flags
        for i in 0..capacity {
            ptr::write(buf_ptr.add(i), MaybeUninit::uninit());
            ptr::write(valid_ptr.add(i), AtomicBool::new(false)); // IPC: false = empty slot
        }

        ptr::write(
            header_ptr,
            BQueue {
                buf: buf_ptr,
                valid: valid_ptr,
                cap: capacity,
                mask: capacity - 1,
                head: AtomicUsize::new(0),      // Line Q02: head = 0
                batch_head: UnsafeCell::new(0), // Line Q02: batch_head = 0
                tail: AtomicUsize::new(0),      // Line Q13: tail = 0
                batch_tail: UnsafeCell::new(0), // Line Q13: batch_tail = 0
                batch_history: AtomicUsize::new(BATCH_SIZE), // Line N02: batch_history = BATCH_SIZE
            },
        );

        &mut *header_ptr
    }

    #[inline]
    fn next(&self, idx: usize) -> usize {
        (idx + 1) & self.mask // NEXT() function in paper
    }

    #[inline]
    fn mod_(&self, idx: usize) -> usize {
        idx & self.mask // MOD() function in paper
    }

    // figure 7
    pub fn push(&self, item: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let batch_head = unsafe { *self.batch_head.get() };

        if head == batch_head {
            // Line Q03: if (head == batch_head)
            // Line Q04: probe = buffer[MOD((head+BATCH_SIZE-1), SIZE)]
            let probe_idx = self.mod_(head + BATCH_SIZE - 1);

            unsafe {
                // Line Q05: if (probe != NULL) - adapted to check valid flag
                if (*self.valid.add(probe_idx)).load(Ordering::Acquire) {
                    return Err(item); // Line Q05: return FAILURE
                }
                // Line Q06: batch_head = MOD((head+BATCH_SIZE), SIZE)
                *self.batch_head.get() = self.mod_(head + BATCH_SIZE);
            }
        }

        // Line Q08: buffer[head] = value
        unsafe {
            ptr::write(self.buf.add(head), MaybeUninit::new(item));
            (*self.valid.add(head)).store(true, Ordering::Release); // IPC: mark slot as filled
        }

        // Line Q09: head = NEXT(head)
        self.head.store(self.next(head), Ordering::Relaxed);
        Ok(()) // Line Q10: return SUCCESS
    }

    // figure 7
    pub fn pop(&self) -> Result<T, ()> {
        let tail = self.tail.load(Ordering::Relaxed);
        let batch_tail = unsafe { *self.batch_tail.get() };

        if tail == batch_tail {
            // Line Q14: if (tail == batch_tail)
            // Line Q15: batch_tail = backtrack_deq()
            match self.adaptive_backtrack_deq() {
                Some(new_batch_tail) => unsafe {
                    *self.batch_tail.get() = self.mod_(new_batch_tail + 1);
                },
                None => {
                    return Err(()); // Line Q16: return FAILURE
                }
            }
        }

        // Additional safety check for IPC
        unsafe {
            if !(*self.valid.add(tail)).load(Ordering::Acquire) {
                return Err(());
            }
        }

        // Line Q18: value = buffer[tail]
        let value = unsafe {
            let item = ptr::read(self.buf.add(tail));
            item.assume_init()
        };

        // Line Q19: buffer[tail] = NULL - adapted to clear valid flag
        unsafe {
            (*self.valid.add(tail)).store(false, Ordering::Release);
        }

        // Line Q20: tail = NEXT(tail)
        self.tail.store(self.next(tail), Ordering::Relaxed);

        Ok(value) // Line Q21: return value
    }

    // Adaptive backtracking from Figure 10
    fn adaptive_backtrack_deq(&self) -> Option<usize> {
        let tail = self.tail.load(Ordering::Relaxed);

        // Line N01: batch_history = BATCH_SIZE (initialized in constructor)
        let mut batch_history = self.batch_history.load(Ordering::Relaxed);

        // Lines N03-N05: increment batch_history if less than BATCH_MAX
        if batch_history < BATCH_SIZE {
            batch_history = (batch_history + INCREMENT).min(BATCH_SIZE);
            self.batch_history.store(batch_history, Ordering::Relaxed); // Store early (differs from paper)
        }

        // Line N04: batch_size = batch_history
        let mut batch_size = batch_history.min(self.cap - 1); // IPC: cap bound check

        // Line N07: while (1)
        loop {
            // Extra check for single element case (not in paper)
            if batch_size == 0 {
                unsafe {
                    if (*self.valid.add(tail)).load(Ordering::Acquire) {
                        self.batch_history.store(1, Ordering::Relaxed);
                        return Some(tail);
                    }
                }
                return None; // Line N13: return FAILURE
            }

            // Line N08: batch_tail = MOD((tail+batch_size-1), SIZE)
            let batch_tail = self.mod_(tail + batch_size - 1);

            // Line N09: if (buffer[batch_tail] != NULL) - adapted for valid flags
            unsafe {
                if (*self.valid.add(batch_tail)).load(Ordering::Acquire) {
                    // Line N17: batch_history = batch_size
                    self.batch_history.store(batch_size, Ordering::Relaxed);
                    return Some(batch_tail); // Line N18: return batch_tail
                }
            }

            // Line N11: batch_size = batch_size >> 1
            batch_size >>= 1;
        }
    }

    pub fn available(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let batch_head = unsafe { *self.batch_head.get() };

        if head != batch_head {
            return true;
        }

        // Check if there's space using binary search (adaptation of batching logic)
        let mut slots_to_probe = BATCH_SIZE.min(self.cap - 1);

        loop {
            if slots_to_probe == 0 {
                return false;
            }

            let probe_idx = self.mod_(head + slots_to_probe - 1);

            unsafe {
                if !(*self.valid.add(probe_idx)).load(Ordering::Acquire) {
                    return true;
                }
            }

            slots_to_probe >>= 1;
        }
    }

    pub fn empty(&self) -> bool {
        let tail = self.tail.load(Ordering::Relaxed);

        // Quick check current position
        unsafe {
            if (*self.valid.add(tail)).load(Ordering::Acquire) {
                return false;
            }
        }

        // Use adaptive backtracking to verify emptiness
        self.adaptive_backtrack_deq().is_none()
    }
}

impl<T: Send + 'static> SpscQueue<T> for BQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        self.push(item).map_err(|_| ())
    }

    fn pop(&self) -> Result<T, Self::PopError> {
        self.pop()
    }

    fn available(&self) -> bool {
        self.available()
    }

    fn empty(&self) -> bool {
        self.empty()
    }
}

impl<T: Send + 'static> Drop for BQueue<T> {
    fn drop(&mut self) {}
}
