// paper in /paper/bqueue.pdf
use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[repr(C)]
pub struct BQueue<T: Send + 'static> {
    buf: *mut MaybeUninit<T>,
    valid: *mut AtomicBool,
    cap: usize,
    mask: usize,
    head: AtomicUsize,
    batch_head: UnsafeCell<usize>,
    tail: AtomicUsize,
    batch_tail: UnsafeCell<usize>,
    batch_history: AtomicUsize,
}

const BATCH_SIZE: usize = 32; // Reduced from 256 for Miri tests
const INCREMENT: usize = 8; // Cache line worth of increments

unsafe impl<T: Send + 'static> Sync for BQueue<T> {}
unsafe impl<T: Send + 'static> Send for BQueue<T> {}

impl<T: Send + 'static> BQueue<T> {
    pub const fn shared_size(capacity: usize) -> usize {
        mem::size_of::<Self>()
            + capacity * mem::size_of::<MaybeUninit<T>>()
            + capacity * mem::size_of::<AtomicBool>()
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

        for i in 0..capacity {
            ptr::write(buf_ptr.add(i), MaybeUninit::uninit());
            ptr::write(valid_ptr.add(i), AtomicBool::new(false));
        }

        ptr::write(
            header_ptr,
            BQueue {
                buf: buf_ptr,
                valid: valid_ptr,
                cap: capacity,
                mask: capacity - 1,
                head: AtomicUsize::new(0),
                batch_head: UnsafeCell::new(0), // Start at 0, will be updated on first push
                tail: AtomicUsize::new(0),
                batch_tail: UnsafeCell::new(0), // Start at 0, will be updated on first pop
                batch_history: AtomicUsize::new(BATCH_SIZE),
            },
        );

        &mut *header_ptr
    }

    #[inline]
    fn next(&self, idx: usize) -> usize {
        (idx + 1) & self.mask
    }

    #[inline]
    fn mod_(&self, idx: usize) -> usize {
        idx & self.mask
    }

    pub fn push(&self, item: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Relaxed);
        let batch_head = unsafe { *self.batch_head.get() };

        if head == batch_head {
            // Probe BATCH_SIZE slots ahead (just ONE probe, no loop!)
            let probe_idx = self.mod_(head + BATCH_SIZE - 1);

            unsafe {
                if (*self.valid.add(probe_idx)).load(Ordering::Acquire) {
                    // Slot is occupied, queue is full
                    return Err(item);
                }
                // Found empty slots, update batch_head
                *self.batch_head.get() = self.mod_(head + BATCH_SIZE);
            }
        }

        unsafe {
            ptr::write(self.buf.add(head), MaybeUninit::new(item));
            (*self.valid.add(head)).store(true, Ordering::Release);
        }

        self.head.store(self.next(head), Ordering::Relaxed);
        Ok(())
    }

    pub fn pop(&self) -> Result<T, ()> {
        let tail = self.tail.load(Ordering::Relaxed);
        let batch_tail = unsafe { *self.batch_tail.get() };

        if tail == batch_tail {
            // Need to find more filled slots using backtracking
            match self.adaptive_backtrack_deq() {
                Some(new_batch_tail) => unsafe {
                    *self.batch_tail.get() = self.mod_(new_batch_tail + 1);
                },
                None => {
                    return Err(());
                }
            }
        }

        unsafe {
            // Check if current slot has data
            if !(*self.valid.add(tail)).load(Ordering::Acquire) {
                return Err(());
            }
        }

        // Read the value
        let value = unsafe {
            let item = ptr::read(self.buf.add(tail));
            item.assume_init()
        };

        unsafe {
            // Clear the valid flag after reading
            (*self.valid.add(tail)).store(false, Ordering::Release);
        }

        self.tail.store(self.next(tail), Ordering::Relaxed);

        Ok(value)
    }

    fn adaptive_backtrack_deq(&self) -> Option<usize> {
        let tail = self.tail.load(Ordering::Relaxed);

        // Start with the historical batch size
        let mut batch_history = self.batch_history.load(Ordering::Relaxed);

        // Increment batch_history if it's less than BATCH_SIZE
        if batch_history < BATCH_SIZE {
            batch_history = (batch_history + INCREMENT).min(BATCH_SIZE);
            self.batch_history.store(batch_history, Ordering::Relaxed);
        }

        let mut batch_size = batch_history.min(self.cap - 1);

        loop {
            if batch_size == 0 {
                // Check if there's at least one element at current tail
                unsafe {
                    if (*self.valid.add(tail)).load(Ordering::Acquire) {
                        self.batch_history.store(1, Ordering::Relaxed);
                        return Some(tail);
                    }
                }
                return None;
            }

            let batch_tail = self.mod_(tail + batch_size - 1);

            unsafe {
                if (*self.valid.add(batch_tail)).load(Ordering::Acquire) {
                    // Found filled slots, update batch_history
                    self.batch_history.store(batch_size, Ordering::Relaxed);
                    return Some(batch_tail);
                }
            }

            // Binary search for filled slots
            batch_size >>= 1;
        }
    }

    pub fn available(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let batch_head = unsafe { *self.batch_head.get() };

        if head != batch_head {
            return true;
        }

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

        // First check current position
        unsafe {
            if (*self.valid.add(tail)).load(Ordering::Acquire) {
                return false;
            }
        }

        // Use adaptive backtracking to check if truly empty
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
