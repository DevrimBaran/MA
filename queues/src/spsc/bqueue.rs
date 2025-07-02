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
}

const BATCH_SIZE: usize = 32; // Reduced from 256 for Miri tests

unsafe impl<T: Send + 'static> Sync for BQueue<T> {}
unsafe impl<T: Send + 'static> Send for BQueue<T> {}

impl<T: Send + 'static> BQueue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        assert!(
            capacity > BATCH_SIZE * 2,
            "capacity must be greater than 2 * BATCH_SIZE"
        );

        let mut buf_vec: Vec<MaybeUninit<T>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buf_vec.push(MaybeUninit::uninit());
        }
        let buf = Box::into_raw(buf_vec.into_boxed_slice()) as *mut MaybeUninit<T>;

        // Create atomic bool array
        let mut valid_vec: Vec<AtomicBool> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            valid_vec.push(AtomicBool::new(false));
        }
        let valid = Box::into_raw(valid_vec.into_boxed_slice()) as *mut AtomicBool;

        BQueue {
            buf,
            valid,
            cap: capacity,
            mask: capacity - 1,
            head: AtomicUsize::new(0),
            batch_head: UnsafeCell::new(0),
            tail: AtomicUsize::new(0),
            batch_tail: UnsafeCell::new(0),
        }
    }

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
                batch_head: UnsafeCell::new(0),
                tail: AtomicUsize::new(0),
                batch_tail: UnsafeCell::new(0),
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
            // Need to probe for more space
            let probe_idx = self.mod_(head + BATCH_SIZE);

            unsafe {
                if (*self.valid.add(probe_idx)).load(Ordering::Acquire) {
                    return Err(item);
                }
            }

            unsafe {
                *self.batch_head.get() = probe_idx;
            }
        }

        unsafe {
            // Write the data to buffer
            ptr::write(self.buf.add(head), MaybeUninit::new(item));
            // Set valid flag with Release ordering to ensure data write happens-before
            (*self.valid.add(head)).store(true, Ordering::Release);
        }

        self.head.store(self.next(head), Ordering::Relaxed);

        Ok(())
    }

    pub fn pop(&self) -> Result<T, ()> {
        let tail = self.tail.load(Ordering::Relaxed);

        unsafe {
            // Check if current tail position has valid data
            if !(*self.valid.add(tail)).load(Ordering::Acquire) {
                // If not, use backtracking to find available data
                match self.backtrack_deq() {
                    Some(new_batch_tail) => {
                        *self.batch_tail.get() = new_batch_tail;
                    }
                    None => {
                        return Err(());
                    }
                }
            }
        }

        // Read the value - the Acquire load above ensures we see the data write
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

    fn backtrack_deq(&self) -> Option<usize> {
        let tail = self.tail.load(Ordering::Relaxed);

        let mut batch_size = BATCH_SIZE.min(self.cap);

        let mut batch_tail;

        loop {
            if batch_size == 0 {
                return None;
            }

            batch_tail = self.mod_(tail + batch_size - 1);

            unsafe {
                if (*self.valid.add(batch_tail)).load(Ordering::Acquire) {
                    return Some(batch_tail);
                }
            }

            if batch_size > 1 {
                batch_size >>= 1;
            } else {
                unsafe {
                    if (*self.valid.add(tail)).load(Ordering::Acquire) {
                        return Some(tail);
                    }
                }

                return None;
            }
        }
    }

    pub fn available(&self) -> bool {
        let head = self.head.load(Ordering::Relaxed);
        let batch_head = unsafe { *self.batch_head.get() };

        if head != batch_head {
            return true;
        }

        let probe_idx = self.mod_(head + BATCH_SIZE);
        unsafe { !(*self.valid.add(probe_idx)).load(Ordering::Acquire) }
    }

    pub fn empty(&self) -> bool {
        let tail = self.tail.load(Ordering::Relaxed);
        unsafe {
            if (*self.valid.add(tail)).load(Ordering::Acquire) {
                return false;
            }
        }

        self.backtrack_deq().is_none()
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
    fn drop(&mut self) {
        if std::mem::needs_drop::<T>() {
            let mut tail = self.tail.load(Ordering::Relaxed);
            let head = self.head.load(Ordering::Relaxed);

            while tail != head {
                unsafe {
                    if (*self.valid.add(tail)).load(Ordering::Relaxed) {
                        let item = ptr::read(self.buf.add(tail));
                        drop(item.assume_init());
                    }
                }
                tail = self.next(tail);
            }
        }

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.buf, self.cap));
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.valid, self.cap));
        }
    }
}
