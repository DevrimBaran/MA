//paper in /paper/spsc/lamport.pdf and Cache‐aware design of general‐purpose Single‐Producer Single‐Consumer queues.pdf
use crate::SpscQueue;
use std::{
    cell::UnsafeCell,
    mem::ManuallyDrop,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
pub struct LamportQueue<T: Send> {
    pub mask: usize,
    pub buf: ManuallyDrop<Box<[UnsafeCell<Option<T>>]>>,
    pub head: AtomicUsize,
    pub tail: AtomicUsize,
    pub owns_buffer: bool,
}

unsafe impl<T: Send> Sync for LamportQueue<T> {}
unsafe impl<T: Send> Send for LamportQueue<T> {}

impl<T: Send> LamportQueue<T> {
    pub fn with_capacity(cap: usize) -> Self {
        assert!(cap.is_power_of_two(), "capacity must be power of two");

        let boxed = (0..cap)
            .map(|_| UnsafeCell::new(None))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            mask: cap - 1,
            buf: ManuallyDrop::new(boxed),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            owns_buffer: true,
        }
    }

    #[inline]
    pub fn idx(&self, i: usize) -> usize {
        i & self.mask
    }
}

impl<T: Send> LamportQueue<T> {
    pub const fn shared_size(cap: usize) -> usize {
        std::mem::size_of::<Self>() + cap * std::mem::size_of::<UnsafeCell<Option<T>>>()
    }
    pub unsafe fn init_in_shared(mem: *mut u8, cap: usize) -> &'static mut Self {
        assert!(cap.is_power_of_two());

        let header = mem as *mut Self;
        let buf_ptr = mem.add(std::mem::size_of::<Self>()) as *mut UnsafeCell<Option<T>>;

        // Initialize all slots to None before creating the slice
        for i in 0..cap {
            buf_ptr.add(i).write(UnsafeCell::new(None));
        }

        let slice = std::slice::from_raw_parts_mut(buf_ptr, cap);
        let boxed = Box::from_raw(slice);

        header.write(Self {
            mask: cap - 1,
            buf: ManuallyDrop::new(boxed),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            owns_buffer: false,
        });

        &mut *header
    }
}

impl<T: Send> LamportQueue<T> {
    #[inline]
    pub fn capacity(&self) -> usize {
        self.mask + 1
    }

    #[inline]
    pub fn head_relaxed(&self) -> usize {
        self.tail.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn tail_relaxed(&self) -> usize {
        self.head.load(Ordering::Relaxed)
    }

    #[inline]
    pub unsafe fn push_unchecked(&mut self, item: T) {
        let tail = self.tail.load(Ordering::Relaxed);
        let slot = self.idx(tail);
        (*self.buf[slot].get()) = Some(item);
        self.tail.store(tail.wrapping_add(1), Ordering::Relaxed);
    }
}

impl<T: Send + 'static> SpscQueue<T> for LamportQueue<T> {
    type PushError = ();
    type PopError = ();

    #[inline]
    fn push(&self, item: T) -> Result<(), ()> {
        let tail = self.tail.load(Ordering::Acquire);
        let next = tail + 1;

        let head = self.head.load(Ordering::Acquire);
        if next == head + self.mask + 1 {
            return Err(());
        }

        let slot = self.idx(tail);
        unsafe { *self.buf[slot].get() = Some(item) };

        self.tail.store(next, Ordering::Release);
        Ok(())
    }

    #[inline]
    fn pop(&self) -> Result<T, ()> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        if head == tail {
            return Err(());
        }

        let slot = self.idx(head);

        let cell_ptr = &self.buf[slot];
        let val = unsafe { (*cell_ptr.get()).take() };

        match val {
            Some(v) => {
                self.head.store(head + 1, Ordering::Release);
                Ok(v)
            }
            None => Err(()),
        }
    }

    #[inline]
    fn available(&self) -> bool {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        tail.wrapping_sub(head) < self.mask
    }

    #[inline]
    fn empty(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head == tail
    }
}

impl<T: Send> Drop for LamportQueue<T> {
    fn drop(&mut self) {
        if std::mem::needs_drop::<T>() {
            let head = *self.head.get_mut();
            let tail = *self.tail.get_mut();

            // Drop all remaining items in the queue
            let mut current = head;
            while current != tail {
                let slot = self.idx(current);
                unsafe {
                    // Take the item out and drop it
                    if let Some(item) = (*self.buf[slot].get()).take() {
                        drop(item);
                    }
                }
                current = current.wrapping_add(1);
            }
        }

        // Only drop the buffer if we own it
        if self.owns_buffer {
            unsafe {
                ManuallyDrop::drop(&mut self.buf);
            }
        }
    }
}
