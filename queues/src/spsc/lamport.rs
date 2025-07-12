//paper in /paper/spsc/lamport.pdf and Cache‐aware design of general‐purpose Single‐Producer Single‐Consumer queues.pdf
//implementation of lamport queue more close to lamport queue from Cache‐aware design of general‐purpose Single‐Producer Single‐Consumer queues.pdf
//padd option for ospsc, dspsc and mspsc. default is unpadded so we can bench original unpadded lamport queue
use crate::SpscQueue;
use std::{
    cell::UnsafeCell,
    mem::ManuallyDrop,
    sync::atomic::{AtomicUsize, Ordering},
};

// Padding traits and types
pub trait PaddingMode: Send + Sync + 'static {
    type Padding: Send + Sync + Default;
}

#[derive(Default)]
pub struct Unpadded;

#[derive(Default)]
pub struct Padded;

#[repr(C)]
#[derive(Default)]
pub struct NoPadding;

#[repr(C)]
pub struct CachePadding {
    _pad: [u8; 120],
}

impl Default for CachePadding {
    fn default() -> Self {
        Self { _pad: [0u8; 120] }
    }
}

impl PaddingMode for Unpadded {
    type Padding = NoPadding;
}

impl PaddingMode for Padded {
    type Padding = CachePadding;
}

#[derive(Debug)]
#[repr(C)]
pub struct LamportQueue<T: Send, P: PaddingMode = Unpadded> {
    pub mask: usize,
    pub buf: ManuallyDrop<Box<[UnsafeCell<Option<T>>]>>, // Circular buffer - Section 3

    pub head: AtomicUsize, // Read index - group B
    _padding1: P::Padding, // Cache line separation

    pub tail: AtomicUsize, // Write index - group A
    _padding2: P::Padding, // Cache line separation

    pub owns_buffer: bool, // IPC adaptation - tracks ownership
}

unsafe impl<T: Send, P: PaddingMode> Sync for LamportQueue<T, P> {}
unsafe impl<T: Send, P: PaddingMode> Send for LamportQueue<T, P> {}

impl<T: Send, P: PaddingMode> LamportQueue<T, P> {
    #[inline]
    pub fn idx(&self, i: usize) -> usize {
        i & self.mask // Fast modulo for power-of-2 sizes
    }
}

impl<T: Send, P: PaddingMode> LamportQueue<T, P> {
    // IPC adaptation - calculate shared memory size
    pub const fn shared_size(cap: usize) -> usize {
        std::mem::size_of::<Self>() + cap * std::mem::size_of::<UnsafeCell<Option<T>>>()
    }

    // IPC adaptation - initialize in pre-allocated shared memory
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
            _padding1: P::Padding::default(),
            tail: AtomicUsize::new(0),
            _padding2: P::Padding::default(),
            owns_buffer: false, // IPC - buffer not heap allocated
        });

        &mut *header
    }
}

impl<T: Send, P: PaddingMode> LamportQueue<T, P> {
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

impl<T: Send + 'static, P: PaddingMode> SpscQueue<T> for LamportQueue<T, P> {
    type PushError = ();
    type PopError = ();

    // lq_enqueue - Figure 2 in paper
    #[inline]
    fn push(&self, item: T) -> Result<(), ()> {
        let tail = self.tail.load(Ordering::Acquire);
        let next = tail + 1;

        let head = self.head.load(Ordering::Acquire); // Check if full
        if next == head + self.mask + 1 {
            return Err(());
        }

        let slot = self.idx(tail);
        unsafe { *self.buf[slot].get() = Some(item) }; // Store item

        self.tail.store(next, Ordering::Release); // store_release_barrier
        Ok(())
    }

    // lq_dequeue - Figure 2 in paper
    #[inline]
    fn pop(&self) -> Result<T, ()> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        if head == tail {
            // Empty check
            return Err(());
        }

        let slot = self.idx(head);

        let cell_ptr = &self.buf[slot];
        let val = unsafe { (*cell_ptr.get()).take() }; // load_acquire_barrier

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

impl<T: Send, P: PaddingMode> Drop for LamportQueue<T, P> {
    fn drop(&mut self) {
        // IPC - cleanup handled externally
    }
}
