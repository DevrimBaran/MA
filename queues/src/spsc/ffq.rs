// In queues/src/spsc/ffq.rs

use crate::SpscQueue;
use core::{cell::UnsafeCell, fmt, mem::MaybeUninit, ptr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// FastForward-inspired implementation for value types
// Uses a separate atomic flag to indicate full/empty state
// This avoids head/tail comparisons while supporting arbitrary types

#[repr(C)]
pub struct Slot<T> {
    // Atomic flag: false = empty, true = full
    flag: AtomicBool,
    // Padding to separate flag from data
    _pad: [u8; 64 - std::mem::size_of::<AtomicBool>()],
    // The actual data
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            _pad: [0u8; 64 - std::mem::size_of::<AtomicBool>()],
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

#[repr(C, align(64))]
pub struct FfqQueue<T: Send + 'static> {
    pub head: AtomicUsize,
    _pad1: [u8; 64 - std::mem::size_of::<AtomicUsize>()],
    pub tail: AtomicUsize,
    _pad2: [u8; 64 - std::mem::size_of::<AtomicUsize>()],
    capacity: usize,
    pub mask: usize,
    buffer: *mut Slot<T>,
    owns_buffer: bool,
    initialized: AtomicBool,
}

unsafe impl<T: Send> Send for FfqQueue<T> {}
unsafe impl<T: Send> Sync for FfqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct FfqPushError<T>(pub T);
#[derive(Debug, PartialEq, Eq)]
pub struct FfqPopError;

impl<T: Send + 'static> FfqQueue<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two() && capacity > 0);

        let layout = std::alloc::Layout::array::<Slot<T>>(capacity)
            .unwrap()
            .align_to(64)
            .unwrap();

        let ptr = unsafe { std::alloc::alloc(layout) as *mut Slot<T> };

        if ptr.is_null() {
            panic!("Failed to allocate buffer");
        }

        unsafe {
            for i in 0..capacity {
                ptr::write(ptr.add(i), Slot::new());
            }
        }

        Self {
            head: AtomicUsize::new(0),
            _pad1: [0u8; 64 - std::mem::size_of::<AtomicUsize>()],
            tail: AtomicUsize::new(0),
            _pad2: [0u8; 64 - std::mem::size_of::<AtomicUsize>()],
            capacity,
            mask: capacity - 1,
            buffer: ptr,
            owns_buffer: true,
            initialized: AtomicBool::new(true),
        }
    }

    pub fn shared_size(capacity: usize) -> usize {
        assert!(capacity.is_power_of_two() && capacity > 0);
        let self_layout = core::alloc::Layout::new::<Self>();
        let buf_layout = core::alloc::Layout::array::<Slot<T>>(capacity).unwrap();
        let (layout, _) = self_layout.extend(buf_layout).unwrap();
        layout.size()
    }

    pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
        assert!(capacity.is_power_of_two() && capacity > 0);
        assert!(!mem.is_null());

        // Zero out everything first
        ptr::write_bytes(mem, 0, Self::shared_size(capacity));

        // Memory barrier
        std::sync::atomic::fence(Ordering::SeqCst);

        let queue_ptr = mem as *mut Self;
        let buf_ptr = mem.add(std::mem::size_of::<Self>()) as *mut Slot<T>;

        // Initialize each slot
        for i in 0..capacity {
            let slot_ptr = buf_ptr.add(i);
            ptr::write(slot_ptr, Slot::new());
        }

        // Another barrier
        std::sync::atomic::fence(Ordering::SeqCst);

        // Now initialize the queue structure
        ptr::write(
            queue_ptr,
            Self {
                head: AtomicUsize::new(0),
                _pad1: [0u8; 64 - std::mem::size_of::<AtomicUsize>()],
                tail: AtomicUsize::new(0),
                _pad2: [0u8; 64 - std::mem::size_of::<AtomicUsize>()],
                capacity,
                mask: capacity - 1,
                buffer: buf_ptr,
                owns_buffer: false,
                initialized: AtomicBool::new(true),
            },
        );

        &mut *queue_ptr
    }

    #[inline]
    fn ensure_initialized(&self) {
        assert!(
            self.initialized.load(Ordering::Acquire),
            "Queue not initialized"
        );
    }

    #[inline]
    fn get_slot(&self, index: usize) -> &Slot<T> {
        unsafe { &*self.buffer.add(index & self.mask) }
    }
}

impl<T: Send + 'static> SpscQueue<T> for FfqQueue<T> {
    type PushError = FfqPushError<T>;
    type PopError = FfqPopError;

    #[inline]
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        self.ensure_initialized();

        let head = self.head.load(Ordering::Relaxed);
        let slot = self.get_slot(head);

        // Check if slot is empty (following FastForward paper logic)
        if slot.flag.load(Ordering::Acquire) {
            return Err(FfqPushError(item));
        }

        // Write data
        unsafe {
            (*slot.data.get()).write(item);
        }

        // Mark slot as full - this is the linearization point
        slot.flag.store(true, Ordering::Release);

        // Advance head
        self.head.store(head.wrapping_add(1), Ordering::Release);

        Ok(())
    }

    #[inline]
    fn pop(&self) -> Result<T, Self::PopError> {
        self.ensure_initialized();

        let tail = self.tail.load(Ordering::Relaxed);
        let slot = self.get_slot(tail);

        // Check if slot is full
        if !slot.flag.load(Ordering::Acquire) {
            return Err(FfqPopError);
        }

        // Read data
        let data = unsafe { (*slot.data.get()).assume_init_read() };

        // Mark slot as empty - this is the linearization point
        slot.flag.store(false, Ordering::Release);

        // Advance tail
        self.tail.store(tail.wrapping_add(1), Ordering::Release);

        Ok(data)
    }

    #[inline]
    fn available(&self) -> bool {
        self.ensure_initialized();

        let head = self.head.load(Ordering::Relaxed);
        let slot = self.get_slot(head);

        // Slot is available if it's empty
        !slot.flag.load(Ordering::Acquire)
    }

    #[inline]
    fn empty(&self) -> bool {
        self.ensure_initialized();

        let tail = self.tail.load(Ordering::Relaxed);
        let slot = self.get_slot(tail);

        // Queue is empty if next slot to dequeue is empty
        !slot.flag.load(Ordering::Acquire)
    }
}

impl<T: Send + 'static> Drop for FfqQueue<T> {
    fn drop(&mut self) {
        if self.owns_buffer && !self.buffer.is_null() {
            unsafe {
                // Drain any remaining items
                if core::mem::needs_drop::<T>() {
                    for i in 0..self.capacity {
                        let slot = self.get_slot(i);
                        if slot.flag.load(Ordering::Relaxed) {
                            // Drop the item
                            ptr::drop_in_place((*slot.data.get()).as_mut_ptr());
                        }
                    }
                }

                let layout = std::alloc::Layout::array::<Slot<T>>(self.capacity)
                    .unwrap()
                    .align_to(64)
                    .unwrap();
                std::alloc::dealloc(self.buffer as *mut u8, layout);
            }
        }
    }
}

impl<T: fmt::Debug + Send + 'static> fmt::Debug for FfqQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FfqQueue")
            .field("capacity", &self.capacity)
            .field("head", &self.head.load(Ordering::Relaxed))
            .field("tail", &self.tail.load(Ordering::Relaxed))
            .field("owns_buffer", &self.owns_buffer)
            .field("initialized", &self.initialized.load(Ordering::Relaxed))
            .finish()
    }
}
