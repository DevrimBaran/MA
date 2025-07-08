// paper in /paper/spsc/ffq.pdf

use crate::SpscQueue;
use core::{cell::UnsafeCell, fmt, mem::MaybeUninit, ptr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// Paper uses NULL to indicate empty/full - we use AtomicBool for Rust type safety
#[repr(C)]
pub struct Slot<T> {
    // Paper: buffer[i] == NULL check - we use explicit flag
    pub flag: AtomicBool,
    // IPC adaptation: padding for cache alignment
    _pad: [u8; 8 - std::mem::size_of::<AtomicBool>()],
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            flag: AtomicBool::new(false), // false = empty (paper's NULL)
            _pad: [0u8; 8 - std::mem::size_of::<AtomicBool>()],
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

// Paper: Figure 1 shows cache-aligned queue structure
#[repr(C, align(64))]
pub struct FfqQueue<T: Send + 'static> {
    // Paper: head index (enqueue position)
    pub head: AtomicUsize, // in paper private, but here public only for unit tests. Pub logic is not used.
    _pad1: [u8; 64 - std::mem::size_of::<AtomicUsize>()], // IPC: cache line padding
    // Paper: tail index (dequeue position)
    pub tail: AtomicUsize, // in paper private, but here public only for unit tests. Pub logic is not used.
    _pad2: [u8; 64 - std::mem::size_of::<AtomicUsize>()], // IPC: cache line padding
    capacity: usize,
    pub mask: usize, // IPC optimization: mask for fast modulo
    buffer: *mut Slot<T>,
    owns_buffer: bool,       // IPC: track memory ownership
    initialized: AtomicBool, // IPC: initialization check
}

unsafe impl<T: Send> Send for FfqQueue<T> {}
unsafe impl<T: Send> Sync for FfqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct FfqPushError<T>(pub T);
#[derive(Debug, PartialEq, Eq)]
pub struct FfqPopError;

impl<T: Send + 'static> FfqQueue<T> {
    // IPC: calculate shared memory size
    pub fn shared_size(capacity: usize) -> usize {
        assert!(capacity.is_power_of_two() && capacity > 0);
        let self_layout = core::alloc::Layout::new::<Self>();
        let buf_layout = core::alloc::Layout::array::<Slot<T>>(capacity).unwrap();
        let (layout, _) = self_layout.extend(buf_layout).unwrap();
        layout.size()
    }

    // IPC: initialize queue in shared memory
    pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
        assert!(capacity.is_power_of_two() && capacity > 0);
        assert!(!mem.is_null());

        // IPC: zero out memory first
        ptr::write_bytes(mem, 0, Self::shared_size(capacity));
        std::sync::atomic::fence(Ordering::SeqCst);

        let queue_ptr = mem as *mut Self;
        let buf_ptr = mem.add(std::mem::size_of::<Self>()) as *mut Slot<T>;

        // Initialize buffer slots
        for i in 0..capacity {
            let slot_ptr = buf_ptr.add(i);
            ptr::write(slot_ptr, Slot::new());
        }

        std::sync::atomic::fence(Ordering::SeqCst);

        // Initialize queue structure
        ptr::write(
            queue_ptr,
            Self {
                head: AtomicUsize::new(0), // Paper: head = 0
                _pad1: [0u8; 64 - std::mem::size_of::<AtomicUsize>()],
                tail: AtomicUsize::new(0), // Paper: tail = 0
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

    // Paper: buffer[index] access
    #[inline]
    pub fn get_slot(&self, index: usize) -> &Slot<T> {
        unsafe { &*self.buffer.add(index & self.mask) }
    }
}

impl<T: Send + 'static> SpscQueue<T> for FfqQueue<T> {
    type PushError = FfqPushError<T>;
    type PopError = FfqPopError;

    // Paper: enqueue_nonblock() - Figure 4
    #[inline]
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        self.ensure_initialized();

        let head = self.head.load(Ordering::Relaxed); // Line 2
        let slot = self.get_slot(head);

        // Line 2: if (NEXT(head) == tail) - we check slot availability
        if slot.flag.load(Ordering::Acquire) {
            return Err(FfqPushError(item)); // Line 3: return EWOULDBLOCK
        }

        // Line 5: buffer[head] = data
        unsafe {
            (*slot.data.get()).write(item);
        }

        // Paper: implicit in buffer[head] = data - we make it explicit
        slot.flag.store(true, Ordering::Release);

        // Line 6: head = NEXT(head)
        self.head.store(head.wrapping_add(1), Ordering::Release);

        Ok(()) // Line 7: return 0
    }

    // Paper: dequeue_nonblock() - Figure 4
    #[inline]
    fn pop(&self) -> Result<T, Self::PopError> {
        self.ensure_initialized();

        let tail = self.tail.load(Ordering::Relaxed); // Access tail
        let slot = self.get_slot(tail);

        // Line 3: if (head == tail) - we check if slot has data
        if !slot.flag.load(Ordering::Acquire) {
            return Err(FfqPopError); // Line 4: return EWOULDBLOCK
        }

        // Line 2: data = buffer[tail]
        let data = unsafe { (*slot.data.get()).assume_init_read() };

        // Line 6: buffer[tail] = NULL
        slot.flag.store(false, Ordering::Release);

        // Line 7: tail = NEXT(tail)
        self.tail.store(tail.wrapping_add(1), Ordering::Release);

        Ok(data) // Line 8: return 0
    }

    // IPC helper: check if queue has space
    #[inline]
    fn available(&self) -> bool {
        self.ensure_initialized();

        let head = self.head.load(Ordering::Relaxed);
        let slot = self.get_slot(head);

        // Slot is available if it's empty
        !slot.flag.load(Ordering::Acquire)
    }

    // IPC helper: check if queue is empty
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
        // IPC: shared memory cleanup handled externally
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
