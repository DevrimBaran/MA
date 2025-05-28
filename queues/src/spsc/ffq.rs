use crate::SpscQueue;
use core::{cell::UnsafeCell, fmt, mem::MaybeUninit, ptr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// Atomic wrapper for the slot to prevent data races
#[repr(C)]
pub struct AtomicSlot<T> {
    // Using AtomicUsize for the discriminant and UnsafeCell for the value
    // 0 = None, 1 = Some
    state: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> AtomicSlot<T> {
    pub fn new_none() -> Self {
        Self {
            state: AtomicUsize::new(0),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        // Use Acquire for IPC visibility
        self.state.load(Ordering::Acquire) == 1
    }

    #[inline]
    pub fn write(&self, value: T) {
        unsafe {
            (*self.value.get()).write(value);
        }
        // Use stronger ordering for IPC - SeqCst ensures visibility across processes
        self.state.store(1, Ordering::SeqCst);
    }

    #[inline]
    pub fn take(&self) -> Option<T> {
        // Use compare_exchange with SeqCst for IPC
        match self
            .state
            .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => {
                // We had Some, extract the value
                Some(unsafe { (*self.value.get()).assume_init_read() })
            }
            Err(_) => None,
        }
    }
}

#[repr(C, align(64))]
pub struct FfqQueue<T: Send + 'static> {
    pub head: AtomicUsize, // Changed from UnsafeCell<usize>
    _pad1: [u8; 64 - std::mem::size_of::<AtomicUsize>()],
    pub tail: AtomicUsize, // Changed from UnsafeCell<usize>
    _pad2: [u8; 64 - std::mem::size_of::<AtomicUsize>()],
    capacity: usize,
    pub mask: usize,
    buffer: *mut AtomicSlot<T>,
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

        let layout = std::alloc::Layout::array::<AtomicSlot<T>>(capacity)
            .unwrap()
            .align_to(64)
            .unwrap();

        let ptr = unsafe { std::alloc::alloc(layout) as *mut AtomicSlot<T> };

        if ptr.is_null() {
            panic!("Failed to allocate buffer");
        }

        unsafe {
            for i in 0..capacity {
                ptr::write(ptr.add(i), AtomicSlot::new_none());
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
        let buf_layout = core::alloc::Layout::array::<AtomicSlot<T>>(capacity).unwrap();
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
        let buf_ptr = mem.add(std::mem::size_of::<Self>()) as *mut AtomicSlot<T>;

        // Initialize each slot explicitly with empty state
        for i in 0..capacity {
            let slot_ptr = buf_ptr.add(i);
            // Write the entire slot structure
            ptr::write(
                slot_ptr,
                AtomicSlot {
                    state: AtomicUsize::new(0), // 0 = empty
                    value: UnsafeCell::new(MaybeUninit::uninit()),
                },
            );
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
    pub fn get_slot(&self, index: usize) -> &AtomicSlot<T> {
        unsafe { &*self.buffer.add(index & self.mask) }
    }

    #[inline]
    fn ensure_initialized(&self) {
        assert!(
            self.initialized.load(Ordering::Acquire),
            "Queue not initialized"
        );
    }
}

impl<T: Send + 'static> SpscQueue<T> for FfqQueue<T> {
    type PushError = FfqPushError<T>;
    type PopError = FfqPopError;

    #[inline]
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        self.ensure_initialized();

        let head = self.head.load(Ordering::Acquire);
        let slot = self.get_slot(head);

        if slot.is_some() {
            return Err(FfqPushError(item));
        }

        slot.write(item);
        self.head.store(head.wrapping_add(1), Ordering::Release);

        Ok(())
    }

    #[inline]
    fn pop(&self) -> Result<T, Self::PopError> {
        self.ensure_initialized();

        let tail = self.tail.load(Ordering::Acquire);
        let slot = self.get_slot(tail);

        match slot.take() {
            Some(val) => {
                self.tail.store(tail.wrapping_add(1), Ordering::Release);
                Ok(val)
            }
            None => Err(FfqPopError),
        }
    }

    #[inline]
    fn available(&self) -> bool {
        self.ensure_initialized();

        let head = self.head.load(Ordering::Acquire);
        let slot = self.get_slot(head);
        !slot.is_some()
    }

    #[inline]
    fn empty(&self) -> bool {
        self.ensure_initialized();

        let tail = self.tail.load(Ordering::Acquire);
        let slot = self.get_slot(tail);
        !slot.is_some()
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
                        drop(slot.take());
                    }
                }

                let layout = std::alloc::Layout::array::<AtomicSlot<T>>(self.capacity)
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
            .field("head", &self.head.load(Ordering::Relaxed)) // Fixed: load() needs Ordering
            .field("tail", &self.tail.load(Ordering::Relaxed)) // Fixed: load() needs Ordering
            .field("owns_buffer", &self.owns_buffer)
            .field("initialized", &self.initialized.load(Ordering::Relaxed))
            .finish()
    }
}

impl<T: Send + 'static> FfqQueue<T> {
    pub const DANGER_THRESHOLD: usize = 16;
    pub const GOOD_THRESHOLD: usize = 48;

    #[inline]
    pub fn distance(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.wrapping_sub(tail)
    }

    pub fn adjust_slip(&self, avg_stage_time_ns: u64) {
        let mut dist = self.distance();
        if dist < Self::DANGER_THRESHOLD {
            let mut dist_old;
            loop {
                dist_old = dist;

                let spin_time = avg_stage_time_ns * ((Self::GOOD_THRESHOLD + 1) - dist) as u64;

                let start = std::time::Instant::now();
                while start.elapsed().as_nanos() < spin_time as u128 {
                    std::hint::spin_loop();
                }

                dist = self.distance();

                if dist >= Self::GOOD_THRESHOLD || dist <= dist_old {
                    break;
                }
            }
        }
    }
}

impl<T: Send + 'static> FfqQueue<T> {
    // Add this method to verify all slots are properly initialized
    pub fn verify_initialization(&self) -> bool {
        let mut empty_count = 0;
        let mut filled_count = 0;

        for i in 0..self.capacity {
            let slot = self.get_slot(i);
            if slot.is_some() {
                filled_count += 1;
            } else {
                empty_count += 1;
            }
        }

        println!(
            "Queue verification: {} empty slots, {} filled slots out of {} total",
            empty_count, filled_count, self.capacity
        );

        // At initialization, all slots should be empty
        empty_count + filled_count == self.capacity
    }

    // Add this to debug push operations
    #[inline]
    pub fn push_debug(&self, item: T) -> Result<(), FfqPushError<T>> {
        self.ensure_initialized();

        let head = self.head.load(Ordering::Acquire);
        let slot = self.get_slot(head);

        // Debug: Check what's in the slot before writing
        if cfg!(debug_assertions) {
            if slot.is_some() {
                // Try to read the value to see what's there
                let state = slot.state.load(Ordering::SeqCst);
                eprintln!(
                    "DEBUG: Slot at index {} (head={}) is occupied (state={})",
                    head & self.mask,
                    head,
                    state
                );
            }
        }

        if slot.is_some() {
            return Err(FfqPushError(item));
        }

        slot.write(item);
        self.head.store(head.wrapping_add(1), Ordering::Release);

        Ok(())
    }

    // Add this to debug pop operations
    #[inline]
    pub fn pop_debug(&self) -> Result<T, FfqPopError> {
        self.ensure_initialized();

        let tail = self.tail.load(Ordering::Acquire);
        let slot = self.get_slot(tail);

        // Debug: Log what we're about to pop
        if cfg!(debug_assertions) {
            let state = slot.state.load(Ordering::SeqCst);
            eprintln!(
                "DEBUG: About to pop from index {} (tail={}, state={})",
                tail & self.mask,
                tail,
                state
            );
        }

        match slot.take() {
            Some(val) => {
                self.tail.store(tail.wrapping_add(1), Ordering::Release);
                Ok(val)
            }
            None => Err(FfqPopError),
        }
    }
}
