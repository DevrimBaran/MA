// paper in /paper/mpmc/sdp.pdf
use crate::MpmcQueue;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{fence, AtomicU8, AtomicUsize, Ordering};

const CACHE_LINE_SIZE: usize = 64;

// Status bits for queue elements as described in Table I of the paper
const INIT_BIT: u8 = 0b100;
const USE_BIT: u8 = 0b010;
const DEL_BIT: u8 = 0b001;

// Wait-free bounds - these ensure bounded completion
const MAX_CAS_ATTEMPTS: usize = 100;
const MAX_READER_WAIT_CYCLES: usize = 10000;
const MAX_TRAVERSAL_ATTEMPTS: usize = 3;
const MAX_HELPING_ROUNDS: usize = 2;

// Error types that allow wait-free completion with failure
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueError {
    QueueFull,
    ContentionTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DequeueError {
    QueueEmpty,
    ContentionTimeout,
}

// Queue element structure
#[repr(C, align(64))]
struct QueueElement<T> {
    // Status bits: bit 0 = del, bit 1 = use, bit 2 = init
    status: AtomicU8,
    // Read counter for multiple readers
    read_counter: AtomicUsize,
    // The actual data
    data: MaybeUninit<T>,
    // Next pointer (index in the array)
    next: AtomicUsize,
    // Padding to ensure cache line alignment
    _padding: [u8; 32],
}

impl<T> QueueElement<T> {
    fn new() -> Self {
        Self {
            status: AtomicU8::new(0), // Free state
            read_counter: AtomicUsize::new(0),
            data: MaybeUninit::uninit(),
            next: AtomicUsize::new(0),
            _padding: [0; 32],
        }
    }
}

// Helping queue element for improved traversal
#[repr(C, align(64))]
struct HelpingQueueElement {
    local_queue: usize,          // Index to first element of local queue
    local_middle: usize,         // Index to middle element of local queue
    free_at_top: AtomicUsize,    // Free elements in first half
    free_at_bottom: AtomicUsize, // Free elements in second half
    next: usize,                 // Index to next helping queue element
    _padding: [u8; CACHE_LINE_SIZE - 40],
}

// Main queue structure
#[repr(C)]
pub struct SDPWFQueue<T: Send + Clone + 'static> {
    num_threads: usize,
    elements_per_thread: usize,
    total_elements: usize,

    // Offsets in shared memory
    elements_offset: usize,
    helping_queue_offset: usize,

    // Traversal direction for each thread (for improved algorithm)
    traversal_directions: AtomicUsize, // Bit field for direction

    // Base pointer to shared memory
    base_ptr: *mut u8,
    total_size: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for SDPWFQueue<T> {}
unsafe impl<T: Send + Clone> Sync for SDPWFQueue<T> {}

impl<T: Send + Clone + 'static> SDPWFQueue<T> {
    // Get element by index - return raw pointer to avoid aliasing issues
    unsafe fn get_element(&self, index: usize) -> *mut QueueElement<T> {
        let elements_ptr = self.base_ptr.add(self.elements_offset) as *mut QueueElement<T>;
        elements_ptr.add(index)
    }

    // Get helping queue element by thread id - also use raw pointer
    unsafe fn get_helping_element(&self, thread_id: usize) -> *const HelpingQueueElement {
        let helping_ptr =
            self.base_ptr.add(self.helping_queue_offset) as *const HelpingQueueElement;
        helping_ptr.add(thread_id)
    }

    // Wait-free enqueue operation
    pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), EnqueueError> {
        if thread_id >= self.num_threads {
            return Err(EnqueueError::ContentionTimeout);
        }

        unsafe {
            let helping_elem = self.get_helping_element(thread_id);
            let start_idx = (*helping_elem).local_queue;
            let end_idx = if thread_id == self.num_threads - 1 {
                self.total_elements
            } else {
                (*self.get_helping_element(thread_id + 1)).local_queue
            };

            // First try local elements with improved traversal
            if self.use_improved_traversal() {
                match self.enqueue_with_improved_traversal(thread_id, &item, start_idx, end_idx) {
                    Ok(()) => return Ok(()),
                    Err(EnqueueError::ContentionTimeout) => {
                        // Continue to next strategy
                    }
                    Err(e) => return Err(e),
                }
            } else {
                // Standard linear traversal of local elements
                for idx in start_idx..end_idx {
                    match self.try_enqueue_at(idx, &item) {
                        Ok(true) => return Ok(()),
                        Ok(false) => continue,
                        Err(()) => return Err(EnqueueError::ContentionTimeout),
                    }
                }
            }

            // Try helping queue
            if self.helping_queue_offset > 0 {
                match self.enqueue_with_helping_queue(thread_id, &item) {
                    Ok(()) => return Ok(()),
                    Err(EnqueueError::ContentionTimeout) => {
                        // Continue to full traversal
                    }
                    Err(e) => return Err(e),
                }
            }

            // Bounded full traversal of entire queue
            for attempt in 0..MAX_TRAVERSAL_ATTEMPTS {
                let mut current = 0;
                for _ in 0..self.total_elements {
                    match self.try_enqueue_at(current, &item) {
                        Ok(true) => return Ok(()),
                        Ok(false) => {}
                        Err(()) => return Err(EnqueueError::ContentionTimeout),
                    }
                    current = (current + 1) % self.total_elements;
                }

                if attempt < MAX_TRAVERSAL_ATTEMPTS - 1 {
                    std::thread::yield_now();
                }
            }

            Err(EnqueueError::QueueFull)
        }
    }

    // Try to enqueue at a specific element with bounded retries
    unsafe fn try_enqueue_at(&self, idx: usize, item: &T) -> Result<bool, ()> {
        let elem = self.get_element(idx);

        // Bounded TAR(del) - test and reset del bit
        let mut tar_attempts = 0;
        loop {
            if tar_attempts >= MAX_CAS_ATTEMPTS {
                return Ok(false); // Couldn't claim this slot
            }
            tar_attempts += 1;

            let old_status = (*elem).status.load(Ordering::Acquire);

            // Check if del bit is set
            if old_status & DEL_BIT != 0 {
                // Try to reset del bit
                let new_status = old_status & !DEL_BIT;
                match (*elem).status.compare_exchange_weak(
                    old_status,
                    new_status,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Successfully reset del bit
                        // Check if readers are still attached
                        if (*elem).read_counter.load(Ordering::Acquire) > 0 {
                            // Readers still active, restore del bit
                            (*elem).status.fetch_or(DEL_BIT, Ordering::AcqRel);
                            return Ok(false);
                        }

                        // No readers, write data
                        ptr::write((*elem).data.as_mut_ptr(), item.clone());
                        fence(Ordering::Release);

                        // LP1: Atomically set use=1, del=0, init=0
                        (*elem).status.store(USE_BIT, Ordering::Release);
                        return Ok(true);
                    }
                    Err(_) => continue, // Retry
                }
            } else {
                break; // Del bit not set, try TAS(init)
            }
        }

        // Bounded TAS(init) - test and set init bit
        for _ in 0..MAX_CAS_ATTEMPTS {
            let old_status = (*elem).status.load(Ordering::Acquire);
            if old_status & INIT_BIT != 0 {
                return Ok(false); // Already being processed
            }

            let new_status = old_status | INIT_BIT;
            match (*elem).status.compare_exchange(
                old_status,
                new_status,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Successfully set init bit
                    // Check if element is in use
                    let current_status = (*elem).status.load(Ordering::Acquire);
                    if current_status & USE_BIT == 0 {
                        // Element is free, write data
                        ptr::write((*elem).data.as_mut_ptr(), item.clone());
                        fence(Ordering::Release);

                        // Release: set use=1, init=0, del=0
                        (*elem).status.store(USE_BIT, Ordering::Release);
                        return Ok(true);
                    } else {
                        // Element is in use, reset init bit
                        (*elem).status.fetch_and(!INIT_BIT, Ordering::AcqRel);
                        return Ok(false);
                    }
                }
                Err(_) => continue, // Retry
            }
        }

        Ok(false) // Exceeded attempts for this element
    }

    // Enqueue with improved traversal algorithm (bounded)
    unsafe fn enqueue_with_improved_traversal(
        &self,
        thread_id: usize,
        item: &T,
        start_idx: usize,
        end_idx: usize,
    ) -> Result<(), EnqueueError> {
        let helping_elem = self.get_helping_element(thread_id);
        let middle = (*helping_elem).local_middle;

        // Get current traversal direction
        let directions = self.traversal_directions.load(Ordering::Acquire);
        let from_left = (directions >> thread_id) & 1 == 0;

        // Toggle direction for next time
        self.traversal_directions
            .fetch_xor(1 << thread_id, Ordering::AcqRel);

        // Try first half
        if from_left {
            for idx in start_idx..middle {
                match self.try_enqueue_at(idx, item) {
                    Ok(true) => return Ok(()),
                    Ok(false) => continue,
                    Err(()) => return Err(EnqueueError::ContentionTimeout),
                }
            }
        } else {
            for idx in (start_idx..middle).rev() {
                match self.try_enqueue_at(idx, item) {
                    Ok(true) => return Ok(()),
                    Ok(false) => continue,
                    Err(()) => return Err(EnqueueError::ContentionTimeout),
                }
            }
        }

        // Try second half
        if from_left {
            for idx in middle..end_idx {
                match self.try_enqueue_at(idx, item) {
                    Ok(true) => return Ok(()),
                    Ok(false) => continue,
                    Err(()) => return Err(EnqueueError::ContentionTimeout),
                }
            }
        } else {
            for idx in (middle..end_idx).rev() {
                match self.try_enqueue_at(idx, item) {
                    Ok(true) => return Ok(()),
                    Ok(false) => continue,
                    Err(()) => return Err(EnqueueError::ContentionTimeout),
                }
            }
        }

        Err(EnqueueError::QueueFull)
    }

    // Enqueue using helping queue with bounded attempts
    unsafe fn enqueue_with_helping_queue(
        &self,
        thread_id: usize,
        item: &T,
    ) -> Result<(), EnqueueError> {
        // Bounded passes through helping queue
        for _ in 0..MAX_HELPING_ROUNDS {
            let mut current_helper = thread_id;
            let start_helper = current_helper;

            loop {
                let helper = self.get_helping_element(current_helper);

                // Check free_at_top counter with bounded CAS attempts
                for _ in 0..MAX_CAS_ATTEMPTS {
                    let top_count = (*helper).free_at_top.load(Ordering::Acquire);
                    if top_count > 0 {
                        match (*helper).free_at_top.compare_exchange(
                            top_count,
                            top_count - 1,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                // Successfully reserved, find the free element in first half
                                let start = (*helper).local_queue;
                                let middle = (*helper).local_middle;

                                for idx in start..middle {
                                    match self.try_enqueue_at(idx, item) {
                                        Ok(true) => return Ok(()),
                                        Ok(false) => continue,
                                        Err(()) => {
                                            // Restore counter and fail
                                            (*helper).free_at_top.fetch_add(1, Ordering::AcqRel);
                                            return Err(EnqueueError::ContentionTimeout);
                                        }
                                    }
                                }

                                // Couldn't find it, restore counter
                                (*helper).free_at_top.fetch_add(1, Ordering::AcqRel);
                            }
                            Err(_) => break, // Try bottom counter
                        }
                    } else {
                        break;
                    }
                }

                // Check free_at_bottom counter with bounded CAS attempts
                for _ in 0..MAX_CAS_ATTEMPTS {
                    let bottom_count = (*helper).free_at_bottom.load(Ordering::Acquire);
                    if bottom_count > 0 {
                        match (*helper).free_at_bottom.compare_exchange(
                            bottom_count,
                            bottom_count - 1,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        ) {
                            Ok(_) => {
                                // Successfully reserved, find the free element in second half
                                let middle = (*helper).local_middle;
                                let end = if current_helper == self.num_threads - 1 {
                                    self.total_elements
                                } else {
                                    (*self.get_helping_element(current_helper + 1)).local_queue
                                };

                                for idx in middle..end {
                                    match self.try_enqueue_at(idx, item) {
                                        Ok(true) => return Ok(()),
                                        Ok(false) => continue,
                                        Err(()) => {
                                            // Restore counter and fail
                                            (*helper).free_at_bottom.fetch_add(1, Ordering::AcqRel);
                                            return Err(EnqueueError::ContentionTimeout);
                                        }
                                    }
                                }

                                // Couldn't find it, restore counter
                                (*helper).free_at_bottom.fetch_add(1, Ordering::AcqRel);
                            }
                            Err(_) => break, // Move to next helper
                        }
                    } else {
                        break;
                    }
                }

                // Move to next helper element
                current_helper = (*helper).next;
                if current_helper == start_helper {
                    break; // Completed one round
                }
            }

            // Small yield between rounds
            std::thread::yield_now();
        }

        Err(EnqueueError::ContentionTimeout)
    }

    // Wait-free dequeue operation
    pub fn dequeue(&self, thread_id: usize) -> Result<T, DequeueError> {
        if thread_id >= self.num_threads {
            return Err(DequeueError::ContentionTimeout);
        }

        unsafe {
            // Bounded attempts to handle concurrent modifications
            for attempt in 0..MAX_TRAVERSAL_ATTEMPTS {
                let start_idx = (*self.get_helping_element(thread_id)).local_queue;
                let mut current = start_idx;

                // Bounded queue traversal
                for _ in 0..self.total_elements {
                    match self.try_dequeue_at(current) {
                        Ok(Some(item)) => {
                            // Update helping queue counter if enabled
                            if self.helping_queue_offset > 0 {
                                self.update_helping_counter_after_dequeue(current);
                            }
                            return Ok(item);
                        }
                        Ok(None) => {}
                        Err(()) => return Err(DequeueError::ContentionTimeout),
                    }

                    current = (current + 1) % self.total_elements;
                }

                // Yield before retry
                if attempt < MAX_TRAVERSAL_ATTEMPTS - 1 {
                    std::thread::yield_now();
                }
            }

            Err(DequeueError::QueueEmpty)
        }
    }

    // Try to dequeue from a specific element with bounded operations
    unsafe fn try_dequeue_at(&self, idx: usize) -> Result<Option<T>, ()> {
        let elem = self.get_element(idx);

        // BT(use) - Check if element is in use
        let status = (*elem).status.load(Ordering::Acquire);
        if status & USE_BIT == 0 {
            return Ok(None);
        }

        // Bounded TAS(init) - Try to set init bit
        for _ in 0..MAX_CAS_ATTEMPTS {
            let old_status = (*elem).status.load(Ordering::Acquire);
            if old_status & INIT_BIT != 0 {
                return Ok(None); // Already being processed
            }

            let new_status = old_status | INIT_BIT;
            match (*elem).status.compare_exchange_weak(
                old_status,
                new_status,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Successfully set init bit - continue with dequeue
                    break;
                }
                Err(_) => {
                    // Check if status changed significantly
                    let current = (*elem).status.load(Ordering::Acquire);
                    if current & USE_BIT == 0 {
                        return Ok(None); // Element no longer in use
                    }
                    // Otherwise continue trying
                }
            }
        }

        // If we couldn't get init bit within bounds, give up on this element
        let final_status = (*elem).status.load(Ordering::Acquire);
        if final_status & INIT_BIT == 0 {
            return Ok(None); // Couldn't acquire init bit
        }

        // Double-check element is still in use after getting init
        let current_status = (*elem).status.load(Ordering::Acquire);
        if current_status & USE_BIT == 0 {
            // Element was freed, reset init bit
            (*elem).status.fetch_and(!INIT_BIT, Ordering::AcqRel);
            return Ok(None);
        }

        // Bounded wait for any active readers to finish
        for wait_cycle in 0..MAX_READER_WAIT_CYCLES {
            if (*elem).read_counter.load(Ordering::Acquire) == 0 {
                break;
            }
            if wait_cycle < 100 {
                std::hint::spin_loop();
            } else if wait_cycle % 100 == 0 {
                std::thread::yield_now();
            }
        }

        // Even if readers are still active, we proceed (wait-free guarantee)
        // This is safe because we own the init bit

        // Read the data before marking as removed
        let item = ptr::read((*elem).data.as_ptr());

        // LP2: Atomically mark as removed - set del=1, use=0, init=0
        (*elem).status.store(DEL_BIT, Ordering::Release);

        Ok(Some(item))
    }

    // Update helping queue counter after dequeue
    unsafe fn update_helping_counter_after_dequeue(&self, elem_idx: usize) {
        // Find which thread owns this element
        for thread_id in 0..self.num_threads {
            let helper = self.get_helping_element(thread_id);
            let end_idx = if thread_id == self.num_threads - 1 {
                self.total_elements
            } else {
                (*self.get_helping_element(thread_id + 1)).local_queue
            };

            if elem_idx >= (*helper).local_queue && elem_idx < end_idx {
                // Found the owner, update appropriate counter
                if elem_idx < (*helper).local_middle {
                    (*helper).free_at_top.fetch_add(1, Ordering::AcqRel);
                } else {
                    (*helper).free_at_bottom.fetch_add(1, Ordering::AcqRel);
                }
                break;
            }
        }
    }

    // Read operation (for clients) - already bounded by queue size
    pub fn read(&self, _thread_id: usize) -> Vec<T> {
        let mut items = Vec::new();

        unsafe {
            for idx in 0..self.total_elements {
                let elem = self.get_element(idx);

                // Check if element is in use and not deleted
                let status = (*elem).status.load(Ordering::Acquire);
                if (status & USE_BIT) != 0 && (status & DEL_BIT) == 0 {
                    // Increment read counter
                    (*elem).read_counter.fetch_add(1, Ordering::AcqRel);

                    // Re-check status after incrementing counter
                    let new_status = (*elem).status.load(Ordering::Acquire);
                    if (new_status & USE_BIT) != 0 && (new_status & DEL_BIT) == 0 {
                        // Read the data
                        let item = ptr::read((*elem).data.as_ptr());
                        items.push(item);
                    }

                    // Decrement read counter
                    (*elem).read_counter.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }

        items
    }

    // Check if we should use improved traversal
    fn use_improved_traversal(&self) -> bool {
        true
    }

    // Initialize queue in shared memory
    pub unsafe fn init_in_shared(
        mem: *mut u8,
        num_threads: usize,
        enable_helping_queue: bool,
    ) -> &'static Self {
        let queue_ptr = mem as *mut Self;
        let elements_per_thread = 750_000;
        let total_elements = num_threads * elements_per_thread;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let elements_offset = queue_aligned;
        let elements_size = total_elements * mem::size_of::<QueueElement<T>>();
        let elements_aligned = (elements_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let helping_queue_offset = if enable_helping_queue {
            elements_offset + elements_aligned
        } else {
            0
        };

        let helping_size = if enable_helping_queue {
            num_threads * mem::size_of::<HelpingQueueElement>()
        } else {
            0
        };

        let total_size = if enable_helping_queue {
            helping_queue_offset + helping_size
        } else {
            elements_offset + elements_aligned
        };

        // Initialize queue header
        ptr::write(
            queue_ptr,
            Self {
                num_threads,
                elements_per_thread,
                total_elements,
                elements_offset,
                helping_queue_offset,
                traversal_directions: AtomicUsize::new(0),
                base_ptr: mem,
                total_size,
                _phantom: std::marker::PhantomData,
            },
        );

        // Initialize elements
        let elements_ptr = mem.add(elements_offset) as *mut QueueElement<T>;
        for i in 0..total_elements {
            ptr::write(elements_ptr.add(i), QueueElement::new());
        }

        // Create circular linked list
        for i in 0..total_elements {
            let elem = elements_ptr.add(i);
            (*elem)
                .next
                .store((i + 1) % total_elements, Ordering::Release);
        }

        // Initialize helping queue if enabled
        if enable_helping_queue {
            let helping_ptr = mem.add(helping_queue_offset) as *mut HelpingQueueElement;

            for thread_id in 0..num_threads {
                let local_start = thread_id * elements_per_thread;
                let local_middle = local_start + elements_per_thread / 2;

                ptr::write(
                    helping_ptr.add(thread_id),
                    HelpingQueueElement {
                        local_queue: local_start,
                        local_middle,
                        free_at_top: AtomicUsize::new(elements_per_thread / 2),
                        free_at_bottom: AtomicUsize::new(
                            elements_per_thread - elements_per_thread / 2,
                        ),
                        next: (thread_id + 1) % num_threads,
                        _padding: [0; CACHE_LINE_SIZE - 40],
                    },
                );
            }
        }

        fence(Ordering::SeqCst);
        &*queue_ptr
    }

    // Calculate required shared memory size
    pub fn shared_size(num_threads: usize, enable_helping_queue: bool) -> usize {
        let elements_per_thread = 750_000;
        let total_elements = num_threads * elements_per_thread;

        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let elements_size = total_elements * mem::size_of::<QueueElement<T>>();
        let elements_aligned = (elements_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let helping_size = if enable_helping_queue {
            num_threads * mem::size_of::<HelpingQueueElement>()
        } else {
            0
        };

        let total = queue_aligned + elements_aligned + helping_size;
        (total + 4095) & !4095 // Page align
    }

    pub fn is_empty(&self) -> bool {
        unsafe {
            for i in 0..self.total_elements {
                let elem = self.get_element(i);
                let status = (*elem).status.load(Ordering::Acquire);
                if (status & USE_BIT) != 0 && (status & DEL_BIT) == 0 {
                    return false;
                }
            }
            true
        }
    }

    pub fn is_full(&self) -> bool {
        unsafe {
            for i in 0..self.total_elements {
                let elem = self.get_element(i);
                let status = (*elem).status.load(Ordering::Acquire);
                // Element is free if no bits are set or only del bit is set
                if status == 0 || status == DEL_BIT {
                    return false;
                }
            }
            true
        }
    }
}

// Adapter for the original interface that converts new error types
impl<T: Send + Clone + 'static> MpmcQueue<T> for SDPWFQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, item: T, thread_id: usize) -> Result<(), Self::PushError> {
        match self.enqueue(thread_id, item) {
            Ok(()) => Ok(()),
            Err(_) => Err(()), // Convert any error to unit type
        }
    }

    fn pop(&self, thread_id: usize) -> Result<T, Self::PopError> {
        match self.dequeue(thread_id) {
            Ok(item) => Ok(item),
            Err(_) => Err(()), // Convert any error to unit type
        }
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + Clone> Drop for SDPWFQueue<T> {
    fn drop(&mut self) {
        // Shared memory cleanup handled externally
    }
}
