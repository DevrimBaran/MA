// queues/src/spmc/david_queue.rs
use std::mem;
use std::ptr;
use std::sync::atomic::{fence, AtomicUsize, Ordering};

use crate::SpmcQueue;

const CACHE_LINE_SIZE: usize = 64;

// Special marker values
const BOTTOM: usize = 0; // ⊥ in paper - empty cell
const TOP: usize = usize::MAX; // ⊤ in paper - dequeued cell

// Fetch&Increment object as per paper
#[repr(C, align(64))]
struct FetchIncrement {
    value: AtomicUsize,
    _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<AtomicUsize>()],
}

impl FetchIncrement {
    fn new() -> Self {
        Self {
            value: AtomicUsize::new(0),
            _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<AtomicUsize>()],
        }
    }

    fn fetch_increment(&self) -> usize {
        self.value.fetch_add(1, Ordering::AcqRel)
    }
}

// Swap object as per paper
#[repr(C, align(64))]
struct SwapCell {
    value: AtomicUsize,
    _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<AtomicUsize>()],
}

impl SwapCell {
    fn new() -> Self {
        Self {
            value: AtomicUsize::new(BOTTOM),
            _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<AtomicUsize>()],
        }
    }

    unsafe fn swap(&self, new_val: usize) -> usize {
        self.value.swap(new_val, Ordering::AcqRel)
    }

    fn load(&self) -> usize {
        self.value.load(Ordering::Acquire)
    }

    fn compare_exchange(&self, current: usize, new: usize) -> Result<usize, usize> {
        self.value
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
    }
}

// Main queue structure
#[repr(C)]
pub struct DavidQueue<T: Send + Clone + 'static> {
    // Shared state
    row: AtomicUsize,          // ROW register
    head_array_offset: usize,  // Offset to HEAD array
    items_array_offset: usize, // Offset to ITEMS array

    // Tracking
    total_enqueued: AtomicUsize,
    total_dequeued: AtomicUsize,

    // Queue configuration
    num_consumers: usize,
    num_rows: usize,
    items_per_row: usize,

    // Memory management
    base_ptr: *mut u8,
    total_size: usize,

    // Item storage pool
    item_pool_offset: usize,
    item_pool_size: usize,
    next_item: AtomicUsize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for DavidQueue<T> {}
unsafe impl<T: Send + Clone> Sync for DavidQueue<T> {}

// Enqueuer state
#[repr(C)]
pub struct EnqueuerState {
    pub enq_row: usize,
    pub tail: usize,
}

impl EnqueuerState {
    pub fn new() -> Self {
        Self {
            enq_row: 0,
            tail: 0,
        }
    }
}

impl<T: Send + Clone + 'static> DavidQueue<T> {
    // Get pointer to HEAD[row]
    unsafe fn get_head(&self, row: usize) -> &FetchIncrement {
        let heads_ptr = self.base_ptr.add(self.head_array_offset) as *const FetchIncrement;
        &*heads_ptr.add(row)
    }

    // Get pointer to ITEMS[row, col]
    unsafe fn get_item(&self, row: usize, col: usize) -> &SwapCell {
        let items_ptr = self.base_ptr.add(self.items_array_offset) as *const SwapCell;
        let index = row * self.items_per_row + col;
        &*items_ptr.add(index)
    }

    // Allocate space for an item in the pool
    unsafe fn allocate_item(&self, item: T) -> usize {
        let idx = self.next_item.fetch_add(1, Ordering::AcqRel);
        if idx >= self.item_pool_size {
            panic!("Item pool exhausted: {} >= {}", idx, self.item_pool_size);
        }

        let items_ptr = self.base_ptr.add(self.item_pool_offset) as *mut T;
        ptr::write(items_ptr.add(idx), item);

        // Return pointer as usize
        items_ptr.add(idx) as usize
    }

    // Retrieve item from pool
    unsafe fn get_pooled_item(&self, ptr_as_usize: usize) -> T {
        let ptr = ptr_as_usize as *mut T;
        ptr::read(ptr)
    }

    // Enqueue operation - faithful to paper
    pub fn enqueue(&self, state: &mut EnqueuerState, item: T) -> Result<(), ()> {
        unsafe {
            // Check bounds
            if state.tail >= self.items_per_row {
                state.enq_row += 1;
                state.tail = 0;
                if state.enq_row >= self.num_rows {
                    return Err(());
                }
            }

            let x_ptr = self.allocate_item(item);

            // Step 1: val ← Swap(ITEMS[enq_row, tail], x)
            let val = self.get_item(state.enq_row, state.tail).swap(x_ptr);

            // Step 2: if val = ⊤ then
            if val == TOP {
                // increment(enq_row); tail ← 0
                state.enq_row += 1;
                state.tail = 0;
                if state.enq_row >= self.num_rows {
                    return Err(());
                }

                // Swap(ITEMS[enq_row, tail], x)
                self.get_item(state.enq_row, state.tail).swap(x_ptr);

                // Write(ROW, enq_row)
                self.row.store(state.enq_row, Ordering::Release);
            }

            // If val wasn't TOP, we successfully stored in the original position
            // Always update ROW to ensure visibility
            self.row.store(state.enq_row, Ordering::Release);

            // increment(tail)
            state.tail += 1;

            // Track enqueue
            self.total_enqueued.fetch_add(1, Ordering::AcqRel);

            Ok(())
        }
    }

    // Dequeue operation - better handling for high contention
    pub fn dequeue(&self, consumer_id: usize) -> Result<T, ()> {
        unsafe {
            // Adaptive retry count based on number of consumers
            let base_attempts = 20;
            let extra_attempts = self.num_consumers * 5;
            let max_attempts = base_attempts + extra_attempts;

            for attempt in 0..max_attempts {
                // First, check if there's anything to dequeue
                let enqueued = self.total_enqueued.load(Ordering::Acquire);
                let dequeued = self.total_dequeued.load(Ordering::Acquire);

                if dequeued >= enqueued {
                    // Add small delay to let producer catch up
                    if attempt < 5 {
                        std::thread::yield_now();
                        continue;
                    }
                    return Err(());
                }

                // Step 1: deq_row ← Read(ROW)
                let deq_row = self.row.load(Ordering::Acquire);

                // For high contention, also try previous rows
                let rows_to_try = if self.num_consumers > 4 { 3 } else { 1 };

                for row_offset in 0..rows_to_try {
                    let try_row = if row_offset == 0 {
                        deq_row
                    } else if deq_row >= row_offset {
                        deq_row - row_offset
                    } else {
                        continue;
                    };

                    // Step 2: head ← Fetch&Increment(HEAD[deq_row])
                    let head = self.get_head(try_row).fetch_increment();

                    // Check bounds
                    if head >= self.items_per_row {
                        continue;
                    }

                    // Step 3: val ← Swap(ITEMS[deq_row, head], ⊤)
                    let val = self.get_item(try_row, head).swap(TOP);

                    if val != BOTTOM && val != TOP {
                        // Got a valid item!
                        self.total_dequeued.fetch_add(1, Ordering::AcqRel);
                        return Ok(self.get_pooled_item(val));
                    }
                }

                // Didn't get anything this round
                if attempt > max_attempts / 2 {
                    // More aggressive scanning in second half of attempts
                    fence(Ordering::SeqCst);

                    let current_row = self.row.load(Ordering::Acquire);
                    let scan_rows = (self.num_consumers / 2).max(2).min(10);

                    // Scan recent rows
                    for row in current_row.saturating_sub(scan_rows)..=current_row {
                        if row >= self.num_rows {
                            continue;
                        }

                        // Random starting position to reduce contention
                        let start_pos = (consumer_id * 17 + attempt * 7) % self.items_per_row;

                        // Scan from random position
                        for i in 0..self.items_per_row {
                            let pos = (start_pos + i) % self.items_per_row;
                            let cell_val = self.get_item(row, pos).load();

                            if cell_val != BOTTOM && cell_val != TOP {
                                match self.get_item(row, pos).compare_exchange(cell_val, TOP) {
                                    Ok(item_ptr) => {
                                        self.total_dequeued.fetch_add(1, Ordering::AcqRel);
                                        return Ok(self.get_pooled_item(item_ptr));
                                    }
                                    Err(_) => continue,
                                }
                            }
                        }
                    }
                }

                // Adaptive backoff based on contention
                if self.num_consumers > 8 {
                    // High contention - more aggressive backoff
                    if attempt % 4 == 0 {
                        std::thread::sleep(std::time::Duration::from_micros(1));
                    } else {
                        std::thread::yield_now();
                    }
                } else if self.num_consumers > 4 {
                    // Medium contention
                    std::thread::yield_now();
                } else {
                    // Low contention - just spin
                    for _ in 0..10 {
                        std::hint::spin_loop();
                    }
                }
            }

            // Final exhaustive check
            fence(Ordering::SeqCst);
            let final_enqueued = self.total_enqueued.load(Ordering::Acquire);
            let final_dequeued = self.total_dequeued.load(Ordering::Acquire);

            if final_dequeued < final_enqueued {
                // Really thorough final scan
                let current_row = self.row.load(Ordering::Acquire);

                // Start from a random row to reduce contention
                let start_row = (consumer_id * 13) % (current_row + 1);

                for i in 0..=current_row {
                    let row = (start_row + i) % (current_row + 1);
                    if row >= self.num_rows {
                        continue;
                    }

                    for pos in 0..self.items_per_row {
                        let cell_val = self.get_item(row, pos).load();
                        if cell_val != BOTTOM && cell_val != TOP {
                            match self.get_item(row, pos).compare_exchange(cell_val, TOP) {
                                Ok(item_ptr) => {
                                    self.total_dequeued.fetch_add(1, Ordering::AcqRel);
                                    return Ok(self.get_pooled_item(item_ptr));
                                }
                                Err(_) => continue,
                            }
                        }
                    }
                }
            }

            Err(())
        }
    }

    // Initialize in shared memory
    pub unsafe fn init_in_shared(
        mem: *mut u8,
        num_consumers: usize,
        enqueuer_state: &mut EnqueuerState,
    ) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Configuration - more rows, smaller size
        let items_per_row = 512; // Smaller rows
        let num_rows = 1000; // Many more rows
        let item_pool_size = 200_000; // Generous pool

        // HEAD array
        let head_array_offset = queue_aligned;
        let head_array_size = num_rows * mem::size_of::<FetchIncrement>();
        let head_array_aligned = (head_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // ITEMS array
        let items_array_offset = head_array_offset + head_array_aligned;
        let items_array_size = num_rows * items_per_row * mem::size_of::<SwapCell>();
        let items_array_aligned = (items_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Item pool
        let item_pool_offset = items_array_offset + items_array_aligned;
        let item_pool_bytes = item_pool_size * mem::size_of::<T>();

        let total_size = item_pool_offset + item_pool_bytes;

        // Initialize queue structure
        ptr::write(
            queue_ptr,
            Self {
                row: AtomicUsize::new(0),
                head_array_offset,
                items_array_offset,
                total_enqueued: AtomicUsize::new(0),
                total_dequeued: AtomicUsize::new(0),
                num_consumers,
                num_rows,
                items_per_row,
                base_ptr: mem,
                total_size,
                item_pool_offset,
                item_pool_size,
                next_item: AtomicUsize::new(0),
                _phantom: std::marker::PhantomData,
            },
        );

        let queue = &mut *queue_ptr;

        // Initialize HEAD array
        let heads_ptr = mem.add(head_array_offset) as *mut FetchIncrement;
        for i in 0..num_rows {
            ptr::write(heads_ptr.add(i), FetchIncrement::new());
        }

        // Initialize ITEMS array
        let items_ptr = mem.add(items_array_offset) as *mut SwapCell;
        for i in 0..(num_rows * items_per_row) {
            ptr::write(items_ptr.add(i), SwapCell::new());
        }

        // Initialize enqueuer state
        enqueuer_state.enq_row = 0;
        enqueuer_state.tail = 0;

        queue
    }

    pub fn shared_size(_num_consumers: usize) -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_per_row = 512;
        let num_rows = 1000;
        let item_pool_size = 200_000;

        let head_array_size = num_rows * mem::size_of::<FetchIncrement>();
        let head_array_aligned = (head_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_array_size = num_rows * items_per_row * mem::size_of::<SwapCell>();
        let items_array_aligned = (items_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let item_pool_bytes = item_pool_size * mem::size_of::<T>();

        let total = queue_aligned + head_array_aligned + items_array_aligned + item_pool_bytes;
        (total + 4095) & !4095
    }

    pub fn is_empty(&self) -> bool {
        let enqueued = self.total_enqueued.load(Ordering::Acquire);
        let dequeued = self.total_dequeued.load(Ordering::Acquire);
        dequeued >= enqueued
    }
}

impl<T: Send + Clone + 'static> SpmcQueue<T> for DavidQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, _item: T, _producer_id: usize) -> Result<(), Self::PushError> {
        panic!("DavidQueue::push requires mutable EnqueuerState - use enqueue() directly");
    }

    fn pop(&self, consumer_id: usize) -> Result<T, Self::PopError> {
        self.dequeue(consumer_id)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn is_full(&self) -> bool {
        self.next_item.load(Ordering::Acquire) >= self.item_pool_size
    }
}
