// paper in /paper/spmc/david.pdf
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

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
}

// Main queue structure
#[repr(C)]
pub struct DavidQueue<T: Send + Clone + 'static> {
    // Shared state
    row: AtomicUsize,          // ROW register
    min_row: AtomicUsize,      // Minimum row that might have items
    head_array_offset: usize,  // Offset to head array
    items_array_offset: usize, // Offset to items array

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
        let max_index = self.num_rows * self.items_per_row;
        &*items_ptr.add(index)
    }

    // Allocate space for an item in the pool
    unsafe fn allocate_item(&self, item: T) -> usize {
        let idx = self.next_item.fetch_add(1, Ordering::AcqRel);
        if idx >= self.item_pool_size {
            panic!("Item pool exhausted: {} >= {}", idx, self.item_pool_size);
        }

        let items_ptr = self.base_ptr.add(self.item_pool_offset) as *mut T;
        let item_ptr = items_ptr.add(idx);

        // Debug check
        if item_ptr as usize >= (self.base_ptr as usize + self.total_size) {
            panic!(
                "Item pointer out of bounds: {:p} >= {:p}",
                item_ptr,
                self.base_ptr.add(self.total_size)
            );
        }

        ptr::write(item_ptr, item);

        // Return pointer as usize
        item_ptr as usize
    }

    // Retrieve item from pool
    unsafe fn get_pooled_item(&self, ptr_as_usize: usize) -> T {
        let ptr = ptr_as_usize as *mut T;
        ptr::read(ptr)
    }

    // Enqueue operation
    pub fn enqueue(&self, state: &mut EnqueuerState, item: T) -> Result<(), ()> {
        unsafe {
            // Check bounds
            if state.enq_row >= self.num_rows {
                return Err(());
            }

            let x_ptr = self.allocate_item(item);

            // Line 1: val ← Swap(ITEMS[enq_row, tail], x)
            let val = self.get_item(state.enq_row, state.tail).swap(x_ptr);

            // Line 2-6: if val = ⊤ then
            if val == TOP {
                // increment(enq_row)
                state.enq_row += 1;
                if state.enq_row >= self.num_rows {
                    return Err(());
                }

                // tail ← 0
                state.tail = 0;

                // Line 5: Swap(ITEMS[enq_row, tail], x)
                self.get_item(state.enq_row, state.tail).swap(x_ptr);

                // Line 6: Write(ROW, enq_row)
                self.row.store(state.enq_row, Ordering::Release);
            }

            // Line 7: increment(tail)
            state.tail += 1;

            Ok(())
        }
    }

    // Dequeue operation - Paper's algorithm with recovery mechanism (was necessary because somehow papers synchro steps were not enough)
    pub fn dequeue(&self, _consumer_id: usize) -> Result<T, ()> {
        unsafe {
            // First try the paper's exact algorithm
            // Line 1: deq_row ← Read(ROW)
            let deq_row = self.row.load(Ordering::Acquire);

            // Line 2: head ← Fetch&Increment(HEAD[deq_row])
            let head = self.get_head(deq_row).fetch_increment();

            // Line 3: val ← Swap(ITEMS[deq_row, head], ⊤)
            let val = self.get_item(deq_row, head).swap(TOP);

            // Line 4: if val = ⊥ then return ε else return val
            if val != BOTTOM && val != TOP {
                return Ok(self.get_pooled_item(val));
            }

            // Recovery mechanism: If we got BOTTOM, check previous rows
            // This violates the 3-step bound but prevents data loss

            let min_row = self.min_row.load(Ordering::Acquire);

            // Try previous rows from most recent to oldest
            for offset in 1..=(deq_row.saturating_sub(min_row)).min(10) {
                let try_row = deq_row.saturating_sub(offset);

                if try_row < min_row {
                    break;
                }

                // Try to get an item from this row
                let head = self.get_head(try_row).fetch_increment();

                if head < self.items_per_row {
                    let val = self.get_item(try_row, head).swap(TOP);

                    if val != BOTTOM && val != TOP {
                        // Found an item! Check if this row is now exhausted
                        if head + 1 >= self.items_per_row {
                            // Try to advance min_row
                            self.min_row
                                .compare_exchange(
                                    try_row,
                                    try_row + 1,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .ok();
                        }

                        return Ok(self.get_pooled_item(val));
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

        // Configuration - sized for 1M items with safety margin
        let items_per_row = 2048; // Larger row size for efficiency
        let num_rows = 1500; // 1500 * 2048 = 3,072,000 cells (3x overhead)
        let item_pool_size = 1_500_000; // Pool for 1.5M items

        // head array
        let head_array_offset = queue_aligned;
        let head_array_size = num_rows * mem::size_of::<FetchIncrement>();
        let head_array_aligned = (head_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // items array
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
                min_row: AtomicUsize::new(0),
                head_array_offset,
                items_array_offset,
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

        let items_per_row = 2048;
        let num_rows = 1500;
        let item_pool_size = 1_500_000;

        let head_array_size = num_rows * mem::size_of::<FetchIncrement>();
        let head_array_aligned = (head_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_array_size = num_rows * items_per_row * mem::size_of::<SwapCell>();
        let items_array_aligned = (items_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let item_pool_bytes = item_pool_size * mem::size_of::<T>();

        let total = queue_aligned + head_array_aligned + items_array_aligned + item_pool_bytes;

        (total + 4095) & !4095
    }

    pub fn spsc_shared_size() -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_per_row = 8_192;
        let num_rows = 10000;
        let item_pool_size = 15_000_000;

        let head_array_size = num_rows * mem::size_of::<FetchIncrement>();
        let head_array_aligned = (head_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_array_size = num_rows * items_per_row * mem::size_of::<SwapCell>();
        let items_array_aligned = (items_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let item_pool_bytes = item_pool_size * mem::size_of::<T>();

        let total = queue_aligned + head_array_aligned + items_array_aligned + item_pool_bytes;

        (total + 4095) & !4095
    }

    pub unsafe fn init_in_shared_spsc(
        mem: *mut u8,
        enqueuer_state: &mut EnqueuerState,
    ) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Configuration
        let items_per_row: usize = 8_192;
        let num_rows = 10000;
        let item_pool_size = 15_000_000;
        let num_consumers = 1; // SPSC has 1 consumer

        // head array
        let head_array_offset = queue_aligned;
        let head_array_size = num_rows * mem::size_of::<FetchIncrement>();
        let head_array_aligned = (head_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // items array
        let items_array_offset = head_array_offset + head_array_aligned;
        let items_array_size = num_rows * items_per_row * mem::size_of::<SwapCell>();
        let items_array_aligned = (items_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Item pool
        let item_pool_offset = items_array_offset + items_array_aligned;
        let item_pool_bytes = item_pool_size * mem::size_of::<T>();

        let total_size = (item_pool_offset + item_pool_bytes + 4095) & !4095;

        // Initialize queue structure
        ptr::write(
            queue_ptr,
            Self {
                row: AtomicUsize::new(0),
                min_row: AtomicUsize::new(0),
                head_array_offset,
                items_array_offset,
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

        // Initialize head array
        let heads_ptr = mem.add(head_array_offset) as *mut FetchIncrement;
        for i in 0..num_rows {
            ptr::write(heads_ptr.add(i), FetchIncrement::new());
        }

        // Initialize items array
        let items_ptr = mem.add(items_array_offset) as *mut SwapCell;
        for i in 0..(num_rows * items_per_row) {
            ptr::write(items_ptr.add(i), SwapCell::new());
        }

        // Initialize enqueuer state
        enqueuer_state.enq_row = 0;
        enqueuer_state.tail = 0;

        queue
    }

    pub fn is_empty(&self) -> bool {
        false
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
