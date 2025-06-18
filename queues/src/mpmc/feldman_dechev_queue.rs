// queues/src/mpmc/feldman_dechev_wf.rs

use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{fence, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::MpmcQueue;

const CACHE_LINE_SIZE: usize = 64;
const MAX_FAILS: usize = 1000;
const EMPTY_TYPE_MASK: u64 = 1; // LSB = 1 for EmptyType
const DELAY_MARK_MASK: u64 = 2; // Second LSB = 1 for delay marked

// Node types stored in the ring buffer
#[derive(Clone, Copy)]
struct Node {
    value: u64, // Stores either EmptyType with seqid or pointer to ValueType
}

impl Node {
    fn new_empty(seqid: u64) -> Self {
        Self {
            value: (seqid << 2) | EMPTY_TYPE_MASK,
        }
    }

    fn new_value(ptr: *mut ValueType<usize>, _seqid: u64) -> Self {
        Self {
            value: ptr as u64, // ValueType pointers have LSB = 0
        }
    }

    fn is_empty(&self) -> bool {
        (self.value & EMPTY_TYPE_MASK) != 0
    }

    fn is_value(&self) -> bool {
        !self.is_empty() && (self.value & DELAY_MARK_MASK) == 0
    }

    fn is_delay_marked(&self) -> bool {
        (self.value & DELAY_MARK_MASK) != 0
    }

    fn set_delay_mark(&mut self) {
        self.value |= DELAY_MARK_MASK;
    }

    fn get_seqid(&self) -> u64 {
        if self.is_empty() {
            self.value >> 2
        } else {
            // For ValueType, seqid is stored in the ValueType struct
            unsafe {
                let ptr = (self.value & !DELAY_MARK_MASK) as *mut ValueType<usize>;
                if ptr.is_null() {
                    0
                } else {
                    (*ptr).seqid
                }
            }
        }
    }

    fn get_value_ptr(&self) -> *mut ValueType<usize> {
        if self.is_empty() {
            ptr::null_mut()
        } else {
            (self.value & !DELAY_MARK_MASK) as *mut ValueType<usize>
        }
    }
}

// ValueType stores the actual enqueued data
#[repr(C)]
struct ValueType<T> {
    seqid: u64,
    value: UnsafeCell<Option<T>>,
}

// Operation records for progress assurance
#[repr(C)]
struct EnqueueOp<T> {
    value: T,
    helper: AtomicPtr<Node>,
}

#[repr(C)]
struct DequeueOp<T> {
    result: AtomicPtr<ValueType<T>>,
    helper: AtomicPtr<Node>,
}

// Main queue structure
#[repr(C)]
pub struct FeldmanDechevWFQueue<T: Send + Clone + 'static> {
    // Queue state
    buffer: *mut AtomicU64, // Array of nodes
    capacity: usize,
    head: AtomicU64,
    tail: AtomicU64,

    // Progress assurance
    announcement_table: *mut AtomicPtr<()>, // Generic op pointers
    num_threads: usize,

    // Memory pools
    value_pool: *mut ValueType<usize>,
    value_pool_size: usize,
    next_value: AtomicUsize,

    enq_op_pool: *mut EnqueueOp<usize>,
    deq_op_pool: *mut DequeueOp<usize>,
    next_enq_op: AtomicUsize,
    next_deq_op: AtomicUsize,

    // Shared memory info
    base_ptr: *mut u8,
    total_size: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for FeldmanDechevWFQueue<T> {}
unsafe impl<T: Send + Clone> Sync for FeldmanDechevWFQueue<T> {}

impl<T: Send + Clone + 'static> FeldmanDechevWFQueue<T> {
    unsafe fn get_node(&self, pos: usize) -> &AtomicU64 {
        &*self.buffer.add(pos % self.capacity)
    }

    unsafe fn backoff(&self, pos: usize, expected: Node) -> bool {
        // Simple exponential backoff
        let mut spins = 1;
        for _ in 0..10 {
            for _ in 0..spins {
                std::hint::spin_loop();
            }
            spins *= 2;

            let current = Node {
                value: self.get_node(pos).load(Ordering::Acquire),
            };
            if current.value != expected.value {
                return true; // Value changed
            }
        }
        false
    }

    pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), ()> {
        unsafe {
            let mut fails = 0;

            // Convert item to usize once, outside the loop
            let value_as_usize = std::mem::transmute_copy::<T, usize>(&item);
            std::mem::forget(item);

            loop {
                if fails >= MAX_FAILS {
                    // Switch to wait-free slow path
                    // Need to convert back for slow path
                    let item_copy = std::mem::transmute_copy::<usize, T>(&value_as_usize);
                    return self.enqueue_slow_path(thread_id, item_copy);
                }

                let seqid = self.tail.fetch_add(1, Ordering::AcqRel);
                let pos = (seqid % self.capacity as u64) as usize;

                loop {
                    fails += 1;
                    if fails >= MAX_FAILS {
                        break;
                    }

                    let node_val = self.get_node(pos).load(Ordering::Acquire);
                    let node = Node { value: node_val };

                    // Check if marked as skipped
                    if node.is_delay_marked() {
                        break; // Get new seqid
                    }

                    if node.is_empty() {
                        let node_seqid = node.get_seqid();

                        if node_seqid < seqid {
                            // Backoff and retry
                            if !self.backoff(pos, node) {
                                continue;
                            }
                        }

                        if node_seqid <= seqid {
                            // Try to enqueue
                            let value_idx = self.next_value.fetch_add(1, Ordering::AcqRel);
                            if value_idx >= self.value_pool_size {
                                return Err(()); // Pool exhausted
                            }

                            let value_ptr = self.value_pool.add(value_idx);

                            ptr::write(
                                value_ptr,
                                ValueType {
                                    seqid,
                                    value: UnsafeCell::new(Some(value_as_usize)),
                                },
                            );

                            let new_node = Node::new_value(value_ptr, seqid);

                            if self
                                .get_node(pos)
                                .compare_exchange(
                                    node_val,
                                    new_node.value,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_ok()
                            {
                                return Ok(());
                            }

                            // CAS failed, continue
                            continue;
                        } else {
                            // node_seqid > seqid, get new seqid
                            break;
                        }
                    } else {
                        // Position has a value, get new seqid
                        break;
                    }
                }
            }
        }
    }

    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            let mut fails = 0;

            loop {
                if fails >= MAX_FAILS {
                    // Switch to wait-free slow path
                    return self.dequeue_slow_path(thread_id);
                }

                if self.is_empty() {
                    return Err(());
                }

                let seqid = self.head.fetch_add(1, Ordering::AcqRel);
                let pos = (seqid % self.capacity as u64) as usize;
                let empty_node = Node::new_empty(seqid + self.capacity as u64);

                loop {
                    fails += 1;
                    if fails >= MAX_FAILS {
                        break;
                    }

                    let node_val = self.get_node(pos).load(Ordering::Acquire);
                    let node = Node { value: node_val };

                    if node.is_delay_marked() && node.is_empty() {
                        // Already processed delay marked empty node
                        if self
                            .get_node(pos)
                            .compare_exchange(
                                node_val,
                                empty_node.value,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            break;
                        }
                        continue;
                    }

                    if node.get_seqid() > seqid {
                        break; // Get new seqid
                    }

                    if node.is_value() {
                        let value_seqid = node.get_seqid();

                        if value_seqid == seqid {
                            // This is our element
                            let mut replacement = empty_node;
                            if node.is_delay_marked() {
                                replacement.set_delay_mark();
                            }

                            if self
                                .get_node(pos)
                                .compare_exchange(
                                    node_val,
                                    replacement.value,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_ok()
                            {
                                let value_ptr = node.get_value_ptr();
                                let value = (*(*value_ptr).value.get()).take().unwrap();
                                // Transmute back to T
                                return Ok(std::mem::transmute_copy::<usize, T>(&value));
                            }

                            // Retry if it was delay marked
                            let current = Node {
                                value: self.get_node(pos).load(Ordering::Acquire),
                            };
                            if current.is_delay_marked() && current.get_seqid() == seqid {
                                // Don't reassign to node, just continue
                                continue;
                            }
                        } else if value_seqid < seqid {
                            // Delayed element, mark for correction
                            if !self.backoff(pos, node) {
                                // Mark as delayed
                                self.atomic_delay_mark(pos);
                            }
                        }
                    } else if node.is_empty() {
                        let empty_seqid = node.get_seqid();

                        if empty_seqid < seqid {
                            if !self.backoff(pos, node) {
                                // Try to advance
                                self.get_node(pos)
                                    .compare_exchange(
                                        node_val,
                                        empty_node.value,
                                        Ordering::AcqRel,
                                        Ordering::Acquire,
                                    )
                                    .ok();
                            }
                        } else {
                            // Mark as delay marked empty with large seqid
                            let current_tail = self.tail.load(Ordering::Acquire);
                            let large_seqid = current_tail + 2 * self.capacity as u64;
                            let mut large_empty = Node::new_empty(large_seqid);
                            large_empty.set_delay_mark();

                            self.get_node(pos)
                                .compare_exchange(
                                    node_val,
                                    large_empty.value,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .ok();
                        }
                    }
                }
            }
        }
    }

    unsafe fn atomic_delay_mark(&self, pos: usize) {
        loop {
            let current = self.get_node(pos).load(Ordering::Acquire);
            let mut node = Node { value: current };

            if node.is_delay_marked() {
                return;
            }

            node.set_delay_mark();

            if self
                .get_node(pos)
                .compare_exchange(current, node.value, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    // Placeholder for wait-free slow paths - would need full implementation
    unsafe fn enqueue_slow_path(&self, _thread_id: usize, _item: T) -> Result<(), ()> {
        // In a full implementation, this would use the announcement table
        // and helper threads as described in the paper
        Err(())
    }

    unsafe fn dequeue_slow_path(&self, _thread_id: usize) -> Result<T, ()> {
        // In a full implementation, this would use the announcement table
        // and helper threads as described in the paper
        Err(())
    }

    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Ring buffer
        let capacity = 65536; // Power of 2 for efficient modulo
        let buffer_offset = queue_aligned;
        let buffer_size = capacity * mem::size_of::<AtomicU64>();
        let buffer_aligned = (buffer_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Announcement table
        let announcement_offset = buffer_offset + buffer_aligned;
        let announcement_size = num_threads * mem::size_of::<AtomicPtr<()>>();
        let announcement_aligned =
            (announcement_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Value pool
        let items_per_thread = 250_000;
        let value_pool_size = num_threads * items_per_thread;
        let value_pool_offset = announcement_offset + announcement_aligned;
        let value_pool_bytes = value_pool_size * mem::size_of::<ValueType<usize>>();
        let value_pool_aligned = (value_pool_bytes + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Op pools
        let op_pool_size = num_threads * 100;
        let enq_op_pool_offset = value_pool_offset + value_pool_aligned;
        let enq_op_pool_bytes = op_pool_size * mem::size_of::<EnqueueOp<usize>>();
        let enq_op_pool_aligned =
            (enq_op_pool_bytes + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let deq_op_pool_offset = enq_op_pool_offset + enq_op_pool_aligned;
        let deq_op_pool_bytes = op_pool_size * mem::size_of::<DequeueOp<usize>>();

        let total_size = deq_op_pool_offset + deq_op_pool_bytes;

        // Initialize buffer
        let buffer_ptr = mem.add(buffer_offset) as *mut AtomicU64;
        for i in 0..capacity {
            let empty_node = Node::new_empty(i as u64);
            ptr::write(buffer_ptr.add(i), AtomicU64::new(empty_node.value));
        }

        // Initialize announcement table
        let announcement_ptr = mem.add(announcement_offset) as *mut AtomicPtr<()>;
        for i in 0..num_threads {
            ptr::write(announcement_ptr.add(i), AtomicPtr::new(ptr::null_mut()));
        }

        // Initialize queue
        ptr::write(
            queue_ptr,
            Self {
                buffer: buffer_ptr,
                capacity,
                head: AtomicU64::new(0),
                tail: AtomicU64::new(0),
                announcement_table: announcement_ptr,
                num_threads,
                value_pool: mem.add(value_pool_offset) as *mut ValueType<usize>,
                value_pool_size,
                next_value: AtomicUsize::new(0),
                enq_op_pool: mem.add(enq_op_pool_offset) as *mut EnqueueOp<usize>,
                deq_op_pool: mem.add(deq_op_pool_offset) as *mut DequeueOp<usize>,
                next_enq_op: AtomicUsize::new(0),
                next_deq_op: AtomicUsize::new(0),
                base_ptr: mem,
                total_size,
                _phantom: std::marker::PhantomData,
            },
        );

        fence(Ordering::SeqCst);
        &mut *queue_ptr
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let capacity = 65536;
        let buffer_size = capacity * mem::size_of::<AtomicU64>();
        let buffer_aligned = (buffer_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let announcement_size = num_threads * mem::size_of::<AtomicPtr<()>>();
        let announcement_aligned =
            (announcement_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_per_thread = 250_000;
        let value_pool_size = num_threads * items_per_thread;
        let value_pool_bytes = value_pool_size * mem::size_of::<ValueType<usize>>();
        let value_pool_aligned = (value_pool_bytes + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let op_pool_size = num_threads * 100;
        let enq_op_pool_bytes = op_pool_size * mem::size_of::<EnqueueOp<usize>>();
        let enq_op_pool_aligned =
            (enq_op_pool_bytes + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let deq_op_pool_bytes = op_pool_size * mem::size_of::<DequeueOp<usize>>();

        let total = queue_aligned
            + buffer_aligned
            + announcement_aligned
            + value_pool_aligned
            + enq_op_pool_aligned
            + deq_op_pool_bytes;
        (total + 4095) & !4095 // Page align
    }

    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) >= self.tail.load(Ordering::Acquire)
    }

    pub fn is_full(&self) -> bool {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        (tail - head) >= self.capacity as u64
    }
}

impl<T: Send + Clone + 'static> MpmcQueue<T> for FeldmanDechevWFQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, item: T, thread_id: usize) -> Result<(), Self::PushError> {
        self.enqueue(thread_id, item)
    }

    fn pop(&self, thread_id: usize) -> Result<T, Self::PopError> {
        self.dequeue(thread_id)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn is_full(&self) -> bool {
        self.is_full()
    }
}
