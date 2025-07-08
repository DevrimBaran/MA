// Paper in /paper/mpmc/feldman_dechev_v1.pdf and feldman_dechev_v2_(full).pdf and feldman_dechev_v3.pdf
use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::MpmcQueue;

const CACHE_LINE_SIZE: usize = 64;
const MAX_FAILS: usize = 1000;
const EMPTY_TYPE_MASK: u64 = 1; // LSB = 1 for EmptyType
const DELAY_MARK_MASK: u64 = 2; // Second LSB = 1 for delay marked
const CHECK_DELAY: usize = 8; // Check for announcements every 8 operations
const ITEMS_PER_THREAD: usize = 200_000;
const OPS_PER_THREAD: usize = 500;

// Node types - Section 3 in paper
// Paper uses separate EmptyNode/ElemNode classes, we use tagged pointers for IPC
#[derive(Clone, Copy)]
pub struct Node {
    value: u64, // Stores either EmptyType with seqid or pointer to ValueType
}

impl Node {
    pub fn new_empty(seqid: u64) -> Self {
        Self {
            value: (seqid << 2) | EMPTY_TYPE_MASK,
        }
    }

    pub fn new_value(ptr: *mut ValueType<usize>, _seqid: u64) -> Self {
        Self {
            value: ptr as u64, // ValueType pointers have LSB = 0
        }
    }

    // is_EmptyNode() - Figure 1
    pub fn is_empty(&self) -> bool {
        (self.value & EMPTY_TYPE_MASK) != 0
    }

    // is_ElemNode() - Figure 1
    pub fn is_value(&self) -> bool {
        !self.is_empty() && (self.value & DELAY_MARK_MASK) == 0
    }

    // is_skipped() - Figure 1
    pub fn is_delay_marked(&self) -> bool {
        (self.value & DELAY_MARK_MASK) != 0
    }

    // set_skipped() - Figure 1
    pub fn set_delay_mark(&mut self) {
        self.value |= DELAY_MARK_MASK;
    }

    pub fn get_seqid(&self) -> u64 {
        if self.is_empty() {
            self.value >> 2
        } else {
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

    pub fn get_value_ptr(&self) -> *mut ValueType<usize> {
        if self.is_empty() {
            ptr::null_mut()
        } else {
            (self.value & !DELAY_MARK_MASK) as *mut ValueType<usize>
        }
    }
}

// ValueType - stores actual enqueued data
#[repr(C)]
pub struct ValueType<T> {
    pub seqid: u64,
    pub value: UnsafeCell<Option<T>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpType {
    Enqueue,
    Dequeue,
}

// EnqueueOp - Section 8 Progress Assurance
#[repr(C)]
pub struct EnqueueOp {
    pub op_type: OpType,
    pub value: AtomicUsize,
    pub seqid: AtomicU64,
    pub complete: AtomicBool, // Replaces paper's helper field
    pub success: AtomicBool,  // IPC adaptation - track success
    pub thread_id: usize,
}

impl EnqueueOp {
    pub fn new(value: usize, thread_id: usize) -> Self {
        Self {
            op_type: OpType::Enqueue,
            value: AtomicUsize::new(value),
            seqid: AtomicU64::new(0),
            complete: AtomicBool::new(false),
            success: AtomicBool::new(false),
            thread_id,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }

    pub fn complete_success(&self) {
        self.success.store(true, Ordering::Release);
        self.complete.store(true, Ordering::Release);
    }

    pub fn complete_failure(&self) {
        self.success.store(false, Ordering::Release);
        self.complete.store(true, Ordering::Release);
    }

    pub fn was_successful(&self) -> bool {
        self.success.load(Ordering::Acquire)
    }
}

// DequeueOp - Section 8 Progress Assurance
#[repr(C)]
pub struct DequeueOp {
    pub op_type: OpType,
    pub result: AtomicUsize,
    pub seqid: AtomicU64,
    pub complete: AtomicBool,
    pub success: AtomicBool,
    pub thread_id: usize,
}

impl DequeueOp {
    pub fn new(thread_id: usize) -> Self {
        Self {
            op_type: OpType::Dequeue,
            result: AtomicUsize::new(0),
            seqid: AtomicU64::new(0),
            complete: AtomicBool::new(false),
            success: AtomicBool::new(false),
            thread_id,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }

    pub fn complete_success(&self, value: usize) {
        self.result.store(value, Ordering::Release);
        self.success.store(true, Ordering::Release);
        self.complete.store(true, Ordering::Release);
    }

    pub fn complete_failure(&self) {
        self.success.store(false, Ordering::Release);
        self.complete.store(true, Ordering::Release);
    }

    pub fn was_successful(&self) -> bool {
        self.success.load(Ordering::Acquire)
    }

    pub fn get_result(&self) -> usize {
        self.result.load(Ordering::Acquire)
    }
}

// WFQueue - corresponds to paper's ring buffer
#[repr(C)]
pub struct FeldmanDechevWFQueue<T: Send + Clone + 'static> {
    // Core ring buffer - Section 4
    buffer: *mut AtomicU64, // Array of nodes
    capacity: usize,
    head: AtomicU64, // head sequence counter
    tail: AtomicU64, // tail sequence counter

    // Progress assurance - Section 8
    announcement_table: *mut AtomicPtr<()>, // Announcement table
    num_threads: usize,
    operation_counter: *mut AtomicUsize, // Per-process op counters
    help_index: *mut AtomicUsize,        // Per-process help indices

    // Memory pools - IPC adaptation (replaces dynamic allocation)
    value_pool: *mut ValueType<usize>,
    value_pool_size: usize,
    next_value: AtomicUsize,

    enq_op_pool: *mut EnqueueOp,
    deq_op_pool: *mut DequeueOp,
    next_enq_op: AtomicUsize,
    next_deq_op: AtomicUsize,

    // IPC monitoring
    active_enq_ops: AtomicUsize,
    active_deq_ops: AtomicUsize,

    // Shared memory management
    base_ptr: *mut u8,
    total_size: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for FeldmanDechevWFQueue<T> {}
unsafe impl<T: Send + Clone> Sync for FeldmanDechevWFQueue<T> {}

impl<T: Send + Clone + 'static> FeldmanDechevWFQueue<T> {
    // get_position() - Figure 1
    unsafe fn get_node(&self, pos: usize) -> &AtomicU64 {
        &*self.buffer.add(pos % self.capacity)
    }

    // backoff() - Figure 1 (enhanced for IPC)
    unsafe fn backoff(&self, pos: usize, expected: Node) -> bool {
        let mut spins = 1;
        for i in 0..10 {
            for _ in 0..spins.min(1024) {
                std::hint::spin_loop();
            }
            spins *= 2;

            let current = Node {
                value: self.get_node(pos).load(Ordering::Acquire),
            };
            if current.value != expected.value {
                return true; // Value changed
            }

            // IPC adaptation - yield for fairness
            if i > 5 {
                std::thread::yield_now();
            }
        }
        false
    }

    // TryHelpAnother() - Section 8 (adapted for periodic checking)
    unsafe fn check_for_announcement(&self, thread_id: usize) {
        let op_count = (*self.operation_counter.add(thread_id)).fetch_add(1, Ordering::AcqRel);

        if op_count % CHECK_DELAY == 0 {
            let help_idx =
                (*self.help_index.add(thread_id)).fetch_add(1, Ordering::AcqRel) % self.num_threads;
            let announced_op = (*self.announcement_table.add(help_idx)).load(Ordering::Acquire);

            if !announced_op.is_null() {
                self.help_operation(announced_op, help_idx);
            }
        }
    }

    // Help another process's operation
    unsafe fn help_operation(&self, op_ptr: *mut (), helper_thread_id: usize) {
        let op_type_ptr = op_ptr as *const OpType;
        let op_type = *op_type_ptr;

        match op_type {
            OpType::Enqueue => {
                let enq_op = op_ptr as *mut EnqueueOp;
                self.help_enqueue(enq_op, helper_thread_id);
            }
            OpType::Dequeue => {
                let deq_op = op_ptr as *mut DequeueOp;
                self.help_dequeue(deq_op, helper_thread_id);
            }
        }
    }

    // EnqueueOp::associate() - Algorithm 6
    unsafe fn help_enqueue(&self, op: *mut EnqueueOp, _helper_thread_id: usize) {
        if (*op).is_complete() {
            return;
        }

        let value = (*op).value.load(Ordering::Acquire);

        // Get seqid for operation
        let mut seqid = (*op).seqid.load(Ordering::Acquire);
        if seqid == 0 {
            seqid = self.tail.fetch_add(1, Ordering::AcqRel);
            (*op)
                .seqid
                .compare_exchange(0, seqid, Ordering::AcqRel, Ordering::Acquire)
                .ok();
            seqid = (*op).seqid.load(Ordering::Acquire);
        }

        let pos = (seqid % self.capacity as u64) as usize;

        // Bounded helping - MAX_FAILS + NUM_THREADS²
        let max_help_attempts = MAX_FAILS + (self.num_threads * self.num_threads);

        for attempt in 0..max_help_attempts {
            if (*op).is_complete() {
                return;
            }

            let node_val = self.get_node(pos).load(Ordering::Acquire);
            let node = Node { value: node_val };

            if node.is_empty() && node.get_seqid() <= seqid && !node.is_delay_marked() {
                // IPC: allocate from pool instead of heap
                let value_idx = self.next_value.fetch_add(1, Ordering::AcqRel);
                if value_idx >= self.value_pool_size {
                    (*op).complete_failure();
                    return;
                }

                let value_ptr = self.value_pool.add(value_idx % self.value_pool_size);
                ptr::write(
                    value_ptr,
                    ValueType {
                        seqid,
                        value: UnsafeCell::new(Some(value)),
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
                    (*op).complete_success();
                    return;
                }
            } else if node.get_seqid() > seqid || node.is_value() {
                (*op).complete_failure();
                return;
            }

            // IPC: periodic yield
            if attempt % 1000 == 0 {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }

        (*op).complete_failure();
    }

    // DequeueOp::associate() - Algorithm 2
    unsafe fn help_dequeue(&self, op: *mut DequeueOp, _helper_thread_id: usize) {
        if (*op).is_complete() {
            return;
        }

        let mut seqid = (*op).seqid.load(Ordering::Acquire);
        if seqid == 0 {
            seqid = self.head.fetch_add(1, Ordering::AcqRel);
            (*op)
                .seqid
                .compare_exchange(0, seqid, Ordering::AcqRel, Ordering::Acquire)
                .ok();
            seqid = (*op).seqid.load(Ordering::Acquire);
        }

        let pos = (seqid % self.capacity as u64) as usize;

        let max_help_attempts = MAX_FAILS + (self.num_threads * self.num_threads);

        for attempt in 0..max_help_attempts {
            if (*op).is_complete() {
                return;
            }

            let node_val = self.get_node(pos).load(Ordering::Acquire);
            let node = Node { value: node_val };

            if node.is_value() && node.get_seqid() == seqid {
                let empty_node = Node::new_empty(seqid + self.capacity as u64);

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
                    let value_ptr = node.get_value_ptr();
                    let value = (*(*value_ptr).value.get()).take().unwrap_or(0);
                    (*op).complete_success(value);
                    return;
                }
            } else if node.get_seqid() > seqid {
                (*op).complete_failure();
                return;
            }

            if attempt % 1000 == 0 {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }

        (*op).complete_failure();
    }

    // make_announcement() - Section 8
    unsafe fn make_announcement(&self, thread_id: usize, op_ptr: *mut ()) {
        (*self.announcement_table.add(thread_id)).store(op_ptr, Ordering::Release);
        fence(Ordering::SeqCst);
    }

    // Clear announcement
    unsafe fn clear_announcement(&self, thread_id: usize) {
        (*self.announcement_table.add(thread_id)).store(ptr::null_mut(), Ordering::Release);
    }

    // Enqueue() - Algorithm 4
    pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), ()> {
        unsafe {
            // Line 1: try_help_another()
            self.check_for_announcement(thread_id);

            let mut fails = 0;

            // IPC: convert to usize for shared memory
            let value_as_usize = std::mem::transmute_copy::<T, usize>(&item);
            std::mem::forget(item);

            loop {
                // Line 11-14: Check MAX_FAILS
                if fails >= MAX_FAILS {
                    return self.enqueue_slow_path(thread_id, value_as_usize);
                }

                // Line 7: seqid = next_tail_seq()
                let seqid = self.tail.fetch_add(1, Ordering::AcqRel);
                // Line 8: pos = get_position(seqid)
                let pos = (seqid % self.capacity as u64) as usize;

                loop {
                    fails += 1;
                    if fails >= MAX_FAILS {
                        break;
                    }

                    // Line 16: node = buffer[pos].load()
                    let node_val = self.get_node(pos).load(Ordering::Acquire);
                    let node = Node { value: node_val };

                    // Line 20: if is_skipped(node)
                    if node.is_delay_marked() {
                        break; // Line 22: break
                    }

                    // Line 26: if node.seqid < seqid
                    if node.is_empty() {
                        let node_seqid = node.get_seqid();

                        if node_seqid < seqid {
                            // Line 27: backoff()
                            if !self.backoff(pos, node) {
                                continue;
                            }
                        }

                        // Line 28: if node.seqid <= seqid and is_EmptyNode(node)
                        if node_seqid <= seqid {
                            // IPC: allocate from pool
                            let value_idx = self.next_value.fetch_add(1, Ordering::AcqRel);
                            if value_idx >= self.value_pool_size {
                                return self.enqueue_slow_path(thread_id, value_as_usize);
                            }

                            let value_ptr = self.value_pool.add(value_idx % self.value_pool_size);

                            ptr::write(
                                value_ptr,
                                ValueType {
                                    seqid,
                                    value: UnsafeCell::new(Some(value_as_usize)),
                                },
                            );

                            let new_node = Node::new_value(value_ptr, seqid);

                            // Line 29: success = buffer[pos].cas(node, n_node)
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
                                return Ok(()); // Line 31: return true
                            }

                            continue;
                        } else {
                            // Line 34: else if node.seqid > seqid
                            break; // Line 35: break
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    // Dequeue() - Algorithm 1
    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            // Line 1: try_help_another()
            self.check_for_announcement(thread_id);

            let mut fails = 0;

            loop {
                // Line 11-15: Check MAX_FAILS
                if fails >= MAX_FAILS {
                    return self.dequeue_slow_path(thread_id);
                }

                // Line 4-6: Check if empty
                if self.is_empty() {
                    return Err(());
                }

                // Line 7: seqid = next_head_seq()
                let seqid = self.head.fetch_add(1, Ordering::AcqRel);
                // Line 8: pos = get_position(seqid)
                let pos = (seqid % self.capacity as u64) as usize;
                // Line 9: n_node = EmptyNode(seqid + capacity)
                let empty_node = Node::new_empty(seqid + self.capacity as u64);

                loop {
                    fails += 1;
                    if fails >= MAX_FAILS {
                        break;
                    }

                    // Line 16: node = buffer[pos].load()
                    let node_val = self.get_node(pos).load(Ordering::Acquire);
                    let node = Node { value: node_val };

                    // Line 20-25: if is_skipped(node) and is_EmptyNode(node)
                    if node.is_delay_marked() && node.is_empty() {
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
                            break; // Line 22: break
                        }
                        continue; // Line 24: continue
                    }

                    // Line 37: if seqid < node.seqid
                    if node.get_seqid() > seqid {
                        break; // Line 38: break
                    }

                    // Line 40: if is_ElemNode(node)
                    if node.is_value() {
                        let value_seqid = node.get_seqid();

                        if value_seqid == seqid {
                            let mut replacement = empty_node;
                            // Line 41-43: Handle delay marking
                            if node.is_delay_marked() {
                                replacement.set_delay_mark();
                            }

                            // Line 44: success = buffer[pos].cas(node, n_node)
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
                                // Line 46-47: Extract value and return
                                let value_ptr = node.get_value_ptr();
                                let value = (*(*value_ptr).value.get()).take().unwrap();
                                // IPC: convert back from usize
                                return Ok(std::mem::transmute_copy::<usize, T>(&value));
                            }

                            let current = Node {
                                value: self.get_node(pos).load(Ordering::Acquire),
                            };
                            if current.is_delay_marked() && current.get_seqid() == seqid {
                                continue;
                            }
                        } else if value_seqid < seqid {
                            // Delayed element - mark for correction
                            if !self.backoff(pos, node) {
                                // Line 34: set_skipped(&buffer[pos])
                                self.atomic_delay_mark(pos);
                            }
                        }
                    } else if node.is_empty() {
                        let empty_seqid = node.get_seqid();

                        // Line 26: if seqid > node.seqid
                        if empty_seqid < seqid {
                            // Line 27: backoff()
                            if !self.backoff(pos, node) {
                                // Line 30: Try to advance
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
                            // IPC: use large seqid to prevent conflicts
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

    // Atomic delay marking (bitmarking) - Section 9
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

    // Wait-Free Enqueue - Algorithm 5
    unsafe fn enqueue_slow_path(&self, thread_id: usize, value: usize) -> Result<(), ()> {
        // IPC: track active operations
        self.active_enq_ops.fetch_add(1, Ordering::AcqRel);

        // IPC: use thread-local slots to prevent exhaustion
        let op_idx = thread_id * OPS_PER_THREAD
            + (self.next_enq_op.fetch_add(1, Ordering::AcqRel) % OPS_PER_THREAD);

        if op_idx >= self.num_threads * OPS_PER_THREAD {
            self.active_enq_ops.fetch_sub(1, Ordering::AcqRel);
            return Err(());
        }

        let op_ptr = self.enq_op_pool.add(op_idx);
        ptr::write(op_ptr, EnqueueOp::new(value, thread_id));

        // Make announcement
        self.make_announcement(thread_id, op_ptr as *mut ());

        // Wait for NUM_THREADS² * CHECK_DELAY operations
        let max_wait_ops = self.num_threads * self.num_threads * CHECK_DELAY;

        for i in 0..max_wait_ops {
            if (*op_ptr).is_complete() {
                break;
            }

            self.help_enqueue(op_ptr, thread_id);

            if i % CHECK_DELAY == 0 {
                self.check_for_announcement(thread_id);
            }

            if i % 1000 == 0 {
                std::thread::yield_now();
            }
        }

        self.clear_announcement(thread_id);
        self.active_enq_ops.fetch_sub(1, Ordering::AcqRel);

        if (*op_ptr).is_complete() && (*op_ptr).was_successful() {
            Ok(())
        } else {
            Err(())
        }
    }

    // Wait-Free Dequeue - Algorithm 3
    unsafe fn dequeue_slow_path(&self, thread_id: usize) -> Result<T, ()> {
        self.active_deq_ops.fetch_add(1, Ordering::AcqRel);

        let op_idx = thread_id * OPS_PER_THREAD
            + (self.next_deq_op.fetch_add(1, Ordering::AcqRel) % OPS_PER_THREAD);

        if op_idx >= self.num_threads * OPS_PER_THREAD {
            self.active_deq_ops.fetch_sub(1, Ordering::AcqRel);
            return Err(());
        }

        let op_ptr = self.deq_op_pool.add(op_idx);
        ptr::write(op_ptr, DequeueOp::new(thread_id));

        self.make_announcement(thread_id, op_ptr as *mut ());

        let max_wait_ops = self.num_threads * self.num_threads * CHECK_DELAY;

        for i in 0..max_wait_ops {
            if (*op_ptr).is_complete() {
                break;
            }

            self.help_dequeue(op_ptr, thread_id);

            if i % CHECK_DELAY == 0 {
                self.check_for_announcement(thread_id);
            }

            if i % 1000 == 0 {
                std::thread::yield_now();
            }
        }

        self.clear_announcement(thread_id);
        self.active_deq_ops.fetch_sub(1, Ordering::AcqRel);

        if (*op_ptr).is_complete() && (*op_ptr).was_successful() {
            let result = (*op_ptr).get_result();
            Ok(std::mem::transmute_copy::<usize, T>(&result))
        } else {
            Err(())
        }
    }

    // Initialize in shared memory (IPC adaptation)
    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Ring buffer
        let capacity = 131072; // Larger for IPC
        let buffer_offset = queue_aligned;
        let buffer_size = capacity * mem::size_of::<AtomicU64>();
        let buffer_aligned = (buffer_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Announcement table
        let announcement_offset = buffer_offset + buffer_aligned;
        let announcement_size = num_threads * mem::size_of::<AtomicPtr<()>>();
        let announcement_aligned =
            (announcement_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Operation counters
        let op_counter_offset = announcement_offset + announcement_aligned;
        let op_counter_size = num_threads * mem::size_of::<AtomicUsize>();
        let op_counter_aligned = (op_counter_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Help indices
        let help_index_offset = op_counter_offset + op_counter_aligned;
        let help_index_size = num_threads * mem::size_of::<AtomicUsize>();
        let help_index_aligned = (help_index_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Value pool
        let value_pool_size = num_threads * ITEMS_PER_THREAD;
        let value_pool_offset = help_index_offset + help_index_aligned;
        let value_pool_bytes = value_pool_size * mem::size_of::<ValueType<usize>>();
        let value_pool_aligned = (value_pool_bytes + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Operation pools
        let op_pool_size = num_threads * OPS_PER_THREAD;
        let enq_op_pool_offset = value_pool_offset + value_pool_aligned;
        let enq_op_pool_bytes = op_pool_size * mem::size_of::<EnqueueOp>();
        let enq_op_pool_aligned =
            (enq_op_pool_bytes + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let deq_op_pool_offset = enq_op_pool_offset + enq_op_pool_aligned;
        let deq_op_pool_bytes = op_pool_size * mem::size_of::<DequeueOp>();
        let deq_op_pool_aligned =
            (deq_op_pool_bytes + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let total_size = deq_op_pool_offset + deq_op_pool_aligned;

        // Initialize buffer with EmptyNodes
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

        // Initialize operation counters
        let op_counter_ptr = mem.add(op_counter_offset) as *mut AtomicUsize;
        for i in 0..num_threads {
            ptr::write(op_counter_ptr.add(i), AtomicUsize::new(0));
        }

        // Initialize help indices - start at different positions
        let help_index_ptr = mem.add(help_index_offset) as *mut AtomicUsize;
        for i in 0..num_threads {
            ptr::write(help_index_ptr.add(i), AtomicUsize::new(i));
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
                operation_counter: op_counter_ptr,
                help_index: help_index_ptr,
                value_pool: mem.add(value_pool_offset) as *mut ValueType<usize>,
                value_pool_size,
                next_value: AtomicUsize::new(0),
                enq_op_pool: mem.add(enq_op_pool_offset) as *mut EnqueueOp,
                deq_op_pool: mem.add(deq_op_pool_offset) as *mut DequeueOp,
                next_enq_op: AtomicUsize::new(0),
                next_deq_op: AtomicUsize::new(0),
                active_enq_ops: AtomicUsize::new(0),
                active_deq_ops: AtomicUsize::new(0),
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

        let capacity = 131072;
        let buffer_size = capacity * mem::size_of::<AtomicU64>();
        let buffer_aligned = (buffer_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let announcement_size = num_threads * mem::size_of::<AtomicPtr<()>>();
        let announcement_aligned =
            (announcement_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let op_counter_size = num_threads * mem::size_of::<AtomicUsize>();
        let op_counter_aligned = (op_counter_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let help_index_size = num_threads * mem::size_of::<AtomicUsize>();
        let help_index_aligned = (help_index_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let value_pool_size = num_threads * ITEMS_PER_THREAD;
        let value_pool_bytes = value_pool_size * mem::size_of::<ValueType<usize>>();
        let value_pool_aligned = (value_pool_bytes + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let op_pool_size = num_threads * OPS_PER_THREAD;
        let enq_op_pool_bytes = op_pool_size * mem::size_of::<EnqueueOp>();
        let enq_op_pool_aligned =
            (enq_op_pool_bytes + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let deq_op_pool_bytes = op_pool_size * mem::size_of::<DequeueOp>();
        let deq_op_pool_aligned =
            (deq_op_pool_bytes + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let total = queue_aligned
            + buffer_aligned
            + announcement_aligned
            + op_counter_aligned
            + help_index_aligned
            + value_pool_aligned
            + enq_op_pool_aligned
            + deq_op_pool_aligned;
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

    // IPC monitoring helpers
    pub fn active_operations(&self) -> (usize, usize) {
        (
            self.active_enq_ops.load(Ordering::Acquire),
            self.active_deq_ops.load(Ordering::Acquire),
        )
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
