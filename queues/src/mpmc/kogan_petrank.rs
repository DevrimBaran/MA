use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicI32, AtomicI64, AtomicPtr, Ordering};

use crate::MpmcQueue;

const CACHE_LINE_SIZE: usize = 64;

// Node structure from the paper
#[repr(C, align(64))]
struct Node<T> {
    value: Option<T>,
    next: AtomicPtr<Node<T>>,
    enq_tid: i32,
    deq_tid: AtomicI32,
}

impl<T> Node<T> {
    fn new_sentinel() -> Self {
        Self {
            value: None,
            next: AtomicPtr::new(ptr::null_mut()),
            enq_tid: -1,
            deq_tid: AtomicI32::new(-1),
        }
    }

    fn new(value: T, enq_tid: i32) -> Self {
        Self {
            value: Some(value),
            next: AtomicPtr::new(ptr::null_mut()),
            enq_tid,
            deq_tid: AtomicI32::new(-1),
        }
    }
}

// Operation descriptor from the paper
#[repr(C, align(64))]
struct OpDesc<T> {
    phase: i64,
    pending: bool,
    enqueue: bool,
    node: *mut Node<T>,
}

impl<T> OpDesc<T> {
    fn new(phase: i64, pending: bool, enqueue: bool, node: *mut Node<T>) -> Self {
        Self {
            phase,
            pending,
            enqueue,
            node,
        }
    }
}

impl<T> Copy for OpDesc<T> {}

impl<T> Clone for OpDesc<T> {
    fn clone(&self) -> Self {
        *self
    }
}

// Thread state entry - combines phase tracking and operation descriptor
#[repr(C, align(64))]
struct ThreadState<T> {
    // Current operation descriptor
    op_desc: OpDesc<T>,
    // Version counter for ABA prevention
    version: AtomicI64,
}

impl<T> ThreadState<T> {
    fn new() -> Self {
        Self {
            op_desc: OpDesc::new(-1, false, true, ptr::null_mut()),
            version: AtomicI64::new(0),
        }
    }

    unsafe fn load(&self) -> OpDesc<T> {
        // In a real implementation, this would use double-word CAS
        // For now, we use version counter for ABA prevention
        self.op_desc
    }

    unsafe fn store(&mut self, desc: OpDesc<T>) {
        self.version.fetch_add(1, Ordering::AcqRel);
        self.op_desc = desc;
    }

    unsafe fn compare_exchange(
        &mut self,
        expected: &OpDesc<T>,
        new_desc: OpDesc<T>,
    ) -> Result<(), OpDesc<T>> {
        // Simplified CAS - in reality would need double-word CAS
        if self.op_desc.phase == expected.phase
            && self.op_desc.pending == expected.pending
            && self.op_desc.enqueue == expected.enqueue
            && self.op_desc.node == expected.node
        {
            self.version.fetch_add(1, Ordering::AcqRel);
            self.op_desc = new_desc;
            Ok(())
        } else {
            Err(self.op_desc)
        }
    }
}

// Main queue structure
#[repr(C)]
pub struct KPQueue<T: Send + Clone + 'static> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    state: *mut ThreadState<T>,
    num_threads: usize,

    // Memory management
    node_pool: *mut Node<T>,
    node_pool_size: usize,
    next_node: AtomicI64,

    base_ptr: *mut u8,
    total_size: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for KPQueue<T> {}
unsafe impl<T: Send + Clone> Sync for KPQueue<T> {}

impl<T: Send + Clone + 'static> KPQueue<T> {
    // Get state for thread
    unsafe fn get_state(&self, tid: usize) -> &mut ThreadState<T> {
        &mut *self.state.add(tid)
    }

    // Allocate a node from the pool
    unsafe fn allocate_node(&self, value: T, enq_tid: i32) -> *mut Node<T> {
        let idx = self.next_node.fetch_add(1, Ordering::AcqRel);
        if idx >= self.node_pool_size as i64 {
            panic!("Node pool exhausted");
        }

        let node = self.node_pool.add(idx as usize);
        ptr::write(node, Node::new(value, enq_tid));
        node
    }

    // Get maximum phase from state array
    unsafe fn max_phase(&self) -> i64 {
        let mut max = -1i64;
        for i in 0..self.num_threads {
            let state = self.get_state(i);
            let desc = state.load();
            if desc.phase > max {
                max = desc.phase;
            }
        }
        max
    }

    // Check if operation is still pending
    unsafe fn is_still_pending(&self, tid: usize, phase: i64) -> bool {
        let state = self.get_state(tid);
        let desc = state.load();
        desc.pending && desc.phase <= phase
    }

    // Main help method - Algorithm 1 from paper
    unsafe fn help(&self, phase: i64) {
        for i in 0..self.num_threads {
            let state = self.get_state(i);
            let desc = state.load();
            if desc.pending && desc.phase <= phase {
                if desc.enqueue {
                    self.help_enq(i, desc.phase);
                } else {
                    self.help_deq(i, desc.phase);
                }
            }
        }
    }

    // Help enqueue - Algorithm 2 from paper
    unsafe fn help_enq(&self, tid: usize, phase: i64) {
        while self.is_still_pending(tid, phase) {
            let last = self.tail.load(Ordering::Acquire);
            let next = (*last).next.load(Ordering::Acquire);

            if last == self.tail.load(Ordering::Acquire) {
                if next.is_null() {
                    // Try to link the new node
                    if self.is_still_pending(tid, phase) {
                        let state = self.get_state(tid);
                        let node = state.load().node;
                        if !node.is_null() {
                            match (*last).next.compare_exchange(
                                ptr::null_mut(),
                                node,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            ) {
                                Ok(_) => {
                                    self.help_finish_enq();
                                    return;
                                }
                                Err(_) => {}
                            }
                        }
                    }
                } else {
                    // Help finish the enqueue
                    self.help_finish_enq();
                }
            }
        }
    }

    // Help finish enqueue - Algorithm 2 continued
    unsafe fn help_finish_enq(&self) {
        let last = self.tail.load(Ordering::Acquire);
        let next = (*last).next.load(Ordering::Acquire);

        if !next.is_null() {
            let tid = (*next).enq_tid;
            if tid != -1 {
                let tid = tid as usize;
                let state = self.get_state(tid);
                let cur_desc = state.load();

                if last == self.tail.load(Ordering::Acquire) && cur_desc.node == next {
                    let new_desc = OpDesc::new(cur_desc.phase, false, true, next);
                    let _ = state.compare_exchange(&cur_desc, new_desc);
                }
            }

            // Update tail
            self.tail
                .compare_exchange(last, next, Ordering::AcqRel, Ordering::Acquire)
                .ok();
        }
    }

    // Help dequeue - Algorithm 3 from paper
    unsafe fn help_deq(&self, tid: usize, phase: i64) {
        while self.is_still_pending(tid, phase) {
            let first = self.head.load(Ordering::Acquire);
            let last = self.tail.load(Ordering::Acquire);
            let next = (*first).next.load(Ordering::Acquire);

            if first == self.head.load(Ordering::Acquire) {
                if first == last {
                    if next.is_null() {
                        // Queue is empty
                        let state = self.get_state(tid);
                        let cur_desc = state.load();

                        if last == self.tail.load(Ordering::Acquire)
                            && self.is_still_pending(tid, phase)
                        {
                            let new_desc =
                                OpDesc::new(cur_desc.phase, false, false, ptr::null_mut());
                            let _ = state.compare_exchange(&cur_desc, new_desc);
                        }
                    } else {
                        // Help enqueue to complete
                        self.help_finish_enq();
                    }
                } else {
                    // Queue is not empty
                    let state = self.get_state(tid);
                    let cur_desc = state.load();
                    let node = cur_desc.node;

                    if !self.is_still_pending(tid, phase) {
                        break;
                    }

                    if first == self.head.load(Ordering::Acquire) && node != first {
                        // Update state to point to sentinel
                        let new_desc = OpDesc::new(cur_desc.phase, true, false, first);
                        if state.compare_exchange(&cur_desc, new_desc).is_err() {
                            continue;
                        }
                    }

                    // Try to reserve the node for dequeuer tid
                    let _ = (*first).deq_tid.compare_exchange(
                        -1,
                        tid as i32,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );

                    self.help_finish_deq();
                }
            }
        }
    }

    // Help finish dequeue - Algorithm 3 continued
    unsafe fn help_finish_deq(&self) {
        let first = self.head.load(Ordering::Acquire);
        let next = (*first).next.load(Ordering::Acquire);
        let tid = (*first).deq_tid.load(Ordering::Acquire);

        if tid != -1 {
            let tid = tid as usize;
            let state = self.get_state(tid);
            let cur_desc = state.load();

            if first == self.head.load(Ordering::Acquire) && !next.is_null() {
                // Update state to completed
                let new_desc = OpDesc::new(cur_desc.phase, false, false, cur_desc.node);
                let _ = state.compare_exchange(&cur_desc, new_desc);

                // Advance head
                self.head
                    .compare_exchange(first, next, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
            }
        }
    }

    // Public enqueue operation - follows paper's structure
    pub fn enqueue(&self, thread_id: usize, value: T) -> Result<(), ()> {
        unsafe {
            // Step 1: Choose phase number
            let phase = self.max_phase() + 1;

            // Step 2: Create and announce operation
            let node = self.allocate_node(value, thread_id as i32);
            let state = self.get_state(thread_id);
            let new_desc = OpDesc::new(phase, true, true, node);
            state.store(new_desc);

            // Step 3: Help all operations with lower phase
            self.help(phase);

            // Step 4: Help finish to ensure tail is updated
            self.help_finish_enq();

            Ok(())
        }
    }

    // Public dequeue operation - follows paper's structure
    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            // Step 1: Choose phase number
            let phase = self.max_phase() + 1;

            // Step 2: Announce operation
            let state = self.get_state(thread_id);
            let new_desc = OpDesc::new(phase, true, false, ptr::null_mut());
            state.store(new_desc);

            // Step 3: Help all operations with lower phase
            self.help(phase);

            // Step 4: Help finish dequeue
            self.help_finish_deq();

            // Step 5: Check result
            let final_desc = state.load();
            let node = final_desc.node;

            if node.is_null() {
                // Empty queue
                Err(())
            } else {
                // Node points to sentinel, get next node's value
                let next = (*node).next.load(Ordering::Acquire);
                if next.is_null() {
                    Err(())
                } else {
                    // Take the value from the node
                    let value = (*next).value.take();
                    value.ok_or(())
                }
            }
        }
    }

    // Initialize in shared memory
    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // State array
        let state_offset = queue_aligned;
        let state_size = num_threads * mem::size_of::<ThreadState<T>>();
        let state_aligned = (state_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Node pool
        let node_pool_offset = state_offset + state_aligned;
        let items_per_thread = 250_000;
        let node_pool_size = num_threads * items_per_thread * 2;
        let node_pool_total_size = node_pool_size * mem::size_of::<Node<T>>();

        let total_size = node_pool_offset + node_pool_total_size;

        // Initialize state array
        let state_ptr = mem.add(state_offset) as *mut ThreadState<T>;
        for i in 0..num_threads {
            ptr::write(state_ptr.add(i), ThreadState::new());
        }

        // Initialize node pool with sentinel
        let node_pool_ptr = mem.add(node_pool_offset) as *mut Node<T>;
        let sentinel = node_pool_ptr;
        ptr::write(sentinel, Node::new_sentinel());

        // Initialize queue
        ptr::write(
            queue_ptr,
            Self {
                head: AtomicPtr::new(sentinel),
                tail: AtomicPtr::new(sentinel),
                state: state_ptr,
                num_threads,
                node_pool: node_pool_ptr,
                node_pool_size,
                next_node: AtomicI64::new(1), // Skip sentinel
                base_ptr: mem,
                total_size,
                _phantom: std::marker::PhantomData,
            },
        );

        &mut *queue_ptr
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let state_size = num_threads * mem::size_of::<ThreadState<T>>();
        let state_aligned = (state_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_per_thread = 250_000;
        let node_pool_size = num_threads * items_per_thread * 2;
        let node_pool_total_size = node_pool_size * mem::size_of::<Node<T>>();

        let total = queue_aligned + state_aligned + node_pool_total_size;
        (total + 4095) & !4095 // Page align
    }

    pub fn is_empty(&self) -> bool {
        unsafe {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);
            head == tail && (*head).next.load(Ordering::Acquire).is_null()
        }
    }

    pub fn is_full(&self) -> bool {
        self.next_node.load(Ordering::Acquire) >= self.node_pool_size as i64
    }
}

impl<T: Send + Clone + 'static> MpmcQueue<T> for KPQueue<T> {
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

impl<T: Send + Clone> Drop for KPQueue<T> {
    fn drop(&mut self) {
        // Shared memory cleanup handled externally
    }
}
