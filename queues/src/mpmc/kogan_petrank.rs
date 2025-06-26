use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicUsize, Ordering};

use crate::MpmcQueue;

const CACHE_LINE_SIZE: usize = 64;
const MAX_THREADS: usize = 64;

// Node structure as in the paper
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

// Operation descriptor as in the paper
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

// Manually implement Copy and Clone for OpDesc
impl<T> Copy for OpDesc<T> {}

impl<T> Clone for OpDesc<T> {
    fn clone(&self) -> Self {
        *self
    }
}

// Atomic wrapper for OpDesc to ensure proper memory operations
#[repr(C, align(64))]
struct AtomicOpDesc<T> {
    // We'll use a 256-bit atomic operation simulated with a mutex-like approach
    // In practice, this would use a 128-bit CAS if available
    data: AtomicUsize, // Points to OpDesc
    version: AtomicUsize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> AtomicOpDesc<T> {
    fn new() -> Self {
        Self {
            data: AtomicUsize::new(0),
            version: AtomicUsize::new(0),
            _phantom: std::marker::PhantomData,
        }
    }

    unsafe fn load(&self) -> OpDesc<T> {
        let ptr = self.data.load(Ordering::Acquire) as *const OpDesc<T>;
        if ptr.is_null() {
            OpDesc::new(-1, false, true, ptr::null_mut())
        } else {
            *ptr
        }
    }

    unsafe fn store(&self, desc: OpDesc<T>, desc_pool: &DescPool<T>) -> *mut OpDesc<T> {
        let desc_ptr = desc_pool.allocate(desc);
        self.version.fetch_add(1, Ordering::AcqRel);
        self.data.store(desc_ptr as usize, Ordering::Release);
        desc_ptr
    }

    unsafe fn compare_exchange(
        &self,
        expected: &OpDesc<T>,
        new_desc: OpDesc<T>,
        desc_pool: &DescPool<T>,
    ) -> Result<(), OpDesc<T>> {
        let current_ptr = self.data.load(Ordering::Acquire) as *mut OpDesc<T>;

        if current_ptr.is_null() && expected.phase == -1 {
            let new_ptr = desc_pool.allocate(new_desc);
            match self.data.compare_exchange(
                0,
                new_ptr as usize,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.version.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }
                Err(_) => Err(self.load()),
            }
        } else if !current_ptr.is_null() {
            let current = &*current_ptr;
            if current.phase == expected.phase
                && current.pending == expected.pending
                && current.enqueue == expected.enqueue
                && current.node == expected.node
            {
                let new_ptr = desc_pool.allocate(new_desc);
                match self.data.compare_exchange(
                    current_ptr as usize,
                    new_ptr as usize,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.version.fetch_add(1, Ordering::AcqRel);
                        Ok(())
                    }
                    Err(_) => Err(self.load()),
                }
            } else {
                Err(*current)
            }
        } else {
            Err(self.load())
        }
    }
}

// Pool for OpDesc allocations
struct DescPool<T> {
    pool: *mut OpDesc<T>,
    next_desc: AtomicUsize,
    pool_size: usize,
}

impl<T> DescPool<T> {
    unsafe fn new(base_ptr: *mut u8, offset: usize, size: usize) -> Self {
        Self {
            pool: base_ptr.add(offset) as *mut OpDesc<T>,
            next_desc: AtomicUsize::new(0),
            pool_size: size,
        }
    }

    unsafe fn allocate(&self, desc: OpDesc<T>) -> *mut OpDesc<T> {
        let idx = self.next_desc.fetch_add(1, Ordering::AcqRel) % self.pool_size;
        let ptr = self.pool.add(idx);
        ptr::write(ptr, desc);
        ptr
    }
}

// Main queue structure
#[repr(C)]
pub struct KPQueue<T: Send + Clone + 'static> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    state: *mut AtomicOpDesc<T>,
    num_threads: usize,

    // Memory management
    node_pool: *mut Node<T>,
    node_pool_size: usize,
    next_node: AtomicUsize,

    desc_pools: *mut DescPool<T>,

    base_ptr: *mut u8,
    total_size: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for KPQueue<T> {}
unsafe impl<T: Send + Clone> Sync for KPQueue<T> {}

impl<T: Send + Clone + 'static> KPQueue<T> {
    // Get state array entry for thread
    unsafe fn get_state(&self, tid: usize) -> &AtomicOpDesc<T> {
        &*self.state.add(tid)
    }

    // Get descriptor pool for thread
    unsafe fn get_desc_pool(&self, tid: usize) -> &DescPool<T> {
        &*self.desc_pools.add(tid)
    }

    // Allocate a node from the pool
    unsafe fn allocate_node(&self, value: T, enq_tid: i32) -> *mut Node<T> {
        let idx = self.next_node.fetch_add(1, Ordering::AcqRel);
        if idx >= self.node_pool_size {
            panic!("Node pool exhausted");
        }

        let node = self.node_pool.add(idx);
        ptr::write(node, Node::new(value, enq_tid));
        node
    }

    // Get maximum phase from state array
    unsafe fn max_phase(&self) -> i64 {
        let mut max = -1i64;
        for i in 0..self.num_threads {
            let desc: OpDesc<T> = self.get_state(i).load();
            if desc.phase > max {
                max = desc.phase;
            }
        }
        max
    }

    // Check if operation is still pending
    unsafe fn is_still_pending(&self, tid: usize, phase: i64) -> bool {
        let desc: OpDesc<T> = self.get_state(tid).load();
        desc.pending && desc.phase <= phase
    }

    // Help method
    unsafe fn help(&self, phase: i64) {
        for i in 0..self.num_threads {
            let desc: OpDesc<T> = self.get_state(i).load();
            if desc.pending && desc.phase <= phase {
                if desc.enqueue {
                    self.help_enq(i, desc.phase);
                } else {
                    self.help_deq(i, desc.phase);
                }
            }
        }
    }

    // Help enqueue operation
    unsafe fn help_enq(&self, tid: usize, phase: i64) {
        while self.is_still_pending(tid, phase) {
            let last = self.tail.load(Ordering::Acquire);
            let next = (*last).next.load(Ordering::Acquire);

            if last == self.tail.load(Ordering::Acquire) {
                if next.is_null() {
                    if self.is_still_pending(tid, phase) {
                        let node = self.get_state(tid).load().node;
                        if !node.is_null() {
                            if (*last)
                                .next
                                .compare_exchange(
                                    ptr::null_mut(),
                                    node,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_ok()
                            {
                                self.help_finish_enq();
                                return;
                            }
                        }
                    }
                } else {
                    self.help_finish_enq();
                }
            }
        }
    }

    // Help finish enqueue
    unsafe fn help_finish_enq(&self) {
        let last = self.tail.load(Ordering::Acquire);
        let next = (*last).next.load(Ordering::Acquire);

        if !next.is_null() {
            let tid = (*next).enq_tid;
            if tid != -1 {
                let tid = tid as usize;
                let cur_desc: OpDesc<T> = self.get_state(tid).load();
                if last == self.tail.load(Ordering::Acquire)
                    && !cur_desc.node.is_null()
                    && cur_desc.node == next
                {
                    let new_desc = OpDesc::new(cur_desc.phase, false, true, next);
                    let _ = self.get_state(tid).compare_exchange(
                        &cur_desc,
                        new_desc,
                        self.get_desc_pool(tid),
                    );
                    self.tail
                        .compare_exchange(last, next, Ordering::AcqRel, Ordering::Acquire)
                        .ok();
                }
            } else {
                self.tail
                    .compare_exchange(last, next, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
            }
        }
    }

    // Help dequeue operation
    unsafe fn help_deq(&self, tid: usize, phase: i64) {
        while self.is_still_pending(tid, phase) {
            let first = self.head.load(Ordering::Acquire);
            let last = self.tail.load(Ordering::Acquire);
            let next = (*first).next.load(Ordering::Acquire);

            if first == self.head.load(Ordering::Acquire) {
                if first == last {
                    if next.is_null() {
                        let cur_desc: OpDesc<T> = self.get_state(tid).load();
                        if last == self.tail.load(Ordering::Acquire)
                            && self.is_still_pending(tid, phase)
                        {
                            let new_desc =
                                OpDesc::new(cur_desc.phase, false, false, ptr::null_mut());
                            let _ = self.get_state(tid).compare_exchange(
                                &cur_desc,
                                new_desc,
                                self.get_desc_pool(tid),
                            );
                        }
                    } else {
                        self.help_finish_enq();
                    }
                } else {
                    let cur_desc: OpDesc<T> = self.get_state(tid).load();
                    let node = cur_desc.node;

                    if !self.is_still_pending(tid, phase) {
                        break;
                    }

                    if first == self.head.load(Ordering::Acquire) && node != first {
                        let new_desc = OpDesc::new(cur_desc.phase, true, false, first);
                        if self
                            .get_state(tid)
                            .compare_exchange(&cur_desc, new_desc, self.get_desc_pool(tid))
                            .is_err()
                        {
                            continue;
                        }
                    }

                    (*first)
                        .deq_tid
                        .compare_exchange(-1, tid as i32, Ordering::AcqRel, Ordering::Acquire)
                        .ok();
                    self.help_finish_deq();
                }
            }
        }
    }

    // Help finish dequeue
    unsafe fn help_finish_deq(&self) {
        let first = self.head.load(Ordering::Acquire);
        let next = (*first).next.load(Ordering::Acquire);
        let tid = (*first).deq_tid.load(Ordering::Acquire);

        if tid != -1 {
            let tid = tid as usize;
            let cur_desc: OpDesc<T> = self.get_state(tid).load();
            if first == self.head.load(Ordering::Acquire) && !next.is_null() {
                let new_desc = OpDesc::new(cur_desc.phase, false, false, cur_desc.node);
                let _ = self.get_state(tid).compare_exchange(
                    &cur_desc,
                    new_desc,
                    self.get_desc_pool(tid),
                );
                self.head
                    .compare_exchange(first, next, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
            }
        }
    }

    // Enqueue operation
    pub fn enqueue(&self, thread_id: usize, value: T) -> Result<(), ()> {
        unsafe {
            let phase = self.max_phase() + 1;
            let node = self.allocate_node(value, thread_id as i32);

            let new_desc = OpDesc::new(phase, true, true, node);
            self.get_state(thread_id)
                .store(new_desc, self.get_desc_pool(thread_id));

            self.help(phase);
            self.help_finish_enq();

            Ok(())
        }
    }

    // Dequeue operation
    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            let phase = self.max_phase() + 1;

            let new_desc = OpDesc::new(phase, true, false, ptr::null_mut());
            self.get_state(thread_id)
                .store(new_desc, self.get_desc_pool(thread_id));

            self.help(phase);
            self.help_finish_deq();

            let node = self.get_state(thread_id).load().node;
            if node.is_null() {
                Err(())
            } else {
                let next = (*node).next.load(Ordering::Acquire);
                if next.is_null() {
                    Err(())
                } else {
                    Ok((*next).value.take().unwrap())
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
        let state_size = num_threads * mem::size_of::<AtomicOpDesc<T>>();
        let state_aligned = (state_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Descriptor pools
        let desc_pools_offset = state_offset + state_aligned;
        let desc_pool_size_per_thread = 10000;
        let desc_pools_size = num_threads * mem::size_of::<DescPool<T>>();
        let desc_pools_aligned = (desc_pools_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Actual descriptor storage
        let desc_storage_offset = desc_pools_offset + desc_pools_aligned;
        let desc_storage_size =
            num_threads * desc_pool_size_per_thread * mem::size_of::<OpDesc<T>>();
        let desc_storage_aligned =
            (desc_storage_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Node pool
        let node_pool_offset = desc_storage_offset + desc_storage_aligned;
        let items_per_thread = 250_000;
        let node_pool_size = num_threads * items_per_thread * 2;
        let node_pool_total_size = node_pool_size * mem::size_of::<Node<T>>();

        let total_size = node_pool_offset + node_pool_total_size;

        // Initialize state array
        let state_ptr = mem.add(state_offset) as *mut AtomicOpDesc<T>;
        for i in 0..num_threads {
            ptr::write(state_ptr.add(i), AtomicOpDesc::<T>::new());
        }

        // Initialize descriptor pools
        let desc_pools_ptr = mem.add(desc_pools_offset) as *mut DescPool<T>;
        for i in 0..num_threads {
            let pool_offset =
                desc_storage_offset + i * desc_pool_size_per_thread * mem::size_of::<OpDesc<T>>();
            ptr::write(
                desc_pools_ptr.add(i),
                DescPool::new(mem, pool_offset, desc_pool_size_per_thread),
            );
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
                next_node: AtomicUsize::new(1), // Skip sentinel
                desc_pools: desc_pools_ptr,
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

        let state_size = num_threads * mem::size_of::<AtomicOpDesc<T>>();
        let state_aligned = (state_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let desc_pools_size = num_threads * mem::size_of::<DescPool<T>>();
        let desc_pools_aligned = (desc_pools_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let desc_pool_size_per_thread = 10000;
        let desc_storage_size =
            num_threads * desc_pool_size_per_thread * mem::size_of::<OpDesc<T>>();
        let desc_storage_aligned =
            (desc_storage_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_per_thread = 250_000;
        let node_pool_size = num_threads * items_per_thread * 2;
        let node_pool_total_size = node_pool_size * mem::size_of::<Node<T>>();

        let total = queue_aligned
            + state_aligned
            + desc_pools_aligned
            + desc_storage_aligned
            + node_pool_total_size;
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
        self.next_node.load(Ordering::Acquire) >= self.node_pool_size
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
