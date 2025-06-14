use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::MpmcQueue;

const CACHE_LINE_SIZE: usize = 64;
const FALSE_SHARING_MULTIPLIER: usize = 8; // Padding multiplier to avoid false sharing

// Operation types for requests
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum OpType {
    None = 0,
    Enqueue = 1,
    Dequeue = 2,
}

// Request structure that workers use to communicate with helper
#[repr(C, align(64))]
struct Request<T> {
    op_type: AtomicU8,
    element: UnsafeCell<Option<T>>,
    is_completed: AtomicBool,
    _padding: [u8; CACHE_LINE_SIZE - 17], // Ensure cache line alignment
}

impl<T> Request<T> {
    fn new() -> Self {
        Self {
            op_type: AtomicU8::new(OpType::None as u8),
            element: UnsafeCell::new(None),
            is_completed: AtomicBool::new(false),
            _padding: [0; CACHE_LINE_SIZE - 17],
        }
    }
}

// Node structure for the internal linked list
struct Node<T> {
    value: Option<T>,
    next: AtomicUsize, // Using usize to store pointer as offset in shared memory
}

impl<T> Node<T> {
    fn new(value: Option<T>) -> Self {
        Self {
            value,
            next: AtomicUsize::new(0),
        }
    }
}

// Main queue structure
#[repr(C)]
pub struct WFQueue<T: Send + Clone + 'static> {
    // Queue state
    head: AtomicUsize,
    tail: AtomicUsize,
    size: AtomicUsize,
    num_threads: usize,

    // Helper control
    helper_should_stop: AtomicBool,
    helper_running: AtomicBool,

    // Memory layout offsets
    state_array_offset: usize,
    nodes_offset: usize,
    node_pool_size: usize,
    next_free_node: AtomicUsize,

    // Shared memory info
    base_ptr: *mut u8,
    total_size: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for WFQueue<T> {}
unsafe impl<T: Send + Clone> Sync for WFQueue<T> {}

// AtomicU8 wrapper for OpType
struct AtomicU8(AtomicUsize);

impl AtomicU8 {
    fn new(val: u8) -> Self {
        Self(AtomicUsize::new(val as usize))
    }

    fn load(&self, ordering: Ordering) -> u8 {
        self.0.load(ordering) as u8
    }

    fn store(&self, val: u8, ordering: Ordering) {
        self.0.store(val as usize, ordering);
    }
}

impl<T: Send + Clone + 'static> WFQueue<T> {
    // Get request for a specific thread
    unsafe fn get_request(&self, thread_id: usize) -> &Request<T> {
        let state_array_ptr = self.base_ptr.add(self.state_array_offset) as *const Request<T>;
        // Apply false sharing padding
        let actual_index = thread_id * FALSE_SHARING_MULTIPLIER;
        &*state_array_ptr.add(actual_index)
    }

    // Get node by index
    unsafe fn get_node(&self, index: usize) -> &mut Node<T> {
        if index == 0 {
            panic!("Attempting to access null node");
        }
        let nodes_ptr = self.base_ptr.add(self.nodes_offset) as *mut Node<T>;
        &mut *nodes_ptr.add(index - 1) // -1 because 0 is reserved for null
    }

    // Allocate a new node
    unsafe fn allocate_node(&self, value: Option<T>) -> usize {
        let index = self.next_free_node.fetch_add(1, Ordering::AcqRel);
        if index >= self.node_pool_size {
            panic!(
                "Node pool exhausted: {} >= {} (num_threads: {})",
                index, self.node_pool_size, self.num_threads
            );
        }
        let node = self.get_node(index + 1); // +1 because 0 is null
        node.value = value;
        node.next.store(0, Ordering::Release);
        index + 1
    }

    // Helper thread main loop - MUST be called from a dedicated process/thread
    pub unsafe fn run_helper(&self) {
        let mut current_index = 0;
        self.helper_running.store(true, Ordering::Release);

        while !self.helper_should_stop.load(Ordering::Acquire) {
            let request = self.get_request(current_index);
            let op_type = request.op_type.load(Ordering::Acquire);

            if op_type != OpType::None as u8 && !request.is_completed.load(Ordering::Acquire) {
                match op_type {
                    1 => {
                        // Enqueue
                        let element = (*request.element.get()).clone();
                        if let Some(elem) = element {
                            let new_node_index = self.allocate_node(Some(elem));
                            let tail_index = self.tail.load(Ordering::Acquire);

                            // Append to tail
                            let tail_node = self.get_node(tail_index);
                            tail_node.next.store(new_node_index, Ordering::Release);

                            // Update tail reference
                            self.tail.store(new_node_index, Ordering::Release);
                            self.size.fetch_add(1, Ordering::AcqRel);
                        }
                        request.is_completed.store(true, Ordering::Release);
                    }
                    2 => {
                        // Dequeue
                        let head_index = self.head.load(Ordering::Acquire);
                        let head_node = self.get_node(head_index);
                        let next_index = head_node.next.load(Ordering::Acquire);

                        if next_index != 0 {
                            // Get the actual element
                            let next_node = self.get_node(next_index);
                            let value = next_node.value.take();

                            // Update head
                            self.head.store(next_index, Ordering::Release);

                            *request.element.get() = value;
                            self.size.fetch_sub(1, Ordering::AcqRel);
                        } else {
                            // Queue is empty
                            *request.element.get() = None;
                        }
                        request.is_completed.store(true, Ordering::Release);
                    }
                    _ => {}
                }
            }

            // Move to next thread in round-robin fashion
            current_index = (current_index + 1) % self.num_threads;

            // Small yield to prevent busy spinning
            if current_index == 0 {
                std::hint::spin_loop();
            }
        }

        // Process any remaining requests before exiting
        for _ in 0..self.num_threads {
            for i in 0..self.num_threads {
                let request = self.get_request(i);
                let op_type = request.op_type.load(Ordering::Acquire);

                if op_type != OpType::None as u8 && !request.is_completed.load(Ordering::Acquire) {
                    match op_type {
                        1 => {
                            // Enqueue
                            let element = (*request.element.get()).clone();
                            if let Some(elem) = element {
                                let new_node_index = self.allocate_node(Some(elem));
                                let tail_index = self.tail.load(Ordering::Acquire);
                                let tail_node = self.get_node(tail_index);
                                tail_node.next.store(new_node_index, Ordering::Release);
                                self.tail.store(new_node_index, Ordering::Release);
                                self.size.fetch_add(1, Ordering::AcqRel);
                            }
                            request.is_completed.store(true, Ordering::Release);
                        }
                        2 => {
                            // Dequeue
                            let head_index = self.head.load(Ordering::Acquire);
                            let head_node = self.get_node(head_index);
                            let next_index = head_node.next.load(Ordering::Acquire);

                            if next_index != 0 {
                                let next_node = self.get_node(next_index);
                                let value = next_node.value.take();
                                self.head.store(next_index, Ordering::Release);
                                *request.element.get() = value;
                                self.size.fetch_sub(1, Ordering::AcqRel);
                            } else {
                                *request.element.get() = None;
                            }
                            request.is_completed.store(true, Ordering::Release);
                        }
                        _ => {}
                    }
                }
            }
        }

        self.helper_running.store(false, Ordering::Release);
    }

    // Initialize queue in shared memory
    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // State array with false sharing padding
        let state_array_size =
            num_threads * FALSE_SHARING_MULTIPLIER * mem::size_of::<Request<T>>();
        let state_array_offset = queue_aligned;
        let state_array_aligned = (state_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Node pool
        let items_per_thread = 250_000;
        let node_pool_size = num_threads * items_per_thread * 2;
        let nodes_offset = state_array_offset + state_array_aligned;
        let nodes_size = node_pool_size * mem::size_of::<Node<T>>();

        let total_size = nodes_offset + nodes_size;

        // Initialize queue header
        ptr::write(
            queue_ptr,
            Self {
                head: AtomicUsize::new(0),
                tail: AtomicUsize::new(0),
                size: AtomicUsize::new(0),
                num_threads,
                helper_should_stop: AtomicBool::new(false),
                helper_running: AtomicBool::new(false),
                state_array_offset,
                nodes_offset,
                node_pool_size,
                next_free_node: AtomicUsize::new(0),
                base_ptr: mem,
                total_size,
                _phantom: std::marker::PhantomData,
            },
        );

        let queue = &mut *queue_ptr;

        // Initialize state array
        let state_array_ptr = mem.add(state_array_offset) as *mut Request<T>;
        for i in 0..num_threads * FALSE_SHARING_MULTIPLIER {
            ptr::write(state_array_ptr.add(i), Request::new());
        }

        // Initialize node pool
        let nodes_ptr = mem.add(nodes_offset) as *mut Node<T>;
        for i in 0..node_pool_size {
            ptr::write(nodes_ptr.add(i), Node::new(None));
        }

        // Initialize sentinel node
        let sentinel_index = queue.allocate_node(None);
        queue.head.store(sentinel_index, Ordering::Release);
        queue.tail.store(sentinel_index, Ordering::Release);

        queue
    }

    // Calculate required shared memory size
    pub fn shared_size(num_threads: usize) -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let state_array_size =
            num_threads * FALSE_SHARING_MULTIPLIER * mem::size_of::<Request<T>>();
        let state_array_aligned = (state_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_per_thread = 250_000;
        let node_pool_size = num_threads * items_per_thread * 2;
        let nodes_size = node_pool_size * mem::size_of::<Node<T>>();

        let total = queue_aligned + state_array_aligned + nodes_size;
        (total + 4095) & !4095 // Page align
    }

    // Stop helper thread
    pub fn stop_helper(&self) {
        self.helper_should_stop.store(true, Ordering::Release);
        while self.helper_running.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
    }

    // Enqueue operation (following paper's Listing 3.2)
    pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), ()> {
        unsafe {
            if thread_id >= self.num_threads {
                return Err(());
            }

            let request = self.get_request(thread_id);

            // Create request with element e and ENQUEUE type (lines 4-5 in paper)
            *request.element.get() = Some(item);
            request.is_completed.store(false, Ordering::Release);

            // Post this request in state array (line 7 in paper)
            request
                .op_type
                .store(OpType::Enqueue as u8, Ordering::Release);

            // Wait until the operation is completed (line 9 in paper)
            while !request.is_completed.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            // Clear the request from state array (line 11 in paper - stateArr[id] = null)
            request.op_type.store(OpType::None as u8, Ordering::Release);
            unsafe {
                *request.element.get() = None;
            }

            Ok(())
        }
    }

    // Dequeue operation (following paper's Listing 3.3)
    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            if thread_id >= self.num_threads {
                return Err(());
            }

            let request = self.get_request(thread_id);

            // Create request with empty value field and DEQUEUE type (lines 4-5 in paper)
            *request.element.get() = None;
            request.is_completed.store(false, Ordering::Release);

            // Put the request in state array (line 7 in paper)
            request
                .op_type
                .store(OpType::Dequeue as u8, Ordering::Release);

            // Wait until the operation completes (line 9 in paper)
            while !request.is_completed.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            // Get result before clearing (line 13 in paper - return req.e)
            let result = (*request.element.get()).take();

            // Clear the request from state array (line 11 in paper)
            request.op_type.store(OpType::None as u8, Ordering::Release);

            result.ok_or(())
        }
    }

    pub fn is_empty(&self) -> bool {
        self.size.load(Ordering::Acquire) == 0
    }

    pub fn is_full(&self) -> bool {
        self.next_free_node.load(Ordering::Acquire) >= self.node_pool_size
    }
}

impl<T: Send + Clone + 'static> MpmcQueue<T> for WFQueue<T> {
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

impl<T: Send + Clone> Drop for WFQueue<T> {
    fn drop(&mut self) {
        // In shared memory context, helper should be stopped externally
    }
}
