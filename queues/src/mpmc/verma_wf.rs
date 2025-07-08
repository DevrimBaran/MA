// paper in /paper/mpmc/TR-IID-Thesis_Mudit_Verma.pdf
use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::MpmcQueue;

const CACHE_LINE_SIZE: usize = 64;
const FALSE_SHARING_MULTIPLIER: usize = 8; // Paper section 3.3: padding by 8x to avoid false sharing

// Paper section 3.1.3: Operation types for requests
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum OpType {
    None = 0,
    Enqueue = 1,
    Dequeue = 2,
}

// Paper section 3.1.3: Request structure from state array
#[repr(C, align(64))]
pub struct Request<T> {
    pub op_type: AtomicU8,
    pub element: UnsafeCell<Option<T>>,
    pub is_completed: AtomicBool,
    _padding: [u8; CACHE_LINE_SIZE - 17], // IPC: explicit cache line padding
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

// Paper section 3.1.3: Node structure for internal linked list
struct Node<T> {
    value: Option<T>,
    next: AtomicUsize, // IPC: pointer as offset instead of raw pointer
}

impl<T> Node<T> {
    fn new(value: Option<T>) -> Self {
        Self {
            value,
            next: AtomicUsize::new(0),
        }
    }
}

// Paper section 3.1.3: Main queue structure (Listing 3.1)
#[repr(C)]
pub struct WFQueue<T: Send + Clone + 'static> {
    // Paper: Queue state from Listing 3.1
    head: AtomicUsize, // IPC: as offset
    tail: AtomicUsize, // IPC: as offset
    size: AtomicUsize,
    pub num_threads: usize, // Paper: workers

    // IPC: Helper control flags
    helper_should_stop: AtomicBool,
    helper_running: AtomicBool,

    // IPC: Memory layout management
    state_array_offset: usize,
    nodes_offset: usize,
    node_pool_size: usize,
    next_free_node: AtomicUsize,

    // IPC: Shared memory tracking
    pub base_ptr: *mut u8,
    total_size: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for WFQueue<T> {}
unsafe impl<T: Send + Clone> Sync for WFQueue<T> {}

// IPC: AtomicU8 wrapper since Rust doesn't have native AtomicU8
pub struct AtomicU8(AtomicUsize);

impl AtomicU8 {
    fn new(val: u8) -> Self {
        Self(AtomicUsize::new(val as usize))
    }

    pub fn load(&self, ordering: Ordering) -> u8 {
        self.0.load(ordering) as u8
    }

    fn store(&self, val: u8, ordering: Ordering) {
        self.0.store(val as usize, ordering);
    }
}

impl<T: Send + Clone + 'static> WFQueue<T> {
    // IPC: Get request from state array using offset calculation
    pub unsafe fn get_request(&self, thread_id: usize) -> &Request<T> {
        let state_array_ptr = self.base_ptr.add(self.state_array_offset) as *const Request<T>;
        let actual_index = thread_id * FALSE_SHARING_MULTIPLIER; // Paper: false sharing padding
        &*state_array_ptr.add(actual_index)
    }

    // IPC: Get node by index (0 is null in paper)
    unsafe fn get_node(&self, index: usize) -> &mut Node<T> {
        if index == 0 {
            panic!("Attempting to access null node");
        }
        let nodes_ptr = self.base_ptr.add(self.nodes_offset) as *mut Node<T>;
        &mut *nodes_ptr.add(index - 1) // -1 because 0 is reserved for null
    }

    // IPC: Pre-allocated node pool instead of dynamic allocation
    unsafe fn allocate_node(&self, value: Option<T>) -> usize {
        let index = self.next_free_node.fetch_add(1, Ordering::AcqRel);
        let node = self.get_node(index + 1); // +1 because 0 is null
        node.value = value;
        node.next.store(0, Ordering::Release);
        index + 1
    }

    // Paper Listing 3.5: Helper algorithm
    pub unsafe fn run_helper(&self) {
        let mut current_index = 0; // Paper line 2: id = 0
        self.helper_running.store(true, Ordering::Release);

        // Paper line 4: infinite loop
        while !self.helper_should_stop.load(Ordering::Acquire) {
            // Paper line 6: read request from state array
            let request = self.get_request(current_index);
            let op_type = request.op_type.load(Ordering::Acquire);

            // Paper line 8-9: if there is a request
            if op_type != OpType::None as u8 && !request.is_completed.load(Ordering::Acquire) {
                match op_type {
                    1 => {
                        // Paper lines 11-22: Enqueue operation
                        let element = (*request.element.get()).clone();
                        if let Some(elem) = element {
                            // Paper line 13: create new node with value e
                            let new_node_index = self.allocate_node(Some(elem));
                            let tail_index = self.tail.load(Ordering::Acquire);

                            // Paper line 15: append node to tail
                            let tail_node = self.get_node(tail_index);
                            tail_node.next.store(new_node_index, Ordering::Release);

                            // Paper line 17: update tail reference
                            self.tail.store(new_node_index, Ordering::Release);
                            // Paper line 19: increase size of queue
                            self.size.fetch_add(1, Ordering::AcqRel);
                        }
                        // Paper line 21: mark request as completed
                        request.is_completed.store(true, Ordering::Release);
                    }
                    2 => {
                        // Paper lines 25-51: Dequeue operation
                        let head_index = self.head.load(Ordering::Acquire);
                        let head_node = self.get_node(head_index);
                        let next_index = head_node.next.load(Ordering::Acquire);

                        if next_index != 0 {
                            // Paper line 35-50: queue has elements
                            let next_node = self.get_node(next_index);
                            let value = next_node.value.take();

                            // Paper line 38: unlink top element
                            self.head.store(next_index, Ordering::Release);

                            // Paper line 45: update request with element
                            *request.element.get() = value;
                            // Paper line 47: decrease size
                            self.size.fetch_sub(1, Ordering::AcqRel);
                        } else {
                            // Paper line 27-32: queue is empty
                            *request.element.get() = None;
                        }
                        // Paper line 49: mark request as completed
                        request.is_completed.store(true, Ordering::Release);
                    }
                    _ => {}
                }
            }

            // Paper line 71: increment index id++%worker
            current_index = (current_index + 1) % self.num_threads;

            // small delay to avoid busy waiting
            if current_index == 0 {
                std::hint::spin_loop();
            }
        }

        // IPC: Clean shutdown - process remaining requests
        for _ in 0..self.num_threads {
            for i in 0..self.num_threads {
                let request = self.get_request(i);
                let op_type = request.op_type.load(Ordering::Acquire);

                if op_type != OpType::None as u8 && !request.is_completed.load(Ordering::Acquire) {
                    match op_type {
                        1 => {
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

    // IPC: Initialize queue in shared memory (adapted from Listing 3.1)
    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // IPC: Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // IPC: State array with false sharing padding
        let state_array_size =
            num_threads * FALSE_SHARING_MULTIPLIER * mem::size_of::<Request<T>>();
        let state_array_offset = queue_aligned;
        let state_array_aligned = (state_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // IPC: Pre-allocated node pool
        let items_per_thread = 150_000;
        let node_pool_size = num_threads * items_per_thread * 2;
        let nodes_offset = state_array_offset + state_array_aligned;
        let nodes_size = node_pool_size * mem::size_of::<Node<T>>();

        let total_size = nodes_offset + nodes_size;

        // Paper Listing 3.1: Initialize queue
        ptr::write(
            queue_ptr,
            Self {
                head: AtomicUsize::new(0),
                tail: AtomicUsize::new(0),
                size: AtomicUsize::new(0),
                num_threads, // Paper: workers = n
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

        // Paper: Initialize state array
        let state_array_ptr = mem.add(state_array_offset) as *mut Request<T>;
        for i in 0..num_threads * FALSE_SHARING_MULTIPLIER {
            ptr::write(state_array_ptr.add(i), Request::new());
        }

        // IPC: Initialize node pool
        let nodes_ptr = mem.add(nodes_offset) as *mut Node<T>;
        for i in 0..node_pool_size {
            ptr::write(nodes_ptr.add(i), Node::new(None));
        }

        // Paper Listing 3.1: Create sentinel node
        let sentinel_index = queue.allocate_node(None);
        queue.head.store(sentinel_index, Ordering::Release);
        queue.tail.store(sentinel_index, Ordering::Release);

        queue
    }

    // IPC: Calculate required shared memory size
    pub fn shared_size(num_threads: usize) -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let state_array_size =
            num_threads * FALSE_SHARING_MULTIPLIER * mem::size_of::<Request<T>>();
        let state_array_aligned = (state_array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_per_thread = 150_000;
        let node_pool_size = num_threads * items_per_thread * 2;
        let nodes_size = node_pool_size * mem::size_of::<Node<T>>();

        let total = queue_aligned + state_array_aligned + nodes_size;
        (total + 4095) & !4095 // Page align
    }

    // IPC: Helper lifecycle management
    pub fn stop_helper(&self) {
        self.helper_should_stop.store(true, Ordering::Release);
        while self.helper_running.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
    }

    // Paper Listing 3.2: Enqueue operation
    pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), ()> {
        unsafe {
            if thread_id >= self.num_threads {
                return Err(());
            }

            // Paper line 3: get thread id (passed as param)
            let request = self.get_request(thread_id);

            // Paper line 5: create request with element e
            *request.element.get() = Some(item);
            request.is_completed.store(false, Ordering::Release);

            // Paper line 7: post request in state array
            request
                .op_type
                .store(OpType::Enqueue as u8, Ordering::Release);

            // Paper line 9: wait until operation is completed
            while !request.is_completed.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            // Paper line 11: clear request from state array
            request.op_type.store(OpType::None as u8, Ordering::Release);
            unsafe {
                *request.element.get() = None;
            }

            Ok(()) // Paper line 13: return true
        }
    }

    // Paper Listing 3.3: Dequeue operation
    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            if thread_id >= self.num_threads {
                return Err(());
            }

            // Paper line 3: get thread id (passed as param)
            let request = self.get_request(thread_id);

            // Paper line 5: create request with empty value field
            *request.element.get() = None;
            request.is_completed.store(false, Ordering::Release);

            // Paper line 7: put request in state array
            request
                .op_type
                .store(OpType::Dequeue as u8, Ordering::Release);

            // Paper line 9: wait until operation completes
            while !request.is_completed.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            // Paper line 13: return req.e
            let result = (*request.element.get()).take();

            // Paper line 11: clear request from state array
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
        // IPC: Helper should be stopped externally before drop
    }
}
