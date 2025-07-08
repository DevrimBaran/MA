// paper in /paper/mpsc/drescher.pdf
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::MpscQueue;

// Node structure - follows paper's linked list node
#[repr(C)]
struct Node<T> {
    item: MaybeUninit<T>,
    next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
    fn new_in_shm(item_val: T, shm_node_ptr: *mut Self) {
        unsafe {
            ptr::addr_of_mut!((*shm_node_ptr).item).write(MaybeUninit::new(item_val));
            let atomic_next_ptr = ptr::addr_of_mut!((*shm_node_ptr).next);
            (*atomic_next_ptr).store(ptr::null_mut(), Ordering::Relaxed);
        }
    }

    fn new_dummy_in_shm(shm_node_ptr: *mut Self) {
        unsafe {
            ptr::addr_of_mut!((*shm_node_ptr).item).write(MaybeUninit::uninit());
            let atomic_next_ptr = ptr::addr_of_mut!((*shm_node_ptr).next);
            (*atomic_next_ptr).store(ptr::null_mut(), Ordering::Relaxed);
        }
    }
}

// Queue structure - Figure 4(a) in paper
#[repr(C)]
pub struct DrescherQueue<T: Send + 'static> {
    head: AtomicPtr<Node<T>>,       // Figure 4(a)
    tail: AtomicPtr<Node<T>>,       // Figure 4(a)
    dummy_node_offset: usize,       // IPC: offset to dummy node in shared memory
    allocation_base: *mut u8,       // IPC: base pointer for shared memory
    allocation_size: usize,         // IPC: total allocation size
    allocation_offset: AtomicUsize, // IPC: current allocation offset for node pool
}

unsafe impl<T: Send + 'static> Sync for DrescherQueue<T> {}
unsafe impl<T: Send + 'static> Send for DrescherQueue<T> {}

impl<T: Send + 'static> DrescherQueue<T> {
    pub fn shared_size(expected_nodes: usize) -> usize {
        let queue_size = std::mem::size_of::<Self>();
        let dummy_size = std::mem::size_of::<Node<T>>();
        let node_space = expected_nodes * std::mem::size_of::<Node<T>>();

        (queue_size + dummy_size + node_space + 1024).next_power_of_two()
    }

    // Initialize in shared memory - IPC adaptation
    pub unsafe fn init_in_shared(mem_ptr: *mut u8, expected_nodes: usize) -> &'static mut Self {
        let total_size = Self::shared_size(expected_nodes);
        let queue_ptr = mem_ptr as *mut Self;

        let queue_end = mem_ptr.add(std::mem::size_of::<Self>());
        let dummy_node_ptr = queue_end as *mut Node<T>;
        let allocation_start = queue_end.add(std::mem::size_of::<Node<T>>());

        // Initialize dummy node - Figure 4(a) requires dummy element
        Node::<T>::new_dummy_in_shm(dummy_node_ptr);

        ptr::write(
            queue_ptr,
            Self {
                head: AtomicPtr::new(dummy_node_ptr), // Figure 4(a): head = ref dummy
                tail: AtomicPtr::new(dummy_node_ptr), // Figure 4(a): tail = ref dummy
                dummy_node_offset: queue_end as usize - mem_ptr as usize,
                allocation_base: mem_ptr,
                allocation_size: total_size,
                allocation_offset: AtomicUsize::new(allocation_start as usize - mem_ptr as usize),
            },
        );

        &mut *queue_ptr
    }

    // IPC: allocate node from pre-allocated pool instead of heap
    unsafe fn alloc_node(&self) -> Option<*mut Node<T>> {
        let node_size = std::mem::size_of::<Node<T>>();
        let node_align = std::mem::align_of::<Node<T>>();

        loop {
            let current_offset = self.allocation_offset.load(Ordering::Relaxed);

            let aligned_offset = (current_offset + node_align - 1) & !(node_align - 1);
            let new_offset = aligned_offset + node_size;

            if new_offset > self.allocation_size {
                return None; // Pool exhausted
            }

            if self
                .allocation_offset
                .compare_exchange_weak(
                    current_offset,
                    new_offset,
                    Ordering::Release,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                let node_ptr = self.allocation_base.add(aligned_offset) as *mut Node<T>;
                return Some(node_ptr);
            }
        }
    }

    // enqueue() - Figure 4(b)
    pub fn push(&self, item_val: T) -> Result<(), T> {
        unsafe {
            // IPC: allocate from pool instead of 'new Node'
            let new_node_ptr = match self.alloc_node() {
                Some(ptr) => ptr,
                None => return Err(item_val),
            };

            Node::new_in_shm(item_val, new_node_ptr);

            // Figure 4(b): prev ← FAS(tail, item)
            let prev_tail_ptr = self.tail.swap(new_node_ptr, Ordering::AcqRel);

            // Figure 4(b): prev.next ← item
            (*prev_tail_ptr).next.store(new_node_ptr, Ordering::Release);

            Ok(())
        }
    }

    // dequeue() - Figure 4(c)
    pub fn pop(&self) -> Option<T> {
        unsafe {
            // Figure 4(c): item ← head
            let item = self.head.load(Ordering::Acquire);

            // Figure 4(c): next ← head.next
            let next = (*item).next.load(Ordering::Acquire);

            // Figure 4(c): if next = 0 then return 0
            if next.is_null() {
                return None;
            }

            // Figure 4(c): head ← next
            self.head.store(next, Ordering::Release);

            // Figure 4(c): if item = ref dummy then...
            let dummy_node = (self.allocation_base.add(self.dummy_node_offset)) as *mut Node<T>;
            if item == dummy_node {
                // Re-enqueue dummy node
                (*dummy_node).next.store(ptr::null_mut(), Ordering::Relaxed);
                let prev_tail = self.tail.swap(dummy_node, Ordering::AcqRel);
                (*prev_tail).next.store(dummy_node, Ordering::Release);

                // Figure 4(c): if head.next = 0 then return 0
                let new_head = self.head.load(Ordering::Acquire);
                if (*new_head).next.load(Ordering::Acquire).is_null() {
                    return None;
                }

                // Figure 4(c): head ← head.next
                let next_after_dummy = (*new_head).next.load(Ordering::Acquire);
                self.head.store(next_after_dummy, Ordering::Release);

                // Figure 4(c): return next
                let item_val = ptr::read(&(*next).item).assume_init();
                return Some(item_val);
            }

            // Figure 4(c): return item
            let item_val = ptr::read(&(*item).item).assume_init();
            Some(item_val)
        }
    }

    // empty() - Figure 4(d)
    pub fn is_empty(&self) -> bool {
        unsafe {
            // Figure 4(d): return head.next = 0
            let head_ptr = self.head.load(Ordering::Acquire);
            (*head_ptr).next.load(Ordering::Acquire).is_null()
        }
    }
}

impl<T: Send + 'static> MpscQueue<T> for DrescherQueue<T> {
    type PushError = T;
    type PopError = ();

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        self.push(item)
    }

    fn pop(&self) -> Result<T, Self::PopError> {
        self.pop().ok_or(())
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn is_full(&self) -> bool {
        // IPC: check if node pool is exhausted
        let current_offset = self.allocation_offset.load(Ordering::Relaxed);
        let node_size = std::mem::size_of::<Node<T>>();
        let node_align = std::mem::align_of::<Node<T>>();
        let aligned_offset = (current_offset + node_align - 1) & !(node_align - 1);
        let needed_space = aligned_offset + node_size;

        needed_space > self.allocation_size
    }
}
