use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::MpscQueue;

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

#[repr(C)]
pub struct DrescherQueue<T: Send + 'static> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    dummy_node_offset: usize,
    free_list: AtomicPtr<Node<T>>,
    allocation_base: *mut u8,
    allocation_size: usize,
    allocation_offset: AtomicUsize,
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

    pub unsafe fn init_in_shared(mem_ptr: *mut u8, expected_nodes: usize) -> &'static mut Self {
        let total_size = Self::shared_size(expected_nodes);
        let queue_ptr = mem_ptr as *mut Self;

        let queue_end = mem_ptr.add(std::mem::size_of::<Self>());
        let dummy_node_ptr = queue_end as *mut Node<T>;
        let allocation_start = queue_end.add(std::mem::size_of::<Node<T>>());

        Node::<T>::new_dummy_in_shm(dummy_node_ptr);

        ptr::write(
            queue_ptr,
            Self {
                head: AtomicPtr::new(dummy_node_ptr),
                tail: AtomicPtr::new(dummy_node_ptr),
                dummy_node_offset: queue_end as usize - mem_ptr as usize,
                free_list: AtomicPtr::new(ptr::null_mut()),
                allocation_base: mem_ptr,
                allocation_size: total_size,
                allocation_offset: AtomicUsize::new(allocation_start as usize - mem_ptr as usize),
            },
        );

        &mut *queue_ptr
    }

    unsafe fn alloc_node(&self) -> Option<*mut Node<T>> {
        // First try to get a node from the free list
        let mut current = self.free_list.load(Ordering::Acquire);
        while !current.is_null() {
            let next = (*current).next.load(Ordering::Relaxed);
            match self.free_list.compare_exchange_weak(
                current,
                next,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(node) => {
                    (*node).next.store(ptr::null_mut(), Ordering::Relaxed);
                    return Some(node);
                }
                Err(actual) => current = actual,
            }
        }

        // Allocate from the pool
        let node_size = std::mem::size_of::<Node<T>>();
        let node_align = std::mem::align_of::<Node<T>>();

        loop {
            let current_offset = self.allocation_offset.load(Ordering::Relaxed);

            let aligned_offset = (current_offset + node_align - 1) & !(node_align - 1);
            let new_offset = aligned_offset + node_size;

            if new_offset > self.allocation_size {
                return None;
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

    unsafe fn free_node(&self, node: *mut Node<T>) {
        let mut current = self.free_list.load(Ordering::Acquire);
        loop {
            (*node).next.store(current, Ordering::Relaxed);
            match self.free_list.compare_exchange_weak(
                current,
                node,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    // ENQUEUE operation from Figure 4(b) - completely faithful to paper
    pub fn push(&self, item_val: T) -> Result<(), T> {
        unsafe {
            let new_node_ptr = match self.alloc_node() {
                Some(ptr) => ptr,
                None => return Err(item_val),
            };

            // Initialize the new node
            Node::new_in_shm(item_val, new_node_ptr);

            // Paper: prev ← FAS(tail, item)
            let prev_tail_ptr = self.tail.swap(new_node_ptr, Ordering::AcqRel);

            // Paper: prev.next ← item
            (*prev_tail_ptr).next.store(new_node_ptr, Ordering::Release);

            Ok(())
        }
    }

    // DEQUEUE operation from Figure 4(c) - following paper's logic precisely
    pub fn pop(&self) -> Option<T> {
        unsafe {
            // Paper: item ← head
            let item = self.head.load(Ordering::Acquire);

            // Paper: next ← head.next
            let next = (*item).next.load(Ordering::Acquire);

            // Paper: if next = 0 then return 0
            if next.is_null() {
                return None;
            }

            // Paper: head ← next
            self.head.store(next, Ordering::Release);

            // Paper: if item = ref dummy then...
            let dummy_node = (self.allocation_base.add(self.dummy_node_offset)) as *mut Node<T>;
            if item == dummy_node {
                // Paper: ENQUEUE(item)
                (*dummy_node).next.store(ptr::null_mut(), Ordering::Relaxed);
                let prev_tail = self.tail.swap(dummy_node, Ordering::AcqRel);
                (*prev_tail).next.store(dummy_node, Ordering::Release);

                // Paper: if head.next = 0 then return 0
                let new_head = self.head.load(Ordering::Acquire);
                if (*new_head).next.load(Ordering::Acquire).is_null() {
                    return None;
                }

                // Paper: head ← head.next
                let next_after_dummy = (*new_head).next.load(Ordering::Acquire);
                self.head.store(next_after_dummy, Ordering::Release);

                // Paper: return next (which we stored earlier as the node after dummy)
                let item_val = ptr::read(&(*next).item).assume_init();
                self.free_node(next);
                return Some(item_val);
            }

            // Paper: return item
            let item_val = ptr::read(&(*item).item).assume_init();
            self.free_node(item);
            Some(item_val)
        }
    }

    // EMPTY operation from Figure 4(d)
    pub fn is_empty(&self) -> bool {
        unsafe {
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
        let current_offset = self.allocation_offset.load(Ordering::Relaxed);
        let node_size = std::mem::size_of::<Node<T>>();
        let node_align = std::mem::align_of::<Node<T>>();
        let aligned_offset = (current_offset + node_align - 1) & !(node_align - 1);
        let needed_space = aligned_offset + node_size;

        needed_space > self.allocation_size
    }
}
