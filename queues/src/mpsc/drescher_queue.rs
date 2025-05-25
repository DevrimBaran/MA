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

    pub fn push(&self, item_val: T) -> Result<(), T> {
        unsafe {
            let new_node_ptr = match self.alloc_node() {
                Some(ptr) => ptr,
                None => return Err(item_val),
            };

            Node::new_in_shm(item_val, new_node_ptr);

            let prev_tail_ptr = self.tail.swap(new_node_ptr, Ordering::AcqRel);

            (*prev_tail_ptr).next.store(new_node_ptr, Ordering::Release);

            Ok(())
        }
    }

    pub fn pop(&self) -> Option<T> {
        unsafe {
            let current_head_node_ptr = self.head.load(Ordering::Relaxed);
            let next_node_ptr = (*current_head_node_ptr).next.load(Ordering::Acquire);

            if next_node_ptr.is_null() {
                return None;
            }

            let dummy_node = (self.allocation_base.add(self.dummy_node_offset)) as *mut Node<T>;

            if current_head_node_ptr == dummy_node {
                (*dummy_node).next.store(ptr::null_mut(), Ordering::Relaxed);
                let prev_tail_before_dummy_requeue = self.tail.swap(dummy_node, Ordering::AcqRel);
                (*prev_tail_before_dummy_requeue)
                    .next
                    .store(dummy_node, Ordering::Release);

                let new_actual_head_ptr = (*next_node_ptr).next.load(Ordering::Acquire);

                if new_actual_head_ptr.is_null() {
                    self.head.store(dummy_node, Ordering::Relaxed);
                } else {
                    self.head.store(new_actual_head_ptr, Ordering::Relaxed);
                }

                let item_val = ptr::read(&(*next_node_ptr).item).assume_init();

                self.free_node(next_node_ptr);

                Some(item_val)
            } else {
                self.head.store(next_node_ptr, Ordering::Relaxed);
                let item_val = ptr::read(&(*current_head_node_ptr).item).assume_init();

                self.free_node(current_head_node_ptr);

                Some(item_val)
            }
        }
    }
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
