// jayanti petrovic wrapper for spsc bench
use crate::mpsc::sesd_jp_queue::{Node as SesdNode, SesdJpQueue};
use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr;

#[derive(Debug, PartialEq, Eq)]
pub struct SesdPushError;

#[derive(Debug, PartialEq, Eq)]
pub struct SesdPopError;

#[repr(C)]
pub struct SesdJpSpscBenchWrapper<T: Send + Clone + 'static> {
    queue: SesdJpQueue<T>,

    nodes_storage: *mut UnsafeCell<SesdNode<T>>,
    available_count: usize,
    capacity: usize,

    free_head: UnsafeCell<usize>,
    free_tail: usize,

    initial_dummy_addr: *mut SesdNode<T>,
    free_later_dummy_addr: *mut SesdNode<T>,
}

unsafe impl<T: Send + Clone + 'static> Send for SesdJpSpscBenchWrapper<T> {}
unsafe impl<T: Send + Clone + 'static> Sync for SesdJpSpscBenchWrapper<T> {}

impl<T: Send + Clone + 'static> SesdJpSpscBenchWrapper<T> {
    pub fn shared_size(pool_capacity: usize) -> usize {
        let mut size = 0;

        size += mem::size_of::<Self>();

        size = (size + mem::align_of::<SesdNode<T>>() - 1) & !(mem::align_of::<SesdNode<T>>() - 1);

        let total_nodes = pool_capacity + 10;
        size += total_nodes * mem::size_of::<UnsafeCell<SesdNode<T>>>();

        size = (size + mem::align_of::<MaybeUninit<T>>() - 1)
            & !(mem::align_of::<MaybeUninit<T>>() - 1);
        size += mem::size_of::<MaybeUninit<T>>();

        size
    }

    pub unsafe fn init_in_shared(shm_ptr: *mut u8, pool_capacity: usize) -> &'static Self {
        if pool_capacity == 0 {
            panic!("Pool capacity cannot be 0");
        }

        let mut offset = 0;

        let self_ptr = shm_ptr as *mut Self;
        offset += mem::size_of::<Self>();

        offset =
            (offset + mem::align_of::<SesdNode<T>>() - 1) & !(mem::align_of::<SesdNode<T>>() - 1);

        let total_nodes = pool_capacity + 10;
        let nodes_storage_ptr = shm_ptr.add(offset) as *mut UnsafeCell<SesdNode<T>>;
        offset += total_nodes * mem::size_of::<UnsafeCell<SesdNode<T>>>();

        offset = (offset + mem::align_of::<MaybeUninit<T>>() - 1)
            & !(mem::align_of::<MaybeUninit<T>>() - 1);
        let help_slot_ptr = shm_ptr.add(offset) as *mut MaybeUninit<T>;

        for i in 0..total_nodes {
            let node_cell_ptr = nodes_storage_ptr.add(i);
            let node_ptr = (*node_cell_ptr).get();
            SesdNode::init_dummy(node_ptr);
        }

        let initial_dummy_addr = (*nodes_storage_ptr.add(0)).get();
        let free_later_dummy_addr = (*nodes_storage_ptr.add(1)).get();

        help_slot_ptr.write(MaybeUninit::uninit());

        let queue_instance = SesdJpQueue::new_in_shm(
            ptr::addr_of_mut!((*self_ptr).queue),
            initial_dummy_addr,
            help_slot_ptr,
            free_later_dummy_addr,
        );

        ptr::write(
            self_ptr,
            Self {
                queue: ptr::read(queue_instance),
                nodes_storage: nodes_storage_ptr,
                available_count: pool_capacity,
                capacity: pool_capacity,
                free_head: UnsafeCell::new(2),
                free_tail: total_nodes,
                initial_dummy_addr,
                free_later_dummy_addr,
            },
        );

        &*self_ptr
    }

    #[inline]
    fn alloc_node(&self) -> *mut SesdNode<T> {
        unsafe {
            let current_head = *self.free_head.get();

            if current_head >= self.free_tail {
                return ptr::null_mut();
            }

            *self.free_head.get() = current_head + 1;

            let node_cell_ptr = self.nodes_storage.add(current_head);
            let node_ptr = (*node_cell_ptr).get();

            SesdNode::init_dummy(node_ptr);

            node_ptr
        }
    }

    #[inline]
    fn free_node(&self, node_ptr: *mut SesdNode<T>) {
        if node_ptr.is_null() {
            return;
        }

        if node_ptr == self.initial_dummy_addr || node_ptr == self.free_later_dummy_addr {
            return;
        }
    }
}

impl<T: Send + Clone + 'static> SpscQueue<T> for SesdJpSpscBenchWrapper<T> {
    type PushError = SesdPushError;
    type PopError = SesdPopError;

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        let new_node = self.alloc_node();
        if new_node.is_null() {
            return Err(SesdPushError);
        }

        self.queue.enqueue2(item, new_node);
        Ok(())
    }

    fn pop(&self) -> Result<T, Self::PopError> {
        let mut node_to_free: *mut SesdNode<T> = ptr::null_mut();
        match self.queue.dequeue2(&mut node_to_free) {
            Some(item) => {
                self.free_node(node_to_free);
                Ok(item)
            }
            None => Err(SesdPopError),
        }
    }

    fn available(&self) -> bool {
        let can_alloc = unsafe { *self.free_head.get() < self.free_tail };
        let queue_available = self.queue.read_frontd().is_some();
        can_alloc || queue_available
    }

    fn empty(&self) -> bool {
        self.queue.read_frontd().is_none()
    }
}
