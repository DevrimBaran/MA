use crate::SpscQueue;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicPtr, Ordering};

#[repr(C)]
pub struct Node<T: Send + Clone> {
    pub item: MaybeUninit<T>, 
    pub next: AtomicPtr<Node<T>>,
}

impl<T: Send + Clone> Node<T> {
    pub unsafe fn init_dummy(node_ptr: *mut Self) {
        ptr::addr_of_mut!((*node_ptr).item).write(MaybeUninit::uninit());
        (*ptr::addr_of_mut!((*node_ptr).next)).store(ptr::null_mut(), Ordering::Relaxed);
    }
}

#[repr(C)]
pub struct SesdJpQueue<T: Send + Clone> {
    first: AtomicPtr<Node<T>>,      
    last: AtomicPtr<Node<T>>,       
    announce: AtomicPtr<Node<T>>,   
    free_later: AtomicPtr<Node<T>>, 
    help: *mut MaybeUninit<T>,      
}

impl<T: Send + Clone> SesdJpQueue<T> {
    pub unsafe fn new_in_shm(
        shm_ptr_self: *mut Self,
        shm_ptr_initial_dummy_node: *mut Node<T>,
        shm_ptr_help_slot: *mut MaybeUninit<T>,
        shm_ptr_free_later_dummy: *mut Node<T>,
    ) -> &'static mut Self {
        Node::init_dummy(shm_ptr_initial_dummy_node);
        Node::init_dummy(shm_ptr_free_later_dummy);
        shm_ptr_help_slot.write(MaybeUninit::uninit());

        ptr::write(shm_ptr_self, SesdJpQueue {
            first: AtomicPtr::new(shm_ptr_initial_dummy_node),
            last: AtomicPtr::new(shm_ptr_initial_dummy_node),
            announce: AtomicPtr::new(ptr::null_mut()),
            free_later: AtomicPtr::new(shm_ptr_free_later_dummy),
            help: shm_ptr_help_slot,
        });
        &mut *shm_ptr_self
    }

    pub fn enqueue2(&self, item_val: T, new_node_ptr: *mut Node<T>) {
        unsafe {
            Node::init_dummy(new_node_ptr);
            let tmp = self.last.load(Ordering::Relaxed);
            ptr::addr_of_mut!((*tmp).item).write(MaybeUninit::new(item_val));
            (*tmp).next.store(new_node_ptr, Ordering::Release);
            self.last.store(new_node_ptr, Ordering::Release);
        }
    }

    pub fn read_fronte(&self) -> Option<T> {
        unsafe {
            let tmp = self.first.load(Ordering::Acquire);
            if tmp == self.last.load(Ordering::Relaxed) {
                return None;
            }
            self.announce.store(tmp, Ordering::Release);
            if tmp != self.first.load(Ordering::Acquire) {
                let help_item_ref = (*self.help).assume_init_ref();
                Some(help_item_ref.clone())
            } else {
                let item_ref = (*tmp).item.assume_init_ref();
                Some(item_ref.clone())
            }
        }
    }

    pub fn dequeue2(&self, node_to_free_pool: &mut *mut Node<T>) -> Option<T> {
        unsafe {
            let tmp = self.first.load(Ordering::Relaxed);
            if tmp == self.last.load(Ordering::Acquire) {
                *node_to_free_pool = ptr::null_mut();
                return None;
            }
            
            let retval = (*(*tmp).item.as_ptr()).clone();
            self.help.write(MaybeUninit::new(retval.clone()));
            let next_ptr = (*tmp).next.load(Ordering::Acquire);
            
            if next_ptr.is_null() {
                *node_to_free_pool = ptr::null_mut();
                return None;
            }
            
            self.first.store(next_ptr, Ordering::Release);
            
            if tmp == self.announce.load(Ordering::Acquire) {
                let tmp_prime = self.free_later.swap(tmp, Ordering::AcqRel);
                *node_to_free_pool = tmp_prime;
            } else {
                *node_to_free_pool = tmp;
            }
            
            Some(retval)
        }
    }

    pub fn read_frontd(&self) -> Option<T> {
        unsafe {
            let tmp = self.first.load(Ordering::Relaxed);
            if tmp == self.last.load(Ordering::Acquire) {
                None
            } else {
                let item_ref = (*tmp).item.assume_init_ref();
                Some(item_ref.clone())
            }
        }
    }
}

// Simple errors
#[derive(Debug, PartialEq, Eq)]
pub struct SesdPushError;

#[derive(Debug, PartialEq, Eq)]  
pub struct SesdPopError;

#[repr(C)]
pub struct SesdJpSpscBenchWrapper<T: Send + Clone + 'static> {
    // The core queue
    queue: SesdJpQueue<T>,
    
    // Simple array-based node pool (like LamportQueue uses an array for items)
    nodes_storage: *mut UnsafeCell<Node<T>>,
    available_count: usize,
    capacity: usize,
    
    // Simple head/tail pointers for the free list - wrapped in UnsafeCell for mutation
    free_head: UnsafeCell<usize>,
    free_tail: usize,
    
    // Store special node addresses for filtering
    initial_dummy_addr: *mut Node<T>,
    free_later_dummy_addr: *mut Node<T>,
}

unsafe impl<T: Send + Clone + 'static> Send for SesdJpSpscBenchWrapper<T> {}
unsafe impl<T: Send + Clone + 'static> Sync for SesdJpSpscBenchWrapper<T> {}

impl<T: Send + Clone + 'static> SesdJpSpscBenchWrapper<T> {
    pub fn shared_size(pool_capacity: usize) -> usize {
        let mut size = 0;
        
        // Size of the wrapper struct itself
        size += mem::size_of::<Self>();
        
        // Align for nodes storage
        size = (size + mem::align_of::<Node<T>>() - 1) & !(mem::align_of::<Node<T>>() - 1);
        
        // Space for the node pool (extra nodes: initial dummy + free_later dummy + working nodes)
        let total_nodes = pool_capacity + 10; // Extra buffer for safety
        size += total_nodes * mem::size_of::<UnsafeCell<Node<T>>>();
        
        // Space for help slot
        size = (size + mem::align_of::<MaybeUninit<T>>() - 1) & !(mem::align_of::<MaybeUninit<T>>() - 1);
        size += mem::size_of::<MaybeUninit<T>>();
        
        size
    }

    pub unsafe fn init_in_shared(shm_ptr: *mut u8, pool_capacity: usize) -> &'static Self {
        if pool_capacity == 0 {
            panic!("Pool capacity cannot be 0");
        }
        
        let mut offset = 0;
        
        // Place the wrapper struct
        let self_ptr = shm_ptr as *mut Self;
        offset += mem::size_of::<Self>();
        
        // Align for nodes
        offset = (offset + mem::align_of::<Node<T>>() - 1) & !(mem::align_of::<Node<T>>() - 1);
        
        // Place nodes storage
        let total_nodes = pool_capacity + 10;
        let nodes_storage_ptr = shm_ptr.add(offset) as *mut UnsafeCell<Node<T>>;
        offset += total_nodes * mem::size_of::<UnsafeCell<Node<T>>>();
        
        // Align for help slot
        offset = (offset + mem::align_of::<MaybeUninit<T>>() - 1) & !(mem::align_of::<MaybeUninit<T>>() - 1);
        let help_slot_ptr = shm_ptr.add(offset) as *mut MaybeUninit<T>;
        
        // Initialize nodes storage
        for i in 0..total_nodes {
            let node_cell_ptr = nodes_storage_ptr.add(i);
            let node_ptr = (*node_cell_ptr).get();
            Node::init_dummy(node_ptr);
        }
        
        // Get special node addresses
        let initial_dummy_addr = (*nodes_storage_ptr.add(0)).get();
        let free_later_dummy_addr = (*nodes_storage_ptr.add(1)).get();
        
        // Initialize help slot
        help_slot_ptr.write(MaybeUninit::uninit());
        
        // Initialize the wrapper first with uninitialized queue
        ptr::write(self_ptr, Self {
            queue: std::mem::MaybeUninit::uninit().assume_init(),
            nodes_storage: nodes_storage_ptr,
            available_count: pool_capacity,
            capacity: pool_capacity,
            free_head: UnsafeCell::new(2), // Start after the two dummy nodes
            free_tail: total_nodes,
            initial_dummy_addr,
            free_later_dummy_addr,
        });
        SesdJpQueue::new_in_shm(
            ptr::addr_of_mut!((*self_ptr).queue),
            initial_dummy_addr,
            help_slot_ptr,
            free_later_dummy_addr,
        );
        
        &*self_ptr
    }

    #[inline]
    fn alloc_node(&self) -> *mut Node<T> {
        unsafe {
            let current_head = *self.free_head.get();
            
            if current_head >= self.free_tail {
                return ptr::null_mut(); // Pool exhausted
            }
            
            // Update head pointer
            *self.free_head.get() = current_head + 1;
            
            let node_cell_ptr = self.nodes_storage.add(current_head);
            let node_ptr = (*node_cell_ptr).get();
            
            // Reinitialize the node for use
            Node::init_dummy(node_ptr);
            
            node_ptr
        }
    }

    #[inline]
    fn free_node(&self, node_ptr: *mut Node<T>) {
        if node_ptr.is_null() {
            return;
        }
        
        // Don't free special dummy nodes
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
        let mut node_to_free: *mut Node<T> = ptr::null_mut();
        match self.queue.dequeue2(&mut node_to_free) {
            Some(item) => {
                self.free_node(node_to_free);
                Ok(item)
            }
            None => Err(SesdPopError)
        }
    }

    fn available(&self) -> bool {
        // Check if we can allocate a node and queue has space
        let can_alloc = unsafe { *self.free_head.get() < self.free_tail };
        let queue_available = self.queue.read_frontd().is_some();
        can_alloc || queue_available
    }

    fn empty(&self) -> bool {
        self.queue.read_frontd().is_none()
    }
}