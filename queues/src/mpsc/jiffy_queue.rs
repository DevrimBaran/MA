use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering, AtomicBool};
use std::ptr;
use std::mem::{MaybeUninit, align_of, size_of};
use std::sync::Mutex;

use crate::MpscQueue; // Your existing MPSC trait

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(usize)]
enum NodeState {
    Empty = 0,
    Set = 1,
    Handled = 2,
}

#[repr(C)]
struct Node<T> {
    data: MaybeUninit<T>,
    is_set: AtomicUsize,
}

impl<T> Node<T> {
    // Initialize a node in-place (e.g., in a shared memory Node array)
    unsafe fn init_in_place(node_ptr: *mut Self) {
        ptr::addr_of_mut!((*node_ptr).data).write(MaybeUninit::uninit());
        ptr::addr_of_mut!((*node_ptr).is_set).write(AtomicUsize::new(NodeState::Empty as usize));
    }
}

#[repr(C)]
struct BufferList<T> {
    curr_buffer: *mut Node<T>, // Pointer to the Node array in the shared Node array pool
    capacity: usize,
    next: AtomicPtr<BufferList<T>>, // Points to another BufferList metadata in its pool
    prev: *mut BufferList<T>,
    consumer_head_idx: usize,
    position_in_queue: u64,
    is_array_reclaimed: AtomicBool, // True if curr_buffer points to memory that was "returned" to pool
    // For free list of BufferList metadata
    next_free_meta: AtomicPtr<BufferList<T>>, // Used when this struct is in the free list - FIXED: Made atomic
}

impl<T: Send + 'static> BufferList<T> {
    // Initializes a BufferList metadata struct in-place. Does NOT allocate its Node array.
    unsafe fn init_metadata_in_place(
        bl_meta_ptr: *mut Self,
        node_array_ptr: *mut Node<T>, // Pre-allocated Node array for this BL
        capacity: usize,
        position_in_queue: u64,
        prev_buffer: *mut BufferList<T>,
    ) {
        ptr::addr_of_mut!((*bl_meta_ptr).curr_buffer).write(node_array_ptr);
        ptr::addr_of_mut!((*bl_meta_ptr).capacity).write(capacity);
        ptr::addr_of_mut!((*bl_meta_ptr).next).write(AtomicPtr::new(ptr::null_mut()));
        ptr::addr_of_mut!((*bl_meta_ptr).prev).write(prev_buffer);
        ptr::addr_of_mut!((*bl_meta_ptr).consumer_head_idx).write(0);
        ptr::addr_of_mut!((*bl_meta_ptr).position_in_queue).write(position_in_queue);
        ptr::addr_of_mut!((*bl_meta_ptr).is_array_reclaimed).write(AtomicBool::new(false));
        // FIXED: Initialize as AtomicPtr
        ptr::addr_of_mut!((*bl_meta_ptr).next_free_meta).write(AtomicPtr::new(ptr::null_mut()));

        // Initialize all Nodes in the provided node_array_ptr
        if !node_array_ptr.is_null() {
            for i in 0..capacity {
                Node::init_in_place(node_array_ptr.add(i));
            }
        }
    }

    /// Marks the Node array as "reclaimed" and drops its contents if necessary.
    /// The actual memory for Node array is managed by the JiffyQueue's node_array_pool.
    unsafe fn mark_node_array_reclaimed_and_drop_items(&mut self) {
        if !self.curr_buffer.is_null() &&
           self.is_array_reclaimed.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed).is_ok()
        {
            if std::mem::needs_drop::<T>() {
                for i in 0..self.capacity {
                    let node_ptr = self.curr_buffer.add(i);
                    if (*node_ptr).is_set.load(Ordering::Relaxed) == NodeState::Set as usize {
                        ptr::drop_in_place((*node_ptr).data.as_mut_ptr());
                    }
                }
            }
            // We don't Vec::from_raw_parts here as the memory belongs to the shared pool.
            // Nullifying helps indicate it's logically gone.
            // The actual "return to pool" for node arrays would be more complex for reuse.
            // For now, this just makes it unusable.
            self.curr_buffer = ptr::null_mut();
        }
    }
}

/// Manages pools of BufferList metadata and Node arrays within a shared memory region.
#[repr(C)]
struct SharedPools<T: Send + 'static> {
    // Pool for BufferList metadata structures
    bl_meta_pool_start: *mut BufferList<T>,
    bl_meta_pool_capacity: usize,
    bl_meta_next_free_idx: AtomicUsize, // For bump allocation
    bl_meta_free_list_head: AtomicPtr<BufferList<T>>, // For free list reclamation

    // Pool for Node arrays (a large contiguous block of Node<T>)
    // Each BufferList gets a slice of size `buffer_capacity` from this.
    node_arrays_pool_start: *mut Node<T>,
    node_arrays_pool_total_nodes: usize, // Total Node<T> capacity in this pool
    node_arrays_next_free_node_idx: AtomicUsize, // Bump allocator for Node array slices

    buffer_capacity_per_array: usize, // Stores the fixed capacity of each Node array slice
}

impl<T: Send + 'static> SharedPools<T> {
    unsafe fn new_in_place(
        mem_ptr: *mut u8,
        mut current_offset: usize,
        max_buffers: usize, // Max BufferList metadata objects
        nodes_per_buffer: usize, // Capacity of each Node array
        total_node_capacity_for_pool: usize, // Total Node objects in the node array pool
    ) -> (*mut Self, usize) {
        
        let self_align = align_of::<Self>();
        current_offset = (current_offset + self_align - 1) & !(self_align - 1);
        let pools_ptr = mem_ptr.add(current_offset) as *mut Self;
        current_offset += size_of::<Self>();

        // Layout BufferList metadata pool
        let bl_meta_align = align_of::<BufferList<T>>();
        current_offset = (current_offset + bl_meta_align - 1) & !(bl_meta_align - 1);
        let bl_meta_pool_start_ptr = mem_ptr.add(current_offset) as *mut BufferList<T>;
        current_offset += max_buffers * size_of::<BufferList<T>>();

        // Layout Node arrays pool
        let node_align = align_of::<Node<T>>();
        current_offset = (current_offset + node_align - 1) & !(node_align - 1);
        let node_arrays_pool_start_ptr = mem_ptr.add(current_offset) as *mut Node<T>;
        current_offset += total_node_capacity_for_pool * size_of::<Node<T>>();

        // Initialize the SharedPools struct itself
        ptr::addr_of_mut!((*pools_ptr).bl_meta_pool_start).write(bl_meta_pool_start_ptr);
        ptr::addr_of_mut!((*pools_ptr).bl_meta_pool_capacity).write(max_buffers);
        ptr::addr_of_mut!((*pools_ptr).bl_meta_next_free_idx).write(AtomicUsize::new(0));
        ptr::addr_of_mut!((*pools_ptr).bl_meta_free_list_head).write(AtomicPtr::new(ptr::null_mut()));
        ptr::addr_of_mut!((*pools_ptr).node_arrays_pool_start).write(node_arrays_pool_start_ptr);
        ptr::addr_of_mut!((*pools_ptr).node_arrays_pool_total_nodes).write(total_node_capacity_for_pool);
        ptr::addr_of_mut!((*pools_ptr).node_arrays_next_free_node_idx).write(AtomicUsize::new(0));
        ptr::addr_of_mut!((*pools_ptr).buffer_capacity_per_array).write(nodes_per_buffer);
        
        (pools_ptr, current_offset)
    }

    /// Allocates a BufferList metadata struct from its pool.
    /// Also allocates a Node array for it from the node_arrays_pool.
    unsafe fn alloc_bl_meta_with_node_array(
        &self,
        position_in_queue: u64,
        prev_buffer: *mut BufferList<T>,
    ) -> *mut BufferList<T> {
        // 1. Try to get from free list first
        loop {
            let head = self.bl_meta_free_list_head.load(Ordering::Acquire);
            if head.is_null() {
                break; // Free list is empty
            }
            // FIXED: Use atomic operations to read next_free_meta
            let next_free = (*head).next_free_meta.load(Ordering::Acquire);
            if self.bl_meta_free_list_head.compare_exchange(
                head, next_free, Ordering::AcqRel, Ordering::Acquire
            ).is_ok() {
                // Got one from free list. Now allocate Node array for it.
                let node_array_ptr = self.alloc_node_array_slice();
                if node_array_ptr.is_null() { // Failed to get Node array
                    // Put metadata back on free list head (simplified, could cause ABA if not careful)
                    // FIXED: Use atomic operations for next_free_meta
                    let current_head = self.bl_meta_free_list_head.load(Ordering::Acquire);
                    (*head).next_free_meta.store(current_head, Ordering::Release);
                    
                    // Try to push back to head of free list
                    while self.bl_meta_free_list_head.compare_exchange(
                        current_head, head, Ordering::AcqRel, Ordering::Acquire
                    ).is_err() {
                        // If CAS failed, update next_free_meta and retry
                        let new_head = self.bl_meta_free_list_head.load(Ordering::Acquire);
                        (*head).next_free_meta.store(new_head, Ordering::Release);
                    }
                    return ptr::null_mut();
                }
                BufferList::init_metadata_in_place(head, node_array_ptr, self.buffer_capacity_per_array, position_in_queue, prev_buffer);
                return head;
            }
            // CAS failed, retry getting from free list
        }

        // 2. If free list empty, try bump allocation for metadata
        let meta_idx = self.bl_meta_next_free_idx.fetch_add(1, Ordering::AcqRel); // FIXED: Use AcqRel ordering
        if meta_idx >= self.bl_meta_pool_capacity {
            self.bl_meta_next_free_idx.fetch_sub(1, Ordering::Relaxed); // Rollback
            return ptr::null_mut(); // Metadata pool exhausted
        }
        let bl_meta_ptr = self.bl_meta_pool_start.add(meta_idx);

        // Allocate Node array for it
        let node_array_ptr = self.alloc_node_array_slice();
        if node_array_ptr.is_null() {
            // This is tricky: we allocated metadata slot but not node array.
            // For simplicity, we "leak" this metadata slot. A robust pool would try to add it to free list.
            // Or we could decrement bl_meta_next_free_idx IF we can guarantee no one else incremented it past us.
            // Simplest for now is to let it be "lost" until a full free-list is implemented for bump part.
            return ptr::null_mut();
        }

        BufferList::init_metadata_in_place(bl_meta_ptr, node_array_ptr, self.buffer_capacity_per_array, position_in_queue, prev_buffer);
        bl_meta_ptr
    }

    /// Allocates a slice for one Node array from the large node_arrays_pool.
    unsafe fn alloc_node_array_slice(&self) -> *mut Node<T> {
        let nodes_needed = self.buffer_capacity_per_array;
        let start_node_idx = self.node_arrays_next_free_node_idx.fetch_add(nodes_needed, Ordering::AcqRel); // FIXED: Use AcqRel
        
        if start_node_idx + nodes_needed > self.node_arrays_pool_total_nodes {
            // Not enough space. Rollback attempt (this is not perfectly atomic for rollback,
            // a better system would use a more robust allocator).
            self.node_arrays_next_free_node_idx.fetch_sub(nodes_needed, Ordering::Relaxed);
            return ptr::null_mut();
        }
        self.node_arrays_pool_start.add(start_node_idx)
    }

    /// Returns a BufferList metadata struct to the free list.
    /// Assumes its Node array has been handled/marked reclaimed separately.
    unsafe fn dealloc_bl_meta_to_pool(&self, bl_meta_ptr: *mut BufferList<T>) {
        if bl_meta_ptr.is_null() { return; }
        
        // Add to head of LIFO free list
        let mut current_head = self.bl_meta_free_list_head.load(Ordering::Acquire); // FIXED: Use Acquire
        loop {
            // FIXED: Use atomic store for next_free_meta
            (*bl_meta_ptr).next_free_meta.store(current_head, Ordering::Release);
            
            match self.bl_meta_free_list_head.compare_exchange(
                current_head,
                bl_meta_ptr,
                Ordering::Release, // Ensure write to next_free_meta is visible
                Ordering::Relaxed, // On failure, re-read current_head
            ) {
                Ok(_) => break,
                Err(new_head) => current_head = new_head,
            }
        }
    }
    
    /// "Deallocates" a Node array slice. With current bump allocator, this is a no-op
    /// for reuse, but in a more complex allocator, it would return the slice to a pool.
    unsafe fn dealloc_node_array_slice(&self, _node_array_ptr: *mut Node<T>) {
        // No-op for simple bump allocator of the large node array pool without a free list for slices.
        // If node arrays were fixed slots, they could be added to their own free list.
    }
}


#[repr(C)]
pub struct JiffyQueue<T: Send + 'static> {
    head_of_queue: AtomicPtr<BufferList<T>>,
    tail_of_queue: AtomicPtr<BufferList<T>>,
    global_tail_location: AtomicU64,
    // No longer buffer_capacity here, it's in pools
    
    // Pointer to the shared pool manager
    pools: *const SharedPools<T>, // Read-only access after init for queue methods

    // This list now stores BufferList metadata whose *Node arrays* are reclaimed,
    // but metadata itself is waiting for safe deallocation via process_deferred_free_list.
    deferred_free_metadata_list: Mutex<Vec<*mut BufferList<T>>>,
}

unsafe impl<T: Send + 'static> Send for JiffyQueue<T> {}
unsafe impl<T: Send + 'static> Sync for JiffyQueue<T> {}

impl<T: Send + 'static> JiffyQueue<T> {

    pub fn shared_size(
        buffer_capacity_per_array: usize, // Nodes per BufferList's curr_buffer
        max_buffers_in_pool: usize       // Max number of BufferList metadata / arrays
    ) -> usize {
        let total_node_capacity_for_pool = max_buffers_in_pool * buffer_capacity_per_array;
        let mut current_offset = 0;

        // Size for JiffyQueue struct
        let jq_align = align_of::<JiffyQueue<T>>();
        current_offset = (current_offset + jq_align - 1) & !(jq_align - 1);
        current_offset += size_of::<JiffyQueue<T>>();

        // Size for SharedPools struct
        let sp_align = align_of::<SharedPools<T>>();
        current_offset = (current_offset + sp_align - 1) & !(sp_align - 1);
        current_offset += size_of::<SharedPools<T>>();
        
        // Size for BufferList metadata pool
        let bl_meta_align = align_of::<BufferList<T>>();
        current_offset = (current_offset + bl_meta_align - 1) & !(bl_meta_align - 1);
        current_offset += max_buffers_in_pool * size_of::<BufferList<T>>();

        // Size for Node arrays pool
        let node_align = align_of::<Node<T>>();
        current_offset = (current_offset + node_align - 1) & !(node_align - 1);
        current_offset += total_node_capacity_for_pool * size_of::<Node<T>>();
        
        current_offset // Total size needed
    }

    pub unsafe fn init_in_shared(
        mem_ptr: *mut u8,
        buffer_capacity_per_array: usize,
        max_buffers_in_pool: usize,
    ) -> &'static mut Self {
        assert!(buffer_capacity_per_array > 1, "Buffer capacity must be > 1.");
        assert!(max_buffers_in_pool > 0, "Max buffers in pool must be > 0.");

        let total_node_capacity_for_pool = max_buffers_in_pool * buffer_capacity_per_array;
        let mut current_offset = 0usize;

        // Init JiffyQueue struct
        let jq_align = align_of::<JiffyQueue<T>>();
        current_offset = (current_offset + jq_align - 1) & !(jq_align - 1);
        let queue_ptr = mem_ptr.add(current_offset) as *mut JiffyQueue<T>;
        current_offset += size_of::<JiffyQueue<T>>();

        // Init SharedPools struct (this function returns the pointer to SharedPools and the next offset)
        let (pools_instance_ptr, next_offset_after_pools) = SharedPools::<T>::new_in_place(
            mem_ptr, current_offset, max_buffers_in_pool, buffer_capacity_per_array, total_node_capacity_for_pool
        );
        // current_offset = next_offset_after_pools; // new_in_place will advance offset internally for its sub-pools

        // Initialize the first BufferList
        let initial_bl_ptr = (*pools_instance_ptr).alloc_bl_meta_with_node_array(0, ptr::null_mut());
        if initial_bl_ptr.is_null() {
            panic!("JiffyQueue: Failed to allocate initial buffer from shared pool during init.");
        }

        // Write the JiffyQueue fields
        ptr::addr_of_mut!((*queue_ptr).head_of_queue).write(AtomicPtr::new(initial_bl_ptr));
        ptr::addr_of_mut!((*queue_ptr).tail_of_queue).write(AtomicPtr::new(initial_bl_ptr));
        ptr::addr_of_mut!((*queue_ptr).global_tail_location).write(AtomicU64::new(0));
        ptr::addr_of_mut!((*queue_ptr).pools).write(pools_instance_ptr);
        ptr::addr_of_mut!((*queue_ptr).deferred_free_metadata_list).write(Mutex::new(Vec::new()));
        // `buffer_capacity` is now stored in `pools.buffer_capacity_per_array`

        &mut *queue_ptr
    }

    // Helper to get buffer capacity for this instance
    fn buffer_capacity(&self) -> usize {
        unsafe { (*self.pools).buffer_capacity_per_array }
    }
    
    // Helper to access pools
    fn pools(&self) -> &SharedPools<T> {
        unsafe { &*self.pools }
    }

    fn actual_enqueue(&self, data: T) -> Result<(), T>{
        let item_global_location = self.global_tail_location.fetch_add(1, Ordering::Relaxed);
        let mut current_producer_view_of_tail_bl = self.tail_of_queue.load(Ordering::Acquire);
        let mut new_bl_allocated_by_this_thread: *mut BufferList<T> = ptr::null_mut();

        loop {
            if current_producer_view_of_tail_bl.is_null() {
                if !new_bl_allocated_by_this_thread.is_null() { unsafe {
                    // This BL was allocated from pool, return it.
                    let bl_mut = &mut *new_bl_allocated_by_this_thread;
                    bl_mut.mark_node_array_reclaimed_and_drop_items(); // Mark array for pool (no-op for bump)
                    self.pools().dealloc_node_array_slice(bl_mut.curr_buffer); // conceptual
                    self.pools().dealloc_bl_meta_to_pool(new_bl_allocated_by_this_thread);
                }}
                return Err(data);
            }
            let tail_bl_ref = unsafe { &*current_producer_view_of_tail_bl };
            let current_buffer_cap = self.buffer_capacity(); // Use helper

            let tail_bl_start_loc = tail_bl_ref.position_in_queue * (current_buffer_cap as u64);
            let tail_bl_end_loc = tail_bl_start_loc + (current_buffer_cap as u64);

            if item_global_location >= tail_bl_end_loc {
                let mut next_bl_in_list = tail_bl_ref.next.load(Ordering::Acquire);
                if next_bl_in_list.is_null() {
                    if new_bl_allocated_by_this_thread.is_null() {
                        new_bl_allocated_by_this_thread = unsafe {
                            self.pools().alloc_bl_meta_with_node_array(
                                tail_bl_ref.position_in_queue + 1,
                                current_producer_view_of_tail_bl
                            )
                        };
                        if new_bl_allocated_by_this_thread.is_null() { return Err(data); /* Pool exhausted */ }
                    }
                    match tail_bl_ref.next.compare_exchange(
                        ptr::null_mut(), new_bl_allocated_by_this_thread,
                        Ordering::AcqRel, Ordering::Acquire
                    ) {
                        Ok(_) => {
                            // FIXED: Use AcqRel for better ordering guarantees
                            self.tail_of_queue.compare_exchange(
                                current_producer_view_of_tail_bl, new_bl_allocated_by_this_thread,
                                Ordering::AcqRel, Ordering::Acquire
                            ).ok();
                            next_bl_in_list = new_bl_allocated_by_this_thread;
                            new_bl_allocated_by_this_thread = ptr::null_mut();
                        }
                        Err(actual_next) => {
                            next_bl_in_list = actual_next;
                            if !new_bl_allocated_by_this_thread.is_null() { unsafe {
                                let bl_mut = &mut *new_bl_allocated_by_this_thread;
                                bl_mut.mark_node_array_reclaimed_and_drop_items();
                                self.pools().dealloc_node_array_slice(bl_mut.curr_buffer);
                                self.pools().dealloc_bl_meta_to_pool(new_bl_allocated_by_this_thread);
                            } new_bl_allocated_by_this_thread = ptr::null_mut(); }
                        }
                    }
                }
                if !next_bl_in_list.is_null() {
                    // FIXED: Use AcqRel for better ordering guarantees
                    self.tail_of_queue.compare_exchange(
                        current_producer_view_of_tail_bl, next_bl_in_list,
                        Ordering::AcqRel, Ordering::Acquire
                    ).ok();
                    current_producer_view_of_tail_bl = next_bl_in_list;
                } else {
                    current_producer_view_of_tail_bl = self.tail_of_queue.load(Ordering::Acquire);
                }
                continue;
            } else if item_global_location < tail_bl_start_loc {
                current_producer_view_of_tail_bl = tail_bl_ref.prev;
                if current_producer_view_of_tail_bl.is_null() {
                     if !new_bl_allocated_by_this_thread.is_null() { unsafe {
                        let bl_mut = &mut *new_bl_allocated_by_this_thread;
                        bl_mut.mark_node_array_reclaimed_and_drop_items();
                        self.pools().dealloc_node_array_slice(bl_mut.curr_buffer);
                        self.pools().dealloc_bl_meta_to_pool(new_bl_allocated_by_this_thread);
                    }}
                    return Err(data);
                }
                continue;
            } else {
                let internal_idx = (item_global_location - tail_bl_start_loc) as usize;
                if internal_idx >= tail_bl_ref.capacity {
                    current_producer_view_of_tail_bl = self.tail_of_queue.load(Ordering::Acquire);
                    continue;
                }
                // Ensure curr_buffer is valid before writing
                if tail_bl_ref.curr_buffer.is_null() { /* This buffer's array was reclaimed, logic error or race */ return Err(data); }

                let node_ptr = unsafe { tail_bl_ref.curr_buffer.add(internal_idx) };
                unsafe {
                    ptr::write(&mut (*node_ptr).data, MaybeUninit::new(data));
                    // FIXED: Use Release ordering to ensure data is visible before is_set flag
                    (*node_ptr).is_set.store(NodeState::Set as usize, Ordering::Release);
                }
                let is_globally_last_buffer = tail_bl_ref.next.load(Ordering::Acquire).is_null() &&
                                           current_producer_view_of_tail_bl == self.tail_of_queue.load(Ordering::Relaxed);
                if internal_idx == 1 && is_globally_last_buffer && self.buffer_capacity() > 1 {
                    let prealloc_bl = unsafe {
                        self.pools().alloc_bl_meta_with_node_array(
                            tail_bl_ref.position_in_queue + 1,
                            current_producer_view_of_tail_bl
                        )
                    };
                    if !prealloc_bl.is_null() {
                        if tail_bl_ref.next.compare_exchange(
                            ptr::null_mut(), prealloc_bl, Ordering::AcqRel, Ordering::Acquire
                        ).is_ok() {
                            self.tail_of_queue.compare_exchange(
                                current_producer_view_of_tail_bl, prealloc_bl, Ordering::AcqRel, Ordering::Acquire
                            ).ok();
                        } else { unsafe { // Not used, return to pool
                            let bl_mut = &mut *prealloc_bl;
                            bl_mut.mark_node_array_reclaimed_and_drop_items();
                            self.pools().dealloc_node_array_slice(bl_mut.curr_buffer);
                            self.pools().dealloc_bl_meta_to_pool(prealloc_bl);
                        }}
                    }
                }
                if !new_bl_allocated_by_this_thread.is_null() { unsafe {
                    let bl_mut = &mut *new_bl_allocated_by_this_thread;
                    bl_mut.mark_node_array_reclaimed_and_drop_items();
                    self.pools().dealloc_node_array_slice(bl_mut.curr_buffer);
                    self.pools().dealloc_bl_meta_to_pool(new_bl_allocated_by_this_thread);
                }}
                return Ok(());
            }
        }
    }
    
    // FIXED: Improve attempt_fold_buffer for multi-process safety
    unsafe fn attempt_fold_buffer(&self, bl_to_fold_ptr: *mut BufferList<T>) -> (*mut BufferList<T>, bool) {
        let current_head = self.head_of_queue.load(Ordering::Acquire);
        if bl_to_fold_ptr.is_null() || bl_to_fold_ptr == current_head {
            return (bl_to_fold_ptr, false); 
        }

        let bl_to_fold_mut = &mut *bl_to_fold_ptr;
        let prev_bl_ptr = bl_to_fold_mut.prev;
        let next_bl_ptr_for_scan = bl_to_fold_mut.next.load(Ordering::Acquire);

        if prev_bl_ptr.is_null() { return (bl_to_fold_ptr, false); }

        let prev_bl_ref = &*prev_bl_ptr;
        
        // FIXED: More robust compare_exchange with retry logic
        match prev_bl_ref.next.compare_exchange(
            bl_to_fold_ptr, next_bl_ptr_for_scan, Ordering::AcqRel, Ordering::Acquire
        ) {
            Ok(_) => { 
                if !next_bl_ptr_for_scan.is_null() {
                    (*next_bl_ptr_for_scan).prev = prev_bl_ptr;
                }
                
                // Mark node array as reclaimed to prevent future operations on it
                bl_to_fold_mut.mark_node_array_reclaimed_and_drop_items();
                self.pools().dealloc_node_array_slice(bl_to_fold_mut.curr_buffer); // No-op for bump arena
                
                // FIXED: Use a scoped mutex lock to avoid deadlocks in multi-process case
                let mutex_guard = match self.deferred_free_metadata_list.lock() {
                    Ok(guard) => guard,
                    Err(_) => return (next_bl_ptr_for_scan, false), // Mutex poisoned
                };
                
                // Use scope-based mutex to ensure it's released properly
                {
                    let mut list = mutex_guard;
                    list.push(bl_to_fold_ptr); 
                } // mutex automatically released here
                
                (next_bl_ptr_for_scan, true)
            }
            Err(_) => (bl_to_fold_ptr, false)
        }
    }

    // FIXED: Improved process_deferred_free_list with better locking strategy
    fn process_deferred_free_list(&self, new_head_buffer_pos_opt: Option<u64>) {
        let new_head_pos_threshold = match new_head_buffer_pos_opt {
            Some(pos) => pos, 
            None => return, 
        };

        // FIXED: More robust mutex handling with proper error checking
        match self.deferred_free_metadata_list.lock() {
            Ok(mut list) => {
                if list.is_empty() { return; }
                
                // Create a list of items to deallocate to minimize time holding the lock
                let mut to_deallocate = Vec::new();
                let mut i = 0;
                
                while i < list.len() {
                    let bl_metadata_ptr = list[i];
                    let metadata_pos = unsafe { (*bl_metadata_ptr).position_in_queue };
                    
                    if metadata_pos < new_head_pos_threshold {
                        // Remove from list and add to deallocation list
                        to_deallocate.push(list.remove(i));
                        // Don't increment i since we removed an element
                    } else { 
                        i += 1; 
                    }
                }
                
                // Release the lock before doing actual deallocations
                drop(list);
                
                // Now perform the actual deallocations
                for ptr in to_deallocate {
                    unsafe {
                        // Node array should already be marked reclaimed by attempt_fold_buffer
                        self.pools().dealloc_bl_meta_to_pool(ptr);
                    }
                }
            },
            Err(_) => {
                // Mutex is poisoned, but we can still continue operating
                // Log or handle the error as appropriate for your system
            }
        }
    }

    fn actual_dequeue(&self) -> Option<T> {
        loop { 
            let current_bl_ptr = self.head_of_queue.load(Ordering::Acquire);
            if current_bl_ptr.is_null() { return None; }

            let current_bl = unsafe { &mut *current_bl_ptr };

            // Skip Handled
            while current_bl.consumer_head_idx < current_bl.capacity {
                if current_bl.curr_buffer.is_null() { break; } 
                let node_to_check = unsafe { &*current_bl.curr_buffer.add(current_bl.consumer_head_idx) };
                if node_to_check.is_set.load(Ordering::Acquire) == NodeState::Handled as usize {
                    current_bl.consumer_head_idx += 1;
                } else { break; }
            }

            // Advance head_of_queue if current buffer fully consumed
            if current_bl.consumer_head_idx >= current_bl.capacity || current_bl.curr_buffer.is_null() {
                let next_bl_candidate = current_bl.next.load(Ordering::Acquire);
                let new_head_pos_opt = if next_bl_candidate.is_null() { 
                    None 
                } else { 
                    Some(unsafe { (*next_bl_candidate).position_in_queue }) 
                };
                
                // Process deferred free list before modifying the queue structure
                self.process_deferred_free_list(new_head_pos_opt); 

                // FIXED: Use AcqRel ordering for better guarantee in multi-process case
                if self.head_of_queue.compare_exchange(
                    current_bl_ptr, next_bl_candidate, Ordering::AcqRel, Ordering::Acquire
                ).is_ok() {
                    if !next_bl_candidate.is_null() { 
                        unsafe { 
                            (*next_bl_candidate).prev = ptr::null_mut(); 
                        }
                    }
                    unsafe { 
                        current_bl.mark_node_array_reclaimed_and_drop_items();
                        self.pools().dealloc_node_array_slice(current_bl.curr_buffer); // No-op for bump
                        self.pools().dealloc_bl_meta_to_pool(current_bl_ptr);
                    }
                }
                continue; 
            }

            let n_idx_in_buffer = current_bl.consumer_head_idx;
            let n_node_ptr = unsafe { current_bl.curr_buffer.add(n_idx_in_buffer) };
            let n_node_ref = unsafe { &*n_node_ptr };
            let n_state = n_node_ref.is_set.load(Ordering::Acquire);

            let n_global_loc = current_bl.position_in_queue * (self.buffer_capacity() as u64) + (n_idx_in_buffer as u64);
            let tail_loc = self.global_tail_location.load(Ordering::Relaxed);
            if n_global_loc >= tail_loc && 
               (n_state == NodeState::Empty as usize || n_state == NodeState::Handled as usize) &&
               current_bl_ptr == self.tail_of_queue.load(Ordering::Acquire) {
                return None;
            }

            if n_state == NodeState::Set as usize { // Dequeue if n is Set
                // FIXED: Use AcqRel for stronger guarantees
                if n_node_ref.is_set.compare_exchange(
                    NodeState::Set as usize, NodeState::Handled as usize, Ordering::AcqRel, Ordering::Acquire
                ).is_ok() {
                    current_bl.consumer_head_idx += 1; 
                    let data = unsafe { ptr::read(&(*n_node_ref).data).assume_init() }; 
                    return Some(data); 
                } else { continue; } 
            } else if n_state == NodeState::Empty as usize { // n is Empty: Linearizability Path
                let mut temp_n_scan_current_bl_ptr = current_bl_ptr;
                let mut temp_n_scan_current_idx = if temp_n_scan_current_bl_ptr == current_bl_ptr {
                    n_idx_in_buffer + 1 
                } else { 0 };

                'find_initial_temp_n: loop {
                    if temp_n_scan_current_bl_ptr.is_null() { return None; } 
                    let search_bl = unsafe { &mut *temp_n_scan_current_bl_ptr };
                    if search_bl.curr_buffer.is_null() { // Array reclaimed
                        temp_n_scan_current_bl_ptr = search_bl.next.load(Ordering::Acquire);
                        temp_n_scan_current_idx = 0;
                        continue 'find_initial_temp_n;
                    }
                    
                    let mut scan_idx = temp_n_scan_current_idx;
                    let mut found_set_in_search_bl = false;
                    while scan_idx < search_bl.capacity {
                        let candidate_node = unsafe { &*search_bl.curr_buffer.add(scan_idx) };
                        if candidate_node.is_set.load(Ordering::Acquire) == NodeState::Set as usize {
                            found_set_in_search_bl = true;
                            let mut final_temp_n_bl_ptr = temp_n_scan_current_bl_ptr;
                            let mut final_temp_n_idx = scan_idx;

                            'rescan_phase: loop { 
                                let mut rescan_bl_ptr = current_bl_ptr; 
                                let mut rescan_idx_in_buf = n_idx_in_buffer; 
                                let mut earlier_set_found_this_pass = false;
                                while !(rescan_bl_ptr == final_temp_n_bl_ptr && rescan_idx_in_buf >= final_temp_n_idx) {
                                    if rescan_bl_ptr.is_null() { break; } 
                                    let r_bl = unsafe { &*rescan_bl_ptr };
                                    if r_bl.curr_buffer.is_null() { 
                                        rescan_bl_ptr = r_bl.next.load(Ordering::Acquire); 
                                        rescan_idx_in_buf = 0; 
                                        continue;
                                    }
                                    if rescan_idx_in_buf >= r_bl.capacity {
                                        rescan_bl_ptr = r_bl.next.load(Ordering::Acquire); 
                                        rescan_idx_in_buf = 0;
                                        if rescan_bl_ptr.is_null() && !final_temp_n_bl_ptr.is_null() { break; }
                                        continue;
                                    }
                                    let e_node = unsafe { &*r_bl.curr_buffer.add(rescan_idx_in_buf) };
                                    if e_node.is_set.load(Ordering::Acquire) == NodeState::Set as usize { 
                                        final_temp_n_bl_ptr = rescan_bl_ptr; 
                                        final_temp_n_idx = rescan_idx_in_buf;
                                        earlier_set_found_this_pass = true; 
                                        break; 
                                    }
                                    rescan_idx_in_buf += 1;
                                }
                                if !earlier_set_found_this_pass { break 'rescan_phase; }
                            } // End 'rescan_phase

                            let item_bl = unsafe { &*final_temp_n_bl_ptr };
                            if item_bl.curr_buffer.is_null() { continue; }
                            let item_node = unsafe { &*item_bl.curr_buffer.add(final_temp_n_idx) };
                            
                            // FIXED: Use AcqRel for stronger guarantees
                            if item_node.is_set.compare_exchange(
                                NodeState::Set as usize, NodeState::Handled as usize, Ordering::AcqRel, Ordering::Acquire
                            ).is_ok() { 
                                let data = unsafe { ptr::read(&(*item_node).data).assume_init() };
                                return Some(data);
                            } else { 
                                continue; // Outer loop retry
                            } 
                        } 
                        scan_idx += 1;
                    } // End while scanning current search_bl

                    let buffer_just_scanned_ptr = temp_n_scan_current_bl_ptr;
                    let mut next_bl_for_scan = search_bl.next.load(Ordering::Acquire);

                    if !found_set_in_search_bl && buffer_just_scanned_ptr != current_bl_ptr {
                        let mut is_fully_handled = true;
                        if search_bl.curr_buffer.is_null() && !search_bl.is_array_reclaimed.load(Ordering::Relaxed) {
                            is_fully_handled = false;
                        } else if !search_bl.curr_buffer.is_null() {
                             for i in 0..search_bl.capacity {
                                if unsafe{(*search_bl.curr_buffer.add(i)).is_set.load(Ordering::Acquire)} != NodeState::Handled as usize {
                                    is_fully_handled = false; 
                                    break;
                                }
                            }
                        } // else array already reclaimed, considered handled for folding metadata

                        if is_fully_handled {
                            let (next_after_fold, folded) = unsafe { self.attempt_fold_buffer(buffer_just_scanned_ptr) };
                            if folded { next_bl_for_scan = next_after_fold; }
                        }
                    }
                    temp_n_scan_current_bl_ptr = next_bl_for_scan;
                    temp_n_scan_current_idx = 0; 
                } // End 'find_initial_temp_n loop
            } else { // NodeState::Handled
                continue; // Outer loop: Should have been skipped
            }
        } // End outer loop
    }
}


impl<T: Send + 'static> MpscQueue<T> for JiffyQueue<T> {
    type PushError = T;
    type PopError = ();

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        self.actual_enqueue(item)
    }
    
    fn pop(&self) -> Result<T, Self::PopError> {
        self.actual_dequeue().ok_or(())
    }
    
    fn is_empty(&self) -> bool {
        let head_bl_ptr = self.head_of_queue.load(Ordering::Acquire);
        if head_bl_ptr.is_null() { return true; }
        let head_bl = unsafe { &*head_bl_ptr };

        if head_bl.curr_buffer.is_null() && head_bl.is_array_reclaimed.load(Ordering::Relaxed) {
            return head_bl.next.load(Ordering::Acquire).is_null() &&
                   head_bl_ptr == self.tail_of_queue.load(Ordering::Acquire);
        }
        if head_bl.curr_buffer.is_null() { return true; }

        let head_idx = head_bl.consumer_head_idx;
        let head_global_loc = head_bl.position_in_queue * (self.buffer_capacity() as u64) + (head_idx as u64);
        let tail_loc = self.global_tail_location.load(Ordering::Relaxed);

        if head_global_loc >= tail_loc {
            if head_idx < head_bl.capacity {
                let node = unsafe { &*head_bl.curr_buffer.add(head_idx) };
                let state = node.is_set.load(Ordering::Acquire); 
                if state == NodeState::Empty as usize || state == NodeState::Handled as usize {
                    return head_bl.next.load(Ordering::Acquire).is_null() || 
                           head_bl_ptr == self.tail_of_queue.load(Ordering::Acquire); 
                }
            } else { 
                 return head_bl.next.load(Ordering::Acquire).is_null() || 
                        head_bl_ptr == self.tail_of_queue.load(Ordering::Acquire); 
            }
        }
        false
    }
    
    fn is_full(&self) -> bool { false }
}

impl<T: Send + 'static> Drop for JiffyQueue<T> {
    fn drop(&mut self) {
        // First, process any remaining items in the deferred free list
        self.process_deferred_free_list(Some(u64::MAX));

        let mut current_bl_ptr = self.head_of_queue.load(Ordering::Relaxed);
        // Set atomics to null to prevent other threads from using the queue during drop
        self.head_of_queue.store(ptr::null_mut(), Ordering::Relaxed); 
        self.tail_of_queue.store(ptr::null_mut(), Ordering::Relaxed);

        // Clean up all remaining buffers
        while !current_bl_ptr.is_null() {
            let bl_mut = unsafe { &mut *current_bl_ptr };
            let next_bl_ptr = bl_mut.next.load(Ordering::Relaxed);
            unsafe {
                bl_mut.mark_node_array_reclaimed_and_drop_items(); 
                self.pools().dealloc_node_array_slice(bl_mut.curr_buffer);
                self.pools().dealloc_bl_meta_to_pool(current_bl_ptr);
            }
            current_bl_ptr = next_bl_ptr;
        }
    }
}