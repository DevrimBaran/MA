use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering, AtomicBool};
use std::ptr;
use std::mem::{MaybeUninit, align_of, size_of};
use std::fmt;

use crate::MpscQueue;



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
    unsafe fn init_in_place(node_ptr: *mut Self) {
        ptr::addr_of_mut!((*node_ptr).data).write(MaybeUninit::uninit());
        ptr::addr_of_mut!((*node_ptr).is_set).write(AtomicUsize::new(NodeState::Empty as usize));
    }
}

#[repr(C)]
struct BufferList<T> {
    curr_buffer: *mut Node<T>, 
    capacity: usize,
    next: AtomicPtr<BufferList<T>>,
    prev: *mut BufferList<T>,
    consumer_head_idx: usize,
    position_in_queue: u64,
    is_array_reclaimed: AtomicBool, 
    next_in_garbage: AtomicPtr<BufferList<T>>,
    next_free_meta: AtomicPtr<BufferList<T>>,
}

impl<T: Send + 'static> BufferList<T> {
    unsafe fn init_metadata_in_place(
        bl_meta_ptr: *mut Self,
        node_array_ptr: *mut Node<T>,
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
        ptr::addr_of_mut!((*bl_meta_ptr).next_in_garbage).write(AtomicPtr::new(ptr::null_mut()));
        ptr::addr_of_mut!((*bl_meta_ptr).next_free_meta).write(AtomicPtr::new(ptr::null_mut()));

        if !node_array_ptr.is_null() {
            for i in 0..capacity {
                Node::init_in_place(node_array_ptr.add(i));
            }
        }
    }

    unsafe fn mark_items_dropped_and_array_reclaimable(&mut self) {
        if self.curr_buffer.is_null() || self.is_array_reclaimed.load(Ordering::Relaxed) {
            if !self.curr_buffer.is_null() && !self.is_array_reclaimed.load(Ordering::Relaxed) {
                self.is_array_reclaimed.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed).ok();
            }
            return;
        }

        if self.is_array_reclaimed.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            if std::mem::needs_drop::<T>() {
                for i in 0..self.capacity {
                    let node_ptr = self.curr_buffer.add(i);
                    if (*node_ptr).is_set.load(Ordering::Relaxed) == NodeState::Set as usize {
                        ptr::drop_in_place((*node_ptr).data.as_mut_ptr());
                    }
                }
            }
        }
    }
}

#[repr(C)]
struct SharedPools<T: Send + 'static> {
    bl_meta_pool_start: *mut BufferList<T>,
    bl_meta_pool_capacity: usize,
    bl_meta_next_free_idx: AtomicUsize,
    bl_meta_free_list_head: AtomicPtr<BufferList<T>>,

    node_arrays_pool_start: *mut Node<T>, 
    node_arrays_pool_total_nodes: usize,  
    node_arrays_next_free_node_idx: AtomicUsize, 
    buffer_capacity_per_array: usize,     

    node_array_slice_free_list_head: AtomicPtr<Node<T>>,
}

impl<T: Send + 'static> SharedPools<T> {
    unsafe fn new_in_place(
        mem_ptr: *mut u8,
        mut current_offset: usize,
        max_buffers_meta: usize, 
        nodes_per_buffer: usize,
        total_node_capacity_for_pool: usize,
    ) -> (*mut Self, usize) {
        let self_align = align_of::<Self>();
        current_offset = (current_offset + self_align - 1) & !(self_align - 1);
        let pools_ptr = mem_ptr.add(current_offset) as *mut Self;
        current_offset += size_of::<Self>();

        let bl_meta_align = align_of::<BufferList<T>>();
        current_offset = (current_offset + bl_meta_align - 1) & !(bl_meta_align - 1);
        let bl_meta_pool_start_ptr = mem_ptr.add(current_offset) as *mut BufferList<T>;
        current_offset += max_buffers_meta * size_of::<BufferList<T>>();

        let node_align = align_of::<Node<T>>();
        current_offset = (current_offset + node_align - 1) & !(node_align - 1);
        let node_arrays_pool_start_ptr = mem_ptr.add(current_offset) as *mut Node<T>;
        current_offset += total_node_capacity_for_pool * size_of::<Node<T>>();

        ptr::addr_of_mut!((*pools_ptr).bl_meta_pool_start).write(bl_meta_pool_start_ptr);
        ptr::addr_of_mut!((*pools_ptr).bl_meta_pool_capacity).write(max_buffers_meta);
        ptr::addr_of_mut!((*pools_ptr).bl_meta_next_free_idx).write(AtomicUsize::new(0));
        ptr::addr_of_mut!((*pools_ptr).bl_meta_free_list_head).write(AtomicPtr::new(ptr::null_mut()));

        ptr::addr_of_mut!((*pools_ptr).node_arrays_pool_start).write(node_arrays_pool_start_ptr);
        ptr::addr_of_mut!((*pools_ptr).node_arrays_pool_total_nodes).write(total_node_capacity_for_pool);
        ptr::addr_of_mut!((*pools_ptr).node_arrays_next_free_node_idx).write(AtomicUsize::new(0));
        ptr::addr_of_mut!((*pools_ptr).buffer_capacity_per_array).write(nodes_per_buffer);
        ptr::addr_of_mut!((*pools_ptr).node_array_slice_free_list_head).write(AtomicPtr::new(ptr::null_mut()));

        (pools_ptr, current_offset)
    }

    unsafe fn alloc_bl_meta_with_node_array(
        &self,
        position_in_queue: u64,
        prev_buffer: *mut BufferList<T>,
    ) -> *mut BufferList<T> {
        loop {
            let head = self.bl_meta_free_list_head.load(Ordering::Acquire);
            if head.is_null() {
                break;
            }
            let next_free = (*head).next_free_meta.load(Ordering::Acquire);
            if self.bl_meta_free_list_head.compare_exchange(
                head, next_free, Ordering::AcqRel, Ordering::Acquire
            ).is_ok() {
                let node_array_ptr = self.alloc_node_array_slice();
                if node_array_ptr.is_null() {
                    let mut current_free_head_meta = self.bl_meta_free_list_head.load(Ordering::Acquire);
                    loop {
                        (*head).next_free_meta.store(current_free_head_meta, Ordering::Release);
                        match self.bl_meta_free_list_head.compare_exchange(
                            current_free_head_meta, head, Ordering::AcqRel, Ordering::Acquire ) {
                            Ok(_) => break,
                            Err(new_head_val) => current_free_head_meta = new_head_val,
                        }
                    }
                    return ptr::null_mut();
                }
                BufferList::init_metadata_in_place(head, node_array_ptr, self.buffer_capacity_per_array, position_in_queue, prev_buffer);
                return head;
            }
        }

        let meta_idx = self.bl_meta_next_free_idx.fetch_add(1, Ordering::AcqRel);
        if meta_idx >= self.bl_meta_pool_capacity {
            self.bl_meta_next_free_idx.fetch_sub(1, Ordering::Relaxed);
            return ptr::null_mut();
        }
        let bl_meta_ptr = self.bl_meta_pool_start.add(meta_idx);
        let node_array_ptr = self.alloc_node_array_slice();
        if node_array_ptr.is_null() {
            return ptr::null_mut();
        }
        BufferList::init_metadata_in_place(bl_meta_ptr, node_array_ptr, self.buffer_capacity_per_array, position_in_queue, prev_buffer);
        bl_meta_ptr
    }

    unsafe fn alloc_node_array_slice(&self) -> *mut Node<T> {
        loop {
            let free_head_slice = self.node_array_slice_free_list_head.load(Ordering::Acquire);
            if free_head_slice.is_null() {
                break; 
            }
            let next_free_in_list = (*(free_head_slice as *mut AtomicPtr<Node<T>>)).load(Ordering::Acquire);
            if self.node_array_slice_free_list_head.compare_exchange(
                free_head_slice,
                next_free_in_list,
                Ordering::AcqRel,
                Ordering::Relaxed, 
            ).is_ok() {
                return free_head_slice;
            }
        }

        let nodes_needed = self.buffer_capacity_per_array;
        let start_node_idx = self.node_arrays_next_free_node_idx.fetch_add(nodes_needed, Ordering::AcqRel);

        if start_node_idx.saturating_add(nodes_needed) > self.node_arrays_pool_total_nodes {
            self.node_arrays_next_free_node_idx.fetch_sub(nodes_needed, Ordering::Relaxed); 
            return ptr::null_mut();
        }
        self.node_arrays_pool_start.add(start_node_idx)
    }

    unsafe fn dealloc_bl_meta_to_pool(&self, bl_meta_ptr: *mut BufferList<T>) {
        if bl_meta_ptr.is_null() { return; }
        
        let mut current_head = self.bl_meta_free_list_head.load(Ordering::Acquire);
        loop {
            (*bl_meta_ptr).next_free_meta.store(current_head, Ordering::Release);
            match self.bl_meta_free_list_head.compare_exchange(
                current_head, bl_meta_ptr, Ordering::AcqRel, Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_head) => current_head = new_head,
            }
        }
    }
    
    unsafe fn dealloc_node_array_slice(&self, node_array_ptr: *mut Node<T>) {
        if node_array_ptr.is_null() {
            return;
        }
        loop {
            let current_free_head_slice = self.node_array_slice_free_list_head.load(Ordering::Acquire);
            (*(node_array_ptr as *mut AtomicPtr<Node<T>>)).store(current_free_head_slice, Ordering::Release);

            if self.node_array_slice_free_list_head.compare_exchange(
                current_free_head_slice,
                node_array_ptr, 
                Ordering::AcqRel, 
                Ordering::Relaxed,
            ).is_ok() {
                break; 
            }
        }
    }
}

#[repr(C)]
pub struct JiffyQueue<T: Send + 'static> {
    head_of_queue: AtomicPtr<BufferList<T>>,
    tail_of_queue: AtomicPtr<BufferList<T>>,
    global_tail_location: AtomicU64,
    pools: *const SharedPools<T>,
    garbage_list_head: AtomicPtr<BufferList<T>>,
}

unsafe impl<T: Send + 'static> Send for JiffyQueue<T> {}
unsafe impl<T: Send + 'static> Sync for JiffyQueue<T> {}

impl<T: Send + 'static> JiffyQueue<T> {
    pub fn shared_size(
        buffer_capacity_per_array: usize,
        max_buffers_in_pool: usize 
    ) -> usize {
        
        let num_buffer_slots_for_node_arrays = max_buffers_in_pool.max(10); 
        let total_node_capacity_for_pool = num_buffer_slots_for_node_arrays * buffer_capacity_per_array;
        let mut current_offset = 0;

        let jq_align = align_of::<JiffyQueue<T>>();
        current_offset = (current_offset + jq_align - 1) & !(jq_align - 1);
        current_offset += size_of::<JiffyQueue<T>>();

        let sp_align = align_of::<SharedPools<T>>();
        current_offset = (current_offset + sp_align - 1) & !(sp_align - 1);
        current_offset += size_of::<SharedPools<T>>();

        let bl_meta_align = align_of::<BufferList<T>>();
        current_offset = (current_offset + bl_meta_align - 1) & !(bl_meta_align - 1);
        current_offset += max_buffers_in_pool * size_of::<BufferList<T>>(); 

        let node_align = align_of::<Node<T>>();
        current_offset = (current_offset + node_align - 1) & !(node_align - 1);
        current_offset += total_node_capacity_for_pool * size_of::<Node<T>>();
        
        current_offset
    }

    pub unsafe fn init_in_shared(
        mem_ptr: *mut u8,
        buffer_capacity_per_array: usize,
        max_buffers_in_pool: usize 
    ) -> &'static mut Self {
        let num_buffer_slots_for_node_arrays = max_buffers_in_pool.max(10);
        let total_node_capacity_for_pool = num_buffer_slots_for_node_arrays * buffer_capacity_per_array;
        let mut current_offset = 0usize;

        let jq_align = align_of::<JiffyQueue<T>>();
        current_offset = (current_offset + jq_align - 1) & !(jq_align - 1);
        let queue_ptr = mem_ptr.add(current_offset) as *mut JiffyQueue<T>;
        current_offset += size_of::<JiffyQueue<T>>();

        let (pools_instance_ptr, _next_offset_after_pools) = SharedPools::<T>::new_in_place(
            mem_ptr, current_offset, max_buffers_in_pool, buffer_capacity_per_array, total_node_capacity_for_pool
        );
        
        let initial_bl_ptr = (*pools_instance_ptr).alloc_bl_meta_with_node_array(0, ptr::null_mut());
        if initial_bl_ptr.is_null() {
            panic!("JiffyQueue: Failed to allocate initial buffer from shared pool during init.");
        }

        ptr::addr_of_mut!((*queue_ptr).head_of_queue).write(AtomicPtr::new(initial_bl_ptr));
        ptr::addr_of_mut!((*queue_ptr).tail_of_queue).write(AtomicPtr::new(initial_bl_ptr));
        ptr::addr_of_mut!((*queue_ptr).global_tail_location).write(AtomicU64::new(0));
        ptr::addr_of_mut!((*queue_ptr).pools).write(pools_instance_ptr);
        ptr::addr_of_mut!((*queue_ptr).garbage_list_head).write(AtomicPtr::new(ptr::null_mut()));
        
        &mut *queue_ptr
    }

    fn buffer_capacity(&self) -> usize { unsafe { (*self.pools).buffer_capacity_per_array } }
    fn pools(&self) -> &SharedPools<T> { unsafe { &*self.pools } }

    fn actual_enqueue(&self, data: T) -> Result<(), T> {
        let item_global_location = self.global_tail_location.fetch_add(1, Ordering::AcqRel);
        let mut current_producer_view_of_tail_bl = self.tail_of_queue.load(Ordering::Acquire);
        let mut new_bl_allocated_by_this_thread: *mut BufferList<T> = ptr::null_mut();

        loop {
            if current_producer_view_of_tail_bl.is_null() { 
                if !new_bl_allocated_by_this_thread.is_null() {
                    unsafe {
                        let bl_meta_ptr = new_bl_allocated_by_this_thread;
                        let node_array_to_dealloc = (*bl_meta_ptr).curr_buffer;
                        (*bl_meta_ptr).mark_items_dropped_and_array_reclaimable(); 
                        if !node_array_to_dealloc.is_null() {
                            self.pools().dealloc_node_array_slice(node_array_to_dealloc);
                        }
                        (*bl_meta_ptr).curr_buffer = ptr::null_mut();
                        self.pools().dealloc_bl_meta_to_pool(bl_meta_ptr);
                    }
                }
                return Err(data);
            }
            let tail_bl_ref = unsafe { &*current_producer_view_of_tail_bl };
            let current_buffer_cap = self.buffer_capacity();

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
                        if new_bl_allocated_by_this_thread.is_null() { return Err(data); } 
                    }
                    match tail_bl_ref.next.compare_exchange(ptr::null_mut(), new_bl_allocated_by_this_thread, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => { 
                            self.tail_of_queue.compare_exchange(current_producer_view_of_tail_bl, new_bl_allocated_by_this_thread, Ordering::AcqRel, Ordering::Relaxed).ok();
                            next_bl_in_list = new_bl_allocated_by_this_thread;
                            new_bl_allocated_by_this_thread = ptr::null_mut(); 
                        }
                        Err(actual_next) => { 
                            next_bl_in_list = actual_next;
                            if !new_bl_allocated_by_this_thread.is_null() { 
                                unsafe {
                                    let bl_meta_ptr = new_bl_allocated_by_this_thread;
                                    let node_array_to_dealloc = (*bl_meta_ptr).curr_buffer;
                                    (*bl_meta_ptr).mark_items_dropped_and_array_reclaimable();
                                    if !node_array_to_dealloc.is_null() {
                                        self.pools().dealloc_node_array_slice(node_array_to_dealloc);
                                    }
                                    (*bl_meta_ptr).curr_buffer = ptr::null_mut();
                                    self.pools().dealloc_bl_meta_to_pool(bl_meta_ptr);
                                }
                                new_bl_allocated_by_this_thread = ptr::null_mut();
                            }
                        }
                    }
                }
                if !next_bl_in_list.is_null() { 
                    self.tail_of_queue.compare_exchange(current_producer_view_of_tail_bl, next_bl_in_list, Ordering::AcqRel, Ordering::Relaxed).ok();
                    current_producer_view_of_tail_bl = next_bl_in_list;
                } else {
                    current_producer_view_of_tail_bl = self.tail_of_queue.load(Ordering::Acquire);
                }
                continue;
            } else if item_global_location < tail_bl_start_loc { 
                current_producer_view_of_tail_bl = tail_bl_ref.prev;
                if current_producer_view_of_tail_bl.is_null() { 
                    if !new_bl_allocated_by_this_thread.is_null() {
                        unsafe {
                            let bl_meta_ptr = new_bl_allocated_by_this_thread;
                            let node_array_to_dealloc = (*bl_meta_ptr).curr_buffer;
                            (*bl_meta_ptr).mark_items_dropped_and_array_reclaimable();
                            if !node_array_to_dealloc.is_null() {
                                self.pools().dealloc_node_array_slice(node_array_to_dealloc);
                            }
                            (*bl_meta_ptr).curr_buffer = ptr::null_mut();
                            self.pools().dealloc_bl_meta_to_pool(bl_meta_ptr);
                        }
                    }
                    return Err(data);
                }
                continue;
            } else { 
                let internal_idx = (item_global_location - tail_bl_start_loc) as usize;
                if internal_idx >= tail_bl_ref.capacity { 
                    current_producer_view_of_tail_bl = self.tail_of_queue.load(Ordering::Acquire); 
                    continue;
                }
                if tail_bl_ref.curr_buffer.is_null() || tail_bl_ref.is_array_reclaimed.load(Ordering::Relaxed) { 
                    current_producer_view_of_tail_bl = self.tail_of_queue.load(Ordering::Acquire);
                    continue;
                }

                let node_ptr = unsafe { tail_bl_ref.curr_buffer.add(internal_idx) };
                unsafe {
                    ptr::write(&mut (*node_ptr).data, MaybeUninit::new(data));
                    (*node_ptr).is_set.store(NodeState::Set as usize, Ordering::Release);
                }

                let is_globally_last_buffer = tail_bl_ref.next.load(Ordering::Acquire).is_null() && current_producer_view_of_tail_bl == self.tail_of_queue.load(Ordering::Relaxed);
                if internal_idx == 1 && is_globally_last_buffer && self.buffer_capacity() > 1 { 
                    let prealloc_bl = unsafe {
                        self.pools().alloc_bl_meta_with_node_array(
                            tail_bl_ref.position_in_queue + 1,
                            current_producer_view_of_tail_bl
                        )
                    };
                    if !prealloc_bl.is_null() {
                        if tail_bl_ref.next.compare_exchange(ptr::null_mut(), prealloc_bl, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                            self.tail_of_queue.compare_exchange(current_producer_view_of_tail_bl, prealloc_bl, Ordering::AcqRel, Ordering::Relaxed).ok();
                        } else { 
                            unsafe {
                                let bl_meta_ptr = prealloc_bl;
                                let node_array_to_dealloc = (*bl_meta_ptr).curr_buffer;
                                (*bl_meta_ptr).mark_items_dropped_and_array_reclaimable();
                                if !node_array_to_dealloc.is_null() {
                                    self.pools().dealloc_node_array_slice(node_array_to_dealloc);
                                }
                                (*bl_meta_ptr).curr_buffer = ptr::null_mut();
                                self.pools().dealloc_bl_meta_to_pool(bl_meta_ptr);
                            }
                        }
                    }
                }

                if !new_bl_allocated_by_this_thread.is_null() {
                    unsafe {
                        let bl_meta_ptr = new_bl_allocated_by_this_thread;
                        let node_array_to_dealloc = (*bl_meta_ptr).curr_buffer;
                        (*bl_meta_ptr).mark_items_dropped_and_array_reclaimable();
                        if !node_array_to_dealloc.is_null() {
                            self.pools().dealloc_node_array_slice(node_array_to_dealloc);
                        }
                        (*bl_meta_ptr).curr_buffer = ptr::null_mut();
                        self.pools().dealloc_bl_meta_to_pool(bl_meta_ptr);
                    }
                }
                return Ok(());
            }
        }
    }

    unsafe fn attempt_fold_buffer(&self, bl_to_fold_ptr: *mut BufferList<T>) -> (*mut BufferList<T>, bool) {
        let current_head_main_q = self.head_of_queue.load(Ordering::Acquire);
        if bl_to_fold_ptr.is_null() || bl_to_fold_ptr == current_head_main_q {
            return (bl_to_fold_ptr, false);
        }

        let bl_to_fold_mut_ref = &mut *bl_to_fold_ptr;
        let prev_bl_ptr = bl_to_fold_mut_ref.prev;
        let next_bl_ptr_for_scan = bl_to_fold_mut_ref.next.load(Ordering::Acquire);

        if prev_bl_ptr.is_null() { 
            return (bl_to_fold_ptr, false);
        }

        let prev_bl_ref = &*prev_bl_ptr;

        match prev_bl_ref.next.compare_exchange(
            bl_to_fold_ptr, next_bl_ptr_for_scan, Ordering::AcqRel, Ordering::Acquire
        ) {
            Ok(_) => { 
                if !next_bl_ptr_for_scan.is_null() {
                    (*next_bl_ptr_for_scan).prev = prev_bl_ptr;
                }

                let node_array_to_dealloc = bl_to_fold_mut_ref.curr_buffer;
                bl_to_fold_mut_ref.mark_items_dropped_and_array_reclaimable();
                if !node_array_to_dealloc.is_null() { 
                    self.pools().dealloc_node_array_slice(node_array_to_dealloc);
                }
                bl_to_fold_mut_ref.curr_buffer = ptr::null_mut();


                let mut current_garbage_head = self.garbage_list_head.load(Ordering::Relaxed); 
                loop {
                    (*bl_to_fold_ptr).next_in_garbage.store(current_garbage_head, Ordering::Release);
                    match self.garbage_list_head.compare_exchange(
                        current_garbage_head, bl_to_fold_ptr,
                        Ordering::AcqRel, Ordering::Relaxed
                    ) {
                        Ok(_) => break,
                        Err(new_head) => current_garbage_head = new_head,
                    }
                }
                (next_bl_ptr_for_scan, true)
            }
            Err(_actual_next) => { 
                (bl_to_fold_ptr, false)
            }
        }
    }

    fn actual_process_garbage_list(&self, new_head_buffer_pos_threshold: u64) {
        let mut garbage_to_process_head = self.garbage_list_head.swap(ptr::null_mut(), Ordering::Acquire);
        if garbage_to_process_head.is_null() {
            return;
        }
        
        let mut still_deferred_list_head: *mut BufferList<T> = ptr::null_mut();
        let mut still_deferred_list_tail: *mut BufferList<T> = ptr::null_mut();

        while !garbage_to_process_head.is_null() {
            let current_garbage_item_ptr = garbage_to_process_head;
            let item_ref = unsafe { &*current_garbage_item_ptr };
            garbage_to_process_head = item_ref.next_in_garbage.load(Ordering::Relaxed); 
            
            let metadata_pos = item_ref.position_in_queue;

            if metadata_pos < new_head_buffer_pos_threshold {
                unsafe {
                    self.pools().dealloc_bl_meta_to_pool(current_garbage_item_ptr);
                }
            } else {
                
                unsafe { (*current_garbage_item_ptr).next_in_garbage.store(still_deferred_list_head, Ordering::Relaxed); }
                still_deferred_list_head = current_garbage_item_ptr;
                if still_deferred_list_tail.is_null() { 
                    still_deferred_list_tail = current_garbage_item_ptr;
                }
            }
        }

        if !still_deferred_list_head.is_null() {
            if still_deferred_list_tail.is_null() { 
                still_deferred_list_tail = still_deferred_list_head;
                unsafe { 
                    while !(*still_deferred_list_tail).next_in_garbage.load(Ordering::Relaxed).is_null() {
                        still_deferred_list_tail = (*still_deferred_list_tail).next_in_garbage.load(Ordering::Relaxed);
                    }
                }
            }
            
            let mut current_global_garbage_head = self.garbage_list_head.load(Ordering::Acquire);
            loop {
                unsafe { (*still_deferred_list_tail).next_in_garbage.store(current_global_garbage_head, Ordering::Release); }
                
                match self.garbage_list_head.compare_exchange(
                    current_global_garbage_head, 
                    still_deferred_list_head,    
                    Ordering::AcqRel,
                    Ordering::Acquire, 
                ) {
                    Ok(_) => break, 
                    Err(new_global_head) => current_global_garbage_head = new_global_head, 
                }
            }
        }
    }
    
    fn actual_dequeue(&self) -> Option<T> {
        'retry_dequeue: loop {
            let current_bl_ptr = self.head_of_queue.load(Ordering::Acquire);

            if current_bl_ptr.is_null() {
                return None;
            }

            let current_bl = unsafe { &mut *current_bl_ptr }; 

            while current_bl.consumer_head_idx < current_bl.capacity {
                if current_bl.curr_buffer.is_null() || current_bl.is_array_reclaimed.load(Ordering::Relaxed) {
                    break; 
                }
                let node_to_check_ptr = unsafe { current_bl.curr_buffer.add(current_bl.consumer_head_idx) };
                let node_to_check_state = unsafe { (*node_to_check_ptr).is_set.load(Ordering::Acquire) };

                if node_to_check_state == NodeState::Handled as usize {
                    current_bl.consumer_head_idx += 1;
                } else {
                    break; 
                }
            }
            
            if current_bl.consumer_head_idx >= current_bl.capacity || current_bl.curr_buffer.is_null() || current_bl.is_array_reclaimed.load(Ordering::Relaxed) {
                let next_bl_candidate = current_bl.next.load(Ordering::Acquire);
                let new_head_pos_opt = if next_bl_candidate.is_null() { None } else { Some(unsafe { (*next_bl_candidate).position_in_queue }) };
                
                if !next_bl_candidate.is_null() || current_bl.curr_buffer.is_null() || current_bl.is_array_reclaimed.load(Ordering::Relaxed) {
                    let threshold = new_head_pos_opt.unwrap_or(u64::MAX); 
                    self.actual_process_garbage_list(threshold);
                }

                if self.head_of_queue.compare_exchange(current_bl_ptr, next_bl_candidate, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    if !next_bl_candidate.is_null() { 
                        unsafe { (*next_bl_candidate).prev = ptr::null_mut(); }
                    }
                    unsafe {
                        let node_array_to_dealloc = current_bl.curr_buffer;
                        current_bl.mark_items_dropped_and_array_reclaimable(); 
                        if !node_array_to_dealloc.is_null() { 
                            self.pools().dealloc_node_array_slice(node_array_to_dealloc);
                        }
                        current_bl.curr_buffer = ptr::null_mut(); 
                        self.pools().dealloc_bl_meta_to_pool(current_bl_ptr);
                    }
                }
                continue 'retry_dequeue; 
            }
            
            let n_idx_in_buffer = current_bl.consumer_head_idx;
            if n_idx_in_buffer >= current_bl.capacity { continue 'retry_dequeue; } 
            if current_bl.curr_buffer.is_null() { continue 'retry_dequeue; }

            let n_node_ptr = unsafe { current_bl.curr_buffer.add(n_idx_in_buffer) };
            let n_node_ref = unsafe { &*n_node_ptr }; 
            let n_state = n_node_ref.is_set.load(Ordering::Acquire);

            let n_global_loc = current_bl.position_in_queue * (self.buffer_capacity() as u64) + (n_idx_in_buffer as u64);
            let tail_loc = self.global_tail_location.load(Ordering::Acquire);

            if n_global_loc >= tail_loc && (n_state == NodeState::Empty as usize || n_state == NodeState::Handled as usize) && current_bl_ptr == self.tail_of_queue.load(Ordering::Acquire) {
                return None;
            }

            if n_state == NodeState::Set as usize { 
                if n_node_ref.is_set.compare_exchange(
                    NodeState::Set as usize, NodeState::Handled as usize, Ordering::AcqRel, Ordering::Relaxed
                ).is_ok() {
                    current_bl.consumer_head_idx += 1; 
                    let data = unsafe { ptr::read(&(*n_node_ref).data).assume_init() }; 
                    return Some(data); 
                } else { 
                    continue 'retry_dequeue;
                } 
            } 
            else if n_state == NodeState::Empty as usize { 
                let mut temp_n_scan_current_bl_ptr = current_bl_ptr;
                let mut temp_n_scan_current_idx = if temp_n_scan_current_bl_ptr == current_bl_ptr { n_idx_in_buffer + 1 } else { 0 };

                'find_initial_temp_n: loop {
                    if temp_n_scan_current_bl_ptr.is_null() { return None; } 
                    let search_bl_mut = unsafe { &mut *temp_n_scan_current_bl_ptr };

                    if search_bl_mut.curr_buffer.is_null() || search_bl_mut.is_array_reclaimed.load(Ordering::Relaxed) {
                        temp_n_scan_current_bl_ptr = search_bl_mut.next.load(Ordering::Acquire);
                        temp_n_scan_current_idx = 0;
                        continue 'find_initial_temp_n;
                    }
                    
                    let mut scan_idx = temp_n_scan_current_idx;
                    let mut found_set_in_search_bl = false;

                    while scan_idx < search_bl_mut.capacity {
                        let candidate_node_ptr = unsafe { search_bl_mut.curr_buffer.add(scan_idx) };
                        let candidate_node_state = unsafe { (*candidate_node_ptr).is_set.load(Ordering::Acquire) };

                        if candidate_node_state == NodeState::Set as usize {
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

                                    if r_bl.curr_buffer.is_null() || r_bl.is_array_reclaimed.load(Ordering::Relaxed) {
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
                                    let e_node_ptr = unsafe { r_bl.curr_buffer.add(rescan_idx_in_buf) };
                                    let e_node_state = unsafe { (*e_node_ptr).is_set.load(Ordering::Acquire) };

                                    if e_node_state == NodeState::Set as usize { 
                                        final_temp_n_bl_ptr = rescan_bl_ptr; 
                                        final_temp_n_idx = rescan_idx_in_buf;
                                        earlier_set_found_this_pass = true; 
                                        break; 
                                    }
                                    rescan_idx_in_buf += 1;
                                } 
                                if !earlier_set_found_this_pass { break 'rescan_phase; } 
                            } 

                            let item_bl_ref = unsafe { &*final_temp_n_bl_ptr }; 
                            if item_bl_ref.curr_buffer.is_null() || item_bl_ref.is_array_reclaimed.load(Ordering::Relaxed) {
                                continue 'retry_dequeue; 
                            }
                            let item_node_ptr_to_cas = unsafe { item_bl_ref.curr_buffer.add(final_temp_n_idx) };
                            let item_node_ref_for_cas = unsafe { &*item_node_ptr_to_cas };
                            
                            if item_node_ref_for_cas.is_set.compare_exchange(
                                NodeState::Set as usize, NodeState::Handled as usize, Ordering::AcqRel, Ordering::Relaxed
                            ).is_ok() { 
                                if final_temp_n_bl_ptr == current_bl_ptr && final_temp_n_idx == current_bl.consumer_head_idx {
                                    current_bl.consumer_head_idx +=1; 
                                }
                                let data = unsafe { ptr::read(&(*item_node_ref_for_cas).data).assume_init() };
                                return Some(data);
                            } else { 
                                continue 'retry_dequeue; 
                            } 
                        } 
                        scan_idx += 1;
                    } 
                    let buffer_just_scanned_ptr = temp_n_scan_current_bl_ptr;
                    let mut next_bl_for_scan = search_bl_mut.next.load(Ordering::Acquire);

                    if !found_set_in_search_bl && buffer_just_scanned_ptr != current_bl_ptr { 
                        let mut is_fully_handled = true;
                        if search_bl_mut.curr_buffer.is_null() || search_bl_mut.is_array_reclaimed.load(Ordering::Relaxed) {
                            if !search_bl_mut.is_array_reclaimed.load(Ordering::Relaxed) { 
                                is_fully_handled = false; 
                            }
                        } else { 
                            for i in 0..search_bl_mut.capacity {
                                if unsafe{(*search_bl_mut.curr_buffer.add(i)).is_set.load(Ordering::Acquire)} != NodeState::Handled as usize {
                                    is_fully_handled = false; 
                                    break;
                                }
                            }
                        }
                        
                        if is_fully_handled {
                            let (_next_after_fold, folded) = unsafe { self.attempt_fold_buffer(buffer_just_scanned_ptr) };
                            if folded {
                                continue 'retry_dequeue; 
                            }
                        }
                    }
                    temp_n_scan_current_bl_ptr = next_bl_for_scan; 
                    temp_n_scan_current_idx = 0; 
                } 
            } else { 
                continue 'retry_dequeue; 
            }
        } 
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
    
        if head_bl.curr_buffer.is_null() || head_bl.is_array_reclaimed.load(Ordering::Relaxed) { 
            return head_bl.next.load(Ordering::Acquire).is_null() && head_bl_ptr == self.tail_of_queue.load(Ordering::Acquire);
        }
    
        let mut temp_head_idx = head_bl.consumer_head_idx; 
        
        
        while temp_head_idx < head_bl.capacity {
            if head_bl.curr_buffer.is_null() || head_bl.is_array_reclaimed.load(Ordering::Relaxed) { 
                return head_bl.next.load(Ordering::Acquire).is_null() && head_bl_ptr == self.tail_of_queue.load(Ordering::Acquire);
            }
            let node_state = unsafe { (*head_bl.curr_buffer.add(temp_head_idx)).is_set.load(Ordering::Acquire) };
            if node_state == NodeState::Handled as usize {
                temp_head_idx += 1;
            } else {
                break; 
            }
        }

        if temp_head_idx >= head_bl.capacity {
            return head_bl.next.load(Ordering::Acquire).is_null() && head_bl_ptr == self.tail_of_queue.load(Ordering::Acquire);
        }
    
        let node_at_temp_head_idx = unsafe { &*head_bl.curr_buffer.add(temp_head_idx) };
        let state_at_temp_head_idx = node_at_temp_head_idx.is_set.load(Ordering::Acquire);

        if state_at_temp_head_idx == NodeState::Set as usize {
            return false; 
        }
        
        if state_at_temp_head_idx == NodeState::Empty as usize {
            let tail_loc = self.global_tail_location.load(Ordering::Acquire);
            let current_item_global_loc = head_bl.position_in_queue * (self.buffer_capacity() as u64) + (temp_head_idx as u64);
            
            if current_item_global_loc >= tail_loc {
                if head_bl.next.load(Ordering::Acquire).is_null() && head_bl_ptr == self.tail_of_queue.load(Ordering::Acquire) {
                    return true;
                }
            }
        }
        
        false 
    }
    
    fn is_full(&self) -> bool { false }
}

impl<T: Send + 'static> Drop for JiffyQueue<T> {
    fn drop(&mut self) {
        self.actual_process_garbage_list(u64::MAX);

        let mut current_bl_ptr = self.head_of_queue.load(Ordering::Relaxed);
        self.head_of_queue.store(ptr::null_mut(), Ordering::Relaxed);
        self.tail_of_queue.store(ptr::null_mut(), Ordering::Relaxed);

        while !current_bl_ptr.is_null() {
            let bl_mut = unsafe { &mut *current_bl_ptr };
            let next_bl_ptr = bl_mut.next.load(Ordering::Relaxed);
            
            unsafe {
                let node_array_to_dealloc = bl_mut.curr_buffer;
                bl_mut.mark_items_dropped_and_array_reclaimable(); 
                if !node_array_to_dealloc.is_null() {
                    self.pools().dealloc_node_array_slice(node_array_to_dealloc);
                }
                bl_mut.curr_buffer = ptr::null_mut(); 
                self.pools().dealloc_bl_meta_to_pool(current_bl_ptr);
            }
            current_bl_ptr = next_bl_ptr;
        }
    }
}