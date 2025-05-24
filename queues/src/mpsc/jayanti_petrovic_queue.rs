use std::ptr;
use std::mem::{self, MaybeUninit};
use std::sync::atomic::{AtomicU64, AtomicPtr, Ordering, fence};
use std::cmp::Ordering as CmpOrdering;

use crate::MpscQueue as MpscQueueTrait;
use super::sesd_jp_queue::{SesdJpQueue, Node as SesdNode};

#[repr(C)]
struct ShmBumpPool<T: Send + Clone + 'static> {
    base: AtomicPtr<u8>,
    current: AtomicPtr<u8>,
    end: *mut u8,
    free_list_head: AtomicPtr<SesdNode<(T, Timestamp)>>,
}

impl<T: Send + Clone + 'static> ShmBumpPool<T> {
    unsafe fn new(start_ptr: *mut u8, size_bytes: usize) -> Self {
        ShmBumpPool {
            base: AtomicPtr::new(start_ptr),
            current: AtomicPtr::new(start_ptr),
            end: start_ptr.add(size_bytes),
            free_list_head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    unsafe fn free_sesd_node(&self, node_ptr: *mut SesdNode<(T, Timestamp)>) {
        if node_ptr.is_null() {
            return;
        }
        let mut head = self.free_list_head.load(Ordering::Acquire);
        loop {
            (*node_ptr).next.store(head, Ordering::Relaxed);
            match self.free_list_head.compare_exchange_weak(
                head,
                node_ptr,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(new_head) => head = new_head,
            }
        }
    }

    unsafe fn alloc_sesd_node(&self) -> *mut SesdNode<(T, Timestamp)> {
        let mut head = self.free_list_head.load(Ordering::Acquire);
        while !head.is_null() {
            let next_node_in_free_list = (*head).next.load(Ordering::Relaxed);
            match self.free_list_head.compare_exchange_weak(
                head,
                next_node_in_free_list,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(popped_node) => {
                    SesdNode::init_dummy(popped_node);
                    return popped_node;
                }
                Err(new_head) => head = new_head,
            }
        }

        let align = mem::align_of::<SesdNode<(T, Timestamp)>>();
        let size = mem::size_of::<SesdNode<(T, Timestamp)>>();
        
        loop {
            let current_ptr_val = self.current.load(Ordering::Relaxed);
            let mut alloc_ptr_usize = current_ptr_val as usize;

            let remainder = alloc_ptr_usize % align;
            if remainder != 0 {
                alloc_ptr_usize += align - remainder;
            }
            
            let next_ptr_val_after_alloc = alloc_ptr_usize + size;

            if next_ptr_val_after_alloc > self.end as usize {
                return ptr::null_mut(); 
            }

            match self.current.compare_exchange(
                current_ptr_val,
                next_ptr_val_after_alloc as *mut u8,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let allocated_node_ptr = alloc_ptr_usize as *mut SesdNode<(T, Timestamp)>;
                    SesdNode::init_dummy(allocated_node_ptr);
                    return allocated_node_ptr;
                }
                Err(_) => {}
            }
        }
    }
}



#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timestamp {
    val: u64,
    pid: usize,
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.val.cmp(&other.val)
            .then_with(|| self.pid.cmp(&other.pid))
    }
}

pub const INFINITY_TS: Timestamp = Timestamp { val: u64::MAX, pid: usize::MAX };

#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct MinInfo {
    ts: Timestamp,
    leaf_idx: usize,
}

impl MinInfo {
    fn infinite() -> Self {
        MinInfo { ts: INFINITY_TS, leaf_idx: usize::MAX }
    }
    fn new(ts: Timestamp, leaf_idx: usize) -> Self {
        MinInfo { ts, leaf_idx }
    }
}


#[repr(C)]
struct TreeNode {
    min_info_ptr: AtomicPtr<MinInfo>,
}

impl TreeNode {
    unsafe fn init_in_shm(node_ptr: *mut Self, initial_min_info_instance_ptr: *mut MinInfo) {
        ptr::addr_of_mut!((*initial_min_info_instance_ptr)).write(MinInfo::infinite());
        ptr::write(node_ptr, TreeNode {
            min_info_ptr: AtomicPtr::new(initial_min_info_instance_ptr),
        });
    }

    #[inline]
    unsafe fn read_min_info(&self) -> MinInfo {
        let ptr = self.min_info_ptr.load(Ordering::Acquire);
        *ptr 
    }
    
    #[inline]
    unsafe fn update_min_info_value_in_slot(&self, new_value: MinInfo) {
        let slot_ptr = self.min_info_ptr.load(Ordering::Relaxed); 
        slot_ptr.write(new_value); 
        fence(Ordering::Release); 
    }
}


#[repr(C)]
pub struct JayantiPetrovicMpscQueue<T: Send + Clone + 'static> {
    counter: AtomicU64, 
    num_producers: usize,
    local_queues_base: *mut SesdJpQueue<(T, Timestamp)>,
    tree_nodes_base: *mut TreeNode,
    min_info_slots_base: *mut MinInfo, 
    sesd_initial_dummies_base: *mut SesdNode<(T, Timestamp)>,
    sesd_help_slots_base: *mut MaybeUninit<(T, Timestamp)>,
    sesd_free_later_dummies_base: *mut SesdNode<(T, Timestamp)>,
    sesd_node_pool: ShmBumpPool<T>,
}

unsafe impl<T: Send + Clone + 'static> Send for JayantiPetrovicMpscQueue<T> {}
unsafe impl<T: Send + Clone + 'static> Sync for JayantiPetrovicMpscQueue<T> {}

impl<T: Send + Clone + 'static> JayantiPetrovicMpscQueue<T> {
    pub fn shared_size(num_producers: usize, sesd_node_pool_capacity: usize) -> usize {
        if num_producers == 0 { return mem::size_of::<Self>(); } 
        let tree_node_count = 2 * num_producers - 1;
        
        let self_size = mem::size_of::<Self>();
        let lq_structs_size = num_producers * mem::size_of::<SesdJpQueue<(T, Timestamp)>>();
        let tree_node_structs_size = tree_node_count * mem::size_of::<TreeNode>();
        let min_info_slots_size = tree_node_count * mem::size_of::<MinInfo>();
        let sesd_initial_dummies_size = num_producers * mem::size_of::<SesdNode<(T, Timestamp)>>();
        let sesd_help_slots_size = num_producers * mem::size_of::<MaybeUninit<(T, Timestamp)>>();
        let sesd_free_later_dummies_size = num_producers * mem::size_of::<SesdNode<(T, Timestamp)>>();
        let sesd_node_pool_managed_bytes = sesd_node_pool_capacity * mem::size_of::<SesdNode<(T, Timestamp)>>();

        let align_offset = |offset: usize, alignment: usize| (offset + alignment - 1) & !(alignment - 1);

        let mut total_size = 0;
        total_size = align_offset(total_size, mem::align_of::<Self>()) + self_size;
        total_size = align_offset(total_size, mem::align_of::<SesdJpQueue<(T, Timestamp)>>()) + lq_structs_size;
        total_size = align_offset(total_size, mem::align_of::<TreeNode>()) + tree_node_structs_size;
        total_size = align_offset(total_size, mem::align_of::<MinInfo>()) + min_info_slots_size;
        total_size = align_offset(total_size, mem::align_of::<SesdNode<(T, Timestamp)>>()) + sesd_initial_dummies_size;
        total_size = align_offset(total_size, mem::align_of::<MaybeUninit<(T, Timestamp)>>()) + sesd_help_slots_size;
        total_size = align_offset(total_size, mem::align_of::<SesdNode<(T, Timestamp)>>()) + sesd_free_later_dummies_size;
        total_size = align_offset(total_size, mem::align_of::<u8>()) + sesd_node_pool_managed_bytes; 
        
        total_size
    }

    pub unsafe fn init_in_shared(
        mem_ptr: *mut u8,
        num_producers: usize,
        sesd_node_pool_capacity: usize,
    ) -> &'static mut Self {
        assert!(num_producers > 0, "Number of producers must be > 0");

        let tree_node_count = 2 * num_producers - 1;
        let mut current_offset = 0usize;

        let align_offset = |offset: usize, alignment: usize| (offset + alignment - 1) & !(alignment - 1);

        current_offset = align_offset(current_offset, mem::align_of::<Self>());
        let self_ptr = mem_ptr.add(current_offset) as *mut Self;
        current_offset += mem::size_of::<Self>();

        current_offset = align_offset(current_offset, mem::align_of::<SesdJpQueue<(T, Timestamp)>>());
        let lq_base_ptr = mem_ptr.add(current_offset) as *mut SesdJpQueue<(T, Timestamp)>;
        current_offset += num_producers * mem::size_of::<SesdJpQueue<(T, Timestamp)>>();

        current_offset = align_offset(current_offset, mem::align_of::<TreeNode>());
        let tree_base_ptr = mem_ptr.add(current_offset) as *mut TreeNode;
        current_offset += tree_node_count * mem::size_of::<TreeNode>();
        
        current_offset = align_offset(current_offset, mem::align_of::<MinInfo>());
        let min_info_slots_ptr = mem_ptr.add(current_offset) as *mut MinInfo;
        current_offset += tree_node_count * mem::size_of::<MinInfo>();

        current_offset = align_offset(current_offset, mem::align_of::<SesdNode<(T, Timestamp)>>());
        let sesd_initial_dummies_ptr = mem_ptr.add(current_offset) as *mut SesdNode<(T, Timestamp)>;
        current_offset += num_producers * mem::size_of::<SesdNode<(T, Timestamp)>>();

        current_offset = align_offset(current_offset, mem::align_of::<MaybeUninit<(T, Timestamp)>>());
        let sesd_help_slots_ptr = mem_ptr.add(current_offset) as *mut MaybeUninit<(T, Timestamp)>;
        current_offset += num_producers * mem::size_of::<MaybeUninit<(T, Timestamp)>>();

        current_offset = align_offset(current_offset, mem::align_of::<SesdNode<(T, Timestamp)>>());
        let sesd_free_later_dummies_ptr = mem_ptr.add(current_offset) as *mut SesdNode<(T, Timestamp)>;
        current_offset += num_producers * mem::size_of::<SesdNode<(T, Timestamp)>>();

        current_offset = align_offset(current_offset, mem::align_of::<u8>()); 
        let sesd_node_pool_start_ptr = mem_ptr.add(current_offset) as *mut u8;
        let sesd_node_pool_managed_bytes = sesd_node_pool_capacity * mem::size_of::<SesdNode<(T, Timestamp)>>();

        ptr::write(self_ptr, Self {
            counter: AtomicU64::new(0),
            num_producers,
            local_queues_base: lq_base_ptr,
            tree_nodes_base: tree_base_ptr,
            min_info_slots_base: min_info_slots_ptr,
            sesd_initial_dummies_base: sesd_initial_dummies_ptr,
            sesd_help_slots_base: sesd_help_slots_ptr,
            sesd_free_later_dummies_base: sesd_free_later_dummies_ptr,
            sesd_node_pool: ShmBumpPool::new(sesd_node_pool_start_ptr, sesd_node_pool_managed_bytes),
        });

        let queue_ref = &mut *self_ptr;

        for i in 0..tree_node_count {
            let tree_node_raw_ptr = queue_ref.tree_nodes_base.add(i);
            let min_info_instance_raw_ptr = queue_ref.min_info_slots_base.add(i); 
            TreeNode::init_in_shm(tree_node_raw_ptr, min_info_instance_raw_ptr);
        }
        
        for i in 0..num_producers {
            let lq_ptr = queue_ref.local_queues_base.add(i);
            let initial_dummy_node = queue_ref.sesd_initial_dummies_base.add(i);
            let help_slot = queue_ref.sesd_help_slots_base.add(i);
            let free_later_dummy = queue_ref.sesd_free_later_dummies_base.add(i);
            SesdJpQueue::new_in_shm(lq_ptr, initial_dummy_node, help_slot, free_later_dummy);
        }
        
        queue_ref
    }

    #[inline]
    unsafe fn get_local_queue(&self, producer_id: usize) -> &SesdJpQueue<(T, Timestamp)> {
        &*self.local_queues_base.add(producer_id)
    }
    
    #[inline]
    unsafe fn get_tree_node(&self, node_idx: usize) -> &TreeNode {
        &*self.tree_nodes_base.add(node_idx)
    }

    #[inline]
    fn get_leaf_tree_node_idx(&self, producer_id: usize) -> usize {
        (self.num_producers - 1) + producer_id
    }
    
    #[inline]
    fn get_parent_idx(&self, tree_node_idx: usize) -> Option<usize> {
        if tree_node_idx == 0 { None } else { Some((tree_node_idx - 1) / 2) }
    }

    #[inline]
    fn get_children_indices(&self, tree_node_idx: usize) -> (Option<usize>, Option<usize>) {
        let left_idx = 2 * tree_node_idx + 1;
        let right_idx = 2 * tree_node_idx + 2;
        let max_node_idx = 2 * self.num_producers - 2; 
        (
            if left_idx <= max_node_idx { Some(left_idx) } else { None },
            if right_idx <= max_node_idx { Some(right_idx) } else { None }
        )
    }
    
    unsafe fn refresh(&self, u_idx: usize) {
        let u_node = self.get_tree_node(u_idx);
        let u_min_info_slot_ptr = u_node.min_info_ptr.load(Ordering::Relaxed); 
        
        let old_min_info_val_at_u = *u_min_info_slot_ptr; 

        let (left_child_idx_opt, right_child_idx_opt) = self.get_children_indices(u_idx);

        let left_ts_info = match left_child_idx_opt {
            Some(lc_idx) => self.get_tree_node(lc_idx).read_min_info(),
            None => MinInfo::infinite(), 
        };
        let right_ts_info = match right_child_idx_opt {
            Some(rc_idx) => self.get_tree_node(rc_idx).read_min_info(),
            None => MinInfo::infinite(), 
        };
            
        let new_min_info_val_for_u = if left_ts_info.ts <= right_ts_info.ts { left_ts_info } else { right_ts_info };

        if old_min_info_val_at_u.ts != new_min_info_val_for_u.ts || old_min_info_val_at_u.leaf_idx != new_min_info_val_for_u.leaf_idx {
            u_min_info_slot_ptr.write(new_min_info_val_for_u);
            fence(Ordering::Release); 
        }
    }

    unsafe fn propagate(&self, producer_id: usize, is_enqueuer: bool) {
        let mut current_tree_node_idx = self.get_leaf_tree_node_idx(producer_id);
        let local_q = self.get_local_queue(producer_id);

        let front_tuple_opt = if is_enqueuer {
            local_q.read_fronte()
        } else {
            local_q.read_frontd()
        };
        
        let leaf_min_info_val = match front_tuple_opt {
            Some((_item, ts)) => MinInfo::new(ts, producer_id),
            None => MinInfo::infinite(),
        };
        
        self.get_tree_node(current_tree_node_idx).update_min_info_value_in_slot(leaf_min_info_val);

        while let Some(parent_idx) = self.get_parent_idx(current_tree_node_idx) {
            current_tree_node_idx = parent_idx;
            self.refresh(current_tree_node_idx); 
            self.refresh(current_tree_node_idx);
        }
    }
    
    unsafe fn alloc_sesd_node_from_pool(&self) -> *mut SesdNode<(T, Timestamp)> {
        let node_ptr = self.sesd_node_pool.alloc_sesd_node(); 
        if node_ptr.is_null() {
            panic!("JayantiPetrovicMpscQueue: SESD node pool exhausted!");
        }
        node_ptr
    }

    pub fn enqueue(&self, producer_id: usize, item: T) -> Result<(), ()> {
        if producer_id >= self.num_producers {
            return Err(()); 
        }
        let tok = self.counter.fetch_add(1, Ordering::Relaxed); 
        let ts = Timestamp { val: tok, pid: producer_id };

        unsafe {
            let local_q = self.get_local_queue(producer_id);
            let new_sesd_node_for_dummy = self.alloc_sesd_node_from_pool();
            if new_sesd_node_for_dummy.is_null() { return Err(()); }
            local_q.enqueue2((item, ts), new_sesd_node_for_dummy);
            self.propagate(producer_id, true);
        }
        Ok(())
    }

    pub fn dequeue(&self) -> Option<T> {
        unsafe {
            if self.num_producers == 0 { return None; } 
            let root_node = self.get_tree_node(0);
            let min_info_at_root = root_node.read_min_info();
    
            if min_info_at_root.ts == INFINITY_TS {
                return None; 
            }
    
            let target_producer_id = min_info_at_root.leaf_idx;
            if target_producer_id >= self.num_producers || target_producer_id == usize::MAX { 
                self.refresh(0); 
                let min_info_at_root_retry = root_node.read_min_info();
                if min_info_at_root_retry.ts == INFINITY_TS || 
                    min_info_at_root_retry.leaf_idx >= self.num_producers ||
                    min_info_at_root_retry.leaf_idx == usize::MAX {
                    return None; 
                }
                return self.dequeue();
            }

            let local_q_to_dequeue = self.get_local_queue(target_producer_id);
            let mut dequeued_node_to_free = ptr::null_mut();
            let item_tuple_opt = local_q_to_dequeue.dequeue2(&mut dequeued_node_to_free);
            
            if !dequeued_node_to_free.is_null() {
                
                let initial_dummy_for_this_q = self.sesd_initial_dummies_base.add(target_producer_id);
                let free_later_dummy_for_this_q = self.sesd_free_later_dummies_base.add(target_producer_id);

                if dequeued_node_to_free != initial_dummy_for_this_q && dequeued_node_to_free != free_later_dummy_for_this_q {
                    self.sesd_node_pool.free_sesd_node(dequeued_node_to_free);
                }
            }

            self.propagate(target_producer_id, false);
            item_tuple_opt.map(|(item, _ts)| item)
        }
    }
}

impl<T: Send + Clone + 'static> MpscQueueTrait<T> for JayantiPetrovicMpscQueue<T> {
    type PushError = (); 
    type PopError = ();

    fn push(&self, _item: T) -> Result<(), Self::PushError> {
        panic!("JayantiPetrovicMpscQueue::push from MpscQueue trait called without producer_id. Use enqueue(pid, item) or BenchMpscQueue::bench_push(item, pid).");
    }

    fn pop(&self) -> Result<T, Self::PopError> {
        self.dequeue().ok_or(())
    }

    fn is_empty(&self) -> bool {
        if self.num_producers == 0 { return true; } 
        unsafe { self.get_tree_node(0).read_min_info().ts == INFINITY_TS }
    }

    fn is_full(&self) -> bool {
        false 
    }
}