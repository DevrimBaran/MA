// paper in /paper/mpsc/jayanti_petrovic.pdf
use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{fence, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use super::sesd_jp_queue::{Node as SesdNode, SesdJpQueue};
use crate::MpscQueue as MpscQueueTrait;

// IPC adaptation - pool for SESD nodes instead of heap allocation
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

    // IPC adaptation - bounded attempts for wait-freedom
    unsafe fn free_sesd_node(&self, node_ptr: *mut SesdNode<(T, Timestamp)>) {
        if node_ptr.is_null() {
            return;
        }
        let mut head = self.free_list_head.load(Ordering::Acquire);
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 3; // Bounded attempts

        loop {
            (*node_ptr).next.store(head, Ordering::Relaxed);
            match self.free_list_head.compare_exchange_weak(
                head,
                node_ptr,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(new_head) => {
                    head = new_head;
                    attempts += 1;
                    if attempts >= MAX_ATTEMPTS {
                        // Give up - node will be lost but wait-freedom preserved
                        break;
                    }
                }
            }
        }
    }

    unsafe fn alloc_sesd_node(&self) -> *mut SesdNode<(T, Timestamp)> {
        // First try free list with bounded attempts
        let mut head = self.free_list_head.load(Ordering::Acquire);
        let mut free_list_attempts = 0;
        const MAX_FREE_LIST_ATTEMPTS: usize = 2;

        while !head.is_null() && free_list_attempts < MAX_FREE_LIST_ATTEMPTS {
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
                Err(new_head) => {
                    head = new_head;
                    free_list_attempts += 1;
                }
            }
        }

        // Fall back to bump allocation with bounded attempts
        let align = mem::align_of::<SesdNode<(T, Timestamp)>>();
        let size = mem::size_of::<SesdNode<(T, Timestamp)>>();
        let mut bump_attempts = 0;
        const MAX_BUMP_ATTEMPTS: usize = 3;

        loop {
            let current_ptr_val = self.current.load(Ordering::Relaxed);

            // Calculate alignment
            let current_addr = current_ptr_val as usize;
            let aligned_addr = (current_addr + align - 1) & !(align - 1);
            let padding = aligned_addr - current_addr;
            let aligned_ptr = current_ptr_val.add(padding);
            let next_ptr = aligned_ptr.add(size);

            if next_ptr as usize > self.end as usize {
                return ptr::null_mut();
            }

            match self.current.compare_exchange(
                current_ptr_val,
                next_ptr,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let allocated_node_ptr = aligned_ptr as *mut SesdNode<(T, Timestamp)>;
                    SesdNode::init_dummy(allocated_node_ptr);
                    return allocated_node_ptr;
                }
                Err(_) => {
                    bump_attempts += 1;
                    if bump_attempts >= MAX_BUMP_ATTEMPTS {
                        return ptr::null_mut();
                    }
                }
            }
        }
    }
}

// Timestamp structure - Section 3 of paper
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timestamp {
    // IPC adaptation: Use u32/u16 to fit in 64 bits (paper uses unbounded integers)
    val: u32,
    pid: u16,
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.val
            .cmp(&other.val)
            .then_with(|| self.pid.cmp(&other.pid))
    }
}

// Paper uses infinity symbol - we use max values
pub const INFINITY_TS: Timestamp = Timestamp {
    val: u32::MAX,
    pid: u16::MAX,
};

// MinInfo stored at tree nodes - Section 1.1
#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct MinInfo {
    ts: Timestamp,
    leaf_idx: u16, // which local queue has the minimum
}

impl MinInfo {
    fn to_u64(self) -> u64 {
        ((self.ts.val as u64) << 32) | ((self.ts.pid as u64) << 16) | (self.leaf_idx as u64)
    }

    fn from_u64(val: u64) -> Self {
        MinInfo {
            ts: Timestamp {
                val: (val >> 32) as u32,
                pid: ((val >> 16) & 0xFFFF) as u16,
            },
            leaf_idx: (val & 0xFFFF) as u16,
        }
    }

    fn infinite() -> Self {
        MinInfo {
            ts: INFINITY_TS,
            leaf_idx: u16::MAX,
        }
    }

    fn new(ts: Timestamp, leaf_idx: usize) -> Self {
        MinInfo {
            ts,
            leaf_idx: leaf_idx as u16,
        }
    }
}

// IPC adaptation: Versioned CAS to simulate LL/SC (paper assumes native LL/SC)
#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct CompactMinInfo {
    version: u32, // Added for CAS-based LL/SC simulation
    ts_val: u16,  // Reduced from u32 to u16
    ts_pid: u8,   // Reduced from u16 to u8
    leaf_idx: u8, // Reduced from u16 to u8
}

impl CompactMinInfo {
    fn to_u64(self) -> u64 {
        ((self.version as u64) << 32)
            | ((self.ts_val as u64) << 16)
            | ((self.ts_pid as u64) << 8)
            | (self.leaf_idx as u64)
    }

    fn from_u64(val: u64) -> Self {
        CompactMinInfo {
            version: (val >> 32) as u32,
            ts_val: ((val >> 16) & 0xFFFF) as u16,
            ts_pid: ((val >> 8) & 0xFF) as u8,
            leaf_idx: (val & 0xFF) as u8,
        }
    }

    fn from_min_info(min_info: MinInfo, version: u32) -> Self {
        CompactMinInfo {
            version,
            ts_val: (min_info.ts.val & 0xFFFF) as u16, // Take lower 16 bits
            ts_pid: (min_info.ts.pid & 0xFF) as u8,    // Max 255 producers
            leaf_idx: (min_info.leaf_idx & 0xFF) as u8, // Max 255 producers
        }
    }

    fn to_min_info(self) -> MinInfo {
        MinInfo {
            ts: Timestamp {
                val: self.ts_val as u32,
                pid: self.ts_pid as u16,
            },
            leaf_idx: self.leaf_idx as u16,
        }
    }

    fn infinite() -> Self {
        CompactMinInfo {
            version: 0,
            ts_val: u16::MAX,
            ts_pid: u8::MAX,
            leaf_idx: u8::MAX,
        }
    }
}

// Tree node structure - Figure 1 in paper
#[repr(C)]
pub struct TreeNode {
    // IPC adaptation: Store CompactMinInfo as atomic u64 for CAS
    compact_min_info: AtomicU64,
}

impl TreeNode {
    unsafe fn init_in_shm(node_ptr: *mut Self) {
        ptr::write(
            node_ptr,
            TreeNode {
                compact_min_info: AtomicU64::new(CompactMinInfo::infinite().to_u64()),
            },
        );
    }

    #[inline]
    unsafe fn read_compact_min_info(&self) -> CompactMinInfo {
        CompactMinInfo::from_u64(self.compact_min_info.load(Ordering::Acquire))
    }

    #[inline]
    unsafe fn read_min_info(&self) -> MinInfo {
        self.read_compact_min_info().to_min_info()
    }

    // Simulates LL/SC using CAS with versioning - Lines 16-18 in Figure 3 (paper says that this is possible)
    #[inline]
    unsafe fn cas_min_info(&self, old_compact: CompactMinInfo, new_min_info: MinInfo) -> bool {
        let new_compact = CompactMinInfo::from_min_info(new_min_info, old_compact.version + 1);

        self.compact_min_info
            .compare_exchange(
                old_compact.to_u64(),
                new_compact.to_u64(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

// Main queue structure - Figure 3 in paper
#[repr(C)]
pub struct JayantiPetrovicMpscQueue<T: Send + Clone + 'static> {
    counter: AtomicU64, // Line 1 in Figure 3 (tok variable)
    num_producers: usize,
    local_queues_base: *mut SesdJpQueue<(T, Timestamp)>, // Q array
    tree_nodes_base: *mut TreeNode,                      // T: treetype
    sesd_initial_dummies_base: *mut SesdNode<(T, Timestamp)>,
    sesd_help_slots_base: *mut MaybeUninit<(T, Timestamp)>,
    sesd_free_later_dummies_base: *mut SesdNode<(T, Timestamp)>,
    sesd_node_pool: ShmBumpPool<T>, // IPC adaptation
}

unsafe impl<T: Send + Clone + 'static> Send for JayantiPetrovicMpscQueue<T> {}
unsafe impl<T: Send + Clone + 'static> Sync for JayantiPetrovicMpscQueue<T> {}

impl<T: Send + Clone + 'static> JayantiPetrovicMpscQueue<T> {
    pub fn shared_size(num_producers: usize, sesd_node_pool_capacity: usize) -> usize {
        if num_producers == 0 {
            return mem::size_of::<Self>();
        }
        let tree_node_count = 2 * num_producers - 1;

        let self_size = mem::size_of::<Self>();
        let lq_structs_size = num_producers * mem::size_of::<SesdJpQueue<(T, Timestamp)>>();
        let tree_node_structs_size = tree_node_count * mem::size_of::<TreeNode>();
        let sesd_initial_dummies_size = num_producers * mem::size_of::<SesdNode<(T, Timestamp)>>();
        let sesd_help_slots_size = num_producers * mem::size_of::<MaybeUninit<(T, Timestamp)>>();
        let sesd_free_later_dummies_size =
            num_producers * mem::size_of::<SesdNode<(T, Timestamp)>>();
        let sesd_node_pool_managed_bytes =
            sesd_node_pool_capacity * mem::size_of::<SesdNode<(T, Timestamp)>>();

        let align_offset =
            |offset: usize, alignment: usize| (offset + alignment - 1) & !(alignment - 1);

        let mut total_size = 0;
        total_size = align_offset(total_size, mem::align_of::<Self>()) + self_size;
        total_size = align_offset(total_size, mem::align_of::<SesdJpQueue<(T, Timestamp)>>())
            + lq_structs_size;
        total_size = align_offset(total_size, mem::align_of::<TreeNode>()) + tree_node_structs_size;
        total_size = align_offset(total_size, mem::align_of::<SesdNode<(T, Timestamp)>>())
            + sesd_initial_dummies_size;
        total_size = align_offset(total_size, mem::align_of::<MaybeUninit<(T, Timestamp)>>())
            + sesd_help_slots_size;
        total_size = align_offset(total_size, mem::align_of::<SesdNode<(T, Timestamp)>>())
            + sesd_free_later_dummies_size;
        total_size = align_offset(total_size, mem::align_of::<u8>()) + sesd_node_pool_managed_bytes;

        total_size
    }

    // IPC adaptation - initialize all data structures in shared memory
    pub unsafe fn init_in_shared(
        mem_ptr: *mut u8,
        num_producers: usize,
        sesd_node_pool_capacity: usize,
    ) -> &'static mut Self {
        assert!(num_producers > 0, "Number of producers must be > 0");
        assert!(
            num_producers <= u16::MAX as usize,
            "Number of producers must fit in u16"
        );

        let tree_node_count = 2 * num_producers - 1;
        let mut current_offset = 0usize;

        let align_offset =
            |offset: usize, alignment: usize| (offset + alignment - 1) & !(alignment - 1);

        current_offset = align_offset(current_offset, mem::align_of::<Self>());
        let self_ptr = mem_ptr.add(current_offset) as *mut Self;
        current_offset += mem::size_of::<Self>();

        current_offset = align_offset(
            current_offset,
            mem::align_of::<SesdJpQueue<(T, Timestamp)>>(),
        );
        let lq_base_ptr = mem_ptr.add(current_offset) as *mut SesdJpQueue<(T, Timestamp)>;
        current_offset += num_producers * mem::size_of::<SesdJpQueue<(T, Timestamp)>>();

        current_offset = align_offset(current_offset, mem::align_of::<TreeNode>());
        let tree_base_ptr = mem_ptr.add(current_offset) as *mut TreeNode;
        current_offset += tree_node_count * mem::size_of::<TreeNode>();

        current_offset = align_offset(current_offset, mem::align_of::<SesdNode<(T, Timestamp)>>());
        let sesd_initial_dummies_ptr = mem_ptr.add(current_offset) as *mut SesdNode<(T, Timestamp)>;
        current_offset += num_producers * mem::size_of::<SesdNode<(T, Timestamp)>>();

        current_offset = align_offset(
            current_offset,
            mem::align_of::<MaybeUninit<(T, Timestamp)>>(),
        );
        let sesd_help_slots_ptr = mem_ptr.add(current_offset) as *mut MaybeUninit<(T, Timestamp)>;
        current_offset += num_producers * mem::size_of::<MaybeUninit<(T, Timestamp)>>();

        current_offset = align_offset(current_offset, mem::align_of::<SesdNode<(T, Timestamp)>>());
        let sesd_free_later_dummies_ptr =
            mem_ptr.add(current_offset) as *mut SesdNode<(T, Timestamp)>;
        current_offset += num_producers * mem::size_of::<SesdNode<(T, Timestamp)>>();

        current_offset = align_offset(current_offset, mem::align_of::<u8>());
        let sesd_node_pool_start_ptr = mem_ptr.add(current_offset) as *mut u8;
        let sesd_node_pool_managed_bytes =
            sesd_node_pool_capacity * mem::size_of::<SesdNode<(T, Timestamp)>>();

        // Initialization - counter = 0
        ptr::write(
            self_ptr,
            Self {
                counter: AtomicU64::new(0),
                num_producers,
                local_queues_base: lq_base_ptr,
                tree_nodes_base: tree_base_ptr,
                sesd_initial_dummies_base: sesd_initial_dummies_ptr,
                sesd_help_slots_base: sesd_help_slots_ptr,
                sesd_free_later_dummies_base: sesd_free_later_dummies_ptr,
                sesd_node_pool: ShmBumpPool::new(
                    sesd_node_pool_start_ptr,
                    sesd_node_pool_managed_bytes,
                ),
            },
        );

        let queue_ref = &mut *self_ptr;

        // Initialize tree nodes
        for i in 0..tree_node_count {
            let tree_node_raw_ptr = queue_ref.tree_nodes_base.add(i);
            TreeNode::init_in_shm(tree_node_raw_ptr);
        }

        // Initialize local queues - Section 2
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
    pub unsafe fn get_tree_node(&self, node_idx: usize) -> &TreeNode {
        &*self.tree_nodes_base.add(node_idx)
    }

    #[inline]
    pub fn get_leaf_tree_node_idx(&self, producer_id: usize) -> usize {
        (self.num_producers - 1) + producer_id
    }

    #[inline]
    fn get_parent_idx(&self, tree_node_idx: usize) -> Option<usize> {
        if tree_node_idx == 0 {
            None
        } else {
            Some((tree_node_idx - 1) / 2)
        }
    }

    #[inline]
    fn get_children_indices(&self, tree_node_idx: usize) -> (Option<usize>, Option<usize>) {
        let left_idx = 2 * tree_node_idx + 1;
        let right_idx = 2 * tree_node_idx + 2;
        let max_node_idx = 2 * self.num_producers - 2;
        (
            if left_idx <= max_node_idx {
                Some(left_idx)
            } else {
                None
            },
            if right_idx <= max_node_idx {
                Some(right_idx)
            } else {
                None
            },
        )
    }

    // refresh() - Lines 16-18 in Figure 3
    pub unsafe fn refresh(&self, u_idx: usize) -> bool {
        let u_node = self.get_tree_node(u_idx);

        // Line 16: LL(currentNode)
        let old_compact = u_node.read_compact_min_info();

        // Line 17: read time stamps in currentNode's children
        let (left_child_idx_opt, right_child_idx_opt) = self.get_children_indices(u_idx);

        let left_ts_info = match left_child_idx_opt {
            Some(lc_idx) => self.get_tree_node(lc_idx).read_min_info(),
            None => MinInfo::infinite(),
        };
        let right_ts_info = match right_child_idx_opt {
            Some(rc_idx) => self.get_tree_node(rc_idx).read_min_info(),
            None => MinInfo::infinite(),
        };

        // Let minT be the smallest time stamp read
        let new_min_info_val_for_u = if left_ts_info.ts <= right_ts_info.ts {
            left_ts_info
        } else {
            right_ts_info
        };

        // Line 18: return SC(currentNode, minT)
        u_node.cas_min_info(old_compact, new_min_info_val_for_u)
    }

    // propagate(Q) - Lines 5-10 in Figure 3
    unsafe fn propagate(&self, producer_id: usize, is_enqueuer: bool) {
        // Line 5: currentNode = Q
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

        // For leaf nodes, we can use a simple store since only one producer updates each leaf
        let leaf_node = self.get_tree_node(current_tree_node_idx);
        let current_compact = leaf_node.read_compact_min_info();
        let new_compact =
            CompactMinInfo::from_min_info(leaf_min_info_val, current_compact.version + 1);
        leaf_node
            .compact_min_info
            .store(new_compact.to_u64(), Ordering::Release);

        // Line 6: repeat
        while let Some(parent_idx) = self.get_parent_idx(current_tree_node_idx) {
            // Line 7: currentNode = parent(currentNode)
            current_tree_node_idx = parent_idx;

            // Line 8: if ¬refresh()
            if !self.refresh(current_tree_node_idx) {
                // Line 9: refresh()
                self.refresh(current_tree_node_idx);
            }
            // Paper says to always do second refresh for correctness
            self.refresh(current_tree_node_idx);
        } // Line 10: until(currentNode == root(T))
    }

    unsafe fn alloc_sesd_node_from_pool(&self) -> *mut SesdNode<(T, Timestamp)> {
        self.sesd_node_pool.alloc_sesd_node()
    }

    // enqueue(p, v) - Lines 1-4 in Figure 3
    pub fn enqueue(&self, producer_id: usize, item: T) -> Result<(), ()> {
        if producer_id >= self.num_producers {
            return Err(());
        }
        // Papers ll/sc changed to fetch_add because in paper the ll/sc is just used as an atomic increment.
        // CAS would be too expensive in execution time so we use FAA to just increment the counter and save it to tok aomically.
        // Line 1: tok = LL(counter)
        let tok = self.counter.fetch_add(1, Ordering::Relaxed) as u32;
        // Line 2: SC(counter, tok + 1) - implicit in fetch_add
        let ts = Timestamp {
            val: tok,
            pid: producer_id as u16,
        };

        unsafe {
            let local_q = self.get_local_queue(producer_id);
            let new_sesd_node_for_dummy = self.alloc_sesd_node_from_pool();
            if new_sesd_node_for_dummy.is_null() {
                return Err(());
            }
            // Line 3: enqueue2(Q[p], (v, (tok, p)))
            local_q.enqueue2((item, ts), new_sesd_node_for_dummy);
            // Line 4: propagate(Q[p])
            self.propagate(producer_id, true);
        }
        Ok(())
    }

    // dequeue(p) - Lines 11-15 in Figure 3
    pub fn dequeue(&self) -> Option<T> {
        unsafe {
            if self.num_producers == 0 {
                return None;
            }
            // Line 11: [t, q] = read(root(T))
            let root_node = self.get_tree_node(0);
            let min_info_at_root = root_node.read_min_info();

            // Line 12: if (q == ⊥) return ⊥
            if min_info_at_root.ts == INFINITY_TS {
                return None;
            }

            let target_producer_id = min_info_at_root.leaf_idx as usize;
            if target_producer_id >= self.num_producers || target_producer_id == u16::MAX as usize {
                self.refresh(0);
                let min_info_at_root_retry = root_node.read_min_info();
                if min_info_at_root_retry.ts == INFINITY_TS
                    || min_info_at_root_retry.leaf_idx as usize >= self.num_producers
                    || min_info_at_root_retry.leaf_idx == u16::MAX
                {
                    return None;
                }
                return self.dequeue();
            }

            let local_q_to_dequeue = self.get_local_queue(target_producer_id);
            let mut dequeued_node_to_free = ptr::null_mut();
            // Line 13: ret = dequeue2(Q[q])
            let item_tuple_opt = local_q_to_dequeue.dequeue2(&mut dequeued_node_to_free);

            // IPC adaptation - return node to pool
            if !dequeued_node_to_free.is_null() {
                let initial_dummy_for_this_q =
                    self.sesd_initial_dummies_base.add(target_producer_id);
                let free_later_dummy_for_this_q =
                    self.sesd_free_later_dummies_base.add(target_producer_id);

                if dequeued_node_to_free != initial_dummy_for_this_q
                    && dequeued_node_to_free != free_later_dummy_for_this_q
                {
                    self.sesd_node_pool.free_sesd_node(dequeued_node_to_free);
                }
            }

            // Line 14: propagate(Q[q])
            self.propagate(target_producer_id, false);
            // Line 15: return ret.val
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
        if self.num_producers == 0 {
            return true;
        }
        unsafe { self.get_tree_node(0).read_min_info().ts == INFINITY_TS }
    }

    fn is_full(&self) -> bool {
        false // Paper assumes unbounded queue
    }
}
