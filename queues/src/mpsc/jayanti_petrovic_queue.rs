// File: queues/src/mpsc/jayanti_petrovic_mpsc_queue.rs
#![allow(dead_code)] // Remove this once integrated

use std::ptr;
use std::mem::{self, MaybeUninit};
use std::sync::atomic::{AtomicU64, AtomicPtr, Ordering, fence};
use std::cmp::Ordering as CmpOrdering;

use crate::MpscQueue as MpscQueueTrait; // Use your project's MpscQueue trait
use super::sesd_jp_queue::{SesdJpQueue, Node as SesdNode};

// --- Shared Memory Pool Allocator (Simple Bump Allocator) ---
#[repr(C)]
struct ShmBumpPool {
    base: AtomicPtr<u8>,
    current: AtomicPtr<u8>,
    end: *mut u8, // Marks the end of the pool region
}

impl ShmBumpPool {
    unsafe fn new(start_ptr: *mut u8, size_bytes: usize) -> Self {
        ShmBumpPool {
            base: AtomicPtr::new(start_ptr),
            current: AtomicPtr::new(start_ptr),
            end: start_ptr.add(size_bytes),
        }
    }

    /// Allocates a block of memory of size `mem::size_of::<U>()` and alignment `mem::align_of::<U>()`.
    /// Returns a null pointer if the pool is exhausted.
    fn alloc<U>(&self) -> *mut U {
        let align = mem::align_of::<U>();
        let size = mem::size_of::<U>();
        
        loop {
            let current_ptr_val = self.current.load(Ordering::Relaxed);
            let mut alloc_ptr_usize = current_ptr_val as usize;

            // Align up
            let remainder = alloc_ptr_usize % align;
            if remainder != 0 {
                alloc_ptr_usize += align - remainder;
            }
            
            let next_ptr_val_after_alloc = alloc_ptr_usize + size;

            if next_ptr_val_after_alloc > self.end as usize {
                // eprintln!("ShmBumpPool: Out of memory. Requested size: {}, align: {}, available: {}", size, align, (self.end as usize) - alloc_ptr_usize);
                return ptr::null_mut(); // Out of memory
            }

            match self.current.compare_exchange(
                current_ptr_val,
                next_ptr_val_after_alloc as *mut u8,
                Ordering::Relaxed, // Can be Relaxed for bump allocator if contention is low or acceptable
                Ordering::Relaxed,
            ) {
                Ok(_) => return alloc_ptr_usize as *mut U,
                Err(_) => { /* CAS failed, retry */ }
            }
        }
    }
}


// --- Timestamp and MinInfo Structures ---
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timestamp {
    val: u64, // From the shared counter
    pid: usize, // Producer ID to break ties
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
    leaf_idx: usize, // Producer ID (index into QUEUES array)
}

impl MinInfo {
    fn infinite() -> Self {
        MinInfo { ts: INFINITY_TS, leaf_idx: usize::MAX }
    }
    fn new(ts: Timestamp, leaf_idx: usize) -> Self {
        MinInfo { ts, leaf_idx }
    }
}

// --- Tree Node Structure ---
#[repr(C)]
struct TreeNode {
    // This pointer will point to a specific slot within `min_info_slots_base`
    min_info_ptr: AtomicPtr<MinInfo>,
}

impl TreeNode {
    /// Initializes a TreeNode in shared memory.
    /// `node_ptr`: pointer to the TreeNode struct itself.
    /// `initial_min_info_instance_ptr`: pointer to the MinInfo struct this TreeNode will manage.
    unsafe fn init_in_shm(node_ptr: *mut Self, initial_min_info_instance_ptr: *mut MinInfo) {
        // Initialize the MinInfo instance itself to infinity
        ptr::addr_of_mut!((*initial_min_info_instance_ptr)).write(MinInfo::infinite());
        // Initialize the TreeNode to point to this MinInfo instance
        ptr::write(node_ptr, TreeNode {
            min_info_ptr: AtomicPtr::new(initial_min_info_instance_ptr),
        });
    }

    /// Atomically reads the MinInfo struct pointed to.
    #[inline]
    unsafe fn read_min_info(&self) -> MinInfo {
        // The pointer itself is atomic, but the read of the value it points to
        // needs to be considered in terms of memory ordering with writes.
        // Acquire ensures that we see writes that happened before this load.
        let ptr = self.min_info_ptr.load(Ordering::Acquire);
        *ptr // Dereference the raw pointer
    }
    
    /// Updates the value in the MinInfo slot.
    /// This is a simplification. True LL/SC or CAS on the MinInfo struct is needed for wait-freedom.
    #[inline]
    unsafe fn update_min_info_value_in_slot(&self, new_value: MinInfo) {
        let slot_ptr = self.min_info_ptr.load(Ordering::Relaxed); // Pointer to the fixed slot
        slot_ptr.write(new_value); // Direct write to the slot
        fence(Ordering::Release); // Ensure this write is visible to other threads
    }
}

// --- Main MPSC Queue ---
#[repr(C)]
pub struct JayantiPetrovicMpscQueue<T: Send + Clone + 'static> {
    counter: AtomicU64, // Shared timestamp counter
    num_producers: usize,

    // Base pointers into shared memory segment
    local_queues_base: *mut SesdJpQueue<(T, Timestamp)>,
    tree_nodes_base: *mut TreeNode,
    min_info_slots_base: *mut MinInfo, // Each TreeNode points into this array

    // Memory for SESD queue components (initial dummies, help_slots, free_later_dummies)
    sesd_initial_dummies_base: *mut SesdNode<(T, Timestamp)>,
    sesd_help_slots_base: *mut MaybeUninit<(T, Timestamp)>,
    sesd_free_later_dummies_base: *mut SesdNode<(T, Timestamp)>,

    // Pool allocator for SESD queue nodes (for enqueue2)
    sesd_node_pool: ShmBumpPool, 
}

unsafe impl<T: Send + Clone> Send for JayantiPetrovicMpscQueue<T> {}
unsafe impl<T: Send + Clone> Sync for JayantiPetrovicMpscQueue<T> {}

impl<T: Send + Clone + 'static> JayantiPetrovicMpscQueue<T> {
    /// Calculates the total shared memory size required.
    pub fn shared_size(num_producers: usize, sesd_node_pool_capacity: usize) -> usize {
        if num_producers == 0 { return mem::size_of::<Self>(); } // Handle empty case
        let tree_node_count = 2 * num_producers - 1;
        
        let self_size = mem::size_of::<Self>();
        let lq_structs_size = num_producers * mem::size_of::<SesdJpQueue<(T, Timestamp)>>();
        let tree_node_structs_size = tree_node_count * mem::size_of::<TreeNode>();
        let min_info_slots_size = tree_node_count * mem::size_of::<MinInfo>();
        let sesd_initial_dummies_size = num_producers * mem::size_of::<SesdNode<(T, Timestamp)>>();
        let sesd_help_slots_size = num_producers * mem::size_of::<MaybeUninit<(T, Timestamp)>>();
        let sesd_free_later_dummies_size = num_producers * mem::size_of::<SesdNode<(T, Timestamp)>>();
        let sesd_node_pool_bytes = sesd_node_pool_capacity * mem::size_of::<SesdNode<(T, Timestamp)>>();

        // Helper for aligning offsets
        let align_offset = |offset: usize, alignment: usize| (offset + alignment - 1) & !(alignment - 1);

        let mut total_size = 0;
        total_size = align_offset(total_size, mem::align_of::<Self>()) + self_size;
        total_size = align_offset(total_size, mem::align_of::<SesdJpQueue<(T, Timestamp)>>()) + lq_structs_size;
        total_size = align_offset(total_size, mem::align_of::<TreeNode>()) + tree_node_structs_size;
        total_size = align_offset(total_size, mem::align_of::<MinInfo>()) + min_info_slots_size;
        total_size = align_offset(total_size, mem::align_of::<SesdNode<(T, Timestamp)>>()) + sesd_initial_dummies_size;
        total_size = align_offset(total_size, mem::align_of::<MaybeUninit<(T, Timestamp)>>()) + sesd_help_slots_size;
        total_size = align_offset(total_size, mem::align_of::<SesdNode<(T, Timestamp)>>()) + sesd_free_later_dummies_size;
        total_size = align_offset(total_size, mem::align_of::<u8>()) + sesd_node_pool_bytes; // Pool items often u8 aligned
        
        total_size
    }

    /// Initializes the queue in the provided shared memory segment.
    /// # Safety
    /// `mem_ptr` must point to a valid shared memory region of at least `shared_size` bytes.
    /// `num_producers` must be greater than 0.
    /// `sesd_node_pool_capacity` is the number of SESD nodes the pool can hold.
    pub unsafe fn init_in_shared(
        mem_ptr: *mut u8,
        num_producers: usize,
        sesd_node_pool_capacity: usize,
    ) -> &'static mut Self {
        assert!(num_producers > 0, "Number of producers must be > 0");

        let tree_node_count = 2 * num_producers - 1;
        let mut current_offset = 0usize;

        let align_offset = |offset: usize, alignment: usize| (offset + alignment - 1) & !(alignment - 1);

        // Layout calculation (order matters for pointer arithmetic)
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

        current_offset = align_offset(current_offset, mem::align_of::<u8>()); // Pool base alignment
        let sesd_node_pool_ptr = mem_ptr.add(current_offset) as *mut u8;
        let sesd_node_pool_bytes = sesd_node_pool_capacity * mem::size_of::<SesdNode<(T, Timestamp)>>();

        // Initialize the main queue struct
        ptr::write(self_ptr, Self {
            counter: AtomicU64::new(0),
            num_producers,
            local_queues_base: lq_base_ptr,
            tree_nodes_base: tree_base_ptr,
            min_info_slots_base: min_info_slots_ptr,
            sesd_initial_dummies_base: sesd_initial_dummies_ptr,
            sesd_help_slots_base: sesd_help_slots_ptr,
            sesd_free_later_dummies_base: sesd_free_later_dummies_ptr,
            sesd_node_pool: ShmBumpPool::new(sesd_node_pool_ptr, sesd_node_pool_bytes),
        });

        let queue_ref = &mut *self_ptr;

        // Initialize TreeNodes: each TreeNode's min_info_ptr points to its designated slot in min_info_slots_base
        for i in 0..tree_node_count {
            let tree_node_raw_ptr = queue_ref.tree_nodes_base.add(i);
            let min_info_instance_raw_ptr = queue_ref.min_info_slots_base.add(i); // Points to the i-th MinInfo slot
            TreeNode::init_in_shm(tree_node_raw_ptr, min_info_instance_raw_ptr);
        }
        
        // Initialize Local SESD Queues
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
        // Bounds check for safety, though in controlled code it might be assumed.
        // assert!(producer_id < self.num_producers, "Producer ID out of bounds");
        &*self.local_queues_base.add(producer_id)
    }
    
    #[inline]
    unsafe fn get_tree_node(&self, node_idx: usize) -> &TreeNode {
        // assert!(node_idx < (2 * self.num_producers - 1), "Tree node index out of bounds");
        &*self.tree_nodes_base.add(node_idx)
    }

    /// Gets the index for the leaf node in the tree array corresponding to producer_id.
    /// Assumes standard complete binary tree layout where leaves are contiguous.
    #[inline]
    fn get_leaf_tree_node_idx(&self, producer_id: usize) -> usize {
        // If num_producers = N, leaves are typically at indices N-1 to 2N-2
        // if root is at 0 and children of node i are 2i+1, 2i+2.
        // Example: N=1, leaf[0] is root (idx 0). N=2, leaves are 1,2. N=3, leaves are 2,3,4.
        // For N leaves, there are N-1 internal nodes. Total 2N-1 nodes.
        // Leaves start at index N-1.
        (self.num_producers - 1) + producer_id
    }
    
    /// Gets the parent index of a node in the tree array. Returns None if node_idx is root.
    #[inline]
    fn get_parent_idx(&self, tree_node_idx: usize) -> Option<usize> {
        if tree_node_idx == 0 { None } else { Some((tree_node_idx - 1) / 2) }
    }

    /// Gets children indices of a node in the tree array. Returns None if child does not exist.
    #[inline]
    fn get_children_indices(&self, tree_node_idx: usize) -> (Option<usize>, Option<usize>) {
        let left_idx = 2 * tree_node_idx + 1;
        let right_idx = 2 * tree_node_idx + 2;
        let max_node_idx = 2 * self.num_producers - 2; // Max valid index for any node
        (
            if left_idx <= max_node_idx { Some(left_idx) } else { None },
            if right_idx <= max_node_idx { Some(right_idx) } else { None }
        )
    }
    
    /// Implements the refresh operation for a tree node `u_idx`.
    /// This version directly writes to the MinInfo slot.
    /// True atomicity for the MinInfo struct update relies on external mechanisms or assumptions.
    unsafe fn refresh(&self, u_idx: usize) {
        let u_node = self.get_tree_node(u_idx);
        // The min_info_ptr for u_node points to a fixed slot in min_info_slots_base
        let u_min_info_slot_ptr = u_node.min_info_ptr.load(Ordering::Relaxed); 
        
        let old_min_info_val_at_u = *u_min_info_slot_ptr; // Read current value

        let (left_child_idx_opt, right_child_idx_opt) = self.get_children_indices(u_idx);

        // A leaf node in the conceptual tree (which corresponds to an internal node in our array
        // if num_producers > 1) would not have children in this function's context,
        // as refresh is called on parents. If u_idx is a leaf of the conceptual tree,
        // its MinInfo is updated directly by propagate from the SESD queue.
        // This refresh is for internal nodes of the conceptual tree.
        let left_ts_info = match left_child_idx_opt {
            Some(lc_idx) => self.get_tree_node(lc_idx).read_min_info(),
            None => MinInfo::infinite(), // Should not happen for internal nodes being refreshed
        };
        let right_ts_info = match right_child_idx_opt {
            Some(rc_idx) => self.get_tree_node(rc_idx).read_min_info(),
            None => MinInfo::infinite(), // Should not happen
        };
            
        let new_min_info_val_for_u = if left_ts_info.ts <= right_ts_info.ts { left_ts_info } else { right_ts_info };

        // Simplified update: direct write to the slot.
        if old_min_info_val_at_u.ts != new_min_info_val_for_u.ts || old_min_info_val_at_u.leaf_idx != new_min_info_val_for_u.leaf_idx {
            u_min_info_slot_ptr.write(new_min_info_val_for_u);
            fence(Ordering::Release); // Ensure this write is visible
        }
    }

    /// Propagates timestamp information up the tree from a given producer's leaf.
    /// `is_enqueuer` is true if called by the enqueuer (uses read_fronte), false if by dequeuer (uses read_frontd).
    unsafe fn propagate(&self, producer_id: usize, is_enqueuer: bool) {
        let mut current_tree_node_idx = self.get_leaf_tree_node_idx(producer_id);
        let local_q = self.get_local_queue(producer_id);

        // Update leaf node's MinInfo based on its local queue's front
        let front_tuple_opt = if is_enqueuer {
            local_q.read_fronte()
        } else {
            local_q.read_frontd()
        };
        
        let leaf_min_info_val = match front_tuple_opt {
            Some((_item, ts)) => MinInfo::new(ts, producer_id),
            None => MinInfo::infinite(),
        };
        
        // Directly update the MinInfo slot for the leaf node
        self.get_tree_node(current_tree_node_idx).update_min_info_value_in_slot(leaf_min_info_val);

        // Propagate upwards to the root
        while let Some(parent_idx) = self.get_parent_idx(current_tree_node_idx) {
            current_tree_node_idx = parent_idx;
            // As per paper's wait-freedom argument for propagate (via refresh):
            // Call refresh twice. If the first fails, the second (even if it also "fails" its SC)
            // ensures the node is up-to-date due to another process's concurrent successful SC.
            self.refresh(current_tree_node_idx); 
            self.refresh(current_tree_node_idx);
        }
    }
    
    /// Allocates a new SESD node from the shared pool.
    /// Panics if the pool is exhausted.
    unsafe fn alloc_sesd_node_from_pool(&self) -> *mut SesdNode<(T, Timestamp)> {
        let node_ptr = self.sesd_node_pool.alloc::<SesdNode<(T, Timestamp)>>();
        if node_ptr.is_null() {
            // This is a critical error. In a real system, might return error or block.
            panic!("JayantiPetrovicMpscQueue: SESD node pool exhausted!");
        }
        node_ptr
    }

    // Enqueue method (Producer P_producer_id)
    pub fn enqueue(&self, producer_id: usize, item: T) -> Result<(), ()> {
        if producer_id >= self.num_producers {
            // eprintln!("Error: Producer ID {} is out of bounds for {} producers.", producer_id, self.num_producers);
            return Err(()); 
        }

        // 1. Get timestamp
        // fetch_add provides sequential consistency by default, which is strong.
        // Relaxed might be okay if pid makes it unique, but AcqRel or SeqCst on counter
        // is safer for global timestamp ordering if that's strictly needed beyond pid.
        // The paper uses LL/SC for this, which implies atomicity and some ordering.
        let tok = self.counter.fetch_add(1, Ordering::Relaxed); // Using Relaxed as pid ensures uniqueness
        let ts = Timestamp { val: tok, pid: producer_id };

        // 2. Enqueue to local queue
        unsafe {
            let local_q = self.get_local_queue(producer_id);
            let new_sesd_node_for_dummy = self.alloc_sesd_node_from_pool();
            // If new_sesd_node_for_dummy is null, alloc_sesd_node_from_pool would have panicked.
            // For robustness, check again or make alloc_sesd_node_from_pool return Result.
            if new_sesd_node_for_dummy.is_null() { return Err(()); /* Should have panicked already */ }
            local_q.enqueue2((item, ts), new_sesd_node_for_dummy);
        
            // 3. Propagate
            self.propagate(producer_id, true /* is_enqueuer */);
        }
        Ok(())
    }

    // Dequeue method (Single Consumer)
    pub fn dequeue(&self) -> Option<T> {
        unsafe {
            if self.num_producers == 0 { return None; } 
            let root_node = self.get_tree_node(0);
            let min_info_at_root = root_node.read_min_info();

            if min_info_at_root.ts == INFINITY_TS {
                return None; // Queue is empty
            }

            let target_producer_id = min_info_at_root.leaf_idx;
            // Basic sanity check, though a correct tree should always yield valid idx if not INFINITY_TS
            if target_producer_id >= self.num_producers { 
                 eprintln!(
                    "JayantiPetrovicMpscQueue: Corrupted tree state. Root MinInfo: {:?}, NumProducers: {}",
                     min_info_at_root, self.num_producers
                 );
                 return None; // Or panic, indicates serious corruption
            }

            let local_q_to_dequeue = self.get_local_queue(target_producer_id);
            
            let mut dequeued_node_to_free = ptr::null_mut();
            let item_tuple_opt = local_q_to_dequeue.dequeue2(&mut dequeued_node_to_free);
            
            // Memory reclamation for `dequeued_node_to_free`
            if !dequeued_node_to_free.is_null() {
                // This node needs to be returned to the `sesd_node_pool`.
                // The current `ShmBumpPool` does not support `free`.
                // This is a memory leak in this simplified implementation.
                // A production queue would use a pool allocator that supports free.
                // For the benchmark, if TOTAL_ITEMS is fixed, the leak might be acceptable
                // if the pool is sized for 2*TOTAL_ITEMS (items + dummies).
            }

            // Propagate the new front of the dequeued local queue
            self.propagate(target_producer_id, false /* not_enqueuer / is_dequeuer */);

            item_tuple_opt.map(|(item, _ts)| item)
        }
    }
}


impl<T: Send + Clone + 'static> MpscQueueTrait<T> for JayantiPetrovicMpscQueue<T> {
    type PushError = (); 
    type PopError = ();

    // This implementation of the MpscQueue trait's `push` method is problematic
    // because it doesn't know the `producer_id`. The benchmark harness MUST call
    // `JayantiPetrovicMpscQueue::enqueue(producer_id, item)` directly via the
    // `BenchMpscQueue` trait's `bench_push` method, which now takes `producer_id`.
    fn push(&self, _item: T) -> Result<(), Self::PushError> {
        // If this method were to be used directly by an arbitrary caller not providing producer_id,
        // it would not work correctly for the Jayanti-Petrovic MPSC logic.
        // For the benchmark, this specific trait method should ideally not be called directly
        // on a JayantiPetrovicMpscQueue instance.
        panic!("JayantiPetrovicMpscQueue::push from MpscQueue trait called without producer_id. Use enqueue(pid, item) or BenchMpscQueue::bench_push(item, pid).");
    }

    fn pop(&self) -> Result<T, Self::PopError> {
        self.dequeue().ok_or(())
    }

    fn is_empty(&self) -> bool {
        if self.num_producers == 0 { return true; } // Should be caught by init assertion
        unsafe { self.get_tree_node(0).read_min_info().ts == INFINITY_TS }
    }

    fn is_full(&self) -> bool {
        // The paper's algorithm is conceptually unbounded in terms of items.
        // Fullness would relate to the underlying node pool for SESD queues.
        // The ShmBumpPool.alloc will return null if full.
        // A precise is_full would need to check if sesd_node_pool.alloc would succeed.
        false // Simplification: assume pool is large enough.
    }
}

