// queues/src/mpmc/polylog_queue.rs
// Wait-Free MPMC Queue based on "A Wait-free Queue with Polylogarithmic Step Complexity"
// by Hossein Naderibeni and Eric Ruppert (PODC 2023)

use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

use crate::MpmcQueue;

const CACHE_LINE_SIZE: usize = 64;
const BLOCKS_PER_NODE: usize = 10_000_000; // Large enough to avoid running out

// Block structure - represents a batch of operations
#[repr(C)]
struct Block<T> {
    // Fields for all blocks
    sumenq: AtomicUsize,    // Total enqueues up to this block (inclusive)
    sumdeq: AtomicUsize,    // Total dequeues up to this block (inclusive)
    super_idx: AtomicUsize, // Approximate index of superblock in parent

    // For internal blocks
    endleft: AtomicUsize,  // Index of last direct subblock in left child
    endright: AtomicUsize, // Index of last direct subblock in right child

    // For leaf blocks
    element: UnsafeCell<Option<T>>, // For enqueue: Some(value), for dequeue: None

    // For root blocks
    size: AtomicUsize, // Queue size after this block's operations
}

impl<T> Block<T> {
    fn new() -> Self {
        Self {
            sumenq: AtomicUsize::new(0),
            sumdeq: AtomicUsize::new(0),
            super_idx: AtomicUsize::new(0),
            endleft: AtomicUsize::new(0),
            endright: AtomicUsize::new(0),
            element: UnsafeCell::new(None),
            size: AtomicUsize::new(0),
        }
    }
}

// Node in the tournament tree
#[repr(C)]
struct Node<T> {
    head: AtomicUsize, // Next position to append

    // Tree pointers (set during initialization)
    left: *const Node<T>,
    right: *const Node<T>,
    parent: *const Node<T>,

    is_leaf: bool,
    is_root: bool,
    process_id: usize, // For leaves only

    // Blocks array follows immediately after this struct in memory
    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send> Send for Node<T> {}
unsafe impl<T: Send> Sync for Node<T> {}

impl<T> Node<T> {
    fn new() -> Self {
        Self {
            head: AtomicUsize::new(1), // Start at 1, blocks[0] is empty sentinel
            left: ptr::null(),
            right: ptr::null(),
            parent: ptr::null(),
            is_leaf: false,
            is_root: false,
            process_id: usize::MAX,
            _phantom: std::marker::PhantomData,
        }
    }

    unsafe fn get_blocks_ptr(&self) -> *mut Block<T> {
        // Blocks array starts immediately after the Node struct
        let node_ptr = self as *const Self as *mut u8;
        let blocks_offset = mem::size_of::<Self>();
        let aligned_offset = (blocks_offset + 63) & !63; // Cache line align
        node_ptr.add(aligned_offset) as *mut Block<T>
    }

    unsafe fn get_block(&self, index: usize) -> Option<&Block<T>> {
        if index >= BLOCKS_PER_NODE {
            return None;
        }
        let blocks = self.get_blocks_ptr();
        Some(&*blocks.add(index))
    }
}

// Main queue structure
#[repr(C)]
pub struct NRQueue<T: Send + Clone + 'static> {
    root: *const Node<T>,
    tree_height: usize,
    num_processes: usize,

    // Memory management
    base_ptr: *mut u8,
    total_size: usize,
    nodes_offset: usize,
    node_size: usize, // Size of one node including its blocks array

    // Shared state
    initialized: AtomicBool,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for NRQueue<T> {}
unsafe impl<T: Send + Clone> Sync for NRQueue<T> {}

impl<T: Send + Clone + 'static> NRQueue<T> {
    unsafe fn get_node_ptr(&self, index: usize) -> *mut Node<T> {
        let nodes_base = self.base_ptr.add(self.nodes_offset);
        nodes_base.add(index * self.node_size) as *mut Node<T>
    }

    unsafe fn get_leaf(&self, process_id: usize) -> &Node<T> {
        let leaf_start = (1 << self.tree_height) - 1;
        &*self.get_node_ptr(leaf_start + process_id)
    }

    unsafe fn get_node(&self, index: usize) -> &Node<T> {
        &*self.get_node_ptr(index)
    }

    // Append operation to leaf
    fn append(&self, process_id: usize, element: Option<T>) {
        unsafe {
            let leaf = self.get_leaf(process_id);
            let h = leaf.head.load(Ordering::Acquire);

            // Check if we have space
            if h >= BLOCKS_PER_NODE {
                panic!(
                    "Block pool exhausted for process {}. Head: {}, Capacity: {}",
                    process_id, h, BLOCKS_PER_NODE
                );
            }

            // Get block
            let blocks = leaf.get_blocks_ptr();
            let block = &*blocks.add(h);

            // Check if this is an enqueue or dequeue before moving element
            let is_enqueue = element.is_some();

            // Set element
            *block.element.get() = element;

            // Update sums based on previous block
            if h > 0 {
                let prev_block = &*blocks.add(h - 1);
                let prev_sumenq = prev_block.sumenq.load(Ordering::Acquire);
                let prev_sumdeq = prev_block.sumdeq.load(Ordering::Acquire);

                if is_enqueue {
                    block.sumenq.store(prev_sumenq + 1, Ordering::Release);
                    block.sumdeq.store(prev_sumdeq, Ordering::Release);
                } else {
                    block.sumenq.store(prev_sumenq, Ordering::Release);
                    block.sumdeq.store(prev_sumdeq + 1, Ordering::Release);
                }
            } else {
                // First block after sentinel
                if is_enqueue {
                    block.sumenq.store(1, Ordering::Release);
                    block.sumdeq.store(0, Ordering::Release);
                } else {
                    block.sumenq.store(0, Ordering::Release);
                    block.sumdeq.store(1, Ordering::Release);
                }
            }

            // Increment head BEFORE propagating
            leaf.head.fetch_add(1, Ordering::AcqRel);

            // Now propagate to root
            if !leaf.parent.is_null() {
                self.propagate(&*leaf.parent);
            }
        }
    }

    // Propagate operations from children to parent
    fn propagate(&self, node: &Node<T>) {
        // Double refresh as per paper
        if !self.refresh(node) {
            self.refresh(node);
        }

        unsafe {
            if !node.is_root && !node.parent.is_null() {
                self.propagate(&*node.parent);
            }
        }
    }

    // Try to append new block to node
    fn refresh(&self, node: &Node<T>) -> bool {
        unsafe {
            let h = node.head.load(Ordering::Acquire);

            // Check if we have space
            if h >= BLOCKS_PER_NODE {
                return false;
            }

            // Help advance children if needed
            if !node.left.is_null() {
                let left = &*node.left;
                let left_head = left.head.load(Ordering::Acquire);
                if left_head > 0 {
                    self.advance(left, left_head - 1);
                }
            }

            if !node.right.is_null() {
                let right = &*node.right;
                let right_head = right.head.load(Ordering::Acquire);
                if right_head > 0 {
                    self.advance(right, right_head - 1);
                }
            }

            // Create new block
            let new_block = self.create_block(node, h);
            if new_block.is_none() {
                return true; // No operations to propagate
            }

            let (numenq, numdeq, endleft, endright, size) = new_block.unwrap();

            // Get block
            let blocks = node.get_blocks_ptr();
            let block = &*blocks.add(h);

            // Check if we can still install (no one else has)
            let current_head = node.head.load(Ordering::Acquire);
            if current_head != h {
                self.advance(node, h);
                return false;
            }

            // Set block fields
            if h > 0 {
                let prev = &*blocks.add(h - 1);
                block.sumenq.store(
                    prev.sumenq.load(Ordering::Acquire) + numenq,
                    Ordering::Release,
                );
                block.sumdeq.store(
                    prev.sumdeq.load(Ordering::Acquire) + numdeq,
                    Ordering::Release,
                );
            } else {
                block.sumenq.store(numenq, Ordering::Release);
                block.sumdeq.store(numdeq, Ordering::Release);
            }

            block.endleft.store(endleft, Ordering::Release);
            block.endright.store(endright, Ordering::Release);

            if node.is_root {
                block.size.store(size, Ordering::Release);
            }

            // Advance head
            self.advance(node, h);
            true
        }
    }

    fn create_block(
        &self,
        node: &Node<T>,
        h: usize,
    ) -> Option<(usize, usize, usize, usize, usize)> {
        unsafe {
            // Get previous block info
            let (prev_endleft, prev_endright, prev_size, prev_sumenq, prev_sumdeq) = if h > 0 {
                let prev = node.get_block(h - 1)?;
                (
                    prev.endleft.load(Ordering::Acquire),
                    prev.endright.load(Ordering::Acquire),
                    if node.is_root {
                        prev.size.load(Ordering::Acquire)
                    } else {
                        0
                    },
                    prev.sumenq.load(Ordering::Acquire),
                    prev.sumdeq.load(Ordering::Acquire),
                )
            } else {
                (0, 0, 0, 0, 0)
            };

            // Get current child positions and their totals
            let mut endleft = prev_endleft;
            let mut endright = prev_endright;
            let mut left_total_enq = 0;
            let mut left_total_deq = 0;
            let mut right_total_enq = 0;
            let mut right_total_deq = 0;

            // Check left child
            if !node.left.is_null() {
                let left = &*node.left;
                let left_head = left.head.load(Ordering::Acquire);

                // Only consider established blocks (head - 1)
                if left_head > 1 {
                    let new_endleft = left_head - 1;
                    if new_endleft > prev_endleft {
                        endleft = new_endleft;
                        // Get total at new position
                        if let Some(block) = left.get_block(endleft) {
                            left_total_enq = block.sumenq.load(Ordering::Acquire);
                            left_total_deq = block.sumdeq.load(Ordering::Acquire);
                        }
                    } else if prev_endleft > 0 {
                        // No new blocks, use previous total
                        if let Some(block) = left.get_block(prev_endleft) {
                            left_total_enq = block.sumenq.load(Ordering::Acquire);
                            left_total_deq = block.sumdeq.load(Ordering::Acquire);
                        }
                    }
                }
            }

            // Check right child
            if !node.right.is_null() {
                let right = &*node.right;
                let right_head = right.head.load(Ordering::Acquire);

                // Only consider established blocks (head - 1)
                if right_head > 1 {
                    let new_endright = right_head - 1;
                    if new_endright > prev_endright {
                        endright = new_endright;
                        // Get total at new position
                        if let Some(block) = right.get_block(endright) {
                            right_total_enq = block.sumenq.load(Ordering::Acquire);
                            right_total_deq = block.sumdeq.load(Ordering::Acquire);
                        }
                    } else if prev_endright > 0 {
                        // No new blocks, use previous total
                        if let Some(block) = right.get_block(prev_endright) {
                            right_total_enq = block.sumenq.load(Ordering::Acquire);
                            right_total_deq = block.sumdeq.load(Ordering::Acquire);
                        }
                    }
                }
            }

            // Total operations from children
            let child_total_enq = left_total_enq + right_total_enq;
            let child_total_deq = left_total_deq + right_total_deq;

            // Calculate new operations in this block
            let numenq = child_total_enq.saturating_sub(prev_sumenq);
            let numdeq = child_total_deq.saturating_sub(prev_sumdeq);

            // Nothing new to propagate
            if numenq == 0 && numdeq == 0 {
                return None;
            }

            // Calculate size for root blocks
            let size = if node.is_root {
                // Calculate the new total operations
                let total_enq_after = prev_sumenq + numenq;
                let total_deq_after = prev_sumdeq + numdeq;

                // The key insight: not all dequeue attempts succeed!
                // Only the first N dequeues (where N = number of enqueues) can succeed
                // The rest must return empty

                // Calculate successful dequeues
                let successful_dequeues = total_deq_after.min(total_enq_after);

                // Queue size = total enqueues - successful dequeues
                let new_size = total_enq_after.saturating_sub(successful_dequeues);

                // Sanity check: size should not increase by more than new enqueues
                // and should not decrease by more than new dequeues
                if numenq >= numdeq {
                    // More enqueues than dequeues in this block
                    prev_size + (numenq - numdeq)
                } else {
                    // More dequeues than enqueues
                    // Size decreases but can't go below 0
                    prev_size.saturating_sub(numdeq - numenq)
                }
            } else {
                0
            };

            Some((numenq, numdeq, endleft, endright, size))
        }
    }

    // Simplified dequeue that correctly handles the queue size
    pub fn simple_dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            // First sync to see current state
            self.sync();

            // Check if there are any items to dequeue by looking at root
            let root = &*self.root;
            let root_head = root.head.load(Ordering::Acquire);

            if root_head <= 1 {
                return Err(());
            }

            // Get the authoritative counts from root
            let root_last_block = root.get_block(root_head - 1).ok_or(())?;
            let total_enqueues_in_root = root_last_block.sumenq.load(Ordering::Acquire);
            let total_dequeues_in_root = root_last_block.sumdeq.load(Ordering::Acquire);
            let queue_size = root_last_block.size.load(Ordering::Acquire);

            // If queue is empty, don't even try
            if queue_size == 0 {
                return Err(());
            }

            // Calculate successful dequeues (those that got items)
            // This is min(dequeues_attempted, enqueues)
            let successful_dequeues = total_enqueues_in_root.min(total_dequeues_in_root);

            // If all items have been dequeued, return empty
            if successful_dequeues >= total_enqueues_in_root {
                return Err(());
            }

            // Now append our dequeue attempt
            self.append(thread_id, None);

            // Force sync to propagate our dequeue
            self.sync();

            // Now find our position
            let leaf = self.get_leaf(thread_id);
            let leaf_head = leaf.head.load(Ordering::Acquire);
            let my_dequeue_block = leaf_head - 1;

            let my_leaf_deq_num = if let Some(block) = leaf.get_block(my_dequeue_block) {
                block.sumdeq.load(Ordering::Acquire)
            } else {
                return Err(());
            };

            // Count total dequeues from all processes to get our global position
            let mut dequeues_before_me = 0;

            for i in 0..self.num_processes {
                let proc_leaf = self.get_leaf(i);
                let proc_head = proc_leaf.head.load(Ordering::Acquire);

                if proc_head > 1 {
                    if let Some(last_block) = proc_leaf.get_block(proc_head - 1) {
                        if i < thread_id {
                            dequeues_before_me += last_block.sumdeq.load(Ordering::Acquire);
                        } else if i == thread_id {
                            dequeues_before_me += my_leaf_deq_num - 1;
                        }
                    }
                }
            }

            // Our global dequeue number
            let my_global_deq_num = dequeues_before_me + 1;

            // Debug
            if my_leaf_deq_num >= 99999 && my_leaf_deq_num <= 100001 {
                eprintln!(
                    "Thread {} dequeue #{} (global #{}), total_enqueues={}",
                    thread_id, my_leaf_deq_num, my_global_deq_num, total_enqueues_in_root
                );
            }

            // CRITICAL: Check if our dequeue would be successful
            // We're successful if we're within the first N dequeues where N = number of enqueues
            if my_global_deq_num > total_enqueues_in_root {
                if my_leaf_deq_num >= 99999 && my_leaf_deq_num <= 100001 {
                    eprintln!("  -> Result: false (beyond enqueues)");
                }
                return Err(());
            }

            // Get the enqueue
            let result = self.get_enqueue_by_rank(my_global_deq_num);

            if my_leaf_deq_num >= 99999 && my_leaf_deq_num <= 100001 {
                eprintln!("  -> Result: {:?}", result.is_ok());
            }

            result
        }
    }

    // Get enqueue by its global rank (1-based)
    fn get_enqueue_by_rank(&self, rank: usize) -> Result<T, ()> {
        unsafe {
            let root = &*self.root;
            let root_head = root.head.load(Ordering::Acquire);

            // Debug
            if rank >= 99999 && rank <= 100001 {
                eprintln!("get_enqueue_by_rank({}), root_head={}", rank, root_head);

                // Check the last few blocks
                if root_head > 3 {
                    for i in (root_head - 3)..root_head {
                        if let Some(block) = root.get_block(i) {
                            let sumenq = block.sumenq.load(Ordering::Acquire);
                            eprintln!("  Block {}: sumenq={}", i, sumenq);
                        }
                    }
                }
            }

            if root_head <= 1 || rank == 0 {
                if rank >= 99999 && rank <= 100001 {
                    eprintln!("  -> Early return: root_head={}, rank={}", root_head, rank);
                }
                return Err(());
            }

            // Check if rank is beyond our total enqueues
            if let Some(last_block) = root.get_block(root_head - 1) {
                let total_enq = last_block.sumenq.load(Ordering::Acquire);
                if rank > total_enq {
                    if rank >= 99999 && rank <= 100001 {
                        eprintln!("  -> Rank {} exceeds total enqueues {}", rank, total_enq);
                    }
                    return Err(());
                }
            }

            // Binary search for the block containing this enqueue
            let mut left = 1;
            let mut right = root_head - 1;
            let mut target_block = 1;

            while left <= right {
                let mid = left + (right - left) / 2;
                if let Some(block) = root.get_block(mid) {
                    let sumenq = block.sumenq.load(Ordering::Acquire);

                    if sumenq >= rank {
                        target_block = mid;
                        right = mid - 1;
                    } else {
                        left = mid + 1;
                    }
                } else {
                    break;
                }
            }

            // Get the block and check it actually contains enqueues
            let block = root.get_block(target_block).ok_or(())?;
            let block_sumenq = block.sumenq.load(Ordering::Acquire);

            // Get rank within block
            let prev_sumenq = if target_block > 0 {
                root.get_block(target_block - 1)
                    .map(|b| b.sumenq.load(Ordering::Acquire))
                    .unwrap_or(0)
            } else {
                0
            };

            let block_numenq = block_sumenq - prev_sumenq;
            let rank_in_block = rank - prev_sumenq;

            // Debug
            if rank >= 99999 && rank <= 100001 {
                eprintln!(
                    "  -> Found in block {}, prev_sumenq={}, block_numenq={}, rank_in_block={}",
                    target_block, prev_sumenq, block_numenq, rank_in_block
                );
            }

            // Check if this block actually has enqueues
            if block_numenq == 0 {
                if rank >= 99999 && rank <= 100001 {
                    eprintln!("  -> ERROR: Block {} has no enqueues!", target_block);
                }
                return Err(());
            }

            // rank_in_block should be between 1 and block_numenq
            if rank_in_block == 0 || rank_in_block > block_numenq {
                if rank >= 99999 && rank <= 100001 {
                    eprintln!(
                        "  -> ERROR: rank_in_block {} out of range [1, {}]!",
                        rank_in_block, block_numenq
                    );
                }
                return Err(());
            }

            self.get_enqueue(root, target_block, rank_in_block)
        }
    }

    pub fn sync(&self) {
        unsafe {
            // First, ensure all leaves have their operations in blocks
            for i in 0..self.num_processes {
                let leaf = self.get_leaf(i);
                let head = leaf.head.load(Ordering::Acquire);

                // If head points to a written but not advanced position, advance it
                if head > 0 && !leaf.parent.is_null() {
                    self.advance(leaf, head - 1);
                }
            }

            // Now propagate everything up the tree
            for pass in 0..5 {
                // From leaves to root
                for level in (0..=self.tree_height).rev() {
                    let start_idx = (1 << level) - 1;
                    let end_idx = (1 << (level + 1)) - 1;

                    for idx in start_idx..end_idx.min(start_idx + (1 << level)) {
                        let node = self.get_node(idx);

                        // Force multiple refreshes
                        for _ in 0..3 {
                            self.refresh(node);

                            // Also try to advance the current head
                            let h = node.head.load(Ordering::Acquire);
                            if h > 0 {
                                self.advance(node, h - 1);
                            }
                        }
                    }
                }

                // Small delay between passes
                if pass < 4 {
                    std::thread::yield_now();
                }
            }
        }
    }

    // Advance head and set super field
    fn advance(&self, node: &Node<T>, h: usize) {
        unsafe {
            // Try to advance head first
            let _ = node
                .head
                .compare_exchange(h, h + 1, Ordering::AcqRel, Ordering::Acquire);

            // Set super field if not root
            if !node.is_root && !node.parent.is_null() {
                let parent = &*node.parent;

                if let Some(block) = node.get_block(h) {
                    // Only set if it's still unset
                    let current_super = block.super_idx.load(Ordering::Acquire);
                    if current_super == 0 {
                        let parent_head = parent.head.load(Ordering::Acquire);
                        // The super index should be at least 1 (can't be in block 0)
                        let super_value = parent_head.max(1);
                        block.super_idx.store(super_value, Ordering::Release);
                    }
                }
            }
        }
    }

    // Find dequeue's position in root
    fn index_dequeue(
        &self,
        mut node: &Node<T>,
        mut block_idx: usize,
        mut rank: usize,
    ) -> (usize, usize) {
        unsafe {
            while !node.is_root {
                let block = node.get_block(block_idx).expect("Block must exist");
                let mut super_idx = block.super_idx.load(Ordering::Acquire);

                // If super_idx is 0, the block hasn't been propagated yet
                // Wait for it to be set
                while super_idx == 0 {
                    std::hint::spin_loop();
                    super_idx = block.super_idx.load(Ordering::Acquire);
                }

                let parent = &*node.parent;
                let is_left = ptr::eq(parent.left, node);

                // The super_idx might be off by one, so we need to check
                let mut actual_super = super_idx;

                // Check if we need to adjust
                if let Some(parent_block) = parent.get_block(super_idx) {
                    let end_field = if is_left {
                        parent_block.endleft.load(Ordering::Acquire)
                    } else {
                        parent_block.endright.load(Ordering::Acquire)
                    };

                    // If our block index is beyond this parent block's range, try the next one
                    if block_idx > end_field {
                        actual_super = super_idx + 1;
                    }
                }

                // Get the actual parent block
                let parent_block = parent
                    .get_block(actual_super)
                    .expect("Parent block must exist");
                let prev_parent = if actual_super > 1 {
                    parent.get_block(actual_super - 1)
                } else {
                    None
                };

                // Calculate position in parent block
                if is_left {
                    let prev_sumdeq = if block_idx > 0 {
                        node.get_block(block_idx - 1)
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    let parent_prev_endleft = prev_parent
                        .map(|b| b.endleft.load(Ordering::Acquire))
                        .unwrap_or(0);
                    let parent_prev_sumdeq = if parent_prev_endleft > 0 {
                        node.get_block(parent_prev_endleft)
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    rank += prev_sumdeq - parent_prev_sumdeq;
                } else {
                    // Right child - add left child's dequeues too
                    let prev_sumdeq = if block_idx > 0 {
                        node.get_block(block_idx - 1)
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    let parent_prev_endright = prev_parent
                        .map(|b| b.endright.load(Ordering::Acquire))
                        .unwrap_or(0);
                    let parent_prev_sumdeq = if parent_prev_endright > 0 {
                        node.get_block(parent_prev_endright)
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    rank += prev_sumdeq - parent_prev_sumdeq;

                    // Add all dequeues from left subtree
                    let left = &*parent.left;
                    let endleft = parent_block.endleft.load(Ordering::Acquire);
                    let prev_endleft = prev_parent
                        .map(|b| b.endleft.load(Ordering::Acquire))
                        .unwrap_or(0);

                    let left_sumdeq = if endleft > 0 {
                        left.get_block(endleft)
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    let left_prev_sumdeq = if prev_endleft > 0 {
                        left.get_block(prev_endleft)
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    rank += left_sumdeq - left_prev_sumdeq;
                }

                node = parent;
                block_idx = actual_super;
            }

            (block_idx, rank)
        }
    }

    // Get specific enqueue from tree
    fn get_enqueue(&self, node: &Node<T>, block_idx: usize, rank: usize) -> Result<T, ()> {
        unsafe {
            if node.is_leaf {
                let block = node.get_block(block_idx).ok_or(())?;

                // First check if this block's sumenq would have this rank
                let prev_sumenq = if block_idx > 0 {
                    node.get_block(block_idx - 1)
                        .map(|b| b.sumenq.load(Ordering::Acquire))
                        .unwrap_or(0)
                } else {
                    0
                };
                let block_sumenq = block.sumenq.load(Ordering::Acquire);
                let block_numenq = block_sumenq - prev_sumenq;

                // Debug
                if block_idx >= 99990 || block_numenq == 0 {
                    eprintln!(
                        "get_enqueue leaf: process_id={}, block_idx={}, rank={}, block_numenq={}, prev_sumenq={}, block_sumenq={}",
                        node.process_id, block_idx, rank, block_numenq, prev_sumenq, block_sumenq
                    );
                }

                // This block should have enqueues
                if block_numenq == 0 {
                    eprintln!(
                        "ERROR: Leaf block {} in process {} has no enqueues!",
                        block_idx, node.process_id
                    );
                    return Err(());
                }

                // Get the element
                let element = (*block.element.get()).clone();

                if element.is_none() {
                    eprintln!(
                        "ERROR: Expected enqueue but found dequeue in block {} of process {}!",
                        block_idx, node.process_id
                    );
                }

                element.ok_or(())
            } else {
                let block = node.get_block(block_idx).ok_or(())?;
                let prev_block = if block_idx > 0 {
                    node.get_block(block_idx - 1)
                } else {
                    None
                };

                // Get the range of subblocks for left and right children
                let prev_endleft = prev_block
                    .map(|b| b.endleft.load(Ordering::Acquire))
                    .unwrap_or(0);
                let endleft = block.endleft.load(Ordering::Acquire);
                let prev_endright = prev_block
                    .map(|b| b.endright.load(Ordering::Acquire))
                    .unwrap_or(0);
                let endright = block.endright.load(Ordering::Acquire);

                // Count enqueues in this block from left child
                let left = &*node.left;
                let left_enq_start = if prev_endleft > 0 {
                    left.get_block(prev_endleft)
                        .map(|b| b.sumenq.load(Ordering::Acquire))
                        .unwrap_or(0)
                } else {
                    0
                };
                let left_enq_end = if endleft > 0 {
                    left.get_block(endleft)
                        .map(|b| b.sumenq.load(Ordering::Acquire))
                        .unwrap_or(0)
                } else {
                    0
                };
                let left_numenq = left_enq_end - left_enq_start;

                // Count enqueues in this block from right child
                let right = &*node.right;
                let right_enq_start = if prev_endright > 0 {
                    right
                        .get_block(prev_endright)
                        .map(|b| b.sumenq.load(Ordering::Acquire))
                        .unwrap_or(0)
                } else {
                    0
                };
                let right_enq_end = if endright > 0 {
                    right
                        .get_block(endright)
                        .map(|b| b.sumenq.load(Ordering::Acquire))
                        .unwrap_or(0)
                } else {
                    0
                };
                let right_numenq = right_enq_end - right_enq_start;

                // Debug problematic cases
                if block_idx >= 100620 && block_idx <= 100625 {
                    eprintln!(
                        "get_enqueue node: block_idx={}, rank={}, left_numenq={}, right_numenq={}, endleft={}, endright={}",
                        block_idx, rank, left_numenq, right_numenq, endleft, endright
                    );
                }

                // Navigate to the correct child based on enqueue count
                if rank <= left_numenq && left_numenq > 0 {
                    // Find the specific block in left child that contains this rank
                    let mut accumulated = 0;
                    let mut target_block = prev_endleft + 1;
                    let mut rank_in_target = rank;

                    for i in (prev_endleft + 1)..=endleft {
                        if let Some(child_block) = left.get_block(i) {
                            let child_sumenq = child_block.sumenq.load(Ordering::Acquire);
                            let child_prev_sumenq = if i > 0 {
                                left.get_block(i - 1)
                                    .map(|b| b.sumenq.load(Ordering::Acquire))
                                    .unwrap_or(0)
                            } else {
                                0
                            };
                            let child_numenq = child_sumenq - child_prev_sumenq;

                            if child_numenq > 0 && accumulated + child_numenq >= rank {
                                target_block = i;
                                rank_in_target = rank - accumulated;
                                break;
                            }
                            accumulated += child_numenq;
                        }
                    }

                    self.get_enqueue(left, target_block, rank_in_target)
                } else if rank > left_numenq && right_numenq > 0 {
                    // In right child
                    let adjusted_rank = rank - left_numenq;

                    // Find the specific block in right child that contains this rank
                    let mut accumulated = 0;
                    let mut target_block = prev_endright + 1;
                    let mut rank_in_target = adjusted_rank;

                    for i in (prev_endright + 1)..=endright {
                        if let Some(child_block) = right.get_block(i) {
                            let child_sumenq = child_block.sumenq.load(Ordering::Acquire);
                            let child_prev_sumenq = if i > 0 {
                                right
                                    .get_block(i - 1)
                                    .map(|b| b.sumenq.load(Ordering::Acquire))
                                    .unwrap_or(0)
                            } else {
                                0
                            };
                            let child_numenq = child_sumenq - child_prev_sumenq;

                            if child_numenq > 0 && accumulated + child_numenq >= adjusted_rank {
                                target_block = i;
                                rank_in_target = adjusted_rank - accumulated;
                                break;
                            }
                            accumulated += child_numenq;
                        }
                    }

                    self.get_enqueue(right, target_block, rank_in_target)
                } else {
                    // This shouldn't happen - rank is beyond this block's enqueues
                    eprintln!(
                        "ERROR: rank {} exceeds block's enqueues ({} + {} = {})",
                        rank,
                        left_numenq,
                        right_numenq,
                        left_numenq + right_numenq
                    );
                    Err(())
                }
            }
        }
    }

    pub fn debug_state(&self) {
        unsafe {
            eprintln!("=== NRQueue Debug State ===");

            // Check root
            let root = &*self.root;
            let root_head = root.head.load(Ordering::Acquire);
            eprintln!("Root head: {}", root_head);

            let mut root_total_enq = 0;
            let mut root_total_deq = 0;
            if root_head > 0 {
                if let Some(last_block) = root.get_block(root_head - 1) {
                    root_total_enq = last_block.sumenq.load(Ordering::Acquire);
                    root_total_deq = last_block.sumdeq.load(Ordering::Acquire);
                    eprintln!(
                        "Root totals - Enq: {}, Deq: {}, Size: {}",
                        root_total_enq,
                        root_total_deq,
                        last_block.size.load(Ordering::Acquire)
                    );
                }
            }

            // Check leaves
            let mut total_leaf_enq = 0;
            let mut total_leaf_deq = 0;
            for i in 0..self.num_processes {
                let leaf = self.get_leaf(i);
                let leaf_head = leaf.head.load(Ordering::Acquire);

                if leaf_head > 1 {
                    if let Some(last_block) = leaf.get_block(leaf_head - 1) {
                        let enq = last_block.sumenq.load(Ordering::Acquire);
                        let deq = last_block.sumdeq.load(Ordering::Acquire);
                        total_leaf_enq += enq;
                        total_leaf_deq += deq;

                        eprintln!(
                            "Leaf {} head: {} (enq: {}, deq: {})",
                            i, leaf_head, enq, deq
                        );
                    }
                }
            }

            eprintln!(
                "Total in leaves - Enq: {}, Deq: {}",
                total_leaf_enq, total_leaf_deq
            );

            // The critical info:
            eprintln!(
                "Discrepancy - Enq not in root: {}, Deq not in root: {}",
                total_leaf_enq - root_total_enq,
                total_leaf_deq - root_total_deq
            );

            eprintln!("==========================");
        }
    }

    pub fn force_complete_sync(&self) {
        unsafe {
            // VERY aggressive sync - keep going until absolutely nothing changes
            let mut changed = true;
            let mut iterations = 0;

            while changed && iterations < 100 {
                changed = false;
                iterations += 1;

                // Get initial state
                let root = &*self.root;
                let initial_root_head = root.head.load(Ordering::Acquire);
                let initial_root_enq = if initial_root_head > 0 {
                    root.get_block(initial_root_head - 1)
                        .map(|b| b.sumenq.load(Ordering::Acquire))
                        .unwrap_or(0)
                } else {
                    0
                };

                // Multiple sync passes
                for _ in 0..5 {
                    self.sync();
                }

                // Check if anything changed
                let new_root_head = root.head.load(Ordering::Acquire);
                let new_root_enq = if new_root_head > 0 {
                    root.get_block(new_root_head - 1)
                        .map(|b| b.sumenq.load(Ordering::Acquire))
                        .unwrap_or(0)
                } else {
                    0
                };

                if new_root_head != initial_root_head || new_root_enq != initial_root_enq {
                    changed = true;
                }
            }
        }
    }

    // Initialize queue in shared memory
    pub unsafe fn init_in_shared(mem: *mut u8, num_processes: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Calculate tree dimensions
        let tree_height = (num_processes as f64).log2().ceil() as usize;
        let tree_size = (1 << (tree_height + 1)) - 1;

        // Calculate node size (node struct + blocks array)
        let node_struct_size = mem::size_of::<Node<T>>();
        let blocks_offset = (node_struct_size + 63) & !63; // Cache line align
        let blocks_size = BLOCKS_PER_NODE * mem::size_of::<Block<T>>();
        let node_size = blocks_offset + blocks_size;
        let node_size_aligned = (node_size + 63) & !63;

        // Memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let nodes_offset = queue_aligned;
        let nodes_total_size = tree_size * node_size_aligned;

        let total_size = nodes_offset + nodes_total_size;

        // Initialize queue
        ptr::write(
            queue_ptr,
            Self {
                root: ptr::null(),
                tree_height,
                num_processes,
                base_ptr: mem,
                total_size,
                nodes_offset,
                node_size: node_size_aligned,
                initialized: AtomicBool::new(false),
                _phantom: std::marker::PhantomData,
            },
        );

        let queue = &mut *queue_ptr;

        // Initialize nodes and their blocks
        for i in 0..tree_size {
            let node_ptr = queue.get_node_ptr(i);
            ptr::write(node_ptr, Node::new());

            // Initialize blocks for this node
            let blocks_ptr = (*node_ptr).get_blocks_ptr();
            for j in 0..BLOCKS_PER_NODE {
                ptr::write(blocks_ptr.add(j), Block::new());
            }
        }

        // Build tree structure
        queue.root = queue.get_node_ptr(0); // Root is at index 0

        for i in 0..tree_size {
            let node = &mut *queue.get_node_ptr(i);

            // Set tree pointers
            if i > 0 {
                node.parent = queue.get_node_ptr((i - 1) / 2);
            }

            let left_idx = 2 * i + 1;
            let right_idx = 2 * i + 2;

            if left_idx < tree_size {
                node.left = queue.get_node_ptr(left_idx);
            }
            if right_idx < tree_size {
                node.right = queue.get_node_ptr(right_idx);
            }

            // Mark leaves and root
            let leaf_start = (1 << tree_height) - 1;
            if i >= leaf_start && i < leaf_start + num_processes {
                node.is_leaf = true;
                node.process_id = i - leaf_start;
            }

            if i == 0 {
                node.is_root = true;
            }

            // Initialize sentinel block (block 0)
            let blocks = node.get_blocks_ptr();
            let sentinel = &*blocks.add(0);
            sentinel.sumenq.store(0, Ordering::Release);
            sentinel.sumdeq.store(0, Ordering::Release);
            sentinel.endleft.store(0, Ordering::Release);
            sentinel.endright.store(0, Ordering::Release);
            sentinel.size.store(0, Ordering::Release);
        }

        queue.initialized.store(true, Ordering::Release);
        queue
    }

    // Calculate required shared memory size
    pub fn shared_size(num_processes: usize) -> usize {
        let tree_height = (num_processes as f64).log2().ceil() as usize;
        let tree_size = (1 << (tree_height + 1)) - 1;

        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Calculate node size
        let node_struct_size = mem::size_of::<Node<T>>();
        let blocks_offset = (node_struct_size + 63) & !63;
        let blocks_size = BLOCKS_PER_NODE * mem::size_of::<Block<T>>();
        let node_size = blocks_offset + blocks_size;
        let node_size_aligned = (node_size + 63) & !63;

        let nodes_total_size = tree_size * node_size_aligned;

        let total = queue_aligned + nodes_total_size;
        (total + 4095) & !4095 // Page align
    }

    // Enqueue operation
    pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), ()> {
        if thread_id >= self.num_processes {
            return Err(());
        }

        self.append(thread_id, Some(item));
        Ok(())
    }

    // Dequeue operation
    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        if thread_id >= self.num_processes {
            return Err(());
        }

        // Use simple_dequeue which has the fixed logic
        self.simple_dequeue(thread_id)
    }

    pub fn is_empty(&self) -> bool {
        unsafe {
            let root_node = &*self.root;
            let head = root_node.head.load(Ordering::Acquire);

            if head <= 1 {
                return true;
            }

            if let Some(block) = root_node.get_block(head - 1) {
                block.size.load(Ordering::Acquire) == 0
            } else {
                true
            }
        }
    }

    pub fn is_full(&self) -> bool {
        false // Unbounded queue
    }
}

impl<T: Send + Clone + 'static> MpmcQueue<T> for NRQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, item: T, thread_id: usize) -> Result<(), Self::PushError> {
        self.enqueue(thread_id, item)
    }

    fn pop(&self, thread_id: usize) -> Result<T, Self::PopError> {
        self.dequeue(thread_id)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + Clone> Drop for NRQueue<T> {
    fn drop(&mut self) {
        // Nothing to clean up - all memory is in the shared region
    }
}
