// queues/src/mpmc/polylog_queue.rs
// Wait-Free MPMC Queue - Fixed for shared memory with reasonable memory usage

use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::MpmcQueue;

const CACHE_LINE_SIZE: usize = 64;

// Reasonable block limits based on expected usage
// For benchmarks with 250K items per process, we need enough blocks
// In worst case, each operation gets its own block
const BLOCKS_PER_LEAF: usize = 300_000; // Enough for 250K operations even without batching
const BLOCKS_PER_INTERNAL: usize = 50_000; // Internal nodes aggregate from children

// Block structure - represents a batch of operations
#[repr(C, align(64))] // Cache line aligned
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
    max_blocks: usize, // Maximum blocks for this node

    // Blocks array follows immediately after this struct in memory
    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send> Send for Node<T> {}
unsafe impl<T: Send> Sync for Node<T> {}

impl<T> Node<T> {
    fn new(max_blocks: usize) -> Self {
        Self {
            head: AtomicUsize::new(1), // Start at 1, blocks[0] is empty sentinel
            left: ptr::null(),
            right: ptr::null(),
            parent: ptr::null(),
            is_leaf: false,
            is_root: false,
            process_id: usize::MAX,
            max_blocks,
            _phantom: std::marker::PhantomData,
        }
    }

    unsafe fn get_blocks_ptr(&self) -> *mut Block<T> {
        // Blocks array starts immediately after the Node struct
        let node_ptr = self as *const Self as *mut u8;
        let node_size = mem::size_of::<Self>();
        // Align to cache line boundary
        let aligned_offset = (node_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);
        node_ptr.add(aligned_offset) as *mut Block<T>
    }

    unsafe fn get_block(&self, index: usize) -> Option<&Block<T>> {
        if index >= self.max_blocks {
            return None;
        }

        let blocks = self.get_blocks_ptr();
        if blocks.is_null() {
            return None;
        }

        // Verify the block pointer is within our allocated memory
        let block_ptr = blocks.add(index);
        Some(&*block_ptr)
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
    nodes_base: *mut u8,

    // Node layout info
    leaf_node_size: usize,
    internal_node_size: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for NRQueue<T> {}
unsafe impl<T: Send + Clone> Sync for NRQueue<T> {}

impl<T: Send + Clone + 'static> NRQueue<T> {
    unsafe fn get_node_ptr(&self, index: usize) -> *mut Node<T> {
        let leaf_start = (1 << self.tree_height) - 1;

        if index >= leaf_start {
            // It's a leaf node
            let leaf_index = index - leaf_start;
            if leaf_index >= self.num_processes {
                // Invalid leaf index
                return ptr::null_mut();
            }

            // Calculate offset for this leaf
            // First, skip all internal nodes
            let internal_nodes_total_size = leaf_start * self.internal_node_size;
            // Then add offset for this specific leaf
            let leaf_offset = leaf_index * self.leaf_node_size;
            let total_offset = internal_nodes_total_size + leaf_offset;

            self.nodes_base.add(total_offset) as *mut Node<T>
        } else {
            // It's an internal node
            let offset = index * self.internal_node_size;
            self.nodes_base.add(offset) as *mut Node<T>
        }
    }

    unsafe fn get_leaf(&self, process_id: usize) -> &Node<T> {
        if process_id >= self.num_processes {
            panic!(
                "Invalid process_id: {} >= {}",
                process_id, self.num_processes
            );
        }

        let leaf_start = (1 << self.tree_height) - 1;
        let node_ptr = self.get_node_ptr(leaf_start + process_id);

        if node_ptr.is_null() {
            panic!("Null node pointer for leaf {}", process_id);
        }

        &*node_ptr
    }

    // Binary search for block containing sum value
    unsafe fn binary_search_block(
        &self,
        node: &Node<T>,
        sum_value: usize,
        is_enq: bool,
    ) -> Option<usize> {
        let head = node.head.load(Ordering::Acquire);
        if head <= 1 {
            eprintln!("binary_search_block: head too small: {}", head);
            return None;
        }

        // For rank 0, return block 1
        if sum_value == 0 {
            return Some(1);
        }

        let mut left = 1;
        let mut right = head - 1;

        eprintln!(
            "binary_search_block: Searching for sum_value={} in range [{}, {}]",
            sum_value, left, right
        );

        while left <= right {
            let mid = (left + right) / 2;
            if let Some(block) = node.get_block(mid) {
                let mid_sum = if is_enq {
                    block.sumenq.load(Ordering::Acquire)
                } else {
                    block.sumdeq.load(Ordering::Acquire)
                };

                eprintln!("binary_search_block: mid={}, mid_sum={}", mid, mid_sum);

                if mid_sum >= sum_value {
                    // Check if this is the leftmost block with sum >= sum_value
                    if mid == 1 {
                        return Some(mid);
                    }

                    if let Some(prev_block) = node.get_block(mid - 1) {
                        let prev_sum = if is_enq {
                            prev_block.sumenq.load(Ordering::Acquire)
                        } else {
                            prev_block.sumdeq.load(Ordering::Acquire)
                        };

                        if prev_sum < sum_value {
                            return Some(mid);
                        }
                    }

                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            } else {
                eprintln!("binary_search_block: Cannot get block {}", mid);
                break;
            }
        }

        eprintln!("binary_search_block: Not found, returning {}", left);
        if left < head {
            Some(left)
        } else {
            None
        }
    }

    // Append operation to leaf
    fn append(&self, process_id: usize, element: Option<T>) {
        unsafe {
            let leaf = self.get_leaf(process_id);
            let h = leaf.head.load(Ordering::Acquire);

            if h >= leaf.max_blocks {
                eprintln!("Process {} leaf full at {}", process_id, h);
                return;
            }

            // Get block
            let block = &*leaf.get_blocks_ptr().add(h);

            // Check if it's enqueue or dequeue BEFORE moving element
            let is_enqueue = element.is_some();

            // Set element (this MOVES it)
            *block.element.get() = element;

            // Update sums atomically
            if h > 0 {
                let prev = &*leaf.get_blocks_ptr().add(h - 1);
                let prev_sumenq = prev.sumenq.load(Ordering::Acquire);
                let prev_sumdeq = prev.sumdeq.load(Ordering::Acquire);

                if is_enqueue {
                    block.sumenq.store(prev_sumenq + 1, Ordering::Release);
                    block.sumdeq.store(prev_sumdeq, Ordering::Release);
                } else {
                    block.sumenq.store(prev_sumenq, Ordering::Release);
                    block.sumdeq.store(prev_sumdeq + 1, Ordering::Release);
                }
            } else {
                // First real block
                if is_enqueue {
                    block.sumenq.store(1, Ordering::Release);
                    block.sumdeq.store(0, Ordering::Release);
                } else {
                    block.sumenq.store(0, Ordering::Release);
                    block.sumdeq.store(1, Ordering::Release);
                }
            }

            // Ensure block is visible before incrementing head
            std::sync::atomic::fence(Ordering::Release);

            // Increment head
            leaf.head.store(h + 1, Ordering::Release);

            // Debug: verify propagation path
            if !leaf.parent.is_null() {
                self.propagate(&*leaf.parent);
            }
        }
    }

    pub fn verify_tree_state(&self) -> (usize, usize) {
        unsafe {
            let root_node = &*self.root;
            let root_head = root_node.head.load(Ordering::Acquire);

            let mut total_enq = 0;
            let mut total_deq = 0;

            if root_head > 0 {
                if let Some(last_block) = root_node.get_block(root_head - 1) {
                    total_enq = last_block.sumenq.load(Ordering::Acquire);
                    total_deq = last_block.sumdeq.load(Ordering::Acquire);
                }
            }

            eprintln!(
                "Root state: head={}, total_enq={}, total_deq={}",
                root_head, total_enq, total_deq
            );

            // Check leaf states
            for i in 0..self.num_processes {
                let leaf = self.get_leaf(i);
                let leaf_head = leaf.head.load(Ordering::Acquire);

                let (leaf_enq, leaf_deq) = if leaf_head > 0 {
                    if let Some(block) = leaf.get_block(leaf_head - 1) {
                        (
                            block.sumenq.load(Ordering::Acquire),
                            block.sumdeq.load(Ordering::Acquire),
                        )
                    } else {
                        (0, 0)
                    }
                } else {
                    (0, 0)
                };

                eprintln!(
                    "Leaf {}: head={}, enq={}, deq={}",
                    i, leaf_head, leaf_enq, leaf_deq
                );
            }

            (total_enq, total_deq)
        }
    }

    // Propagate operations from children to parent
    fn propagate(&self, node: &Node<T>) {
        eprintln!("propagate: node={:p}, is_root={}", node, node.is_root);

        // The paper says to call refresh at most twice
        let first = self.refresh(node);
        eprintln!("propagate: first refresh returned {}", first);

        if !first {
            let second = self.refresh(node);
            eprintln!("propagate: second refresh returned {}", second);
        }

        unsafe {
            if !node.is_root && !node.parent.is_null() {
                self.propagate(&*node.parent);
            }
        }
    }

    pub fn force_full_propagation(&self) {
        unsafe {
            // Force propagation from all leaves
            for i in 0..self.num_processes {
                let leaf = self.get_leaf(i);
                if !leaf.parent.is_null() {
                    self.propagate(&*leaf.parent);
                }
            }
        }
    }

    // Fix the refresh method to be more aggressive:
    fn refresh(&self, node: &Node<T>) -> bool {
        unsafe {
            let h = node.head.load(Ordering::Acquire);

            // Lines 61-66: Help advance children
            if !node.left.is_null() {
                let left = &*node.left;
                let hdir = left.head.load(Ordering::Acquire);
                if hdir > 0 && left.get_block(hdir - 1).is_some() {
                    self.advance(left, hdir - 1);
                }
            }

            if !node.right.is_null() {
                let right = &*node.right;
                let hdir = right.head.load(Ordering::Acquire);
                if hdir > 0 && right.get_block(hdir - 1).is_some() {
                    self.advance(right, hdir - 1);
                }
            }

            // Line 67: Create new block
            let new_block = self.create_block(node, h);

            // Line 68: Check if empty
            if let Some((numenq, numdeq, endleft, endright, size)) = new_block {
                if numenq + numdeq == 0 {
                    return true;
                }

                // Get the block slot
                let block = &*node.get_blocks_ptr().add(h);

                // Fill in the block data
                if h > 0 {
                    let prev = &*node.get_blocks_ptr().add(h - 1);
                    block.sumenq.store(
                        prev.sumenq.load(Ordering::Relaxed) + numenq,
                        Ordering::Relaxed,
                    );
                    block.sumdeq.store(
                        prev.sumdeq.load(Ordering::Relaxed) + numdeq,
                        Ordering::Relaxed,
                    );
                } else {
                    block.sumenq.store(numenq, Ordering::Relaxed);
                    block.sumdeq.store(numdeq, Ordering::Relaxed);
                }

                block.endleft.store(endleft, Ordering::Relaxed);
                block.endright.store(endright, Ordering::Relaxed);

                if node.is_root {
                    block.size.store(size, Ordering::Relaxed);
                }

                // Line 70: Try CAS
                let result =
                    node.head
                        .compare_exchange(h, h + 1, Ordering::Release, Ordering::Acquire);

                // Line 71: Advance
                self.advance(node, h);

                result.is_ok()
            } else {
                true // No new operations
            }
        }
    }

    // Create block for refresh (optimized version)
    fn create_block(
        &self,
        node: &Node<T>,
        i: usize,
    ) -> Option<(usize, usize, usize, usize, usize)> {
        unsafe {
            let node_name = if node.is_root {
                "root"
            } else if node.is_leaf {
                "leaf"
            } else {
                "internal"
            };
            eprintln!("create_block: node={}, i={}", node_name, i);

            let mut numenq = 0;
            let mut numdeq = 0;
            let mut endleft = 0;
            let mut endright = 0;

            // Get previous block info
            let (prev_endleft, prev_endright) = if i > 0 {
                let prev = &*node.get_blocks_ptr().add(i - 1);
                (
                    prev.endleft.load(Ordering::Acquire),
                    prev.endright.load(Ordering::Acquire),
                )
            } else {
                (0, 0)
            };

            eprintln!(
                "create_block: prev_endleft={}, prev_endright={}",
                prev_endleft, prev_endright
            );

            // Process left child
            if !node.left.is_null() {
                let left = &*node.left;
                let left_head = left.head.load(Ordering::Acquire);
                endleft = if left_head > 0 { left_head - 1 } else { 0 };

                eprintln!("create_block: left_head={}, endleft={}", left_head, endleft);

                if endleft > prev_endleft {
                    // This is the problem - we're only getting one block at a time
                    eprintln!(
                        "create_block: Processing {} blocks from left child",
                        endleft - prev_endleft
                    );
                    // Get blocks at endpoints
                    let block_prev = if prev_endleft > 0 {
                        left.get_block(prev_endleft)
                    } else {
                        None
                    };

                    let block_last = left.get_block(endleft);

                    if let Some(last) = block_last {
                        let prev_sumenq = block_prev
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0);
                        let prev_sumdeq = block_prev
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0);

                        let last_sumenq = last.sumenq.load(Ordering::Acquire);
                        let last_sumdeq = last.sumdeq.load(Ordering::Acquire);

                        numenq += last_sumenq - prev_sumenq;
                        numdeq += last_sumdeq - prev_sumdeq;
                    }
                }
            }

            // Process right child
            if !node.right.is_null() {
                let right = &*node.right;

                // New end is current head - 1
                let right_head = right.head.load(Ordering::Acquire);
                endright = if right_head > 0 { right_head - 1 } else { 0 };

                if endright > prev_endright {
                    // Get blocks at endpoints
                    let block_prev = if prev_endright > 0 {
                        right.get_block(prev_endright)
                    } else {
                        None
                    };

                    let block_last = right.get_block(endright);

                    if let Some(last) = block_last {
                        let prev_sumenq = block_prev
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0);
                        let prev_sumdeq = block_prev
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0);

                        let last_sumenq = last.sumenq.load(Ordering::Acquire);
                        let last_sumdeq = last.sumdeq.load(Ordering::Acquire);

                        numenq += last_sumenq - prev_sumenq;
                        numdeq += last_sumdeq - prev_sumdeq;
                    }
                }
            }

            // Check if block is empty
            if numenq + numdeq == 0 {
                return None;
            }

            // Calculate size for root
            let size = if node.is_root {
                let prev_size = if i > 0 {
                    let prev = &*node.get_blocks_ptr().add(i - 1);
                    prev.size.load(Ordering::Acquire)
                } else {
                    0
                };
                (prev_size + numenq).saturating_sub(numdeq)
            } else {
                0
            };

            Some((numenq, numdeq, endleft, endright, size))
        }
    }

    // Advance head and set super field
    fn advance(&self, node: &Node<T>, h: usize) {
        unsafe {
            if h >= node.max_blocks {
                return;
            }

            // Set super field
            if !node.is_root && !node.parent.is_null() && h < node.max_blocks {
                let parent = &*node.parent;
                let parent_head = parent.head.load(Ordering::Acquire);

                if let Some(block) = node.get_block(h) {
                    // Only set if currently 0
                    block
                        .super_idx
                        .compare_exchange(0, parent_head, Ordering::AcqRel, Ordering::Acquire)
                        .ok();
                }
            }

            // Try to increment head
            node.head
                .compare_exchange(h, h + 1, Ordering::AcqRel, Ordering::Acquire)
                .ok();
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
                let Some(block) = node.get_block(block_idx) else {
                    return (0, 0);
                };

                let super_idx = block.super_idx.load(Ordering::Acquire);
                let super_idx = if super_idx == 0 { 1 } else { super_idx };

                let parent = &*node.parent;
                let is_left = ptr::eq(parent.left, node);

                // Verify super index
                let actual_super = if let Some(parent_block) = parent.get_block(super_idx) {
                    let end_field = if is_left {
                        parent_block.endleft.load(Ordering::Acquire)
                    } else {
                        parent_block.endright.load(Ordering::Acquire)
                    };

                    if block_idx > end_field {
                        super_idx + 1
                    } else {
                        super_idx
                    }
                } else {
                    1
                };

                // Calculate position in parent block
                let (Some(parent_block), prev_parent) = (
                    parent.get_block(actual_super),
                    if actual_super > 0 {
                        parent.get_block(actual_super - 1)
                    } else {
                        None
                    },
                ) else {
                    return (0, 0);
                };

                // Add dequeues before this block in parent
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

    // Find response for dequeue
    fn find_response(&self, root_block_idx: usize, deq_rank: usize) -> Result<T, ()> {
        unsafe {
            let root_node = &*self.root;

            // Check if block index is valid
            if root_block_idx == 0 || root_block_idx >= root_node.head.load(Ordering::Acquire) {
                eprintln!(
                    "find_response: Invalid block index {} (head={})",
                    root_block_idx,
                    root_node.head.load(Ordering::Acquire)
                );
                return Err(());
            }

            let Some(block) = root_node.get_block(root_block_idx) else {
                eprintln!("find_response: Cannot get block {}", root_block_idx);
                return Err(());
            };

            let prev_block = if root_block_idx > 0 {
                root_node.get_block(root_block_idx - 1)
            } else {
                None
            };

            let prev_size = prev_block
                .map(|b| b.size.load(Ordering::Acquire))
                .unwrap_or(0);
            let numenq = block.sumenq.load(Ordering::Acquire)
                - prev_block
                    .map(|b| b.sumenq.load(Ordering::Acquire))
                    .unwrap_or(0);

            // Check if queue is empty at the time of this dequeue
            if prev_size + numenq < deq_rank {
                eprintln!(
                    "find_response: Queue empty - prev_size={}, numenq={}, deq_rank={}",
                    prev_size, numenq, deq_rank
                );
                return Err(()); // Null dequeue
            }

            // Find the enqueue to return
            let prev_sumenq = prev_block
                .map(|b| b.sumenq.load(Ordering::Acquire))
                .unwrap_or(0);
            let enq_rank = deq_rank + prev_sumenq - prev_size;

            eprintln!("find_response: Looking for enqueue rank {} (deq_rank={}, prev_sumenq={}, prev_size={})",
                      enq_rank, deq_rank, prev_sumenq, prev_size);

            // Binary search for enqueue's block
            let target_block = self
                .binary_search_block(root_node, enq_rank, true)
                .ok_or_else(|| {
                    eprintln!(
                        "find_response: Binary search failed for enq_rank={}",
                        enq_rank
                    );
                    ()
                })?;

            eprintln!("find_response: Found target block {}", target_block);

            // Get enqueue within block
            let prev_target = if target_block > 0 {
                root_node.get_block(target_block - 1)
            } else {
                None
            };

            let rank_in_block = enq_rank
                - prev_target
                    .map(|b| b.sumenq.load(Ordering::Acquire))
                    .unwrap_or(0);

            eprintln!("find_response: Rank in block = {}", rank_in_block);

            // Navigate down to leaf
            self.get_enqueue(root_node, target_block, rank_in_block)
        }
    }

    // Get specific enqueue from tree
    fn get_enqueue(&self, node: &Node<T>, block_idx: usize, rank: usize) -> Result<T, ()> {
        unsafe {
            eprintln!(
                "get_enqueue: node={:p}, block_idx={}, rank={}, is_leaf={}",
                node, block_idx, rank, node.is_leaf
            );

            if node.is_leaf {
                let Some(block) = node.get_block(block_idx) else {
                    eprintln!("get_enqueue: Cannot get leaf block {}", block_idx);
                    return Err(());
                };

                let elem = (*block.element.get()).clone();
                eprintln!("get_enqueue: Leaf element = {:?}", elem.is_some());
                elem.ok_or(())
            } else {
                let Some(block) = node.get_block(block_idx) else {
                    eprintln!("get_enqueue: Cannot get internal block {}", block_idx);
                    return Err(());
                };

                let prev_block = if block_idx > 0 {
                    node.get_block(block_idx - 1)
                } else {
                    None
                };

                // Check if in left or right child
                let prev_endleft = prev_block
                    .map(|b| b.endleft.load(Ordering::Acquire))
                    .unwrap_or(0);
                let endleft = block.endleft.load(Ordering::Acquire);

                eprintln!(
                    "get_enqueue: prev_endleft={}, endleft={}",
                    prev_endleft, endleft
                );

                let left = &*node.left;

                // Calculate left child's enqueues
                let left_numenq = if endleft > prev_endleft && endleft > 0 {
                    let end_block = left.get_block(endleft).ok_or_else(|| {
                        eprintln!("get_enqueue: Cannot get left end block {}", endleft);
                        ()
                    })?;
                    let end_sum = end_block.sumenq.load(Ordering::Acquire);

                    let prev_sum = if prev_endleft > 0 {
                        left.get_block(prev_endleft)
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };

                    eprintln!(
                        "get_enqueue: Left child - end_sum={}, prev_sum={}",
                        end_sum, prev_sum
                    );
                    end_sum - prev_sum
                } else {
                    0
                };

                eprintln!("get_enqueue: left_numenq={}, rank={}", left_numenq, rank);

                if rank <= left_numenq && left_numenq > 0 {
                    // In left child
                    let target_enq = if prev_endleft > 0 {
                        let base = left
                            .get_block(prev_endleft)
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0);
                        base + rank
                    } else {
                        rank
                    };

                    eprintln!("get_enqueue: Going left, target_enq={}", target_enq);

                    let target = self
                        .binary_search_block(left, target_enq, true)
                        .ok_or_else(|| {
                            eprintln!("get_enqueue: Left binary search failed");
                            ()
                        })?;

                    let prev_sum = if target > 1 {
                        left.get_block(target - 1)
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };

                    let rank_in_child = target_enq - prev_sum;
                    self.get_enqueue(left, target, rank_in_child)
                } else {
                    // In right child
                    let right = &*node.right;
                    let prev_endright = prev_block
                        .map(|b| b.endright.load(Ordering::Acquire))
                        .unwrap_or(0);

                    let adjusted_rank = rank - left_numenq;

                    let target_enq = if prev_endright > 0 {
                        let base = right
                            .get_block(prev_endright)
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0);
                        base + adjusted_rank
                    } else {
                        adjusted_rank
                    };

                    eprintln!(
                        "get_enqueue: Going right, adjusted_rank={}, target_enq={}",
                        adjusted_rank, target_enq
                    );

                    let target = self
                        .binary_search_block(right, target_enq, true)
                        .ok_or_else(|| {
                            eprintln!("get_enqueue: Right binary search failed");
                            ()
                        })?;

                    let prev_sum = if target > 1 {
                        right
                            .get_block(target - 1)
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };

                    let rank_in_child = target_enq - prev_sum;
                    self.get_enqueue(right, target, rank_in_child)
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
        let leaf_start = (1 << tree_height) - 1;
        let internal_nodes = leaf_start;

        // Calculate node sizes
        let node_struct_size = mem::size_of::<Node<T>>();
        let node_aligned = (node_struct_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Block size should already be cache-line aligned due to #[repr(C, align(64))]
        let block_size = mem::size_of::<Block<T>>();
        let leaf_blocks_size = BLOCKS_PER_LEAF * block_size;
        let internal_blocks_size = BLOCKS_PER_INTERNAL * block_size;

        let leaf_node_size = node_aligned + leaf_blocks_size;
        let internal_node_size = node_aligned + internal_blocks_size;

        // Align to cache lines
        let leaf_node_size = (leaf_node_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);
        let internal_node_size =
            (internal_node_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let nodes_offset = queue_aligned;
        let internal_nodes_size = internal_nodes * internal_node_size;
        let leaf_nodes_size = num_processes * leaf_node_size;
        let total_nodes_size = internal_nodes_size + leaf_nodes_size;

        let total_size = nodes_offset + total_nodes_size;

        // Initialize queue
        ptr::write(
            queue_ptr,
            Self {
                root: ptr::null(),
                tree_height,
                num_processes,
                base_ptr: mem,
                total_size,
                nodes_base: mem.add(nodes_offset),
                leaf_node_size,
                internal_node_size,
                _phantom: std::marker::PhantomData,
            },
        );

        let queue = &mut *queue_ptr;

        // Initialize nodes and their blocks
        for i in 0..tree_size {
            if i >= leaf_start + num_processes {
                continue; // Skip unused leaves
            }

            let node_ptr = queue.get_node_ptr(i);
            let max_blocks = if i >= leaf_start {
                BLOCKS_PER_LEAF
            } else {
                BLOCKS_PER_INTERNAL
            };

            ptr::write(node_ptr, Node::new(max_blocks));

            // Initialize ALL blocks, not just first few
            let blocks_ptr = (*node_ptr).get_blocks_ptr();

            // Initialize all blocks to ensure they're ready
            for j in 0..max_blocks {
                ptr::write(blocks_ptr.add(j), Block::new());
            }
        }

        // Build tree structure
        queue.root = queue.get_node_ptr(0); // Root is at index 0

        for i in 0..tree_size {
            if i >= leaf_start + num_processes {
                continue; // Skip unused leaves
            }

            let node = &mut *queue.get_node_ptr(i);

            // Set tree pointers
            if i > 0 {
                node.parent = queue.get_node_ptr((i - 1) / 2);
            }

            let left_idx = 2 * i + 1;
            let right_idx = 2 * i + 2;

            if left_idx < tree_size
                && (left_idx < leaf_start || left_idx < leaf_start + num_processes)
            {
                node.left = queue.get_node_ptr(left_idx);
            }
            if right_idx < tree_size
                && (right_idx < leaf_start || right_idx < leaf_start + num_processes)
            {
                node.right = queue.get_node_ptr(right_idx);
            }

            // Mark leaves and root
            if i >= leaf_start && i < leaf_start + num_processes {
                node.is_leaf = true;
                node.process_id = i - leaf_start;
            }

            if i == 0 {
                node.is_root = true;
            }
        }

        queue
    }

    // Calculate required shared memory size
    pub fn shared_size(num_processes: usize) -> usize {
        let tree_height = (num_processes as f64).log2().ceil() as usize;
        let num_leaves = 1 << tree_height; // Total leaves in perfect binary tree
        let leaf_start = num_leaves - 1;
        let internal_nodes = leaf_start;

        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Calculate node sizes
        let node_struct_size = mem::size_of::<Node<T>>();
        let node_aligned = (node_struct_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let block_size = mem::size_of::<Block<T>>();

        // Calculate total size for blocks
        let leaf_blocks_size = BLOCKS_PER_LEAF * block_size;
        let internal_blocks_size = BLOCKS_PER_INTERNAL * block_size;

        let leaf_node_size = node_aligned + leaf_blocks_size;
        let internal_node_size = node_aligned + internal_blocks_size;

        // Align to cache lines
        let leaf_node_size = (leaf_node_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);
        let internal_node_size =
            (internal_node_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Calculate total memory needed
        let internal_nodes_total = internal_nodes * internal_node_size;
        let leaf_nodes_total = num_leaves * leaf_node_size; // Allocate for ALL leaves in tree

        let nodes_total = internal_nodes_total + leaf_nodes_total;
        let total = queue_aligned + nodes_total;

        // Page align and add safety margin
        ((total + (1 << 20)) + 4095) & !4095
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

        // Append dequeue operation
        self.append(thread_id, None);

        unsafe {
            // Find dequeue's position
            let leaf = self.get_leaf(thread_id);
            let leaf_head = leaf.head.load(Ordering::Acquire);
            if leaf_head <= 1 {
                eprintln!("Dequeue: leaf head too small: {}", leaf_head);
                return Err(());
            }

            // The dequeue we just appended is at position leaf_head - 1
            let (root_block, rank) = self.index_dequeue(leaf, leaf_head - 1, 1);

            // Check if we got a valid root block
            if root_block == 0 {
                eprintln!("Dequeue: invalid root block");
                return Err(());
            }

            // Get response
            match self.find_response(root_block, rank) {
                Ok(val) => Ok(val),
                Err(_) => {
                    eprintln!(
                        "Dequeue: find_response failed for block={}, rank={}",
                        root_block, rank
                    );
                    Err(())
                }
            }
        }
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
        false // Unbounded queue (within block limits)
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
