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

            // Increment head
            leaf.head.fetch_add(1, Ordering::AcqRel);

            // Propagate to root
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

    // Force propagation of all pending operations to root
    pub fn flush(&self) {
        unsafe {
            // Force refresh on all nodes from leaves to root
            let tree_height = self.tree_height;

            // Start from leaves and work up
            for level in (0..=tree_height).rev() {
                let start_idx = (1 << level) - 1;
                let end_idx = (1 << (level + 1)) - 1;

                for idx in start_idx..end_idx.min(start_idx + (1 << level)) {
                    let node = self.get_node(idx);

                    // Force refresh twice to ensure propagation
                    self.refresh(node);
                    self.refresh(node);
                }
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

    // Create block for refresh
    fn create_block(
        &self,
        node: &Node<T>,
        h: usize,
    ) -> Option<(usize, usize, usize, usize, usize)> {
        unsafe {
            let mut endleft = 0;
            let mut endright = 0;
            let mut numenq = 0;
            let mut numdeq = 0;

            // Get previous block's ends
            let prev_endleft = if h > 0 {
                node.get_block(h - 1)
                    .map(|b| b.endleft.load(Ordering::Acquire))
                    .unwrap_or(0)
            } else {
                0
            };

            let prev_endright = if h > 0 {
                node.get_block(h - 1)
                    .map(|b| b.endright.load(Ordering::Acquire))
                    .unwrap_or(0)
            } else {
                0
            };

            // Get current heads
            if !node.left.is_null() {
                let left = &*node.left;
                endleft = left.head.load(Ordering::Acquire).saturating_sub(1);

                // Count operations from left child
                if endleft > prev_endleft {
                    for i in (prev_endleft + 1)..=endleft {
                        if let Some(block) = left.get_block(i) {
                            let sumenq = block.sumenq.load(Ordering::Acquire);
                            let sumdeq = block.sumdeq.load(Ordering::Acquire);
                            let prev_sumenq = if i > 0 {
                                left.get_block(i - 1)
                                    .map(|b| b.sumenq.load(Ordering::Acquire))
                                    .unwrap_or(0)
                            } else {
                                0
                            };
                            let prev_sumdeq = if i > 0 {
                                left.get_block(i - 1)
                                    .map(|b| b.sumdeq.load(Ordering::Acquire))
                                    .unwrap_or(0)
                            } else {
                                0
                            };
                            numenq += sumenq - prev_sumenq;
                            numdeq += sumdeq - prev_sumdeq;
                        }
                    }
                }
            }

            if !node.right.is_null() {
                let right = &*node.right;
                endright = right.head.load(Ordering::Acquire).saturating_sub(1);

                // Count operations from right child
                if endright > prev_endright {
                    for i in (prev_endright + 1)..=endright {
                        if let Some(block) = right.get_block(i) {
                            let sumenq = block.sumenq.load(Ordering::Acquire);
                            let sumdeq = block.sumdeq.load(Ordering::Acquire);
                            let prev_sumenq = if i > 0 {
                                right
                                    .get_block(i - 1)
                                    .map(|b| b.sumenq.load(Ordering::Acquire))
                                    .unwrap_or(0)
                            } else {
                                0
                            };
                            let prev_sumdeq = if i > 0 {
                                right
                                    .get_block(i - 1)
                                    .map(|b| b.sumdeq.load(Ordering::Acquire))
                                    .unwrap_or(0)
                            } else {
                                0
                            };
                            numenq += sumenq - prev_sumenq;
                            numdeq += sumdeq - prev_sumdeq;
                        }
                    }
                }
            }

            if numenq + numdeq == 0 {
                return None; // No new operations
            }

            // Calculate size for root blocks
            let size = if node.is_root {
                let prev_size = if h > 0 {
                    node.get_block(h - 1)
                        .map(|b| b.size.load(Ordering::Acquire))
                        .unwrap_or(0)
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
            // Try to advance head first
            let advanced = node
                .head
                .compare_exchange(h, h + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();

            // Set super field if not root
            if !node.is_root && !node.parent.is_null() {
                let parent = &*node.parent;

                if let Some(block) = node.get_block(h) {
                    // Only set if we were the ones who advanced, or if it's still unset
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

    // Find response for dequeue
    fn find_response(&self, root_block_idx: usize, deq_rank: usize) -> Result<T, ()> {
        unsafe {
            let root_node = &*self.root;
            let block = root_node.get_block(root_block_idx).ok_or(())?;
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

            // Check if queue is empty
            if prev_size + numenq < deq_rank {
                return Err(()); // Null dequeue
            }

            // Find the enqueue to return
            let prev_sumenq = prev_block
                .map(|b| b.sumenq.load(Ordering::Acquire))
                .unwrap_or(0);
            let enq_rank = deq_rank + prev_sumenq - prev_size;

            // Binary search for enqueue's block
            let mut left = 1;
            let mut right = root_block_idx;
            let mut target_block = left;

            while left <= right {
                let mid = (left + right) / 2;
                if let Some(mid_block) = root_node.get_block(mid) {
                    let mid_sumenq = mid_block.sumenq.load(Ordering::Acquire);
                    if mid_sumenq >= enq_rank {
                        target_block = mid;
                        right = mid - 1;
                    } else {
                        left = mid + 1;
                    }
                }
            }

            // Get enqueue within block
            let _target_block_obj = root_node.get_block(target_block).ok_or(())?;
            let prev_target = if target_block > 0 {
                root_node.get_block(target_block - 1)
            } else {
                None
            };

            let rank_in_block = enq_rank
                - prev_target
                    .map(|b| b.sumenq.load(Ordering::Acquire))
                    .unwrap_or(0);

            // Navigate down to leaf
            self.get_enqueue(root_node, target_block, rank_in_block)
        }
    }

    // Get specific enqueue from tree
    fn get_enqueue(&self, node: &Node<T>, block_idx: usize, rank: usize) -> Result<T, ()> {
        unsafe {
            if node.is_leaf {
                let block = node.get_block(block_idx).ok_or(())?;
                (*block.element.get()).clone().ok_or(())
            } else {
                let block = node.get_block(block_idx).ok_or(())?;
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

                let left = &*node.left;
                let left_numenq = if endleft > prev_endleft {
                    let mut sum = 0;
                    for i in (prev_endleft + 1)..=endleft {
                        if let Some(child_block) = left.get_block(i) {
                            let sumenq = child_block.sumenq.load(Ordering::Acquire);
                            let prev_sumenq = if i > 0 {
                                left.get_block(i - 1)
                                    .map(|b| b.sumenq.load(Ordering::Acquire))
                                    .unwrap_or(0)
                            } else {
                                0
                            };
                            sum += sumenq - prev_sumenq;
                        }
                    }
                    sum
                } else {
                    0
                };

                if rank <= left_numenq {
                    // In left child
                    let mut target = prev_endleft + 1;
                    let mut accumulated = 0;

                    for i in (prev_endleft + 1)..=endleft {
                        if let Some(child_block) = left.get_block(i) {
                            let sumenq = child_block.sumenq.load(Ordering::Acquire);
                            let prev_sumenq = if i > 0 {
                                left.get_block(i - 1)
                                    .map(|b| b.sumenq.load(Ordering::Acquire))
                                    .unwrap_or(0)
                            } else {
                                0
                            };
                            let block_numenq = sumenq - prev_sumenq;

                            if accumulated + block_numenq >= rank {
                                target = i;
                                break;
                            }
                            accumulated += block_numenq;
                        }
                    }

                    let rank_in_child = rank - accumulated;
                    self.get_enqueue(left, target, rank_in_child)
                } else {
                    // In right child
                    let right = &*node.right;
                    let prev_endright = prev_block
                        .map(|b| b.endright.load(Ordering::Acquire))
                        .unwrap_or(0);
                    let endright = block.endright.load(Ordering::Acquire);

                    let mut target = prev_endright + 1;
                    let mut accumulated = 0;
                    let adjusted_rank = rank - left_numenq;

                    for i in (prev_endright + 1)..=endright {
                        if let Some(child_block) = right.get_block(i) {
                            let sumenq = child_block.sumenq.load(Ordering::Acquire);
                            let prev_sumenq = if i > 0 {
                                right
                                    .get_block(i - 1)
                                    .map(|b| b.sumenq.load(Ordering::Acquire))
                                    .unwrap_or(0)
                            } else {
                                0
                            };
                            let block_numenq = sumenq - prev_sumenq;

                            if accumulated + block_numenq >= adjusted_rank {
                                target = i;
                                break;
                            }
                            accumulated += block_numenq;
                        }
                    }

                    let rank_in_child = adjusted_rank - accumulated;
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

        // Append dequeue operation
        self.append(thread_id, None);

        unsafe {
            // Find dequeue's position
            let leaf = self.get_leaf(thread_id);
            let leaf_head = leaf.head.load(Ordering::Acquire);
            let (root_block, rank) = self.index_dequeue(leaf, leaf_head - 1, 1);

            // Get response
            self.find_response(root_block, rank)
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
        false // Unbounded queue
    }

    // Get total enqueued items in root
    pub fn total_enqueued(&self) -> usize {
        unsafe {
            let root_node = &*self.root;
            let head = root_node.head.load(Ordering::Acquire);

            if head <= 1 {
                return 0;
            }

            if let Some(block) = root_node.get_block(head - 1) {
                block.sumenq.load(Ordering::Acquire)
            } else {
                0
            }
        }
    }

    // Get total dequeued items in root
    pub fn total_dequeued(&self) -> usize {
        unsafe {
            let root_node = &*self.root;
            let head = root_node.head.load(Ordering::Acquire);

            if head <= 1 {
                return 0;
            }

            if let Some(block) = root_node.get_block(head - 1) {
                block.sumdeq.load(Ordering::Acquire)
            } else {
                0
            }
        }
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
