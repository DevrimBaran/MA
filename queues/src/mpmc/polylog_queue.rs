// queues/src/mpmc/polylog_queue.rs
// Wait-Free MPMC Queue - Fixed without stack overflow

use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::MpmcQueue;

const CACHE_LINE_SIZE: usize = 64;
const BLOCKS_PER_LEAF: usize = 300_000;
const BLOCKS_PER_INTERNAL: usize = 50_000;

// Block structure - represents a batch of operations
#[repr(C, align(64))]
struct Block<T> {
    sumenq: AtomicUsize,
    sumdeq: AtomicUsize,
    super_idx: AtomicUsize,
    endleft: AtomicUsize,
    endright: AtomicUsize,
    element: UnsafeCell<Option<T>>,
    size: AtomicUsize,
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
    head: AtomicUsize,
    left: *const Node<T>,
    right: *const Node<T>,
    parent: *const Node<T>,
    is_leaf: bool,
    is_root: bool,
    process_id: usize,
    max_blocks: usize,
    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send> Send for Node<T> {}
unsafe impl<T: Send> Sync for Node<T> {}

impl<T> Node<T> {
    fn new(max_blocks: usize) -> Self {
        Self {
            head: AtomicUsize::new(1),
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
        let node_ptr = self as *const Self as *mut u8;
        let node_size = mem::size_of::<Self>();
        let aligned_offset = (node_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);
        node_ptr.add(aligned_offset) as *mut Block<T>
    }

    unsafe fn get_block(&self, index: usize) -> Option<&Block<T>> {
        if index >= self.max_blocks || index == 0 {
            return None;
        }
        let blocks = self.get_blocks_ptr();
        if blocks.is_null() {
            return None;
        }
        Some(&*blocks.add(index))
    }
}

// Main queue structure
#[repr(C)]
pub struct NRQueue<T: Send + Clone + 'static> {
    root: *const Node<T>,
    tree_height: usize,
    num_processes: usize,
    base_ptr: *mut u8,
    total_size: usize,
    nodes_base: *mut u8,
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
            let leaf_index = index - leaf_start;
            if leaf_index >= self.num_processes {
                return ptr::null_mut();
            }
            let internal_nodes_total_size = leaf_start * self.internal_node_size;
            let leaf_offset = leaf_index * self.leaf_node_size;
            let total_offset = internal_nodes_total_size + leaf_offset;
            self.nodes_base.add(total_offset) as *mut Node<T>
        } else {
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

    // Append operation to leaf
    fn append(&self, process_id: usize, element: Option<T>) {
        unsafe {
            let leaf = self.get_leaf(process_id);
            let h = leaf.head.load(Ordering::Acquire);

            if h >= leaf.max_blocks {
                panic!("Leaf {} is full at {}", process_id, h);
            }

            let block = &*leaf.get_blocks_ptr().add(h);
            let is_enqueue = element.is_some();
            *block.element.get() = element;

            // Update cumulative sums
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

            // Increment head
            leaf.head.store(h + 1, Ordering::Release);

            // Propagate up the tree
            let mut current = leaf as *const Node<T>;
            while !(*current).is_root && !(*current).parent.is_null() {
                current = (*current).parent;
                // Try refresh twice
                self.refresh(&*current);
                self.refresh(&*current);
            }
        }
    }

    // Simple refresh that tries to create a block with all pending operations
    fn refresh(&self, node: &Node<T>) -> bool {
        unsafe {
            let h = node.head.load(Ordering::Acquire);

            if h >= node.max_blocks {
                return false;
            }

            // Get previous block info
            let (prev_endleft, prev_endright) = if h > 1 {
                let prev = node.get_block(h - 1).unwrap();
                (
                    prev.endleft.load(Ordering::Acquire),
                    prev.endright.load(Ordering::Acquire),
                )
            } else {
                (0, 0)
            };

            let mut total_enq = 0;
            let mut total_deq = 0;
            let mut new_endleft = prev_endleft;
            let mut new_endright = prev_endright;

            // Process left child
            if !node.left.is_null() {
                let left = &*node.left;
                let left_head = left.head.load(Ordering::Acquire);

                if left_head > 1 {
                    new_endleft = left_head - 1;

                    if new_endleft > prev_endleft {
                        if let Some(end_block) = left.get_block(new_endleft) {
                            let end_sumenq = end_block.sumenq.load(Ordering::Acquire);
                            let end_sumdeq = end_block.sumdeq.load(Ordering::Acquire);

                            if prev_endleft > 0 {
                                if let Some(prev_block) = left.get_block(prev_endleft) {
                                    total_enq +=
                                        end_sumenq - prev_block.sumenq.load(Ordering::Acquire);
                                    total_deq +=
                                        end_sumdeq - prev_block.sumdeq.load(Ordering::Acquire);
                                }
                            } else {
                                total_enq += end_sumenq;
                                total_deq += end_sumdeq;
                            }
                        }
                    }
                }
            }

            // Process right child
            if !node.right.is_null() {
                let right = &*node.right;
                let right_head = right.head.load(Ordering::Acquire);

                if right_head > 1 {
                    new_endright = right_head - 1;

                    if new_endright > prev_endright {
                        if let Some(end_block) = right.get_block(new_endright) {
                            let end_sumenq = end_block.sumenq.load(Ordering::Acquire);
                            let end_sumdeq = end_block.sumdeq.load(Ordering::Acquire);

                            if prev_endright > 0 {
                                if let Some(prev_block) = right.get_block(prev_endright) {
                                    total_enq +=
                                        end_sumenq - prev_block.sumenq.load(Ordering::Acquire);
                                    total_deq +=
                                        end_sumdeq - prev_block.sumdeq.load(Ordering::Acquire);
                                }
                            } else {
                                total_enq += end_sumenq;
                                total_deq += end_sumdeq;
                            }
                        }
                    }
                }
            }

            // No new operations
            if total_enq + total_deq == 0 {
                return true;
            }

            // Create new block
            let block = &*node.get_blocks_ptr().add(h);

            // Set cumulative sums
            if h > 1 {
                let prev = node.get_block(h - 1).unwrap();
                block.sumenq.store(
                    prev.sumenq.load(Ordering::Relaxed) + total_enq,
                    Ordering::Release,
                );
                block.sumdeq.store(
                    prev.sumdeq.load(Ordering::Relaxed) + total_deq,
                    Ordering::Release,
                );
            } else {
                block.sumenq.store(total_enq, Ordering::Release);
                block.sumdeq.store(total_deq, Ordering::Release);
            }

            block.endleft.store(new_endleft, Ordering::Release);
            block.endright.store(new_endright, Ordering::Release);

            // For root, calculate queue size
            if node.is_root {
                let prev_size = if h > 1 {
                    node.get_block(h - 1).unwrap().size.load(Ordering::Acquire)
                } else {
                    0
                };
                let new_size = (prev_size + total_enq).saturating_sub(total_deq);
                block.size.store(new_size, Ordering::Release);
            }

            // Set super index
            if !node.parent.is_null() {
                let parent = &*node.parent;
                let parent_head = parent.head.load(Ordering::Acquire);
                block.super_idx.store(parent_head, Ordering::Release);
            }

            // Try to install block
            node.head
                .compare_exchange(h, h + 1, Ordering::Release, Ordering::Acquire)
                .is_ok()
        }
    }

    // Binary search implementation
    unsafe fn binary_search_block(
        &self,
        node: &Node<T>,
        target_sum: usize,
        is_enq: bool,
    ) -> Option<usize> {
        let head = node.head.load(Ordering::Acquire);
        if head <= 1 || target_sum == 0 {
            return None;
        }

        let mut left = 1;
        let mut right = head - 1;

        while left <= right {
            let mid = (left + right) / 2;
            if let Some(block) = node.get_block(mid) {
                let mid_sum = if is_enq {
                    block.sumenq.load(Ordering::Acquire)
                } else {
                    block.sumdeq.load(Ordering::Acquire)
                };

                if mid_sum >= target_sum {
                    if mid == 1 {
                        return Some(mid);
                    }

                    if let Some(prev_block) = node.get_block(mid - 1) {
                        let prev_sum = if is_enq {
                            prev_block.sumenq.load(Ordering::Acquire)
                        } else {
                            prev_block.sumdeq.load(Ordering::Acquire)
                        };

                        if prev_sum < target_sum {
                            return Some(mid);
                        }
                    }

                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            } else {
                break;
            }
        }

        None
    }

    // Find dequeue position in root
    fn index_dequeue(&self, node: &Node<T>, block_idx: usize, rank: usize) -> (usize, usize) {
        unsafe {
            let mut current_node = node;
            let mut current_block_idx = block_idx;
            let mut current_rank = rank;

            while !current_node.is_root {
                let Some(block) = current_node.get_block(current_block_idx) else {
                    return (0, 0);
                };

                let parent = &*current_node.parent;
                let is_left = ptr::eq(parent.left, current_node);

                // Find superblock
                let mut super_idx = block.super_idx.load(Ordering::Acquire);
                if super_idx == 0 {
                    super_idx = 1;
                }

                let parent_head = parent.head.load(Ordering::Acquire);
                if super_idx >= parent_head {
                    super_idx = parent_head - 1;
                }

                // Adjust position
                if let Some(parent_block) = parent.get_block(super_idx) {
                    let end_field = if is_left {
                        parent_block.endleft.load(Ordering::Acquire)
                    } else {
                        parent_block.endright.load(Ordering::Acquire)
                    };

                    // Find correct superblock
                    while current_block_idx > end_field && super_idx + 1 < parent_head {
                        super_idx += 1;
                        if let Some(next_block) = parent.get_block(super_idx) {
                            let next_end = if is_left {
                                next_block.endleft.load(Ordering::Acquire)
                            } else {
                                next_block.endright.load(Ordering::Acquire)
                            };
                            if current_block_idx <= next_end {
                                break;
                            }
                        }
                    }
                }

                // Calculate rank in parent
                let prev_parent = if super_idx > 1 {
                    parent.get_block(super_idx - 1)
                } else {
                    None
                };

                if is_left {
                    let prev_sumdeq = if current_block_idx > 1 {
                        current_node
                            .get_block(current_block_idx - 1)
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };

                    let parent_prev_endleft = prev_parent
                        .map(|b| b.endleft.load(Ordering::Acquire))
                        .unwrap_or(0);

                    let parent_prev_sumdeq = if parent_prev_endleft > 0 {
                        current_node
                            .get_block(parent_prev_endleft)
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };

                    current_rank += prev_sumdeq - parent_prev_sumdeq;
                } else {
                    // Right child
                    let prev_sumdeq = if current_block_idx > 1 {
                        current_node
                            .get_block(current_block_idx - 1)
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };

                    let parent_prev_endright = prev_parent
                        .map(|b| b.endright.load(Ordering::Acquire))
                        .unwrap_or(0);

                    let parent_prev_sumdeq = if parent_prev_endright > 0 {
                        current_node
                            .get_block(parent_prev_endright)
                            .map(|b| b.sumdeq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };

                    current_rank += prev_sumdeq - parent_prev_sumdeq;

                    // Add left child dequeues
                    if let Some(parent_block) = parent.get_block(super_idx) {
                        let left = &*parent.left;
                        let endleft = parent_block.endleft.load(Ordering::Acquire);
                        let prev_endleft = prev_parent
                            .map(|b| b.endleft.load(Ordering::Acquire))
                            .unwrap_or(0);

                        let left_dequeues = if endleft > 0 {
                            left.get_block(endleft)
                                .map(|b| b.sumdeq.load(Ordering::Acquire))
                                .unwrap_or(0)
                        } else {
                            0
                        };

                        let prev_left_dequeues = if prev_endleft > 0 {
                            left.get_block(prev_endleft)
                                .map(|b| b.sumdeq.load(Ordering::Acquire))
                                .unwrap_or(0)
                        } else {
                            0
                        };

                        current_rank += left_dequeues - prev_left_dequeues;
                    }
                }

                current_node = parent;
                current_block_idx = super_idx;
            }

            (current_block_idx, current_rank)
        }
    }

    // Find response for dequeue
    fn find_response(&self, root_block_idx: usize, deq_rank: usize) -> Result<T, ()> {
        unsafe {
            let root_node = &*self.root;

            let Some(block) = root_node.get_block(root_block_idx) else {
                return Err(());
            };

            let prev_block = if root_block_idx > 1 {
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
                return Err(());
            }

            // Find the enqueue
            let prev_sumenq = prev_block
                .map(|b| b.sumenq.load(Ordering::Acquire))
                .unwrap_or(0);
            let enq_rank = deq_rank + prev_sumenq - prev_size;

            let target_block = self
                .binary_search_block(root_node, enq_rank, true)
                .ok_or(())?;

            let prev_target = if target_block > 1 {
                root_node.get_block(target_block - 1)
            } else {
                None
            };

            let rank_in_block = enq_rank
                - prev_target
                    .map(|b| b.sumenq.load(Ordering::Acquire))
                    .unwrap_or(0);

            self.get_enqueue(root_node, target_block, rank_in_block)
        }
    }

    // Get enqueue from tree
    fn get_enqueue(&self, node: &Node<T>, block_idx: usize, rank: usize) -> Result<T, ()> {
        unsafe {
            if node.is_leaf {
                let Some(block) = node.get_block(block_idx) else {
                    return Err(());
                };
                (*block.element.get()).clone().ok_or(())
            } else {
                let Some(block) = node.get_block(block_idx) else {
                    return Err(());
                };

                let prev_block = if block_idx > 1 {
                    node.get_block(block_idx - 1)
                } else {
                    None
                };

                let prev_endleft = prev_block
                    .map(|b| b.endleft.load(Ordering::Acquire))
                    .unwrap_or(0);
                let endleft = block.endleft.load(Ordering::Acquire);

                // Calculate left child enqueues
                let left = &*node.left;
                let left_enqueues = if endleft > prev_endleft {
                    let end_sum = left
                        .get_block(endleft)
                        .map(|b| b.sumenq.load(Ordering::Acquire))
                        .unwrap_or(0);
                    let prev_sum = if prev_endleft > 0 {
                        left.get_block(prev_endleft)
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    end_sum - prev_sum
                } else {
                    0
                };

                if rank <= left_enqueues && left_enqueues > 0 {
                    // In left child
                    let target_enq = if prev_endleft > 0 {
                        left.get_block(prev_endleft)
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0)
                            + rank
                    } else {
                        rank
                    };

                    let target = self.binary_search_block(left, target_enq, true).ok_or(())?;
                    let prev_sum = if target > 1 {
                        left.get_block(target - 1)
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    self.get_enqueue(left, target, target_enq - prev_sum)
                } else {
                    // In right child
                    let right = &*node.right;
                    let prev_endright = prev_block
                        .map(|b| b.endright.load(Ordering::Acquire))
                        .unwrap_or(0);

                    let adjusted_rank = rank - left_enqueues;
                    let target_enq = if prev_endright > 0 {
                        right
                            .get_block(prev_endright)
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0)
                            + adjusted_rank
                    } else {
                        adjusted_rank
                    };

                    let target = self
                        .binary_search_block(right, target_enq, true)
                        .ok_or(())?;
                    let prev_sum = if target > 1 {
                        right
                            .get_block(target - 1)
                            .map(|b| b.sumenq.load(Ordering::Acquire))
                            .unwrap_or(0)
                    } else {
                        0
                    };
                    self.get_enqueue(right, target, target_enq - prev_sum)
                }
            }
        }
    }

    pub fn force_full_propagation(&self) {
        unsafe {
            // Simple bottom-up propagation
            for _ in 0..self.tree_height {
                // Propagate from all leaves
                for i in 0..self.num_processes {
                    let leaf = self.get_leaf(i);
                    let mut current = leaf as *const Node<T>;

                    // Go up one level at a time
                    if !(*current).parent.is_null() {
                        current = (*current).parent;
                        self.refresh(&*current);
                        self.refresh(&*current);
                    }
                }
            }
        }
    }

    // Initialize queue
    pub unsafe fn init_in_shared(mem: *mut u8, num_processes: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        let tree_height = (num_processes as f64).log2().ceil() as usize;
        let tree_size = (1 << (tree_height + 1)) - 1;
        let leaf_start = (1 << tree_height) - 1;
        let internal_nodes = leaf_start;

        let node_struct_size = mem::size_of::<Node<T>>();
        let node_aligned = (node_struct_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let block_size = mem::size_of::<Block<T>>();
        let leaf_blocks_size = BLOCKS_PER_LEAF * block_size;
        let internal_blocks_size = BLOCKS_PER_INTERNAL * block_size;

        let leaf_node_size = node_aligned + leaf_blocks_size;
        let internal_node_size = node_aligned + internal_blocks_size;

        let leaf_node_size = (leaf_node_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);
        let internal_node_size =
            (internal_node_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let nodes_offset = queue_aligned;
        let internal_nodes_size = internal_nodes * internal_node_size;
        let leaf_nodes_size = num_processes * leaf_node_size;
        let total_nodes_size = internal_nodes_size + leaf_nodes_size;

        let total_size = nodes_offset + total_nodes_size;

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

        // Initialize nodes
        for i in 0..tree_size {
            if i >= leaf_start + num_processes {
                continue;
            }

            let node_ptr = queue.get_node_ptr(i);
            let max_blocks = if i >= leaf_start {
                BLOCKS_PER_LEAF
            } else {
                BLOCKS_PER_INTERNAL
            };

            ptr::write(node_ptr, Node::new(max_blocks));

            // Initialize sentinel block
            let blocks_ptr = (*node_ptr).get_blocks_ptr();
            ptr::write(blocks_ptr, Block::new());
        }

        // Build tree
        queue.root = queue.get_node_ptr(0);

        for i in 0..tree_size {
            if i >= leaf_start + num_processes {
                continue;
            }

            let node = &mut *queue.get_node_ptr(i);

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

    pub fn shared_size(num_processes: usize) -> usize {
        let tree_height = (num_processes as f64).log2().ceil() as usize;
        let num_leaves = 1 << tree_height;
        let leaf_start = num_leaves - 1;
        let internal_nodes = leaf_start;

        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let node_struct_size = mem::size_of::<Node<T>>();
        let node_aligned = (node_struct_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let block_size = mem::size_of::<Block<T>>();
        let leaf_blocks_size = BLOCKS_PER_LEAF * block_size;
        let internal_blocks_size = BLOCKS_PER_INTERNAL * block_size;

        let leaf_node_size = node_aligned + leaf_blocks_size;
        let internal_node_size = node_aligned + internal_blocks_size;

        let leaf_node_size = (leaf_node_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);
        let internal_node_size =
            (internal_node_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let internal_nodes_total = internal_nodes * internal_node_size;
        let leaf_nodes_total = num_leaves * leaf_node_size;

        let nodes_total = internal_nodes_total + leaf_nodes_total;
        let total = queue_aligned + nodes_total;

        ((total + (1 << 20)) + 4095) & !4095
    }

    pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), ()> {
        if thread_id >= self.num_processes {
            return Err(());
        }
        self.append(thread_id, Some(item));
        Ok(())
    }

    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        if thread_id >= self.num_processes {
            return Err(());
        }

        self.append(thread_id, None);

        unsafe {
            // Force propagation first
            self.force_full_propagation();

            let leaf = self.get_leaf(thread_id);
            let leaf_head = leaf.head.load(Ordering::Acquire);

            if leaf_head <= 1 {
                return Err(());
            }

            let (root_block, rank) = self.index_dequeue(leaf, leaf_head - 1, 1);

            if root_block == 0 {
                return Err(());
            }

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
        false
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
        // Nothing to clean up
    }
}
