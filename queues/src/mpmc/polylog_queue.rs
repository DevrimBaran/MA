use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

use crate::MpmcQueue;

const CACHE_LINE_SIZE: usize = 64;
const BLOCKS_PER_NODE: usize = 2_000_000;
const MAX_SPIN_ITERATIONS: usize = 1000;
const MAX_BLOCK_SEARCH_ITERATIONS: usize = 100;

#[repr(C)]
struct Block<T> {
    sumenq: AtomicUsize,
    sumdeq: AtomicUsize,
    super_idx: AtomicUsize,

    endleft: AtomicUsize,
    endright: AtomicUsize,

    element: UnsafeCell<Option<T>>,

    size: AtomicUsize,
    next_deq: AtomicUsize,
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
            next_deq: AtomicUsize::new(0),
        }
    }
}

#[repr(C)]
struct Node<T> {
    head: AtomicUsize,

    left: *const Node<T>,
    right: *const Node<T>,
    parent: *const Node<T>,

    is_leaf: bool,
    is_root: bool,
    process_id: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send> Send for Node<T> {}
unsafe impl<T: Send> Sync for Node<T> {}

impl<T> Node<T> {
    fn new() -> Self {
        Self {
            head: AtomicUsize::new(1),
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
        let node_ptr = self as *const Self as *mut u8;
        let blocks_offset = mem::size_of::<Self>();
        let aligned_offset = (blocks_offset + 63) & !63;
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

#[repr(C)]
pub struct NRQueue<T: Send + Clone + 'static> {
    root: *const Node<T>,
    tree_height: usize,
    num_processes: usize,

    base_ptr: *mut u8,
    total_size: usize,
    nodes_offset: usize,
    node_size: usize,

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

    fn append(&self, process_id: usize, element: Option<T>) {
        unsafe {
            let leaf = self.get_leaf(process_id);
            let h = leaf.head.load(Ordering::Acquire);

            if h >= BLOCKS_PER_NODE {
                panic!(
                    "Block pool exhausted for process {}. Head: {}, Capacity: {}",
                    process_id, h, BLOCKS_PER_NODE
                );
            }

            let blocks = leaf.get_blocks_ptr();
            let block = &*blocks.add(h);

            let is_enqueue = element.is_some();

            *block.element.get() = element;

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
                if is_enqueue {
                    block.sumenq.store(1, Ordering::Release);
                    block.sumdeq.store(0, Ordering::Release);
                } else {
                    block.sumenq.store(0, Ordering::Release);
                    block.sumdeq.store(1, Ordering::Release);
                }
            }

            leaf.head.fetch_add(1, Ordering::AcqRel);

            if !leaf.parent.is_null() {
                self.propagate(&*leaf.parent);
            }
        }
    }

    fn propagate(&self, node: &Node<T>) {
        if !self.refresh(node) {
            self.refresh(node);
        }

        unsafe {
            if !node.is_root && !node.parent.is_null() {
                self.propagate(&*node.parent);
            }
        }
    }

    fn refresh(&self, node: &Node<T>) -> bool {
        unsafe {
            let h = node.head.load(Ordering::Acquire);

            if h >= BLOCKS_PER_NODE {
                return false;
            }

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

            let new_block = self.create_block(node, h);
            if new_block.is_none() {
                return true;
            }

            let (numenq, numdeq, endleft, endright, size) = new_block.unwrap();

            let blocks = node.get_blocks_ptr();
            let block = &*blocks.add(h);

            let current_head = node.head.load(Ordering::Acquire);
            if current_head != h {
                self.advance(node, h);
                return false;
            }

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

            let (mut endleft, mut left_total_enq, mut left_total_deq) = (prev_endleft, 0, 0);
            if !node.left.is_null() {
                let left = &*node.left;
                let left_head = left.head.load(Ordering::Acquire);
                if left_head > 1 && left_head - 1 > prev_endleft {
                    endleft = left_head - 1;
                }
                if endleft > 0 {
                    let blk = left.get_block(endleft)?;
                    left_total_enq = blk.sumenq.load(Ordering::Acquire);
                    left_total_deq = blk.sumdeq.load(Ordering::Acquire);
                }
            }

            let (mut endright, mut right_total_enq, mut right_total_deq) = (prev_endright, 0, 0);
            if !node.right.is_null() {
                let right = &*node.right;
                let right_head = right.head.load(Ordering::Acquire);
                if right_head > 1 && right_head - 1 > prev_endright {
                    endright = right_head - 1;
                }
                if endright > 0 {
                    let blk = right.get_block(endright)?;
                    right_total_enq = blk.sumenq.load(Ordering::Acquire);
                    right_total_deq = blk.sumdeq.load(Ordering::Acquire);
                }
            }

            let child_total_enq = left_total_enq + right_total_enq;
            let child_total_deq = left_total_deq + right_total_deq;
            let numenq = child_total_enq.saturating_sub(prev_sumenq);
            let numdeq = child_total_deq.saturating_sub(prev_sumdeq);
            if numenq == 0 && numdeq == 0 {
                return None;
            }

            let size = if node.is_root {
                let total_enq_after = prev_sumenq + numenq;
                let total_deq_after = prev_sumdeq + numdeq;
                let successful_deq = total_enq_after.min(total_deq_after);
                total_enq_after - successful_deq
            } else {
                0
            };

            Some((numenq, numdeq, endleft, endright, size))
        }
    }

    pub fn simple_dequeue(&self, thread_id: usize) -> Result<T, ()> {
        const MAX_RETRIES: usize = 3;

        for _ in 0..=MAX_RETRIES {
            self.sync();

            unsafe {
                let root = &*self.root;
                let root_head = root.head.load(Ordering::Acquire);
                if root_head <= 1 {
                    return Err(());
                }

                let last_blk = root.get_block(root_head - 1).ok_or(())?;
                let total_enq = last_blk.sumenq.load(Ordering::Acquire);
                let total_deq = last_blk.sumdeq.load(Ordering::Acquire);
                let size_now = last_blk.size.load(Ordering::Acquire);

                if size_now == 0 {
                    return Err(());
                }

                // Remember the state before our dequeue
                let snapshot_head = root_head;
                let snapshot_enq = total_enq;
                let snapshot_deq = total_deq;

                // Append our dequeue
                self.append(thread_id, None);
                self.sync();

                // Now find where our dequeue ended up in the root
                let new_root_head = root.head.load(Ordering::Acquire);

                // Search for our dequeue in the root blocks
                let mut my_dequeue_rank = None;

                // Start from where we were and search forward
                for idx in snapshot_head..=new_root_head {
                    if let Some(block) = root.get_block(idx) {
                        let block_total_deq = block.sumdeq.load(Ordering::Acquire);
                        let prev_total_deq = if idx > 0 {
                            root.get_block(idx - 1)
                                .map(|b| b.sumdeq.load(Ordering::Acquire))
                                .unwrap_or(0)
                        } else {
                            0
                        };

                        // Our dequeue should be somewhere in this range
                        if prev_total_deq <= snapshot_deq && block_total_deq > snapshot_deq {
                            // Found the block containing our dequeue
                            // Our rank is snapshot_deq + 1 (we're the next dequeue after the snapshot)
                            my_dequeue_rank = Some(snapshot_deq + 1);
                            break;
                        }
                    }
                }

                let my_rank = my_dequeue_rank.unwrap_or(snapshot_deq + 1);

                // Check if our dequeue is valid (within the number of enqueues)
                let final_root_head = root.head.load(Ordering::Acquire);
                if let Some(final_block) = root.get_block(final_root_head - 1) {
                    let final_total_enq = final_block.sumenq.load(Ordering::Acquire);

                    // If our dequeue rank exceeds total enqueues, the queue was empty
                    if my_rank > final_total_enq {
                        return Err(());
                    }
                }

                // Now get the element
                if let Ok(val) = self.get_enqueue_by_rank(my_rank) {
                    return Ok(val);
                }
            }
        }

        Err(())
    }

    #[inline(always)]
    fn get_enqueue_by_rank(&self, rank: usize) -> Result<T, ()> {
        const MAX_RETRIES: usize = 3;
        for _try in 0..MAX_RETRIES {
            if _try > 0 {
                self.force_complete_sync();
            }
            unsafe {
                let root = &*self.root;
                let root_head = root.head.load(Ordering::Acquire);
                if root_head <= 1 || rank == 0 {
                    return Err(());
                }
                let total_enq = root
                    .get_block(root_head - 1)
                    .map(|b| b.sumenq.load(Ordering::Acquire))
                    .unwrap_or(0);
                if rank > total_enq {
                    return Err(());
                }

                let (mut lo, mut hi, mut blk) = (1, root_head - 1, 1);
                while lo <= hi {
                    let mid = lo + (hi - lo) / 2;
                    if let Some(b) = root.get_block(mid) {
                        if b.sumenq.load(Ordering::Acquire) >= rank {
                            blk = mid;
                            hi = mid.saturating_sub(1);
                        } else {
                            lo = mid + 1;
                        }
                    } else {
                        break;
                    }
                }

                let mut prev_enq = if blk > 0 {
                    root.get_block(blk - 1)
                        .map(|b| b.sumenq.load(Ordering::Acquire))
                        .unwrap_or(0)
                } else {
                    0
                };

                // Bounded loop - at most we'll search through p blocks
                let max_search = self.num_processes.min(MAX_BLOCK_SEARCH_ITERATIONS);
                for _ in 0..max_search {
                    let block = match root.get_block(blk) {
                        Some(b) => b,
                        None => break,
                    };
                    let blk_sum_enq = block.sumenq.load(Ordering::Acquire);
                    let blk_num_enq = blk_sum_enq - prev_enq;
                    if blk_num_enq > 0 && rank <= blk_sum_enq {
                        let rank_in_blk = rank - prev_enq;
                        return self.get_enqueue(root, blk, rank_in_blk);
                    }
                    blk += 1;
                    prev_enq = blk_sum_enq;
                }
            }
        }
        Err(())
    }

    pub fn sync(&self) {
        unsafe {
            // Help all leaves advance
            for i in 0..self.num_processes {
                let leaf = self.get_leaf(i);
                let head = leaf.head.load(Ordering::Acquire);

                if head > 0 && !leaf.parent.is_null() {
                    self.advance(leaf, head - 1);
                }
            }

            // Bounded number of passes through the tree
            let max_passes = 5.min(self.tree_height + 2);
            for pass in 0..max_passes {
                for level in (0..=self.tree_height).rev() {
                    let start_idx = (1 << level) - 1;
                    let end_idx = (1 << (level + 1)) - 1;

                    for idx in start_idx..end_idx.min(start_idx + (1 << level)) {
                        let node = self.get_node(idx);

                        // Bounded refresh attempts
                        for _ in 0..3 {
                            self.refresh(node);

                            let h = node.head.load(Ordering::Acquire);
                            if h > 0 {
                                self.advance(node, h - 1);
                            }
                        }
                    }
                }

                if pass < max_passes - 1 {
                    std::thread::yield_now();
                }
            }
        }
    }

    fn advance(&self, node: &Node<T>, h: usize) {
        unsafe {
            let _ = node
                .head
                .compare_exchange(h, h + 1, Ordering::AcqRel, Ordering::Acquire);

            if !node.is_root && !node.parent.is_null() {
                let parent = &*node.parent;

                if let Some(block) = node.get_block(h) {
                    let current_super = block.super_idx.load(Ordering::Acquire);
                    if current_super == 0 {
                        let parent_head = parent.head.load(Ordering::Acquire);
                        let super_value = parent_head.max(1);
                        block.super_idx.store(super_value, Ordering::Release);
                    }
                }
            }
        }
    }

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

                // FIXED: Bounded spin with helping
                let mut spin_count = 0;
                while super_idx == 0 && spin_count < MAX_SPIN_ITERATIONS {
                    // Help advance the parent
                    if !node.parent.is_null() {
                        let parent = &*node.parent;
                        let parent_head = parent.head.load(Ordering::Acquire);
                        if parent_head > 0 {
                            self.advance(parent, parent_head - 1);
                        }

                        // Also help refresh the parent to propagate our block
                        self.refresh(parent);
                    }

                    spin_count += 1;
                    if spin_count % 10 == 0 {
                        std::thread::yield_now();
                    }

                    super_idx = block.super_idx.load(Ordering::Acquire);
                }

                // If still 0 after bounded spinning, use conservative estimate
                if super_idx == 0 {
                    let parent = &*node.parent;
                    super_idx = parent.head.load(Ordering::Acquire).max(1);
                }

                let parent = &*node.parent;
                let is_left = ptr::eq(parent.left, node);

                let mut actual_super = super_idx;

                if let Some(parent_block) = parent.get_block(super_idx) {
                    let end_field = if is_left {
                        parent_block.endleft.load(Ordering::Acquire)
                    } else {
                        parent_block.endright.load(Ordering::Acquire)
                    };

                    if block_idx > end_field {
                        actual_super = super_idx + 1;
                    }
                }

                let parent_block = parent
                    .get_block(actual_super)
                    .expect("Parent block must exist");
                let prev_parent = if actual_super > 1 {
                    parent.get_block(actual_super - 1)
                } else {
                    None
                };

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

    #[inline(always)]
    fn get_enqueue(
        &self,
        mut node: &Node<T>,
        mut blk_idx: usize,
        mut rank: usize,
    ) -> Result<T, ()> {
        unsafe {
            // Bounded depth - at most tree_height iterations
            for _ in 0..=self.tree_height {
                if node.is_leaf {
                    // Bounded search through blocks in leaf
                    let max_leaf_search = self.num_processes.min(MAX_BLOCK_SEARCH_ITERATIONS);
                    for _ in 0..max_leaf_search {
                        let head_limit = node.head.load(Ordering::Acquire).saturating_sub(1);
                        if blk_idx > head_limit {
                            return Err(());
                        }
                        let blk = node.get_block(blk_idx).ok_or(())?;
                        let prev = if blk_idx > 0 {
                            node.get_block(blk_idx - 1)
                                .map(|b| b.sumenq.load(Ordering::Acquire))
                                .unwrap_or(0)
                        } else {
                            0
                        };
                        let num_enq = blk.sumenq.load(Ordering::Acquire) - prev;
                        if num_enq == 0 || rank > num_enq {
                            rank -= num_enq;
                            blk_idx += 1;
                            continue;
                        }
                        return (*blk.element.get()).clone().ok_or(());
                    }
                    return Err(()); // Not found after bounded search
                }

                // For internal nodes, bounded search
                let max_internal_search = (2 * self.num_processes).min(MAX_BLOCK_SEARCH_ITERATIONS);
                let mut found = false;
                for _ in 0..max_internal_search {
                    let head_limit = node.head.load(Ordering::Acquire).saturating_sub(1);
                    if blk_idx > head_limit {
                        return Err(());
                    }
                    let blk = node.get_block(blk_idx).ok_or(())?;
                    let prev_blk = if blk_idx > 0 {
                        node.get_block(blk_idx - 1)
                    } else {
                        None
                    };

                    let (left_num, right_num) = {
                        let (prev_el, el) = (
                            prev_blk
                                .map(|b| b.endleft.load(Ordering::Acquire))
                                .unwrap_or(0),
                            blk.endleft.load(Ordering::Acquire),
                        );
                        let (prev_er, er) = (
                            prev_blk
                                .map(|b| b.endright.load(Ordering::Acquire))
                                .unwrap_or(0),
                            blk.endright.load(Ordering::Acquire),
                        );
                        let left = &*node.left;
                        let right = &*node.right;

                        let left_start = if prev_el > 0 {
                            left.get_block(prev_el)
                                .map(|b| b.sumenq.load(Ordering::Acquire))
                                .unwrap_or(0)
                        } else {
                            0
                        };
                        let left_end = if el > 0 {
                            left.get_block(el)
                                .map(|b| b.sumenq.load(Ordering::Acquire))
                                .unwrap_or(0)
                        } else {
                            0
                        };

                        let right_start = if prev_er > 0 {
                            right
                                .get_block(prev_er)
                                .map(|b| b.sumenq.load(Ordering::Acquire))
                                .unwrap_or(0)
                        } else {
                            0
                        };
                        let right_end = if er > 0 {
                            right
                                .get_block(er)
                                .map(|b| b.sumenq.load(Ordering::Acquire))
                                .unwrap_or(0)
                        } else {
                            0
                        };

                        (left_end - left_start, right_end - right_start)
                    };
                    let total_num = left_num + right_num;
                    if total_num == 0 || rank > total_num {
                        rank -= total_num;
                        blk_idx += 1;
                        continue;
                    }

                    // Found the block containing our element
                    if rank <= left_num && left_num > 0 {
                        node = &*node.left;
                        blk_idx = blk.endleft.load(Ordering::Acquire);
                    } else {
                        rank -= left_num;
                        node = &*node.right;
                        blk_idx = blk.endright.load(Ordering::Acquire);
                    }
                    found = true;
                    break;
                }

                if !found {
                    return Err(()); // Element not found after bounded search
                }
            }

            Err(()) // Tree too deep, shouldn't happen
        }
    }

    pub fn debug_state(&self) {
        unsafe {
            eprintln!("=== NRQueue Debug State ===");

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
            let mut changed = true;
            let mut iterations = 0;
            let max_iterations = (self.tree_height * 10).min(100); // Bounded iterations

            while changed && iterations < max_iterations {
                changed = false;
                iterations += 1;

                let root = &*self.root;
                let initial_root_head = root.head.load(Ordering::Acquire);
                let initial_root_enq = if initial_root_head > 0 {
                    root.get_block(initial_root_head - 1)
                        .map(|b| b.sumenq.load(Ordering::Acquire))
                        .unwrap_or(0)
                } else {
                    0
                };

                // Bounded number of sync attempts
                for _ in 0..5 {
                    self.sync();
                }

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

    pub unsafe fn init_in_shared(mem: *mut u8, num_processes: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        let tree_height = (num_processes as f64).log2().ceil() as usize;
        let tree_size = (1 << (tree_height + 1)) - 1;

        let node_struct_size = mem::size_of::<Node<T>>();
        let blocks_offset = (node_struct_size + 63) & !63;
        let blocks_size = BLOCKS_PER_NODE * mem::size_of::<Block<T>>();
        let node_size = blocks_offset + blocks_size;
        let node_size_aligned = (node_size + 63) & !63;

        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let nodes_offset = queue_aligned;
        let nodes_total_size = tree_size * node_size_aligned;

        let total_size = nodes_offset + nodes_total_size;

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

        for i in 0..tree_size {
            let node_ptr = queue.get_node_ptr(i);
            ptr::write(node_ptr, Node::new());

            let blocks_ptr = (*node_ptr).get_blocks_ptr();
            for j in 0..BLOCKS_PER_NODE {
                ptr::write(blocks_ptr.add(j), Block::new());
            }
        }

        queue.root = queue.get_node_ptr(0);

        for i in 0..tree_size {
            let node = &mut *queue.get_node_ptr(i);

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

            let leaf_start = (1 << tree_height) - 1;
            if i >= leaf_start && i < leaf_start + num_processes {
                node.is_leaf = true;
                node.process_id = i - leaf_start;
            }

            if i == 0 {
                node.is_root = true;
            }

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

    pub fn shared_size(num_processes: usize) -> usize {
        let tree_height = (num_processes as f64).log2().ceil() as usize;
        let tree_size = (1 << (tree_height + 1)) - 1;

        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let node_struct_size = mem::size_of::<Node<T>>();
        let blocks_offset = (node_struct_size + 63) & !63;
        let blocks_size = BLOCKS_PER_NODE * mem::size_of::<Block<T>>();
        let node_size = blocks_offset + blocks_size;
        let node_size_aligned = (node_size + 63) & !63;

        let nodes_total_size = tree_size * node_size_aligned;

        let total = queue_aligned + nodes_total_size;
        (total + 4095) & !4095
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
    fn drop(&mut self) {}
}
