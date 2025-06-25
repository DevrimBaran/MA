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

    // FIX: Store the blocks pointer directly
    blocks_ptr: *mut Block<T>,

    // FIX: Store node's index for navigation
    node_index: usize,

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
            blocks_ptr: ptr::null_mut(),
            node_index: 0,
            _phantom: std::marker::PhantomData,
        }
    }

    // FIX: Simply return the stored pointer
    unsafe fn get_blocks_ptr(&self) -> *mut Block<T> {
        self.blocks_ptr
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

    // FIX: Calculate blocks pointer from base_ptr during initialization
    unsafe fn calculate_blocks_ptr(
        base_ptr: *mut u8,
        nodes_offset: usize,
        node_index: usize,
        node_size: usize,
    ) -> *mut Block<T> {
        let node_offset = nodes_offset + (node_index * node_size);
        let node_base = base_ptr.add(node_offset);

        let node_struct_size = mem::size_of::<Node<T>>();
        let blocks_offset = (node_struct_size + 63) & !63;

        node_base.add(blocks_offset) as *mut Block<T>
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
        const MAX_RETRIES: usize = 50; // Significantly increased
        const INITIAL_SYNC_ROUNDS: usize = 10; // More aggressive initial sync

        unsafe {
            // First, ensure strong synchronization before we check availability
            for _ in 0..INITIAL_SYNC_ROUNDS {
                self.sync();
            }

            let root = &*self.root;
            let initial_root_head = root.head.load(Ordering::Acquire);

            // Get the current queue state AFTER synchronization
            let (initial_total_enq, initial_total_deq) = if initial_root_head > 0 {
                if let Some(last_block) = root.get_block(initial_root_head - 1) {
                    (
                        last_block.sumenq.load(Ordering::Acquire),
                        last_block.sumdeq.load(Ordering::Acquire),
                    )
                } else {
                    (0, 0)
                }
            } else {
                (0, 0)
            };

            // Check if queue is empty
            if initial_total_enq <= initial_total_deq {
                return Err(());
            }

            // Now append our dequeue - we know there's at least one item
            self.append(thread_id, None);

            // Our dequeue will be the (initial_total_deq + 1)th dequeue
            let my_deq_rank = initial_total_deq + 1;

            // Aggressive synchronization to ensure our dequeue propagates
            for _ in 0..INITIAL_SYNC_ROUNDS {
                self.force_complete_sync();
            }

            // Now find which enqueue we should return
            for retry in 0..MAX_RETRIES {
                let current_root_head = root.head.load(Ordering::Acquire);

                // Search through blocks to verify our dequeue is registered
                for block_idx in 1..current_root_head {
                    if let Some(block) = root.get_block(block_idx) {
                        let block_total_deq = block.sumdeq.load(Ordering::Acquire);

                        // Check if this block contains our dequeue
                        if block_total_deq >= my_deq_rank {
                            let block_total_enq = block.sumenq.load(Ordering::Acquire);

                            // Ensure there are enough enqueues for our dequeue
                            if my_deq_rank <= block_total_enq {
                                // Try to get the enqueue multiple times with sync
                                for attempt in 0..5 {
                                    match self.get_enqueue_by_rank(my_deq_rank) {
                                        Ok(val) => return Ok(val),
                                        Err(_) => {
                                            // Synchronize and retry
                                            self.force_complete_sync();
                                            std::thread::yield_now();
                                        }
                                    }
                                }
                            } else {
                                // This dequeue found the queue empty
                                return Err(());
                            }
                        }
                    }
                }

                // Our dequeue hasn't appeared in root yet, synchronize more
                if retry < MAX_RETRIES - 1 {
                    for _ in 0..5 {
                        self.force_complete_sync();
                    }
                    // Exponential backoff
                    std::thread::sleep(std::time::Duration::from_micros((retry + 1) as u64));
                }
            }

            // Final attempt - check the very latest state
            self.force_complete_sync();
            let final_root_head = root.head.load(Ordering::Acquire);

            if let Some(last_block) = root.get_block(final_root_head - 1) {
                let final_total_enq = last_block.sumenq.load(Ordering::Acquire);
                let final_total_deq = last_block.sumdeq.load(Ordering::Acquire);

                // Our dequeue should definitely be counted by now
                if final_total_deq >= my_deq_rank && my_deq_rank <= final_total_enq {
                    // One last attempt to get the value
                    match self.get_enqueue_by_rank(my_deq_rank) {
                        Ok(val) => return Ok(val),
                        Err(_) => {
                            // This is the data loss case - we know the item exists but can't find it
                            eprintln!("ERROR: NRQueue data loss - dequeue {} should get enqueue {} but failed",
                                     my_deq_rank, my_deq_rank);
                        }
                    }
                }
            }

            Err(())
        }
    }

    #[inline(always)]
    fn get_enqueue_by_rank(&self, rank: usize) -> Result<T, ()> {
        unsafe {
            let root = &*self.root;

            // Synchronize before searching
            self.sync();

            let root_head = root.head.load(Ordering::Acquire);

            if root_head <= 1 || rank == 0 {
                return Err(());
            }

            // Binary search for the block containing our rank
            let mut left = 1;
            let mut right = root_head - 1;
            let mut target_block = 0;

            while left <= right {
                let mid = left + (right - left) / 2;
                if let Some(block) = root.get_block(mid) {
                    let block_sum_enq = block.sumenq.load(Ordering::Acquire);
                    if block_sum_enq >= rank {
                        target_block = mid;
                        right = mid - 1;
                    } else {
                        left = mid + 1;
                    }
                } else {
                    break;
                }
            }

            if target_block == 0 {
                return Err(());
            }

            // Verify the block still contains our rank after potential racing updates
            if let Some(block) = root.get_block(target_block) {
                let block_sum_enq = block.sumenq.load(Ordering::Acquire);
                if block_sum_enq < rank {
                    // Race condition - retry with synchronization
                    self.sync();
                    return self.get_enqueue_by_rank(rank);
                }
            }

            let prev_sum_enq = if target_block > 1 {
                root.get_block(target_block - 1)
                    .map(|b| b.sumenq.load(Ordering::Acquire))
                    .unwrap_or(0)
            } else {
                0
            };

            let rank_in_block = rank - prev_sum_enq;

            // Add retry logic for get_enqueue as well
            for attempt in 0..3 {
                match self.get_enqueue(root, target_block, rank_in_block) {
                    Ok(val) => return Ok(val),
                    Err(_) if attempt < 2 => {
                        self.sync();
                        continue;
                    }
                    Err(_) => return Err(()),
                }
            }

            Err(())
        }
    }

    pub fn sync(&self) {
        unsafe {
            for i in 0..self.num_processes {
                let leaf = self.get_leaf(i);
                let head = leaf.head.load(Ordering::Acquire);

                if head > 1 {
                    self.advance(leaf, head - 1);

                    if !leaf.parent.is_null() {
                        self.refresh(&*leaf.parent);
                    }
                }
            }

            for _ in 0..3 {
                for level in (0..=self.tree_height).rev() {
                    let start_idx = (1 << level) - 1;
                    let end_idx = (1 << (level + 1)) - 1;

                    for idx in start_idx..end_idx.min(start_idx + (1 << level)) {
                        let node = self.get_node(idx);

                        for _ in 0..3 {
                            self.refresh(node);
                        }

                        let h = node.head.load(Ordering::Acquire);
                        if h > 0 {
                            self.advance(node, h - 1);
                        }
                    }
                }
            }

            let root = &*self.root;
            for _ in 0..3 {
                self.refresh(root);
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

    #[inline(always)]
    fn get_enqueue(
        &self,
        mut node: &Node<T>,
        mut blk_idx: usize,
        mut rank: usize,
    ) -> Result<T, ()> {
        unsafe {
            for _ in 0..=self.tree_height {
                if node.is_leaf {
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
                    return Err(());
                }

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
                    return Err(());
                }
            }

            Err(())
        }
    }

    pub fn force_complete_sync(&self) {
        unsafe {
            for _ in 0..3 {
                self.sync();
            }

            for level in (0..=self.tree_height).rev() {
                let start_idx = (1 << level) - 1;
                let end_idx = (1 << (level + 1)) - 1;

                for idx in start_idx..end_idx.min(start_idx + (1 << level)) {
                    let node = self.get_node(idx);
                    for _ in 0..2 {
                        self.refresh(node);
                    }
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

        // FIX: Initialize nodes with proper blocks pointers
        for i in 0..tree_size {
            let node_ptr = queue.get_node_ptr(i);
            ptr::write(node_ptr, Node::new());

            let node = &mut *node_ptr;
            node.node_index = i;

            // Calculate and store blocks pointer
            node.blocks_ptr = Self::calculate_blocks_ptr(mem, nodes_offset, i, node_size_aligned);

            // Initialize blocks
            let blocks_ptr = node.blocks_ptr;
            for j in 0..BLOCKS_PER_NODE {
                ptr::write(blocks_ptr.add(j), Block::new());
            }
        }

        queue.root = queue.get_node_ptr(0);

        // Set up tree structure
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

            // Initialize sentinel block
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
