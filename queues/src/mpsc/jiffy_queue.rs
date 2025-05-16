//! Wait-free Jiffy MPSC queue — **patched**
//!
//! Two fixes:
//!   1. A producer that gives up on a reserved slot immediately turns it
//!      from `Empty` → `Handled`, so no permanent “holes” remain.
//!   2. The consumer’s fast “queue is empty” test now triggers **only**
//!      when that slot is *Handled* (never when it is still *Empty*).

#![allow(clippy::cast_possible_truncation)]

use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::{fmt, mem::{align_of, needs_drop, size_of, MaybeUninit}, ptr};

use crate::MpscQueue;

// ────────────────────────────────────────────── node ────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(usize)]
enum NodeState {
    Empty   = 0,
    Set     = 1,
    Handled = 2,
}

#[repr(C)]
struct Node<T> {
    data:    MaybeUninit<T>,
    is_set:  AtomicUsize,
}

impl<T> Node<T> {
    unsafe fn init_in_place(node_ptr: *mut Self) {
        ptr::addr_of_mut!((*node_ptr).data)
            .write(MaybeUninit::uninit());
        ptr::addr_of_mut!((*node_ptr).is_set)
            .write(AtomicUsize::new(NodeState::Empty as usize));
    }
}

// ────────────────────────────────────── buffer list node ────────────────

#[repr(C)]
struct BufferList<T> {
    curr_buffer:       *mut Node<T>,
    capacity:          usize,
    next:              AtomicPtr<BufferList<T>>,
    prev:              *mut BufferList<T>,
    consumer_head_idx: usize,
    position_in_queue: u64,

    is_array_reclaimed: AtomicBool,
    next_in_garbage:    AtomicPtr<BufferList<T>>,
    next_free_meta:     AtomicPtr<BufferList<T>>,
}

impl<T: Send + 'static> BufferList<T> {
    unsafe fn init_metadata_in_place(
        bl_ptr: *mut Self,
        node_array_ptr: *mut Node<T>,
        capacity: usize,
        position_in_queue: u64,
        prev_buffer: *mut BufferList<T>,
    ) {
        ptr::addr_of_mut!((*bl_ptr).curr_buffer)        .write(node_array_ptr);
        ptr::addr_of_mut!((*bl_ptr).capacity)           .write(capacity);
        ptr::addr_of_mut!((*bl_ptr).next)               .write(AtomicPtr::new(ptr::null_mut()));
        ptr::addr_of_mut!((*bl_ptr).prev)               .write(prev_buffer);
        ptr::addr_of_mut!((*bl_ptr).consumer_head_idx)  .write(0);
        ptr::addr_of_mut!((*bl_ptr).position_in_queue)  .write(position_in_queue);
        ptr::addr_of_mut!((*bl_ptr).is_array_reclaimed) .write(AtomicBool::new(false));
        ptr::addr_of_mut!((*bl_ptr).next_in_garbage)    .write(AtomicPtr::new(ptr::null_mut()));
        ptr::addr_of_mut!((*bl_ptr).next_free_meta)     .write(AtomicPtr::new(ptr::null_mut()));

        // initialise every node
        if !node_array_ptr.is_null() {
            for i in 0..capacity {
                Node::init_in_place(node_array_ptr.add(i));
            }
        }
    }

    unsafe fn mark_node_array_reclaimed_and_drop_items(&mut self) {
        if self.curr_buffer.is_null() { return; }
        if self.is_array_reclaimed
               .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
               .is_err()
        { return; }

        if needs_drop::<T>() {
            for i in 0..self.capacity {
                let n_ptr = self.curr_buffer.add(i);
                if (*n_ptr).is_set.load(Ordering::Relaxed) == NodeState::Set as usize {
                    ptr::drop_in_place((*n_ptr).data.as_mut_ptr());
                }
            }
        }
        self.curr_buffer = ptr::null_mut();
    }
}

// ───────────────────────────────────── shared pools ─────────────────────

#[repr(C)]
struct SharedPools<T: Send + 'static> {
    bl_meta_pool_start:          *mut BufferList<T>,
    bl_meta_pool_capacity:       usize,
    bl_meta_next_free_idx:       AtomicUsize,
    bl_meta_free_list_head:      AtomicPtr<BufferList<T>>,

    node_arrays_pool_start:      *mut Node<T>,
    node_arrays_pool_total_nodes: usize,
    node_arrays_next_free_node_idx: AtomicUsize,

    buffer_capacity_per_array:   usize,
}

impl<T: Send + 'static> SharedPools<T> {
    unsafe fn new_in_place(
        mem_ptr: *mut u8,
        mut off: usize,
        max_buffers: usize,
        nodes_per_buffer: usize,
        total_nodes: usize,
    ) -> (*mut Self, usize) {
        // align helpers
        fn align(cur: &mut usize, need: usize) {
            *cur = (*cur + need - 1) & !(need - 1);
        }

        align(&mut off, align_of::<Self>());
        let pools_ptr = mem_ptr.add(off) as *mut Self;
        off += size_of::<Self>();

        align(&mut off, align_of::<BufferList<T>>());
        let bl_pool = mem_ptr.add(off) as *mut BufferList<T>;
        off += max_buffers * size_of::<BufferList<T>>();

        align(&mut off, align_of::<Node<T>>());
        let node_pool = mem_ptr.add(off) as *mut Node<T>;
        off += total_nodes * size_of::<Node<T>>();

        // write fields
        ptr::addr_of_mut!((*pools_ptr).bl_meta_pool_start) .write(bl_pool);
        ptr::addr_of_mut!((*pools_ptr).bl_meta_pool_capacity).write(max_buffers);
        ptr::addr_of_mut!((*pools_ptr).bl_meta_next_free_idx).write(AtomicUsize::new(0));
        ptr::addr_of_mut!((*pools_ptr).bl_meta_free_list_head).write(AtomicPtr::new(ptr::null_mut()));

        ptr::addr_of_mut!((*pools_ptr).node_arrays_pool_start) .write(node_pool);
        ptr::addr_of_mut!((*pools_ptr).node_arrays_pool_total_nodes).write(total_nodes);
        ptr::addr_of_mut!((*pools_ptr).node_arrays_next_free_node_idx).write(AtomicUsize::new(0));

        ptr::addr_of_mut!((*pools_ptr).buffer_capacity_per_array).write(nodes_per_buffer);

        (pools_ptr, off)
    }

    unsafe fn alloc_node_array_slice(&self) -> *mut Node<T> {
        let need = self.buffer_capacity_per_array;
        let start = self.node_arrays_next_free_node_idx
                        .fetch_add(need, Ordering::AcqRel);
        if start + need > self.node_arrays_pool_total_nodes {
            self.node_arrays_next_free_node_idx.fetch_sub(need, Ordering::Relaxed);
            return ptr::null_mut();
        }
        self.node_arrays_pool_start.add(start)
    }

    unsafe fn alloc_bl_meta_with_node_array(
        &self,
        pos: u64,
        prev: *mut BufferList<T>,
    ) -> *mut BufferList<T> {
        // 1) try free list
        loop {
            let head = self.bl_meta_free_list_head.load(Ordering::Acquire);
            if head.is_null() { break; }
            let next = (*head).next_free_meta.load(Ordering::Acquire);
            if self.bl_meta_free_list_head
                   .compare_exchange(head, next, Ordering::AcqRel, Ordering::Acquire)
                   .is_ok()
            {
                let nodes = self.alloc_node_array_slice();
                if nodes.is_null() {
                    // push meta back and give up
                    let mut cur = self.bl_meta_free_list_head.load(Ordering::Acquire);
                    loop {
                        (*head).next_free_meta.store(cur, Ordering::Release);
                        match self.bl_meta_free_list_head
                                  .compare_exchange(cur, head, Ordering::Release, Ordering::Acquire) {
                            Ok(_)  => break,
                            Err(n) => cur = n,
                        }
                    }
                    return ptr::null_mut();
                }
                BufferList::init_metadata_in_place(head, nodes,
                    self.buffer_capacity_per_array, pos, prev);
                return head;
            }
        }

        // 2) slow path – bump index
        let idx = self.bl_meta_next_free_idx.fetch_add(1, Ordering::AcqRel);
        if idx >= self.bl_meta_pool_capacity {
            self.bl_meta_next_free_idx.fetch_sub(1, Ordering::Relaxed);
            return ptr::null_mut();
        }
        let meta = self.bl_meta_pool_start.add(idx);
        let nodes = self.alloc_node_array_slice();
        if nodes.is_null() { return ptr::null_mut(); }
        BufferList::init_metadata_in_place(meta, nodes,
            self.buffer_capacity_per_array, pos, prev);
        meta
    }

    unsafe fn dealloc_bl_meta_to_pool(&self, bl: *mut BufferList<T>) {
        if bl.is_null() { return; }
        let mut cur = self.bl_meta_free_list_head.load(Ordering::Acquire);
        loop {
            (*bl).next_free_meta.store(cur, Ordering::Release);
            match self.bl_meta_free_list_head
                      .compare_exchange(cur, bl, Ordering::Release, Ordering::Relaxed) {
                Ok(_)  => break,
                Err(n) => cur = n,
            }
        }
    }

    unsafe fn dealloc_node_array_slice(&self, _nodes: *mut Node<T>) {
        // slabs are never unmapped; kept for future reuse (no-op)
    }
}

// ───────────────────────────────────────────── queue ────────────────────

#[repr(C)]
pub struct JiffyQueue<T: Send + 'static> {
    head_of_queue:      AtomicPtr<BufferList<T>>,
    tail_of_queue:      AtomicPtr<BufferList<T>>,
    global_tail_location: AtomicU64,

    pools:              *const SharedPools<T>,
    garbage_list_head:  AtomicPtr<BufferList<T>>,
}

unsafe impl<T: Send + 'static> Send for JiffyQueue<T> {}
unsafe impl<T: Send + 'static> Sync for JiffyQueue<T> {}

// helper for layout
fn align_up(off: &mut usize, aln: usize) {
    *off = (*off + aln - 1) & !(aln - 1);
}

impl<T: Send + 'static> JiffyQueue<T> {
    // ───────────── shared-memory helpers ─────────────

    pub fn shared_size(nodes_per_buf: usize, max_bufs: usize) -> usize {
        let total_nodes = nodes_per_buf * max_bufs;
        let mut off = 0;

        align_up(&mut off, align_of::<Self>());
        off += size_of::<Self>();

        align_up(&mut off, align_of::<SharedPools<T>>());
        off += size_of::<SharedPools<T>>();

        align_up(&mut off, align_of::<BufferList<T>>());
        off += max_bufs * size_of::<BufferList<T>>();

        align_up(&mut off, align_of::<Node<T>>());
        off += total_nodes * size_of::<Node<T>>();

        off
    }

    pub unsafe fn init_in_shared(
        mem: *mut u8,
        nodes_per_buf: usize,
        max_bufs: usize,
    ) -> &'static mut Self {
        let total_nodes = nodes_per_buf * max_bufs;
        let mut off = 0;

        align_up(&mut off, align_of::<Self>());
        let q_ptr = mem.add(off) as *mut Self;
        off += size_of::<Self>();

        let (pools_ptr, _) =
            SharedPools::<T>::new_in_place(mem, off, max_bufs,
                                           nodes_per_buf, total_nodes);

        // allocate first buffer
        let first_bl = (*pools_ptr)
            .alloc_bl_meta_with_node_array(0, ptr::null_mut());
        if first_bl.is_null() { panic!("JiffyQueue init failed"); }

        // write fields
        ptr::addr_of_mut!((*q_ptr).head_of_queue)     .write(AtomicPtr::new(first_bl));
        ptr::addr_of_mut!((*q_ptr).tail_of_queue)     .write(AtomicPtr::new(first_bl));
        ptr::addr_of_mut!((*q_ptr).global_tail_location).write(AtomicU64::new(0));
        ptr::addr_of_mut!((*q_ptr).pools)             .write(pools_ptr);
        ptr::addr_of_mut!((*q_ptr).garbage_list_head) .write(AtomicPtr::new(ptr::null_mut()));

        &mut *q_ptr
    }

    #[inline] fn pools(&self) -> &SharedPools<T> { unsafe { &*self.pools } }
    #[inline] fn buffer_capacity(&self) -> usize { self.pools().buffer_capacity_per_array }

    // ─────────── patch #1: mark abandoned slot handled ────────────
    unsafe fn mark_location_handled(&self, loc: u64) {
        let cap = self.pools().buffer_capacity_per_array as u64;
        let buf_pos  = loc / cap;
        let idx      = (loc % cap) as usize;

        let mut bl = self.head_of_queue.load(Ordering::Acquire);
        while !bl.is_null() {
            let r = &*bl;
            if r.position_in_queue == buf_pos {
                if !r.curr_buffer.is_null() && idx < r.capacity {
                    let n = r.curr_buffer.add(idx);
                    (*n).is_set.compare_exchange(
                        NodeState::Empty as usize,
                        NodeState::Handled as usize,
                        Ordering::AcqRel, Ordering::Acquire).ok();
                }
                break;
            }
            bl = r.next.load(Ordering::Acquire);
        }
    }

    // ───────────────────────────────────────────────── enqueue ────

    fn actual_enqueue(&self, data: T) -> Result<(), T> {
        let loc = self.global_tail_location.fetch_add(1, Ordering::AcqRel);

        let mut tail_bl = self.tail_of_queue.load(Ordering::Acquire);
        let mut new_bl: *mut BufferList<T> = ptr::null_mut();

        loop {
            if tail_bl.is_null() {
                unsafe { self.mark_location_handled(loc); }
                if !new_bl.is_null() {
                    unsafe {
                        let bm = &mut *new_bl;
                        bm.mark_node_array_reclaimed_and_drop_items();
                        self.pools().dealloc_node_array_slice(bm.curr_buffer);
                        self.pools().dealloc_bl_meta_to_pool(new_bl);
                    }
                }
                return Err(data);
            }

            let tail = unsafe { &*tail_bl };
            let cap  = self.buffer_capacity();
            let start = tail.position_in_queue * cap as u64;
            let end   = start + cap as u64;

            if loc >= end {
                // need later buffer
                let mut next = tail.next.load(Ordering::Acquire);
                if next.is_null() {
                    if new_bl.is_null() {
                        new_bl = unsafe {
                            self.pools().alloc_bl_meta_with_node_array(
                                tail.position_in_queue + 1, tail_bl)
                        };
                        if new_bl.is_null() {
                            unsafe { self.mark_location_handled(loc); }
                            return Err(data);
                        }
                    }
                    match tail.next.compare_exchange(
                        ptr::null_mut(), new_bl, Ordering::AcqRel, Ordering::Acquire) {
                        Ok(_) => {
                            self.tail_of_queue.compare_exchange(
                                tail_bl, new_bl, Ordering::AcqRel, Ordering::Acquire).ok();
                            next = new_bl;
                            new_bl = ptr::null_mut();
                        }
                        Err(actual) => {
                            next = actual;
                            if !new_bl.is_null() {
                                unsafe {
                                    let bm = &mut *new_bl;
                                    bm.mark_node_array_reclaimed_and_drop_items();
                                    self.pools().dealloc_node_array_slice(bm.curr_buffer);
                                    self.pools().dealloc_bl_meta_to_pool(new_bl);
                                }
                                new_bl = ptr::null_mut();
                            }
                        }
                    }
                }
                if !next.is_null() {
                    self.tail_of_queue.compare_exchange(
                        tail_bl, next, Ordering::AcqRel, Ordering::Acquire).ok();
                    tail_bl = next;
                } else {
                    tail_bl = self.tail_of_queue.load(Ordering::Acquire);
                }
                continue;
            } else if loc < start {
                // walk backwards (rare)
                tail_bl = tail.prev;
                if tail_bl.is_null() {
                    unsafe { self.mark_location_handled(loc); }
                    return Err(data);
                }
                continue;
            } else {
                // this buffer
                let idx = (loc - start) as usize;
                if idx >= tail.capacity {
                    tail_bl = self.tail_of_queue.load(Ordering::Acquire);
                    continue;
                }
                if tail.curr_buffer.is_null() {
                    unsafe { self.mark_location_handled(loc); }
                    return Err(data);
                }

                let n = unsafe { tail.curr_buffer.add(idx) };
                unsafe {
                    ptr::write(&mut (*n).data, MaybeUninit::new(data));
                    (*n).is_set.store(NodeState::Set as usize, Ordering::Release);
                }

                // opportunistic pre-alloc
                let is_last = tail.next.load(Ordering::Acquire).is_null()
                           && tail_bl == self.tail_of_queue.load(Ordering::Relaxed);
                if idx == 1 && is_last && cap > 1 {
                    let pre = unsafe {
                        self.pools().alloc_bl_meta_with_node_array(
                            tail.position_in_queue + 1, tail_bl)
                    };
                    if !pre.is_null() {
                        if tail.next.compare_exchange(
                            ptr::null_mut(), pre, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                            self.tail_of_queue.compare_exchange(
                                tail_bl, pre, Ordering::AcqRel, Ordering::Acquire).ok();
                        } else {
                            unsafe {
                                let bm = &mut *pre;
                                bm.mark_node_array_reclaimed_and_drop_items();
                                self.pools().dealloc_node_array_slice(bm.curr_buffer);
                                self.pools().dealloc_bl_meta_to_pool(pre);
                            }
                        }
                    }
                }

                if !new_bl.is_null() {
                    unsafe {
                        let bm = &mut *new_bl;
                        bm.mark_node_array_reclaimed_and_drop_items();
                        self.pools().dealloc_node_array_slice(bm.curr_buffer);
                        self.pools().dealloc_bl_meta_to_pool(new_bl);
                    }
                }
                return Ok(());
            }
        }
    }

    // ────────────────────────────────────────────── dequeue ──────

    #[inline]
    fn actual_process_garbage_list(&self, new_head_pos_thresh: u64) {
        let mut g = self.garbage_list_head.swap(ptr::null_mut(), Ordering::Acquire);
        if g.is_null() { return; }

        let mut deferred: *mut BufferList<T> = ptr::null_mut();
        while !g.is_null() {
            let cur = g;
            g = unsafe { (*cur).next_in_garbage.load(Ordering::Relaxed) };
            let pos = unsafe { (*cur).position_in_queue };
            if pos < new_head_pos_thresh {
                unsafe { self.pools().dealloc_bl_meta_to_pool(cur); }
            } else {
                unsafe { (*cur).next_in_garbage.store(deferred, Ordering::Relaxed); }
                deferred = cur;
            }
        }
        if !deferred.is_null() {
            let mut curr_head = self.garbage_list_head.load(Ordering::Relaxed);
            let mut tail = deferred;
            while !unsafe{(*tail).next_in_garbage.load(Ordering::Relaxed)}.is_null() {
                tail = unsafe { (*tail).next_in_garbage.load(Ordering::Relaxed) };
            }
            loop {
                unsafe { (*tail).next_in_garbage.store(curr_head, Ordering::Relaxed); }
                match self.garbage_list_head.compare_exchange(
                    curr_head, deferred, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(n) => curr_head = n,
                }
            }
        }
    }

    unsafe fn attempt_fold_buffer(&self, bl: *mut BufferList<T>)
        -> (*mut BufferList<T>, bool)
    {
        let head = self.head_of_queue.load(Ordering::Acquire);
        if bl.is_null() || bl == head { return (bl, false); }

        let mut bl_mut = &mut *bl;
        let prev = bl_mut.prev;
        let next = bl_mut.next.load(Ordering::Acquire);
        if prev.is_null() { return (bl, false); }

        if (&*prev).next.compare_exchange(
            bl, next, Ordering::AcqRel, Ordering::Acquire).is_ok()
        {
            if !next.is_null() {
                (*next).prev = prev;
            }
            bl_mut.mark_node_array_reclaimed_and_drop_items();
            bl_mut = &mut *bl; // still valid

            // add to garbage
            let mut g_head = self.garbage_list_head.load(Ordering::Relaxed);
            loop {
                bl_mut.next_in_garbage.store(g_head, Ordering::Relaxed);
                match self.garbage_list_head.compare_exchange(
                    g_head, bl, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(n) => g_head = n,
                }
            }
            (next, true)
        } else {
            (bl, false)
        }
    }

    fn actual_dequeue(&self) -> Option<T> {
        'retry: loop {
            let bl_ptr = self.head_of_queue.load(Ordering::Acquire);
            if bl_ptr.is_null() { return None; }

            let bl = unsafe { &mut *bl_ptr };

            // skip handled nodes
            while bl.consumer_head_idx < bl.capacity {
                if bl.curr_buffer.is_null() { break; }
                let n_ptr = unsafe { bl.curr_buffer.add(bl.consumer_head_idx) };
                if unsafe { (*n_ptr).is_set.load(Ordering::Acquire) }
                    == NodeState::Handled as usize
                { bl.consumer_head_idx += 1; }
                else { break }
            }

            if bl.consumer_head_idx >= bl.capacity || bl.curr_buffer.is_null() {
                let next = bl.next.load(Ordering::Acquire);
                let thresh = if next.is_null() { u64::MAX }
                             else           { unsafe{(*next).position_in_queue}};
                self.actual_process_garbage_list(thresh);

                if self.head_of_queue
                       .compare_exchange(bl_ptr, next, Ordering::AcqRel, Ordering::Acquire)
                       .is_ok()
                {
                    if !next.is_null() {
                        unsafe { (*next).prev = ptr::null_mut(); }
                    }
                    unsafe {
                        bl.mark_node_array_reclaimed_and_drop_items();
                        self.pools().dealloc_bl_meta_to_pool(bl_ptr);
                    }
                }
                continue 'retry;
            }

            let idx = bl.consumer_head_idx;
            if bl.curr_buffer.is_null() { continue 'retry; }

            let node_ptr = unsafe { bl.curr_buffer.add(idx) };
            let n_state  = unsafe { (*node_ptr).is_set.load(Ordering::Acquire) };

            // ── patch #2: “queue empty” check looks only at *Handled* ──
            let n_loc = bl.position_in_queue * self.buffer_capacity() as u64 + idx as u64;
            let tail  = self.global_tail_location.load(Ordering::Acquire);
            if n_loc >= tail
               && n_state == NodeState::Handled as usize
               && bl_ptr == self.tail_of_queue.load(Ordering::Acquire)
            { return None; }

            // fast path success
            if n_state == NodeState::Set as usize {
                if unsafe { (*node_ptr).is_set.compare_exchange(
                        NodeState::Set as usize, NodeState::Handled as usize,
                        Ordering::AcqRel, Ordering::Acquire) }.is_ok()
                {
                    bl.consumer_head_idx += 1;
                    let data = unsafe { ptr::read(&(*node_ptr).data).assume_init() };
                    return Some(data);
                } else {
                    continue 'retry;
                }
            }

            // otherwise run long scan (identical to original impl) …  
            // *** kept unchanged for brevity; copy from your source ***
            /*  (the mega-scan for earlier items, attempt_fold_buffer etc.)  */
            // For space: entire long scan is unchanged from original code.
            // ----------------------------------------------------------------
            let mut scan_bl   = bl_ptr;
            let mut scan_idx  = idx+1;
            'outer: loop {
                if scan_bl.is_null() { return None; }
                let sb = unsafe { &mut *scan_bl };
                if sb.curr_buffer.is_null() {
                    scan_bl  = sb.next.load(Ordering::Acquire);
                    scan_idx = 0;
                    continue 'outer;
                }
                let mut i = scan_idx;
                let mut found = false;
                while i < sb.capacity {
                    let cand = unsafe { sb.curr_buffer.add(i) };
                    if unsafe { (*cand).is_set.load(Ordering::Acquire)} == NodeState::Set as usize {
                        found = true;
                        let mut f_bl  = scan_bl;
                        let mut f_idx = i;
                        'rescan: loop {
                            let mut rb  = bl_ptr;
                            let mut ridx= idx;
                            let mut earlier = false;
                            while !(rb == f_bl && ridx >= f_idx) {
                                if rb.is_null() { break; }
                                let r = unsafe { &*rb };
                                if r.curr_buffer.is_null() {
                                    rb   = r.next.load(Ordering::Acquire);
                                    ridx = 0;
                                    continue;
                                }
                                if ridx >= r.capacity {
                                    rb = r.next.load(Ordering::Acquire);
                                    ridx=0;
                                    continue;
                                }
                                let e = unsafe { r.curr_buffer.add(ridx) };
                                if unsafe { (*e).is_set.load(Ordering::Acquire)} == NodeState::Set as usize {
                                    f_bl  = rb;
                                    f_idx = ridx;
                                    earlier = true;
                                    break;
                                }
                                ridx += 1;
                            }
                            if !earlier { break 'rescan; }
                        }
                        let fbr = unsafe { &*f_bl };
                        if fbr.curr_buffer.is_null() { continue 'retry; }
                        let f_node = unsafe { fbr.curr_buffer.add(f_idx) };
                        if unsafe { (*f_node).is_set.compare_exchange(
                                NodeState::Set as usize, NodeState::Handled as usize,
                                Ordering::AcqRel, Ordering::Acquire)}.is_ok()
                        {
                            let data = unsafe { ptr::read(&(*f_node).data).assume_init() };
                            return Some(data);
                        } else { continue 'retry; }
                    }
                    i+=1;
                }
                let buf_scanned = scan_bl;
                let mut next_bl = sb.next.load(Ordering::Acquire);
                if !found && buf_scanned != bl_ptr {
                    // maybe fold
                    let mut all_handled = true;
                    if sb.curr_buffer.is_null() {
                        if !sb.is_array_reclaimed.load(Ordering::Relaxed) {
                            all_handled=false;
                        }
                    } else {
                        for i in 0..sb.capacity {
                            if unsafe{(*sb.curr_buffer.add(i)).is_set.load(Ordering::Acquire)}
                                != NodeState::Handled as usize {
                                all_handled=false; break;
                            }
                        }
                    }
                    if all_handled {
                        let (after, folded) = unsafe { self.attempt_fold_buffer(buf_scanned) };
                        if folded { next_bl = after; }
                    }
                }
                scan_bl  = next_bl;
                scan_idx = 0;
            }
        }
    }

    // ─────────────────────────────——— public trait impl ───────────

    fn is_empty(&self) -> bool {
        let head = self.head_of_queue.load(Ordering::Acquire);
        if head.is_null() { return true; }
        let bl = unsafe { &*head };

        if bl.curr_buffer.is_null() {
            return bl.next.load(Ordering::Acquire).is_null()
                && head == self.tail_of_queue.load(Ordering::Acquire);
        }
        let idx = bl.consumer_head_idx;
        if idx >= bl.capacity {
            return bl.next.load(Ordering::Acquire).is_null()
                && head == self.tail_of_queue.load(Ordering::Acquire);
        }
        let head_loc = bl.position_in_queue * self.buffer_capacity() as u64 + idx as u64;
        let tail_loc = self.global_tail_location.load(Ordering::Acquire);
        if head_loc < tail_loc { return false; }

        let node = unsafe { &*bl.curr_buffer.add(idx) };
        let st   = node.is_set.load(Ordering::Acquire);
        if st == NodeState::Set as usize { return false; }
        if st == NodeState::Handled as usize {
            return bl.next.load(Ordering::Acquire).is_null()
                && head == self.tail_of_queue.load(Ordering::Acquire);
        }
        /* st == Empty ⇒ be conservative: not empty */
        false
    }

    fn is_full(&self) -> bool { false }
}

impl<T: Send + 'static> MpscQueue<T> for JiffyQueue<T> {
    type PushError = T;
    type PopError  = ();

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        self.actual_enqueue(item)
    }
    fn pop(&self) -> Result<T, Self::PopError> {
        self.actual_dequeue().ok_or(())
    }
    fn is_empty(&self) -> bool { self.is_empty() }
    fn is_full(&self)  -> bool { false }
}

impl<T: Send + 'static> Drop for JiffyQueue<T> {
    fn drop(&mut self) {
        self.actual_process_garbage_list(u64::MAX);

        let mut cur = self.head_of_queue.load(Ordering::Relaxed);
        self.head_of_queue.store(ptr::null_mut(), Ordering::Relaxed);
        self.tail_of_queue.store(ptr::null_mut(), Ordering::Relaxed);

        while !cur.is_null() {
            let next = unsafe { (*cur).next.load(Ordering::Relaxed) };
            unsafe {
                (*cur).mark_node_array_reclaimed_and_drop_items();
                self.pools().dealloc_node_array_slice((*cur).curr_buffer);
                self.pools().dealloc_bl_meta_to_pool(cur);
            }
            cur = next;
        }
    }
}
