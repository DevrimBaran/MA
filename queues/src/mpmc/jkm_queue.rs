// paper in /paper/mpmc/jkm.pdf
use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{self, AtomicUsize, Ordering};

use crate::MpmcQueue;
use crossbeam_utils::atomic::AtomicCell;

#[inline(always)]
const fn pack_u128(hi: usize, lo: usize) -> u128 {
    ((hi as u128) << 64) | lo as u128
}
#[inline(always)]
fn unpack_u128(w: u128) -> (usize, usize) {
    ((w >> 64) as usize, w as usize)
}

type Timestamp = u128;
const TS_ST_EMPTY: usize = usize::MAX;
const TS_P_EMPTY: usize = usize::MAX;
const TIMESTAMP_EMPTY: Timestamp = pack_u128(TS_ST_EMPTY, TS_P_EMPTY);

const DEQ_J_INIT: usize = usize::MAX;
const DEQ_ID_INIT: usize = usize::MAX;
const DEQ_OPS_INIT: u128 = pack_u128(DEQ_J_INIT, DEQ_ID_INIT);

const DEQ_J_EMPTY: usize = usize::MAX;
const DEQ_ID_EMPTY: usize = usize::MAX - 1;
const DEQ_OPS_EMPTY: u128 = pack_u128(DEQ_J_EMPTY, DEQ_ID_EMPTY);

const DEQ_OPS_SIZE: usize = 500_000;

#[repr(C)]
struct QueueItem<T> {
    val: UnsafeCell<Option<T>>,
    timestamp: AtomicCell<Timestamp>,
}
impl<T> QueueItem<T> {
    const fn new() -> Self {
        Self {
            val: UnsafeCell::new(None),
            timestamp: AtomicCell::new(TIMESTAMP_EMPTY),
        }
    }
}

#[repr(C, align(64))]
struct MaxRegister {
    v: AtomicUsize,
}
impl MaxRegister {
    fn new(x: usize) -> Self {
        Self {
            v: AtomicUsize::new(x),
        }
    }
    fn max_read(&self) -> usize {
        self.v.load(Ordering::SeqCst)
    }
    fn max_write(&self, val: usize) {
        let mut cur = self.v.load(Ordering::SeqCst);
        while val > cur {
            match self
                .v
                .compare_exchange(cur, val, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => {
                    atomic::fence(Ordering::SeqCst);
                    break;
                }
                Err(x) => cur = x,
            }
        }
    }
}

#[repr(C, align(64))]
pub struct FetchAndInc {
    pub v: AtomicUsize,
}
impl FetchAndInc {
    fn new(x: usize) -> Self {
        Self {
            v: AtomicUsize::new(x),
        }
    }
    fn fetch_inc(&self) -> usize {
        let result = self.v.fetch_add(1, Ordering::SeqCst);
        atomic::fence(Ordering::SeqCst);
        result
    }
}

#[repr(C)]
pub struct JKMQueue<T: Send + Clone + 'static> {
    enq_counter: MaxRegister,
    pub deq_counter: FetchAndInc,
    pub successful_dequeues: AtomicUsize,

    head: *const [MaxRegister],
    tail: *const [AtomicUsize],
    items: *const [QueueItem<T>],
    tree: *const [AtomicCell<u128>],
    deq_ops: *const [AtomicCell<u128>],

    pub num_processes: usize,
    pub num_dequeuers: usize,
    items_per_process: usize,
    tree_size: usize,

    // Debug counters
    debug_items_written: AtomicUsize,
    debug_items_taken: AtomicUsize,
    debug_deq_ops_completed: AtomicUsize,
    debug_deq_ops_empty: AtomicUsize,
    debug_enq_completed: AtomicUsize,
    debug_deq_ops_non_empty: AtomicUsize,
    debug_take_failures: AtomicUsize,
    debug_op_changed: AtomicUsize,

    _phantom: std::marker::PhantomData<T>,
}
unsafe impl<T: Send + Clone + 'static> Send for JKMQueue<T> {}
unsafe impl<T: Send + Clone + 'static> Sync for JKMQueue<T> {}

// ---------- layout helpers ---------------------------------------------------
impl<T: Send + Clone + 'static> JKMQueue<T> {
    fn layout(n_enq: usize) -> (Layout, [usize; 5]) {
        let ipp = 200_000;
        let tree_sz = if n_enq == 0 {
            0
        } else {
            n_enq.next_power_of_two() * 2 - 1
        };

        let root = Layout::new::<Self>();
        let (l_head, o_head) = root
            .extend(Layout::array::<MaxRegister>(n_enq).unwrap())
            .unwrap();
        let (l_tail, o_tail) = l_head
            .extend(Layout::array::<AtomicUsize>(n_enq).unwrap())
            .unwrap();
        let (l_items, o_items) = l_tail
            .extend(Layout::array::<QueueItem<T>>(n_enq * ipp).unwrap())
            .unwrap();
        let (l_tree, o_tree) = l_items
            .extend(Layout::array::<AtomicCell<u128>>(tree_sz).unwrap())
            .unwrap();
        let (l_final, o_ops) = l_tree
            .extend(Layout::array::<AtomicCell<u128>>(DEQ_OPS_SIZE).unwrap())
            .unwrap();

        (
            l_final.pad_to_align(),
            [o_head, o_tail, o_items, o_tree, o_ops],
        )
    }

    pub fn shared_size(n_enq: usize, _n_deq: usize) -> usize {
        Self::layout(n_enq).0.size()
    }

    /// # Safety
    ///
    /// The caller must supply a shared-memory block of at least `shared_size`
    /// bytes, properly aligned.
    pub unsafe fn init_in_shared(mem: *mut u8, n_enq: usize, n_deq: usize) -> &'static mut Self {
        let ipp = 200_000;
        let tree_sz = if n_enq == 0 {
            0
        } else {
            n_enq.next_power_of_two() * 2 - 1
        };
        let off = Self::layout(n_enq).1;

        let head_ptr = mem.add(off[0]) as *mut MaxRegister;
        let tail_ptr = mem.add(off[1]) as *mut AtomicUsize;
        let items_ptr = mem.add(off[2]) as *mut QueueItem<T>;
        let tree_ptr = mem.add(off[3]) as *mut AtomicCell<u128>;
        let ops_ptr = mem.add(off[4]) as *mut AtomicCell<u128>;

        let self_ptr = mem as *mut Self;
        ptr::write(
            self_ptr,
            Self {
                enq_counter: MaxRegister::new(0),
                deq_counter: FetchAndInc::new(1),
                successful_dequeues: AtomicUsize::new(0),
                head: ptr::slice_from_raw_parts(head_ptr, n_enq),
                tail: ptr::slice_from_raw_parts(tail_ptr, n_enq),
                items: ptr::slice_from_raw_parts(items_ptr, n_enq * ipp),
                tree: ptr::slice_from_raw_parts(tree_ptr, tree_sz),
                deq_ops: ptr::slice_from_raw_parts(ops_ptr, DEQ_OPS_SIZE),
                num_processes: n_enq,
                num_dequeuers: n_deq,
                items_per_process: ipp,
                tree_size: tree_sz,
                debug_items_written: AtomicUsize::new(0),
                debug_items_taken: AtomicUsize::new(0),
                debug_deq_ops_completed: AtomicUsize::new(0),
                debug_deq_ops_empty: AtomicUsize::new(0),
                debug_enq_completed: AtomicUsize::new(0),
                debug_deq_ops_non_empty: AtomicUsize::new(0),
                debug_take_failures: AtomicUsize::new(0),
                debug_op_changed: AtomicUsize::new(0),
                _phantom: std::marker::PhantomData,
            },
        );

        for i in 0..n_enq {
            head_ptr.add(i).write(MaxRegister::new(0));
            tail_ptr.add(i).write(AtomicUsize::new(0));
        }
        for i in 0..(n_enq * ipp) {
            items_ptr.add(i).write(QueueItem::new());
        }
        for i in 0..tree_sz {
            tree_ptr.add(i).write(AtomicCell::new(TIMESTAMP_EMPTY));
        }
        for i in 0..DEQ_OPS_SIZE {
            ops_ptr.add(i).write(AtomicCell::new(DEQ_OPS_INIT));
        }

        atomic::fence(Ordering::SeqCst);

        &mut *self_ptr
    }

    // ---------- tiny index helpers for the binary tree ----------------------
    #[inline(always)]
    fn parent(i: usize) -> usize {
        (i - 1) / 2
    }
    #[inline(always)]
    fn left(i: usize) -> usize {
        2 * i + 1
    }
    #[inline(always)]
    fn right(i: usize) -> usize {
        2 * i + 2
    }
    #[inline(always)]
    fn leaf_for(&self, p: usize) -> usize {
        (self.tree_size + 1) / 2 - 1 + p
    }

    // ---------- refresh a tree node (leaf or internal) ----------------------
    unsafe fn refresh(&self, idx: usize, is_leaf: bool) -> bool {
        atomic::fence(Ordering::SeqCst);

        let node = (*self.tree).get_unchecked(idx);
        let old = node.load();

        let new = if is_leaf {
            let p = idx - ((self.tree_size + 1) / 2 - 1);
            atomic::fence(Ordering::SeqCst);
            let h = (*self.head).get_unchecked(p).max_read();
            atomic::fence(Ordering::SeqCst);
            let t = (*self.tail).get_unchecked(p).load(Ordering::SeqCst);
            atomic::fence(Ordering::SeqCst);

            if h >= t {
                TIMESTAMP_EMPTY
            } else {
                atomic::fence(Ordering::SeqCst);
                let ts = (*self.items)
                    .get_unchecked(p * self.items_per_process + h)
                    .timestamp
                    .load();
                atomic::fence(Ordering::SeqCst);
                ts
            }
        } else {
            atomic::fence(Ordering::SeqCst);
            let l = (*self.tree).get_unchecked(Self::left(idx)).load();
            atomic::fence(Ordering::SeqCst);
            let r = (*self.tree).get_unchecked(Self::right(idx)).load();
            atomic::fence(Ordering::SeqCst);
            match (l == TIMESTAMP_EMPTY, r == TIMESTAMP_EMPTY) {
                (true, true) => TIMESTAMP_EMPTY,
                (true, false) => r,
                (false, true) => l,
                (false, false) => l.min(r),
            }
        };

        if old == new {
            false
        } else {
            let result = node.compare_exchange(old, new).is_ok();
            if result {
                atomic::fence(Ordering::SeqCst);
            }
            result
        }
    }

    unsafe fn propagate(&self, p: usize) {
        atomic::fence(Ordering::SeqCst);

        let mut idx = self.leaf_for(p);

        // Refresh leaf multiple times with strong synchronization
        for _ in 0..6 {
            // Reduced from 7 to 6
            self.refresh(idx, true);
            atomic::fence(Ordering::SeqCst);
        }

        // Propagate up the tree with strong synchronization
        while idx > 0 {
            idx = Self::parent(idx);
            for _ in 0..6 {
                // Reduced from 7 to 6
                self.refresh(idx, false);
                atomic::fence(Ordering::SeqCst);
            }
        }

        // Final fence to ensure all updates are visible
        atomic::fence(Ordering::SeqCst);
    }

    unsafe fn finish_deq(&self, num: usize) {
        let deq_op_node = (*self.deq_ops).get_unchecked(num);

        // Multiple attempts to complete the operation
        for attempt in 0..self.num_dequeuers * 10 {
            // Check if already done
            let current = deq_op_node.load();
            if current != DEQ_OPS_INIT {
                return;
            }

            // Strong synchronization before reading tree
            atomic::fence(Ordering::SeqCst);
            atomic::fence(Ordering::SeqCst);

            let root_ts = (*self.tree).get_unchecked(0).load();
            atomic::fence(Ordering::SeqCst);
            let (_, id) = unpack_u128(root_ts);

            let final_val = if id == TS_P_EMPTY {
                DEQ_OPS_EMPTY
            } else {
                let h = (*self.head).get_unchecked(id).max_read();
                let t = (*self.tail).get_unchecked(id).load(Ordering::SeqCst);

                if h >= t {
                    DEQ_OPS_EMPTY
                } else {
                    pack_u128(h, id)
                }
            };

            // Try to write the result
            match deq_op_node.compare_exchange(DEQ_OPS_INIT, final_val) {
                Ok(_) => {
                    // Successfully wrote - now update head if non-empty
                    if final_val != DEQ_OPS_EMPTY {
                        let (h, id) = unpack_u128(final_val);
                        (*self.head).get_unchecked(id).max_write(h + 1);

                        // Excessive synchronization after claiming
                        for _ in 0..6 {
                            atomic::fence(Ordering::SeqCst);
                        }

                        // Multiple propagations to ensure visibility
                        for _ in 0..6 {
                            self.propagate(id);
                            atomic::fence(Ordering::SeqCst);
                        }

                        self.debug_deq_ops_non_empty.fetch_add(1, Ordering::SeqCst);
                    } else {
                        self.debug_deq_ops_empty.fetch_add(1, Ordering::SeqCst);
                    }
                    atomic::fence(Ordering::SeqCst);
                    return;
                }
                Err(_) => {
                    // Someone else completed it
                    return;
                }
            }
        }
    }

    unsafe fn update_tree(&self, num: usize) {
        // Multiple strong fences for synchronization
        atomic::fence(Ordering::SeqCst);
        atomic::fence(Ordering::SeqCst);

        if num >= DEQ_OPS_SIZE {
            return;
        }

        let op = (*self.deq_ops).get_unchecked(num).load();
        atomic::fence(Ordering::SeqCst);

        if op == DEQ_OPS_INIT {
            return;
        }

        let (j, id) = unpack_u128(op);

        if id != DEQ_ID_INIT && id != DEQ_ID_EMPTY && id < self.num_processes {
            // The head should already be updated by finish_deq
            // But ensure it's at least j+1
            (*self.head).get_unchecked(id).max_write(j + 1);

            // Multiple strong fences
            atomic::fence(Ordering::SeqCst);
            atomic::fence(Ordering::SeqCst);
            atomic::fence(Ordering::SeqCst); // Keep extra fence

            // Multiple propagations for reliability
            for _ in 0..6 {
                // Reduced from 7 to 6
                self.propagate(id);
                atomic::fence(Ordering::SeqCst);
            }
        }

        atomic::fence(Ordering::SeqCst);
    }

    // ---------- enqueue / dequeue -------------------------------------------
    pub fn enqueue(&self, pid: usize, x: T) -> Result<(), ()> {
        if pid >= self.num_processes {
            return Err(());
        }
        unsafe {
            // Strong synchronization at start
            atomic::fence(Ordering::SeqCst);
            atomic::fence(Ordering::SeqCst);
            atomic::fence(Ordering::SeqCst);

            let t = (*self.tail).get_unchecked(pid).load(Ordering::SeqCst);
            if t >= self.items_per_process {
                return Err(());
            }

            let st = self.enq_counter.max_read();
            let timestamp = pack_u128(st, pid);
            let item = (*self.items).get_unchecked(pid * self.items_per_process + t);

            // Write value first with extreme synchronization
            *item.val.get() = Some(x);
            self.debug_items_written.fetch_add(1, Ordering::SeqCst);

            // Excessive barriers to ensure value is written
            for _ in 0..6 {
                // Reduced from 7 to 6
                atomic::fence(Ordering::SeqCst);
            }

            // Write timestamp with more barriers
            item.timestamp.store(timestamp);

            // More excessive barriers
            for _ in 0..6 {
                // Reduced from 7 to 6
                atomic::fence(Ordering::SeqCst);
            }

            // Publish by incrementing tail with synchronization
            (*self.tail)
                .get_unchecked(pid)
                .store(t + 1, Ordering::SeqCst);

            // Synchronize after tail update
            for _ in 0..6 {
                // Reduced from 7 to 6
                atomic::fence(Ordering::SeqCst);
            }

            // Update global counter
            self.enq_counter.max_write(st + 1);

            // More synchronization
            for _ in 0..6 {
                // Reduced from 7 to 6
                atomic::fence(Ordering::SeqCst);
            }

            // Multiple propagations for maximum reliability
            for _ in 0..6 {
                // Reduced from 7 to 6
                self.propagate(pid);
                atomic::fence(Ordering::SeqCst);
            }

            self.debug_enq_completed.fetch_add(1, Ordering::SeqCst);

            // Final excessive synchronization
            for _ in 0..6 {
                // Reduced from 7 to 6
                atomic::fence(Ordering::SeqCst);
            }

            Ok(())
        }
    }

    pub fn dequeue(&self, _tid: usize) -> Result<T, ()> {
        unsafe {
            atomic::fence(Ordering::SeqCst);

            let num = self.deq_counter.fetch_inc();

            // Check if we've exceeded the deq_ops array size
            if num >= DEQ_OPS_SIZE {
                return Err(());
            }

            // Help operations in the range [max(1, num - k + 1), num]
            let help_start = if num > self.num_dequeuers {
                num - self.num_dequeuers + 1
            } else {
                1
            };

            // Execute helping in order with multiple passes
            for pass in 0..2 {
                for i in help_start..=num {
                    if i >= DEQ_OPS_SIZE {
                        break;
                    }

                    atomic::fence(Ordering::SeqCst);
                    let op_val = (*self.deq_ops).get_unchecked(i).load();
                    if op_val == DEQ_OPS_INIT {
                        // Update tree for all previous operations
                        for j in (i.saturating_sub(10))..i {
                            if j > 0 {
                                self.update_tree(j);
                            }
                        }
                        atomic::fence(Ordering::SeqCst);
                        self.finish_deq(i);
                        atomic::fence(Ordering::SeqCst);
                    }
                }

                if pass == 0 {
                    atomic::fence(Ordering::SeqCst);
                }
            }

            // Get our result with very long waiting
            let mut op = (*self.deq_ops).get_unchecked(num).load();
            let mut wait_cycles = 0;
            const MAX_WAIT: usize = 1000000;

            while op == DEQ_OPS_INIT && wait_cycles < MAX_WAIT {
                // Help ALL operations multiple times
                for _ in 0..3 {
                    atomic::fence(Ordering::SeqCst);

                    // Only help operations within valid range
                    let max_help = num.min(DEQ_OPS_SIZE - 1);
                    for i in help_start..=max_help {
                        let op_val = (*self.deq_ops).get_unchecked(i).load();
                        if op_val == DEQ_OPS_INIT {
                            if i > 1 {
                                self.update_tree(i - 1);
                            }
                            self.finish_deq(i);
                        }
                    }
                }

                atomic::fence(Ordering::SeqCst);
                atomic::fence(Ordering::SeqCst);
                op = (*self.deq_ops).get_unchecked(num).load();

                // Progressive backoff with occasional yields
                if wait_cycles < 1000 {
                    for _ in 0..10 {
                        std::hint::spin_loop();
                    }
                } else if wait_cycles % 1000 == 0 {
                    // Periodic yield to prevent deadlock
                    std::thread::yield_now();
                } else {
                    for _ in 0..100 {
                        std::hint::spin_loop();
                    }
                }

                wait_cycles += 1;
            }

            // Check if we timed out
            if op == DEQ_OPS_INIT {
                eprintln!(
                    "ERROR: Dequeue {} timed out waiting for result after {} cycles",
                    num, wait_cycles
                );
                // Try one more aggressive helping round
                for i in 1..=num {
                    if i >= DEQ_OPS_SIZE {
                        break;
                    }
                    let op_val = (*self.deq_ops).get_unchecked(i).load();
                    if op_val == DEQ_OPS_INIT {
                        if i > 1 {
                            self.update_tree(i - 1);
                        }
                        self.finish_deq(i);
                    }
                }
                // Final check
                op = (*self.deq_ops).get_unchecked(num).load();
                if op == DEQ_OPS_INIT {
                    return Err(());
                }
            }

            let (j, id) = unpack_u128(op);

            if id == DEQ_ID_EMPTY {
                return Err(());
            }

            if id == DEQ_ID_INIT || id >= self.num_processes || j >= self.items_per_process {
                return Err(());
            }

            // Take the value with a retry loop
            atomic::fence(Ordering::SeqCst);
            atomic::fence(Ordering::SeqCst);
            atomic::fence(Ordering::SeqCst);

            let item = (*self.items).get_unchecked(id * self.items_per_process + j);

            // Try to take the value multiple times
            let mut take_attempts = 0;
            let max_take_attempts = 10000; // Increased from 1000

            while take_attempts < max_take_attempts {
                // Extreme synchronization before critical operation
                for _ in 0..6 {
                    // Reduced from 7 to 6
                    atomic::fence(Ordering::SeqCst);
                }

                // Check if value is available
                let val_ptr = item.val.get();
                let has_value = (*val_ptr).is_some();

                if has_value {
                    // Try to take it with extreme care
                    atomic::fence(Ordering::SeqCst);
                    atomic::fence(Ordering::SeqCst);
                    atomic::fence(Ordering::SeqCst); // Keep extra fence

                    let result = (*val_ptr).take();

                    // Extreme synchronization after take
                    for _ in 0..6 {
                        // Reduced from 7 to 6
                        atomic::fence(Ordering::SeqCst);
                    }

                    if let Some(value) = result {
                        self.debug_items_taken.fetch_add(1, Ordering::SeqCst);
                        self.debug_deq_ops_completed.fetch_add(1, Ordering::SeqCst);

                        // Update the tree for THIS operation many times
                        for _ in 0..12 {
                            // Reduced from 15 to 12
                            self.update_tree(num);
                            atomic::fence(Ordering::SeqCst);
                        }

                        return Ok(value);
                    }
                }

                // Value was None or take failed
                take_attempts += 1;

                // Re-check bounds with synchronization
                atomic::fence(Ordering::SeqCst);
                let h = (*self.head).get_unchecked(id).max_read();
                atomic::fence(Ordering::SeqCst);
                let t = (*self.tail).get_unchecked(id).load(Ordering::SeqCst);
                atomic::fence(Ordering::SeqCst);

                if j >= t {
                    // The item hasn't been enqueued yet - this shouldn't happen
                    eprintln!(
                        "ERROR: Trying to take item {} from process {} but tail is only {}",
                        j, id, t
                    );
                    self.debug_take_failures.fetch_add(1, Ordering::SeqCst);

                    // But wait a bit in case it's being written right now
                    if take_attempts < 100 {
                        std::thread::sleep(std::time::Duration::from_micros(100));
                        continue;
                    }
                    return Err(());
                }

                if j < h {
                    // Someone else already took this item
                    eprintln!(
                        "ERROR: Trying to take item {} from process {} but head is already {}",
                        j, id, h
                    );
                    self.debug_take_failures.fetch_add(1, Ordering::SeqCst);
                    return Err(());
                }

                // Aggressive backoff with heavy synchronization
                if take_attempts < 10 {
                    for _ in 0..100 {
                        atomic::fence(Ordering::SeqCst);
                        std::hint::spin_loop();
                    }
                } else if take_attempts < 100 {
                    atomic::fence(Ordering::SeqCst);
                    std::thread::yield_now();
                    atomic::fence(Ordering::SeqCst);
                } else if take_attempts < 1000 {
                    atomic::fence(Ordering::SeqCst);
                    std::thread::sleep(std::time::Duration::from_micros(100));
                    atomic::fence(Ordering::SeqCst);
                } else {
                    // Longer sleep for later attempts
                    atomic::fence(Ordering::SeqCst);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    atomic::fence(Ordering::SeqCst);
                }

                // Periodically force synchronization
                if take_attempts % 100 == 0 {
                    self.force_sync();
                }
            }

            // Failed to take after many attempts
            eprintln!(
                "ERROR: Failed to take item after {} attempts. Deq {}, item[{}][{}]",
                max_take_attempts, num, id, j
            );
            self.debug_take_failures.fetch_add(1, Ordering::SeqCst);
            Err(())
        }
    }

    pub fn is_empty(&self) -> bool {
        unsafe {
            atomic::fence(Ordering::SeqCst);
            for p in 0..self.num_processes {
                let h = (*self.head).get_unchecked(p).max_read();
                let t = (*self.tail).get_unchecked(p).load(Ordering::SeqCst);
                if h < t {
                    return false;
                }
            }
            true
        }
    }

    pub fn is_full(&self) -> bool {
        false
    }

    pub fn force_sync(&self) {
        unsafe {
            for round in 0..3 {
                atomic::fence(Ordering::SeqCst);

                // First ensure all tree updates are applied
                let max_deq = self.deq_counter.v.load(Ordering::SeqCst);
                for i in 1..=max_deq.min(DEQ_OPS_SIZE - 1) {
                    self.update_tree(i);
                    if i % 100 == 0 {
                        atomic::fence(Ordering::SeqCst);
                    }
                }

                atomic::fence(Ordering::SeqCst);

                // Then propagate all trees multiple times
                for _ in 0..2 {
                    for p in 0..self.num_processes {
                        self.propagate(p);
                    }
                    atomic::fence(Ordering::SeqCst);
                }

                if round < 2 {
                    std::thread::yield_now();
                }
            }

            atomic::fence(Ordering::SeqCst);
        }
    }

    pub fn total_items(&self) -> usize {
        unsafe {
            atomic::fence(Ordering::SeqCst);
            let mut total = 0;
            for p in 0..self.num_processes {
                let h = (*self.head).get_unchecked(p).max_read();
                let t = (*self.tail).get_unchecked(p).load(Ordering::SeqCst);
                if t > h {
                    total += t - h;
                }
            }
            total
        }
    }

    pub fn finalize_pending_dequeues(&self) {
        unsafe {
            for round in 0..5 {
                atomic::fence(Ordering::SeqCst);

                let max_deq = self.deq_counter.v.load(Ordering::SeqCst);

                // First pass: complete all pending operations
                for i in 1..=max_deq.min(DEQ_OPS_SIZE - 1) {
                    atomic::fence(Ordering::SeqCst);
                    let op_val = (*self.deq_ops).get_unchecked(i).load();

                    if op_val == DEQ_OPS_INIT {
                        if i > 1 {
                            self.update_tree(i - 1);
                            atomic::fence(Ordering::SeqCst);
                        }
                        self.finish_deq(i);
                        atomic::fence(Ordering::SeqCst);
                    }
                }

                // Second pass: ensure all tree updates are applied
                for i in 1..=max_deq.min(DEQ_OPS_SIZE - 1) {
                    self.update_tree(i);
                }

                // Force complete propagation
                self.force_sync();

                if round < 4 {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
    }

    pub fn print_debug_stats(&self) {
        println!("JKMQueue Debug Stats:");
        println!(
            "  Items written: {}",
            self.debug_items_written.load(Ordering::Acquire)
        );
        println!(
            "  Items taken: {}",
            self.debug_items_taken.load(Ordering::Acquire)
        );
        println!(
            "  Enqueues completed: {}",
            self.debug_enq_completed.load(Ordering::Acquire)
        );
        println!(
            "  Dequeue ops completed: {}",
            self.debug_deq_ops_completed.load(Ordering::Acquire)
        );
        println!(
            "  Dequeue ops empty: {}",
            self.debug_deq_ops_empty.load(Ordering::Acquire)
        );
        println!(
            "  Dequeue ops non-empty: {}",
            self.debug_deq_ops_non_empty.load(Ordering::Acquire)
        );
        println!(
            "  Take failures: {}",
            self.debug_take_failures.load(Ordering::Acquire)
        );
        println!(
            "  Op changed: {}",
            self.debug_op_changed.load(Ordering::Acquire)
        );

        let deq_counter = self.deq_counter.v.load(Ordering::Acquire);
        println!("  Dequeue counter: {}", deq_counter);

        // Check per-process queues
        unsafe {
            let mut total_queued = 0;
            for p in 0..self.num_processes {
                let h = (*self.head).get_unchecked(p).max_read();
                let t = (*self.tail).get_unchecked(p).load(Ordering::SeqCst);
                if t > h {
                    println!(
                        "  Process {} queue: head={}, tail={}, items={}",
                        p,
                        h,
                        t,
                        t - h
                    );
                    total_queued += t - h;
                }
            }
            println!("  Total items still in queues: {}", total_queued);

            // Check deq_ops status
            let mut pending_deq_ops = 0;
            let mut completed_deq_ops = 0;
            let mut empty_deq_ops = 0;

            let check_limit = deq_counter.min(DEQ_OPS_SIZE);
            for i in 1..check_limit {
                let op = (*self.deq_ops).get_unchecked(i).load();
                if op == DEQ_OPS_INIT {
                    pending_deq_ops += 1;
                } else if op == DEQ_OPS_EMPTY {
                    empty_deq_ops += 1;
                } else {
                    completed_deq_ops += 1;
                }
            }

            println!(
                "  Deq ops - pending: {}, completed: {}, empty: {}",
                pending_deq_ops, completed_deq_ops, empty_deq_ops
            );

            // Sanity check
            let total_deq_ops = self.debug_deq_ops_non_empty.load(Ordering::Acquire)
                + self.debug_deq_ops_empty.load(Ordering::Acquire);
            println!(
                "  Total deq ops finished: {} (should match completed + empty)",
                total_deq_ops
            );
        }
    }
}

impl<T: Send + Clone + 'static> MpmcQueue<T> for JKMQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, v: T, id: usize) -> Result<(), Self::PushError> {
        if id < self.num_processes {
            self.enqueue(id, v)
        } else {
            Err(())
        }
    }
    fn pop(&self, id: usize) -> Result<T, Self::PopError> {
        self.dequeue(id)
    }
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
    fn is_full(&self) -> bool {
        self.is_full()
    }
}
