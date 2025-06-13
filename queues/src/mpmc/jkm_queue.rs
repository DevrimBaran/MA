use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{self, AtomicUsize, Ordering};

use crate::MpmcQueue;
use crossbeam_utils::atomic::AtomicCell;

// ---------- helpers ---------------------------------------------------------
#[inline(always)]
const fn pack_u128(hi: usize, lo: usize) -> u128 {
    ((hi as u128) << 64) | lo as u128
}
#[inline(always)]
fn unpack_u128(w: u128) -> (usize, usize) {
    ((w >> 64) as usize, w as usize)
}

// ---------- constants -------------------------------------------------------
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

// ---------- queue cell ------------------------------------------------------
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

// ---------- tiny atomics ----------------------------------------------------
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
        self.v.load(Ordering::SeqCst) // STRICTER: Use SeqCst for reads
    }
    fn max_write(&self, val: usize) {
        let mut cur = self.v.load(Ordering::SeqCst);
        while val > cur {
            match self
                .v
                .compare_exchange(cur, val, Ordering::SeqCst, Ordering::SeqCst) // STRICTER: No weak CAS
            {
                Ok(_) => {
                    atomic::fence(Ordering::SeqCst); // STRICTER: Add fence after write
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
        let result = self.v.fetch_add(1, Ordering::SeqCst); // STRICTER: Use SeqCst
        atomic::fence(Ordering::SeqCst); // STRICTER: Add fence
        result
    }
}

// ---------- main queue ------------------------------------------------------
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

    // STRICTER: Add version counters for synchronization
    tree_version: AtomicUsize,
    op_version: AtomicUsize,

    num_processes: usize,
    num_dequeuers: usize,
    items_per_process: usize,
    tree_size: usize,
    _phantom: std::marker::PhantomData<T>,
}
unsafe impl<T: Send + Clone + 'static> Send for JKMQueue<T> {}
unsafe impl<T: Send + Clone + 'static> Sync for JKMQueue<T> {}

// ---------- layout helpers ---------------------------------------------------
impl<T: Send + Clone + 'static> JKMQueue<T> {
    fn layout(n_enq: usize) -> (Layout, [usize; 7]) {
        let ipp = 50_000;
        let deq_ops_sz = 100_000;
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
        let (l_ops, o_ops) = l_tree
            .extend(Layout::array::<AtomicCell<u128>>(deq_ops_sz).unwrap())
            .unwrap();
        let (l_tv, o_tv) = l_ops.extend(Layout::new::<AtomicUsize>()).unwrap();
        let (l_final, o_ov) = l_tv.extend(Layout::new::<AtomicUsize>()).unwrap();

        (
            l_final.pad_to_align(),
            [o_head, o_tail, o_items, o_tree, o_ops, o_tv, o_ov],
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
        let ipp = 50_000;
        let deq_ops_sz = 100_000;
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
                deq_ops: ptr::slice_from_raw_parts(ops_ptr, deq_ops_sz),
                tree_version: AtomicUsize::new(0),
                op_version: AtomicUsize::new(0),
                num_processes: n_enq,
                num_dequeuers: n_deq,
                items_per_process: ipp,
                tree_size: tree_sz,
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
        for i in 0..deq_ops_sz {
            ops_ptr.add(i).write(AtomicCell::new(DEQ_OPS_INIT));
        }

        // STRICTER: Full fence after initialization
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
        atomic::fence(Ordering::SeqCst); // STRICTER: Fence before refresh

        let node = (*self.tree).get_unchecked(idx);
        let old = node.load();

        let new = if is_leaf {
            let p = idx - ((self.tree_size + 1) / 2 - 1);
            let h = (*self.head).get_unchecked(p).max_read();
            let t = (*self.tail).get_unchecked(p).load(Ordering::SeqCst); // STRICTER: SeqCst

            if h >= t {
                TIMESTAMP_EMPTY
            } else {
                (*self.items)
                    .get_unchecked(p * self.items_per_process + h)
                    .timestamp
                    .load()
            }
        } else {
            let l = (*self.tree).get_unchecked(Self::left(idx)).load();
            let r = (*self.tree).get_unchecked(Self::right(idx)).load();
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
                atomic::fence(Ordering::SeqCst); // STRICTER: Fence after successful update
                self.tree_version.fetch_add(1, Ordering::SeqCst); // STRICTER: Version increment
            }
            result
        }
    }

    unsafe fn propagate(&self, p: usize) {
        atomic::fence(Ordering::SeqCst); // STRICTER: Start with fence

        let mut idx = self.leaf_for(p);

        // STRICTER: Refresh leaf multiple times with barriers
        for _ in 0..3 {
            self.refresh(idx, true);
            atomic::fence(Ordering::SeqCst);
        }

        // Propagate up the tree with more synchronization
        while idx > 0 {
            idx = Self::parent(idx);
            for _ in 0..3 {
                self.refresh(idx, false);
                atomic::fence(Ordering::SeqCst);
            }
        }

        atomic::fence(Ordering::SeqCst); // STRICTER: End with fence
    }

    unsafe fn finish_deq(&self, num: usize) {
        atomic::fence(Ordering::SeqCst); // STRICTER: Start with fence

        let deq_op_node = (*self.deq_ops).get_unchecked(num);

        // STRICTER: Loop until we successfully write
        loop {
            let current = deq_op_node.load();
            if current != DEQ_OPS_INIT {
                return;
            }

            // Read root to determine result
            atomic::fence(Ordering::SeqCst);
            let root_ts = (*self.tree).get_unchecked(0).load();
            atomic::fence(Ordering::SeqCst);

            let (_, id) = unpack_u128(root_ts);

            let final_val = if id == TS_P_EMPTY {
                DEQ_OPS_EMPTY
            } else {
                let h = (*self.head).get_unchecked(id).max_read();
                pack_u128(h, id)
            };

            // Try to write the result with strong CAS
            if deq_op_node
                .compare_exchange(DEQ_OPS_INIT, final_val)
                .is_ok()
            {
                atomic::fence(Ordering::SeqCst);
                self.op_version.fetch_add(1, Ordering::SeqCst);
                break;
            }

            // STRICTER: Spin a bit before retrying
            for _ in 0..10 {
                std::hint::spin_loop();
            }
        }
    }

    unsafe fn update_tree(&self, num: usize) {
        atomic::fence(Ordering::SeqCst); // STRICTER: Start with fence

        if num >= 100_000 {
            return;
        }

        let op = (*self.deq_ops).get_unchecked(num).load();
        if op == DEQ_OPS_INIT {
            return;
        }

        let (j, id) = unpack_u128(op);

        if id != DEQ_ID_INIT && id != DEQ_ID_EMPTY && id < self.num_processes {
            (*self.head).get_unchecked(id).max_write(j + 1);
            atomic::fence(Ordering::SeqCst); // STRICTER: Fence after head update

            // STRICTER: Multiple propagations with fences
            for _ in 0..2 {
                self.propagate(id);
                atomic::fence(Ordering::SeqCst);
            }
        }
    }

    // ---------- enqueue / dequeue -------------------------------------------
    pub fn enqueue(&self, pid: usize, x: T) -> Result<(), ()> {
        if pid >= self.num_processes {
            return Err(());
        }
        unsafe {
            atomic::fence(Ordering::SeqCst); // STRICTER: Start with fence

            let t = (*self.tail).get_unchecked(pid).load(Ordering::SeqCst);
            if t >= self.items_per_process {
                return Err(());
            }

            let st = self.enq_counter.max_read();
            let timestamp = pack_u128(st, pid);
            let item = (*self.items).get_unchecked(pid * self.items_per_process + t);

            // Write value and timestamp with barriers
            *item.val.get() = Some(x);
            atomic::fence(Ordering::SeqCst); // STRICTER: Fence between writes
            item.timestamp.store(timestamp);
            atomic::fence(Ordering::SeqCst); // STRICTER: Fence before publish

            // Publish by incrementing tail
            (*self.tail)
                .get_unchecked(pid)
                .store(t + 1, Ordering::SeqCst); // STRICTER: SeqCst store
            atomic::fence(Ordering::SeqCst); // STRICTER: Fence after publish

            // Update global counter and propagate
            self.enq_counter.max_write(st + 1);
            atomic::fence(Ordering::SeqCst);

            // STRICTER: Multiple propagations
            for _ in 0..3 {
                self.propagate(pid);
                atomic::fence(Ordering::SeqCst);
            }

            Ok(())
        }
    }

    pub fn dequeue(&self, _tid: usize) -> Result<T, ()> {
        unsafe {
            atomic::fence(Ordering::SeqCst); // STRICTER: Start with fence

            let num = self.deq_counter.fetch_inc();

            // STRICTER: More aggressive helping with larger range
            let help_start = if num > self.num_dequeuers * 2 {
                num - self.num_dequeuers * 2
            } else {
                1
            };

            // Execute helping in order with fences
            for i in help_start..=num {
                if i >= 100_000 {
                    break;
                }

                atomic::fence(Ordering::SeqCst);
                let op_val = (*self.deq_ops).get_unchecked(i).load();
                if op_val == DEQ_OPS_INIT {
                    // Update tree for previous operation first
                    if i > 1 {
                        self.update_tree(i - 1);
                        atomic::fence(Ordering::SeqCst);
                    }
                    self.finish_deq(i);
                    atomic::fence(Ordering::SeqCst);
                }
            }

            // STRICTER: Wait for our operation with retries
            let mut retry_count = 0;
            let mut op = (*self.deq_ops).get_unchecked(num).load();

            while op == DEQ_OPS_INIT && retry_count < 1000 {
                atomic::fence(Ordering::SeqCst);

                // Help more aggressively
                for i in 1..=num.min(99999) {
                    let op_val = (*self.deq_ops).get_unchecked(i).load();
                    if op_val == DEQ_OPS_INIT {
                        if i > 1 {
                            self.update_tree(i - 1);
                        }
                        self.finish_deq(i);
                    }
                }

                atomic::fence(Ordering::SeqCst);
                op = (*self.deq_ops).get_unchecked(num).load();
                retry_count += 1;

                // STRICTER: Exponential backoff
                for _ in 0..(1 << retry_count.min(8)) {
                    std::hint::spin_loop();
                }
            }

            let (j, id) = unpack_u128(op);

            if id == DEQ_ID_EMPTY {
                return Err(());
            }

            if id == DEQ_ID_INIT || id >= self.num_processes || j >= self.items_per_process {
                return Err(());
            }

            // STRICTER: Take the value with full synchronization
            atomic::fence(Ordering::SeqCst);
            let item = (*self.items).get_unchecked(id * self.items_per_process + j);
            let result = (*item.val.get()).take();
            atomic::fence(Ordering::SeqCst);

            // CRITICAL: Update the tree for THIS operation with multiple attempts
            for _ in 0..3 {
                self.update_tree(num);
                atomic::fence(Ordering::SeqCst);
            }

            result.ok_or(())
        }
    }

    // ---------- observers ----------------------------------------------------
    pub fn is_empty(&self) -> bool {
        unsafe {
            atomic::fence(Ordering::SeqCst);
            // Check if any sub-queue has items
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

    /// Force a complete synchronization of the queue state
    pub fn force_sync(&self) {
        unsafe {
            // STRICTER: Multiple rounds of synchronization
            for round in 0..5 {
                atomic::fence(Ordering::SeqCst);

                // First ensure all tree updates are applied
                let max_deq = self.deq_counter.v.load(Ordering::SeqCst);
                for i in 1..=max_deq.min(99999) {
                    self.update_tree(i);
                    if i % 100 == 0 {
                        atomic::fence(Ordering::SeqCst);
                    }
                }

                atomic::fence(Ordering::SeqCst);

                // Then propagate all trees multiple times
                for _ in 0..3 {
                    for p in 0..self.num_processes {
                        self.propagate(p);
                    }
                    atomic::fence(Ordering::SeqCst);
                }

                // STRICTER: Add delay between rounds
                if round < 4 {
                    std::thread::sleep(std::time::Duration::from_micros(10));
                }
            }

            atomic::fence(Ordering::SeqCst);
        }
    }

    /// Get the total number of items currently in the queue
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

    /// Ensure all pending dequeue operations are completed
    pub fn finalize_pending_dequeues(&self) {
        unsafe {
            // STRICTER: Multiple rounds with increasing aggressiveness
            for round in 0..5 {
                atomic::fence(Ordering::SeqCst);

                let max_deq = self.deq_counter.v.load(Ordering::SeqCst);

                // First pass: complete all pending operations
                for i in 1..=max_deq.min(99999) {
                    atomic::fence(Ordering::SeqCst);
                    let op_val = (*self.deq_ops).get_unchecked(i).load();

                    // If operation is still pending, complete it
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
                for i in 1..=max_deq.min(99999) {
                    atomic::fence(Ordering::SeqCst);
                    let op_val = (*self.deq_ops).get_unchecked(i).load();
                    if op_val != DEQ_OPS_INIT && op_val != DEQ_OPS_EMPTY {
                        let (j, id) = unpack_u128(op_val);
                        if id < self.num_processes {
                            // Ensure head is updated
                            (*self.head).get_unchecked(id).max_write(j + 1);
                            atomic::fence(Ordering::SeqCst);
                            // Propagate the change multiple times
                            for _ in 0..3 {
                                self.propagate(id);
                                atomic::fence(Ordering::SeqCst);
                            }
                        }
                    }
                }

                // Force complete propagation
                self.force_sync();

                // STRICTER: Add delay between rounds
                if round < 4 {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
    }

    // Debug helper
    pub fn debug_state(&self) {
        unsafe {
            println!("=== JKM Queue Debug State ===");
            println!("Enq counter: {}", self.enq_counter.max_read());
            let deq_count = self.deq_counter.v.load(Ordering::SeqCst);
            println!("Deq counter: {}", deq_count);

            for p in 0..self.num_processes {
                let h = (*self.head).get_unchecked(p).max_read();
                let t = (*self.tail).get_unchecked(p).load(Ordering::SeqCst);
                println!(
                    "Process {}: head={}, tail={}, items={}",
                    p,
                    h,
                    t,
                    t.saturating_sub(h)
                );
            }

            println!("Tree root: {:?}", (*self.tree).get_unchecked(0).load());
            println!("Total items in queue: {}", self.total_items());

            // Check for assigned but not taken items
            let mut assigned_not_taken = 0;
            for i in 1..deq_count.min(100) {
                let op = (*self.deq_ops).get_unchecked(i).load();
                if op != DEQ_OPS_INIT && op != DEQ_OPS_EMPTY {
                    let (j, id) = unpack_u128(op);
                    if id < self.num_processes && j < self.items_per_process {
                        let item = (*self.items).get_unchecked(id * self.items_per_process + j);
                        if (*item.val.get()).is_some() {
                            assigned_not_taken += 1;
                            println!(
                                "  Deq op {}: assigned but not taken from items[{}][{}]",
                                i, id, j
                            );
                        }
                    }
                }
            }
            if assigned_not_taken > 0 {
                println!(
                    "WARNING: {} items assigned but not taken!",
                    assigned_not_taken
                );
            }
        }
    }

    /// Force complete synchronization - used in benchmarks  
    pub fn force_complete_sync(&self) {
        self.finalize_pending_dequeues();
    }
}

// ---------- trait glue -------------------------------------------------------
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
