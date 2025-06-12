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
        self.v.load(Ordering::Acquire)
    }
    fn max_write(&self, val: usize) {
        let mut cur = self.v.load(Ordering::Relaxed);
        while val > cur {
            match self
                .v
                .compare_exchange_weak(cur, val, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(x) => cur = x,
            }
        }
    }
}

#[repr(C, align(64))]
struct FetchAndInc {
    v: AtomicUsize,
}
impl FetchAndInc {
    fn new(x: usize) -> Self {
        Self {
            v: AtomicUsize::new(x),
        }
    }
    fn fetch_inc(&self) -> usize {
        self.v.fetch_add(1, Ordering::AcqRel)
    }
}

// ---------- main queue ------------------------------------------------------
#[repr(C)]
pub struct JKMQueue<T: Send + Clone + 'static> {
    enq_counter: MaxRegister,
    deq_counter: FetchAndInc,

    head: *const [MaxRegister],
    tail: *const [AtomicUsize],
    items: *const [QueueItem<T>],
    tree: *const [AtomicCell<u128>],
    deq_ops: *const [AtomicCell<u128>],

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
    fn layout(n_enq: usize) -> (Layout, [usize; 5]) {
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
        let (l_final, o_ops) = l_tree
            .extend(Layout::array::<AtomicCell<u128>>(deq_ops_sz).unwrap())
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
                head: ptr::slice_from_raw_parts(head_ptr, n_enq),
                tail: ptr::slice_from_raw_parts(tail_ptr, n_enq),
                items: ptr::slice_from_raw_parts(items_ptr, n_enq * ipp),
                tree: ptr::slice_from_raw_parts(tree_ptr, tree_sz),
                deq_ops: ptr::slice_from_raw_parts(ops_ptr, deq_ops_sz),
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
        let node = (*self.tree).get_unchecked(idx);
        let old = node.load();

        let new = if is_leaf {
            let p = idx - ((self.tree_size + 1) / 2 - 1);
            let h = (*self.head).get_unchecked(p).max_read();
            let t = (*self.tail).get_unchecked(p).load(Ordering::Acquire);

            if h == t {
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
            node.compare_exchange(old, new).is_ok()
        }
    }

    unsafe fn propagate(&self, p: usize) {
        let mut idx = self.leaf_for(p);

        // Refresh leaf twice as per paper
        self.refresh(idx, true);
        self.refresh(idx, true);

        // Propagate up the tree
        while idx > 0 {
            idx = Self::parent(idx);
            self.refresh(idx, false);
            self.refresh(idx, false);
        }
    }

    unsafe fn finish_deq(&self, num: usize) {
        let deq_op_node = (*self.deq_ops).get_unchecked(num);

        // Use CAS loop to ensure atomic update
        loop {
            let current = deq_op_node.load();
            if current != DEQ_OPS_INIT {
                return; // Already completed by another thread
            }

            // Refresh tree to get current state
            if self.tree_size == 1 {
                // Single node tree - it's both root and leaf
                self.refresh(0, true);
            } else {
                // Multi-node tree - refresh from leaves up
                for p in 0..self.num_processes {
                    let leaf_idx = self.leaf_for(p);
                    self.refresh(leaf_idx, true);
                }

                // Propagate to ensure root is current
                for p in 0..self.num_processes {
                    let mut idx = self.leaf_for(p);
                    while idx > 0 {
                        idx = Self::parent(idx);
                        self.refresh(idx, false);
                    }
                }
            }

            let root_ts = (*self.tree).get_unchecked(0).load();
            let (_, id) = unpack_u128(root_ts);

            let final_val = if id == TS_P_EMPTY {
                DEQ_OPS_EMPTY
            } else {
                let h = (*self.head).get_unchecked(id).max_read();
                pack_u128(h, id)
            };

            // Try to update - if it fails, another thread completed it
            match deq_op_node.compare_exchange(DEQ_OPS_INIT, final_val) {
                Ok(_) => return,
                Err(actual) if actual != DEQ_OPS_INIT => return, // Completed by another thread
                Err(_) => continue,                              // Retry
            }
        }
    }

    unsafe fn update_tree(&self, num: usize) {
        if num == 0 {
            return; // No operation 0 to update
        }

        let op = (*self.deq_ops).get_unchecked(num).load();
        let (j, id) = unpack_u128(op);

        // Update tree even for empty dequeue to maintain consistency
        if id != DEQ_ID_INIT {
            if id != DEQ_ID_EMPTY {
                // Normal case - advance head
                (*self.head).get_unchecked(id).max_write(j + 1);
                self.propagate(id);
            }
            // For empty dequeue, tree should already reflect empty state
        }
    }

    // ---------- enqueue / dequeue -------------------------------------------
    pub fn enqueue(&self, pid: usize, x: T) -> Result<(), ()> {
        if pid >= self.num_processes {
            return Err(());
        }
        unsafe {
            let t = (*self.tail).get_unchecked(pid).load(Ordering::Relaxed);
            if t >= self.items_per_process {
                return Err(());
            }

            let st = self.enq_counter.max_read();
            let timestamp = pack_u128(st, pid);
            let item = (*self.items).get_unchecked(pid * self.items_per_process + t);

            // Write value and timestamp
            *item.val.get() = Some(x);
            item.timestamp.store(timestamp);

            // Publish by incrementing tail
            (*self.tail)
                .get_unchecked(pid)
                .store(t + 1, Ordering::Release);

            // Update global counter and propagate
            self.enq_counter.max_write(st + 1);
            self.propagate(pid);

            Ok(())
        }
    }

    pub fn dequeue(&self, _tid: usize) -> Result<T, ()> {
        unsafe {
            let num = self.deq_counter.fetch_inc();

            // Calculate helping range
            let start = if num > self.num_dequeuers {
                num.saturating_sub(self.num_dequeuers - 1)
            } else {
                1
            };

            // Help pending operations
            for i in start..=num {
                let op_val = (*self.deq_ops).get_unchecked(i).load();
                if op_val == DEQ_OPS_INIT {
                    if i > 1 {
                        self.update_tree(i - 1);
                    }
                    self.finish_deq(i);
                }
            }

            // Wait for our operation to complete
            let mut retry_count = 0;
            loop {
                let op = (*self.deq_ops).get_unchecked(num).load();
                if op != DEQ_OPS_INIT {
                    let (j, id) = unpack_u128(op);

                    if id == DEQ_ID_EMPTY {
                        return Err(());
                    }

                    if id == DEQ_ID_INIT {
                        // This shouldn't happen after helping
                        if retry_count > 100 {
                            return Err(());
                        }
                        retry_count += 1;
                        self.finish_deq(num);
                        std::hint::spin_loop();
                        continue;
                    }

                    // Bounds check
                    if id >= self.num_processes || j >= self.items_per_process {
                        return Err(());
                    }

                    // Get the item
                    let item = (*self.items).get_unchecked(id * self.items_per_process + j);

                    // Take the value
                    match (*item.val.get()).take() {
                        Some(val) => return Ok(val),
                        None => {
                            // Value was already taken - this indicates a bug
                            return Err(());
                        }
                    }
                }

                retry_count += 1;
                if retry_count > 10000 {
                    // Something is wrong - our operation should have been completed
                    return Err(());
                }
                std::hint::spin_loop();
            }
        }
    }

    // ---------- observers ----------------------------------------------------
    pub fn is_empty(&self) -> bool {
        unsafe {
            // Refresh entire tree from leaves up
            for p in 0..self.num_processes {
                let leaf_idx = self.leaf_for(p);
                self.refresh(leaf_idx, true);
            }

            // Propagate changes up
            for p in 0..self.num_processes {
                let mut idx = self.leaf_for(p);
                while idx > 0 {
                    idx = Self::parent(idx);
                    self.refresh(idx, false);
                }
            }

            (*self.tree).get_unchecked(0).load() == TIMESTAMP_EMPTY
        }
    }

    pub fn is_full(&self) -> bool {
        false
    }

    // Debug helper
    pub fn debug_state(&self) {
        unsafe {
            println!("=== JKM Queue Debug State ===");
            println!("Enq counter: {}", self.enq_counter.max_read());
            println!(
                "Deq counter: {}",
                self.deq_counter.v.load(Ordering::Acquire)
            );

            for p in 0..self.num_processes {
                let h = (*self.head).get_unchecked(p).max_read();
                let t = (*self.tail).get_unchecked(p).load(Ordering::Acquire);
                println!(
                    "Process {}: head={}, tail={}, items={}",
                    p,
                    h,
                    t,
                    t.saturating_sub(h)
                );
            }

            println!("Tree root: {:?}", (*self.tree).get_unchecked(0).load());
        }
    }
}

// ---------- trait glue -------------------------------------------------------
impl<T: Send + Clone + 'static> MpmcQueue<T> for JKMQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, v: T, id: usize) -> Result<(), Self::PushError> {
        // Ensure producer ID is in valid range
        if id < self.num_processes {
            self.enqueue(id, v)
        } else {
            Err(())
        }
    }
    fn pop(&self, id: usize) -> Result<T, Self::PopError> {
        // Dequeue doesn't actually use the thread ID in JKM algorithm
        self.dequeue(id)
    }
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
    fn is_full(&self) -> bool {
        self.is_full()
    }
}
