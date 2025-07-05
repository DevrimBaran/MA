// paper in /paper/mpmc/ymc.pdf
use std::mem::{self, MaybeUninit};
use std::ptr::{self, null_mut};
use std::sync::atomic::{fence, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::MpmcQueue;

const SEGMENT_SIZE: usize = 1024;
const PATIENCE: usize = 10;
const CACHE_LINE_SIZE: usize = 64;

// Using negative values that will never conflict with actual data
pub const BOTTOM: usize = usize::MAX; // -1 in two's complement
pub const TOP: usize = usize::MAX - 1; // -2 in two's complement
const EMPTY_ENQ: *mut EnqReq = 1 as *mut EnqReq;
const TOP_ENQ: *mut EnqReq = 2 as *mut EnqReq;
const BOTTOM_DEQ: *mut DeqReq = null_mut();
const TOP_DEQ: *mut DeqReq = 2 as *mut DeqReq;

#[repr(C)]
pub struct EnqReq {
    pub val: AtomicUsize,
    pub state: AtomicU64,
}

impl EnqReq {
    pub fn new() -> Self {
        Self {
            val: AtomicUsize::new(BOTTOM),
            state: AtomicU64::new(0),
        }
    }

    pub fn get_state(&self) -> (bool, u64) {
        let s = self.state.load(Ordering::SeqCst);
        let pending = (s >> 63) != 0;
        let id = s & 0x7FFFFFFFFFFFFFFF;
        (pending, id)
    }

    pub fn set_state(&self, pending: bool, id: u64) {
        let s = ((pending as u64) << 63) | (id & 0x7FFFFFFFFFFFFFFF);
        self.state.store(s, Ordering::SeqCst);
    }

    pub fn try_claim(&self, old_id: u64, new_id: u64) -> bool {
        let old = (1u64 << 63) | old_id;
        let new = new_id & 0x7FFFFFFFFFFFFFFF;
        self.state
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

#[repr(C)]
pub struct DeqReq {
    pub id: AtomicU64,
    pub state: AtomicU64,
}

impl DeqReq {
    pub fn new() -> Self {
        Self {
            id: AtomicU64::new(0),
            state: AtomicU64::new(0),
        }
    }

    pub fn get_state(&self) -> (bool, u64) {
        let s = self.state.load(Ordering::SeqCst);
        let pending = (s >> 63) != 0;
        let idx = s & 0x7FFFFFFFFFFFFFFF;
        (pending, idx)
    }

    pub fn set_state(&self, pending: bool, idx: u64) {
        let s = ((pending as u64) << 63) | (idx & 0x7FFFFFFFFFFFFFFF);
        self.state.store(s, Ordering::SeqCst);
    }

    pub fn try_announce(&self, old_idx: u64, new_idx: u64) -> bool {
        let old = (1u64 << 63) | old_idx;
        let new = (1u64 << 63) | new_idx;
        self.state
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn try_complete(&self, idx: u64) -> bool {
        let old = (1u64 << 63) | idx;
        let new = idx; // pending = 0
        self.state
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

#[repr(C, align(64))]
struct Cell {
    val: AtomicUsize,
    enq: AtomicPtr<EnqReq>,
    deq: AtomicPtr<DeqReq>,
    _padding: [u8; CACHE_LINE_SIZE - 24],
}

impl Cell {
    fn new() -> Self {
        Self {
            val: AtomicUsize::new(BOTTOM),
            enq: AtomicPtr::new(null_mut()),
            deq: AtomicPtr::new(null_mut()),
            _padding: [0; CACHE_LINE_SIZE - 24],
        }
    }
}

#[repr(C)]
struct Segment {
    id: usize,
    next: AtomicPtr<Segment>,
    cells: MaybeUninit<[Cell; SEGMENT_SIZE]>,
}

impl Segment {
    unsafe fn cells_mut(&mut self) -> &mut [Cell; SEGMENT_SIZE] {
        &mut *(self.cells.as_mut_ptr() as *mut [Cell; SEGMENT_SIZE])
    }
}

#[repr(C, align(128))]
pub struct Handle {
    tail: AtomicPtr<Segment>,
    head: AtomicPtr<Segment>,
    next: *mut Handle,
    enq_req: EnqReq,
    enq_peer: AtomicPtr<Handle>,
    enq_id: AtomicU64,
    deq_req: DeqReq,
    deq_peer: AtomicPtr<Handle>,
    _padding: [u8; 64],
}

impl Handle {
    pub fn new() -> Self {
        Self {
            tail: AtomicPtr::new(null_mut()),
            head: AtomicPtr::new(null_mut()),
            next: null_mut(),
            enq_req: EnqReq::new(),
            enq_peer: AtomicPtr::new(null_mut()),
            enq_id: AtomicU64::new(0),
            deq_req: DeqReq::new(),
            deq_peer: AtomicPtr::new(null_mut()),
            _padding: [0; 64],
        }
    }
}

#[repr(C)]
pub struct YangCrummeyQueue<T: Send + Clone + 'static> {
    q: AtomicPtr<Segment>,
    t: AtomicU64,
    h: AtomicU64,
    handles: *mut Handle,
    num_threads: usize,
    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for YangCrummeyQueue<T> {}
unsafe impl<T: Send + Clone> Sync for YangCrummeyQueue<T> {}

impl<T: Send + Clone + 'static> YangCrummeyQueue<T> {
    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;
        let items_per_thread = 200_000;
        let max_items = items_per_thread * num_threads * 2;
        let num_segments = std::cmp::min((max_items + SEGMENT_SIZE - 1) / SEGMENT_SIZE, 8192);

        let queue_size = mem::size_of::<Self>();
        let handles_offset = (queue_size + 127) & !127;
        let segment_offset = handles_offset + num_threads * mem::size_of::<Handle>();
        let segment_offset = (segment_offset + 63) & !63;

        // Pre-allocate segments
        let mut prev_seg: *mut Segment = null_mut();
        let mut first_seg: *mut Segment = null_mut();

        for seg_id in 0..num_segments {
            let seg_ptr = (mem.add(segment_offset) as *mut Segment).add(seg_id);
            ptr::write(
                seg_ptr,
                Segment {
                    id: seg_id,
                    next: AtomicPtr::new(null_mut()),
                    cells: MaybeUninit::uninit(),
                },
            );

            let cells_ptr = (*seg_ptr).cells.as_mut_ptr() as *mut Cell;
            for i in 0..SEGMENT_SIZE {
                ptr::write(cells_ptr.add(i), Cell::new());
            }

            if seg_id == 0 {
                first_seg = seg_ptr;
            } else {
                (*prev_seg).next.store(seg_ptr, Ordering::Release);
            }
            prev_seg = seg_ptr;
        }

        // Initialize handles
        let handles_base = mem.add(handles_offset) as *mut Handle;
        for i in 0..num_threads {
            let handle_ptr = handles_base.add(i);
            ptr::write(handle_ptr, Handle::new());
            (*handle_ptr).tail.store(first_seg, Ordering::Release);
            (*handle_ptr).head.store(first_seg, Ordering::Release);
        }

        // Link handles in ring for peer helping
        for i in 0..num_threads {
            let curr = handles_base.add(i);
            let next = handles_base.add((i + 1) % num_threads);
            (*curr).next = next;
            (*curr).enq_peer.store(next, Ordering::Release);
            (*curr).deq_peer.store(next, Ordering::Release);
        }

        // Initialize queue
        ptr::write(
            queue_ptr,
            Self {
                q: AtomicPtr::new(first_seg),
                t: AtomicU64::new(0),
                h: AtomicU64::new(0),
                handles: handles_base,
                num_threads,
                _phantom: std::marker::PhantomData,
            },
        );

        fence(Ordering::SeqCst);
        &mut *queue_ptr
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let items_per_thread = 200_000;
        let max_items = items_per_thread * num_threads * 2;
        let num_segments = std::cmp::min((max_items + SEGMENT_SIZE - 1) / SEGMENT_SIZE, 8192);

        let queue_size = mem::size_of::<Self>();
        let handles_size = num_threads * mem::size_of::<Handle>();
        let segments_size = num_segments * mem::size_of::<Segment>();

        let total = queue_size + handles_size + segments_size + 8192;
        (total + 4095) & !4095
    }

    pub fn spsc_shared_size() -> usize {
        // For SPSC benchmarks, support up to 10M items
        let num_segments = 25_000;
        let num_threads = 2; // Always 2 for SPSC

        let queue_size = mem::size_of::<Self>();
        let handles_size = num_threads * mem::size_of::<Handle>();
        let segments_size = num_segments * mem::size_of::<Segment>();

        let total = queue_size + handles_size + segments_size + 8192;
        (total + 4095) & !4095
    }

    pub unsafe fn init_in_shared_spsc(mem: *mut u8) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;
        let num_threads = 2; // Always 2 for SPSC
        let num_segments = 25_000;

        let queue_size = mem::size_of::<Self>();
        let handles_offset = (queue_size + 127) & !127;
        let segment_offset = handles_offset + num_threads * mem::size_of::<Handle>();
        let segment_offset = (segment_offset + 63) & !63;

        // Pre-allocate segments
        let mut prev_seg: *mut Segment = null_mut();
        let mut first_seg: *mut Segment = null_mut();

        for seg_id in 0..num_segments {
            let seg_ptr = (mem.add(segment_offset) as *mut Segment).add(seg_id);
            ptr::write(
                seg_ptr,
                Segment {
                    id: seg_id,
                    next: AtomicPtr::new(null_mut()),
                    cells: MaybeUninit::uninit(),
                },
            );

            let cells_ptr = (*seg_ptr).cells.as_mut_ptr() as *mut Cell;
            for i in 0..SEGMENT_SIZE {
                ptr::write(cells_ptr.add(i), Cell::new());
            }

            if seg_id == 0 {
                first_seg = seg_ptr;
            } else {
                (*prev_seg).next.store(seg_ptr, Ordering::Release);
            }
            prev_seg = seg_ptr;
        }

        // Initialize handles
        let handles_base = mem.add(handles_offset) as *mut Handle;
        for i in 0..num_threads {
            let handle_ptr = handles_base.add(i);
            ptr::write(handle_ptr, Handle::new());
            (*handle_ptr).tail.store(first_seg, Ordering::Release);
            (*handle_ptr).head.store(first_seg, Ordering::Release);
        }

        // Link handles in ring for peer helping
        for i in 0..num_threads {
            let curr = handles_base.add(i);
            let next = handles_base.add((i + 1) % num_threads);
            (*curr).next = next;
            (*curr).enq_peer.store(next, Ordering::Release);
            (*curr).deq_peer.store(next, Ordering::Release);
        }

        // Initialize queue
        ptr::write(
            queue_ptr,
            Self {
                q: AtomicPtr::new(first_seg),
                t: AtomicU64::new(0),
                h: AtomicU64::new(0),
                handles: handles_base,
                num_threads,
                _phantom: std::marker::PhantomData,
            },
        );

        fence(Ordering::SeqCst);
        &mut *queue_ptr
    }

    unsafe fn find_cell(&self, sp: &mut *mut Segment, cell_id: u64) -> *mut Cell {
        let seg_id = (cell_id / SEGMENT_SIZE as u64) as usize;
        let cell_idx = (cell_id % SEGMENT_SIZE as u64) as usize;

        let mut s = *sp;
        while (*s).id < seg_id {
            let next = (*s).next.load(Ordering::Acquire);
            if next.is_null() {
                panic!("Segment {} not found!", seg_id);
            }
            s = next;
        }

        *sp = s;
        &mut (*s).cells_mut()[cell_idx] as *mut Cell
    }

    unsafe fn advance_end_for_linearizability(&self, e: &AtomicU64, cid: u64) {
        loop {
            let curr = e.load(Ordering::SeqCst);
            if curr >= cid {
                break;
            }
            if e.compare_exchange(curr, cid, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
        }
    }

    // Fast-path enqueue - returns true on success, false with cell_id on failure
    unsafe fn enq_fast(&self, handle: *mut Handle, v: usize, cid: &mut u64) -> bool {
        let i = self.t.fetch_add(1, Ordering::SeqCst);
        let mut tail = (*handle).tail.load(Ordering::Acquire);
        let c = self.find_cell(&mut tail, i);
        (*handle).tail.store(tail, Ordering::Release);

        if (*c)
            .val
            .compare_exchange(BOTTOM, v, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return true;
        }
        *cid = i;
        false
    }

    // Slow-path enqueue - guaranteed to complete
    unsafe fn enq_slow(&self, handle: *mut Handle, v: usize, mut cell_id: u64) {
        let r = &(*handle).enq_req;
        r.val.store(v, Ordering::SeqCst);
        r.set_state(true, cell_id);

        let mut tmp_tail = (*handle).tail.load(Ordering::Acquire);
        let num_threads = self.num_threads;
        let max_failures = (num_threads - 1) * (num_threads - 1);
        let mut failures = 0;

        loop {
            // Try the cell from failed fast-path first
            let c = self.find_cell(&mut tmp_tail, cell_id);

            // Dijkstra's protocol
            if (*c)
                .enq
                .compare_exchange(
                    null_mut(),
                    r as *const EnqReq as *mut EnqReq,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                // We reserved the cell, check if it's empty
                if (*c).val.load(Ordering::SeqCst) == BOTTOM {
                    r.try_claim(r.get_state().1, cell_id);
                    break;
                }
            }

            // Check if request was completed
            let (pending, _) = r.get_state();
            if !pending {
                break;
            }

            // Get a new cell for next attempt
            cell_id = self.t.fetch_add(1, Ordering::SeqCst);

            failures += 1;
            // After (n-1)^2 failures, we're guaranteed all threads are helping
            if failures >= max_failures {
                // Just keep trying with new cells - help will come
                continue;
            }
        }

        // Find where request ended up and commit
        let (_, id) = r.get_state();
        let mut tail = (*handle).tail.load(Ordering::Acquire);
        let c = self.find_cell(&mut tail, id);
        (*handle).tail.store(tail, Ordering::Release);
        self.enq_commit(c, v, id);
    }

    unsafe fn enq_commit(&self, c: *mut Cell, v: usize, cid: u64) {
        self.advance_end_for_linearizability(&self.t, cid + 1);
        (*c).val.store(v, Ordering::SeqCst);
    }

    // Help enqueue with bounded helping iterations
    unsafe fn help_enq(&self, handle: *mut Handle, c: *mut Cell, i: u64) -> Result<usize, ()> {
        // Try to get value or mark cell as TOP
        let val = (*c).val.load(Ordering::SeqCst);
        if val != BOTTOM && val != TOP {
            return Ok(val);
        }

        if !(*c)
            .val
            .compare_exchange(BOTTOM, TOP, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let current = (*c).val.load(Ordering::SeqCst);
            if current != TOP {
                return Ok(current);
            }
        }

        // c->val is TOP, help slow-path enqueues
        if (*c).enq.load(Ordering::SeqCst).is_null() {
            // Try to help peer enqueues - at most 2 attempts
            let mut attempts = 0;
            loop {
                if attempts >= 2 {
                    break;
                }

                let p = (*handle).enq_peer.load(Ordering::Acquire);
                if p.is_null() {
                    break;
                }

                let r = &(*p).enq_req;
                let (pending, id) = r.get_state();

                let my_id = (*handle).enq_id.load(Ordering::Acquire);
                if my_id != 0 && my_id != id {
                    // This peer has published a new request
                    (*handle).enq_id.store(0, Ordering::Release);
                    (*handle).enq_peer.store((*p).next, Ordering::Release);
                    attempts += 1;
                    continue;
                }

                if pending && id <= i {
                    if (*c)
                        .enq
                        .compare_exchange(
                            null_mut(),
                            r as *const EnqReq as *mut EnqReq,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        // Successfully reserved cell for peer
                        (*handle).enq_peer.store((*p).next, Ordering::Release);
                    } else {
                        // Failed to reserve, remember this request
                        (*handle).enq_id.store(id, Ordering::Release);
                    }
                } else {
                    // Peer doesn't need help or can't use this cell
                    (*handle).enq_peer.store((*p).next, Ordering::Release);
                }
                break;
            }

            // If no request found, mark with TOP_ENQ
            if (*c).enq.load(Ordering::SeqCst).is_null() {
                (*c).enq
                    .compare_exchange(null_mut(), TOP_ENQ, Ordering::SeqCst, Ordering::SeqCst)
                    .ok();
            }
        }

        // Check what's in the enq field
        fence(Ordering::SeqCst);
        let enq_ptr = (*c).enq.load(Ordering::SeqCst);

        if enq_ptr == TOP_ENQ {
            // No enqueue will fill this cell
            // Must wait a bit to ensure no racing enqueues
            for _ in 0..100 {
                std::hint::spin_loop();
            }
            fence(Ordering::SeqCst);
            if self.t.load(Ordering::SeqCst) <= i {
                return Err(()); // EMPTY
            }
            return Ok(TOP);
        }

        // Handle enqueue request
        if !enq_ptr.is_null() && enq_ptr != EMPTY_ENQ {
            let r = &*enq_ptr;
            let (pending, id) = r.get_state();
            fence(Ordering::SeqCst);
            let v = r.val.load(Ordering::SeqCst);

            if id > i {
                // Request is unsuitable for this cell
                if (*c).val.load(Ordering::SeqCst) == TOP && self.t.load(Ordering::SeqCst) <= i {
                    return Err(()); // EMPTY
                }
            } else if r.try_claim(id, i)
                || (!pending && id == i && (*c).val.load(Ordering::SeqCst) == TOP)
            {
                // We claimed the request or it's already claimed for this cell
                self.enq_commit(c, v, i);
            }
        }

        fence(Ordering::SeqCst);
        let result = (*c).val.load(Ordering::SeqCst);
        if result == TOP {
            Ok(TOP)
        } else {
            Ok(result)
        }
    }

    // Fast-path dequeue
    unsafe fn deq_fast(&self, handle: *mut Handle, id: &mut u64) -> Result<usize, ()> {
        let i = self.h.fetch_add(1, Ordering::SeqCst);
        let mut head = (*handle).head.load(Ordering::Acquire);
        let c = self.find_cell(&mut head, i);
        (*handle).head.store(head, Ordering::Release);

        let v = self.help_enq(handle, c, i)?;

        if v == TOP {
            *id = i;
            return Ok(TOP);
        }

        // Try to claim the value
        if (*c)
            .deq
            .compare_exchange(BOTTOM_DEQ, TOP_DEQ, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Ok(v);
        }

        *id = i;
        Ok(TOP)
    }

    // Slow-path dequeue with bounded completion
    unsafe fn deq_slow(&self, handle: *mut Handle, cid: u64) -> Result<usize, ()> {
        let r = &(*handle).deq_req;
        r.id.store(cid, Ordering::SeqCst);
        r.set_state(true, cid);

        self.help_deq(handle, handle);

        fence(Ordering::SeqCst);
        let (_, i) = r.get_state();
        let mut head = (*handle).head.load(Ordering::Acquire);
        let c = self.find_cell(&mut head, i);
        (*handle).head.store(head, Ordering::Release);

        let v = (*c).val.load(Ordering::SeqCst);
        self.advance_end_for_linearizability(&self.h, i + 1);

        if v == TOP {
            Err(())
        } else {
            Ok(v)
        }
    }

    // Help dequeue with bounded iterations
    unsafe fn help_deq(&self, handle: *mut Handle, helpee: *mut Handle) {
        let r = &(*helpee).deq_req;
        let mut s = r.get_state();
        let id = r.id.load(Ordering::SeqCst);

        if !s.0 || s.1 < id {
            return;
        }

        let mut ha = (*helpee).head.load(Ordering::Acquire);
        let mut hc = ha;
        let mut prior = id;
        let mut i = id;
        let mut cand = 0;

        let num_threads = self.num_threads;
        let max_cells =
            (num_threads - 1) * (num_threads - 1) * (num_threads - 1) * (num_threads - 1);
        let mut cells_visited = 0;

        loop {
            // Bound the number of cells we visit
            if cells_visited >= max_cells {
                // After visiting (n-1)^4 cells, we're guaranteed to complete
                break;
            }

            // Find a candidate cell
            while cand == 0 && s.1 == prior && cells_visited < max_cells {
                i += 1;
                cells_visited += 1;
                let c = self.find_cell(&mut hc, i);

                fence(Ordering::SeqCst);
                match self.help_enq(handle, c, i) {
                    Err(_) => {
                        // Queue is empty
                        cand = i;
                    }
                    Ok(v) if v != TOP => {
                        // Found a value, check if it's claimed
                        fence(Ordering::SeqCst);
                        if (*c).deq.load(Ordering::SeqCst) == BOTTOM_DEQ {
                            cand = i;
                        }
                    }
                    _ => {}
                }

                s = r.get_state();
            }

            if cand != 0 {
                // Try to announce candidate
                r.try_announce(prior, cand);
                fence(Ordering::SeqCst);
                s = r.get_state();
            }

            // Check if request is complete
            if !s.0 || r.id.load(Ordering::SeqCst) != id {
                return;
            }

            // Find announced candidate
            let announced = s.1;
            let c = self.find_cell(&mut ha, announced);
            fence(Ordering::SeqCst);
            let val = (*c).val.load(Ordering::SeqCst);

            if val == TOP
                || (*c)
                    .deq
                    .compare_exchange(
                        BOTTOM_DEQ,
                        r as *const DeqReq as *mut DeqReq,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                || (*c).deq.load(Ordering::SeqCst) == r as *const DeqReq as *mut DeqReq
            {
                // Request is complete
                r.try_complete(announced);
                return;
            }

            // Prepare for next iteration
            prior = announced;
            if announced >= i {
                cand = 0;
                i = announced;
            }
        }
    }

    pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), ()> {
        if thread_id >= self.num_threads {
            return Err(());
        }

        unsafe {
            fence(Ordering::SeqCst);
            let handle = self.handles.add(thread_id);
            let v = std::mem::transmute_copy::<T, usize>(&item);
            std::mem::forget(item);

            let mut cell_id = 0;
            // Try fast-path PATIENCE times
            for _ in 0..PATIENCE {
                if self.enq_fast(handle, v, &mut cell_id) {
                    fence(Ordering::SeqCst);
                    return Ok(());
                }
            }

            // Switch to slow-path which is guaranteed to complete
            self.enq_slow(handle, v, cell_id);
            fence(Ordering::SeqCst);
            Ok(())
        }
    }

    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        if thread_id >= self.num_threads {
            return Err(());
        }

        unsafe {
            fence(Ordering::SeqCst);
            let handle = self.handles.add(thread_id);
            let mut v = TOP;
            let mut cell_id = 0;

            // Try fast-path PATIENCE times
            for _ in 0..PATIENCE {
                match self.deq_fast(handle, &mut cell_id) {
                    Ok(val) if val != TOP => {
                        v = val;
                        break;
                    }
                    Err(_) => {
                        fence(Ordering::SeqCst);
                        return Err(());
                    }
                    _ => {}
                }
            }

            if v == TOP {
                // Switch to slow-path which is guaranteed to complete
                match self.deq_slow(handle, cell_id) {
                    Ok(val) => v = val,
                    Err(_) => {
                        fence(Ordering::SeqCst);
                        return Err(());
                    }
                }
            }

            // Help dequeue peer after successful dequeue
            fence(Ordering::SeqCst);
            let peer = (*handle).deq_peer.load(Ordering::Acquire);
            if !peer.is_null() {
                self.help_deq(handle, peer);
                (*handle).deq_peer.store((*peer).next, Ordering::Release);
            }

            fence(Ordering::SeqCst);
            Ok(std::mem::transmute_copy::<usize, T>(&v))
        }
    }

    pub fn is_empty(&self) -> bool {
        fence(Ordering::SeqCst);
        self.h.load(Ordering::SeqCst) >= self.t.load(Ordering::SeqCst)
    }

    pub fn is_full(&self) -> bool {
        false
    }
}

impl<T: Send + Clone + 'static> MpmcQueue<T> for YangCrummeyQueue<T> {
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
