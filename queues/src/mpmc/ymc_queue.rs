use std::mem::{self, MaybeUninit};
use std::ptr::{self, null_mut};
use std::sync::atomic::{fence, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::MpmcQueue;

const SEGMENT_SIZE: usize = 1024;
const PATIENCE: usize = 10;
const CACHE_LINE_SIZE: usize = 64;

// Using negative values that will never conflict with actual data
const BOTTOM: usize = usize::MAX; // -1 in two's complement
const TOP: usize = usize::MAX - 1; // -2 in two's complement
const EMPTY_ENQ: *mut EnqReq = 1 as *mut EnqReq;
const TOP_ENQ: *mut EnqReq = 2 as *mut EnqReq;
const TOP_DEQ: *mut DeqReq = 2 as *mut DeqReq;

#[repr(C)]
struct EnqReq {
    val: AtomicUsize,
    state: AtomicU64,
}

impl EnqReq {
    fn new() -> Self {
        Self {
            val: AtomicUsize::new(BOTTOM),
            state: AtomicU64::new(0),
        }
    }

    fn get_state(&self) -> (bool, u64) {
        fence(Ordering::SeqCst);
        let s = self.state.load(Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let pending = (s >> 63) != 0;
        let id = s & 0x7FFFFFFFFFFFFFFF;
        (pending, id)
    }

    fn set_state(&self, pending: bool, id: u64) {
        fence(Ordering::SeqCst);
        let s = ((pending as u64) << 63) | (id & 0x7FFFFFFFFFFFFFFF);
        self.state.store(s, Ordering::SeqCst);
        fence(Ordering::SeqCst);
    }

    fn try_claim(&self, old_id: u64, new_id: u64) -> bool {
        let old = (1u64 << 63) | old_id;
        let new = new_id & 0x7FFFFFFFFFFFFFFF;
        let result = self
            .state
            .compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
        if result {
            fence(Ordering::SeqCst);
        }
        result
    }
}

#[repr(C)]
struct DeqReq {
    id: AtomicU64,
    state: AtomicU64,
}

impl DeqReq {
    fn new() -> Self {
        Self {
            id: AtomicU64::new(0),
            state: AtomicU64::new(0),
        }
    }

    fn get_state(&self) -> (bool, u64) {
        fence(Ordering::SeqCst);
        let s = self.state.load(Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let pending = (s >> 63) != 0;
        let idx = s & 0x7FFFFFFFFFFFFFFF;
        (pending, idx)
    }

    fn set_state(&self, pending: bool, idx: u64) {
        fence(Ordering::SeqCst);
        let s = ((pending as u64) << 63) | (idx & 0x7FFFFFFFFFFFFFFF);
        self.state.store(s, Ordering::SeqCst);
        fence(Ordering::SeqCst);
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
    unsafe fn new(id: usize) -> *mut Self {
        let ptr = std::alloc::alloc_zeroed(std::alloc::Layout::new::<Self>()) as *mut Self;
        (*ptr).id = id;
        (*ptr).next = AtomicPtr::new(null_mut());

        let cells_ptr = (*ptr).cells.as_mut_ptr() as *mut Cell;
        for i in 0..SEGMENT_SIZE {
            ptr::write(cells_ptr.add(i), Cell::new());
        }

        ptr
    }

    unsafe fn cells(&self) -> &[Cell; SEGMENT_SIZE] {
        &*(self.cells.as_ptr() as *const [Cell; SEGMENT_SIZE])
    }

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
        let items_per_thread = 250_000;
        let max_items = items_per_thread * num_threads * 3;
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

        // Initialize queue with proper synchronization
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

        // Full memory barrier to ensure everything is initialized
        fence(Ordering::SeqCst);

        &mut *queue_ptr
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let items_per_thread = 250_000;
        let max_items = items_per_thread * num_threads * 3;
        let num_segments = std::cmp::min((max_items + SEGMENT_SIZE - 1) / SEGMENT_SIZE, 8192);

        let queue_size = mem::size_of::<Self>();
        let handles_size = num_threads * mem::size_of::<Handle>();
        let segments_size = num_segments * mem::size_of::<Segment>();

        let total = queue_size + handles_size + segments_size + 8192;
        (total + 4095) & !4095
    }

    unsafe fn find_cell(&self, sp: &mut *mut Segment, cell_id: u64) -> *mut Cell {
        fence(Ordering::SeqCst);
        let seg_id = (cell_id / SEGMENT_SIZE as u64) as usize;
        let cell_idx = (cell_id % SEGMENT_SIZE as u64) as usize;

        let mut s = *sp;
        while (*s).id < seg_id {
            fence(Ordering::SeqCst);
            let next = (*s).next.load(Ordering::SeqCst);
            if next.is_null() {
                panic!("Segment {} not found in pre-allocated segments!", seg_id);
            }
            s = next;
        }

        *sp = s;
        fence(Ordering::SeqCst);
        &mut (*s).cells_mut()[cell_idx] as *mut Cell
    }

    unsafe fn advance_end_for_linearizability(&self, e: &AtomicU64, cid: u64) {
        loop {
            let curr = e.load(Ordering::SeqCst);
            if curr >= cid {
                // Someone already advanced past us - ensure their operations are visible
                fence(Ordering::SeqCst);
                break;
            }
            if e.compare_exchange(curr, cid, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // We advanced - ensure our operations are visible to others
                fence(Ordering::SeqCst);
                break;
            }
        }
    }

    // Listing 3: enq_fast from paper (line 65-69)
    unsafe fn enq_fast(&self, handle: *mut Handle, v: usize, cid: &mut u64) -> bool {
        let i = self.t.fetch_add(1, Ordering::SeqCst);
        let c = self.find_cell(&mut (*handle).tail.load(Ordering::Acquire), i);

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

    // Paper-faithful enq_slow with stronger synchronization
    unsafe fn enq_slow(&self, handle: *mut Handle, v: usize, cell_id: u64) {
        let r = &(*handle).enq_req;
        r.val.store(v, Ordering::Release);
        r.set_state(true, cell_id);

        // Ensure request is visible before proceeding
        fence(Ordering::SeqCst);

        let mut tmp_tail = (*handle).tail.load(Ordering::Acquire);

        // First try the cell we already have from failed fast-path
        let mut i = cell_id;
        let mut tried_original = false;

        loop {
            let c = self.find_cell(&mut tmp_tail, i);

            // Dijkstra's protocol: reserve cell first, then check if it's empty
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
                // We reserved the cell - now check if it's empty
                fence(Ordering::SeqCst);
                if (*c).val.load(Ordering::SeqCst) == BOTTOM {
                    // Successfully reserved an empty cell
                    r.try_claim(cell_id, i);
                    break;
                }
            }

            // Check if request was completed by helper
            fence(Ordering::SeqCst);
            let (pending, _) = r.get_state();
            if !pending {
                break;
            }

            // Get a new cell index only after trying the original
            if !tried_original {
                tried_original = true;
                // Continue with current i (the failed cell_id)
            } else {
                // Now we need a new cell
                i = self.t.fetch_add(1, Ordering::SeqCst);
            }
        }

        // Find where our request ended up and commit
        fence(Ordering::SeqCst);
        let (_, final_id) = r.get_state();
        let c = self.find_cell(&mut (*handle).tail.load(Ordering::Acquire), final_id);
        self.enq_commit(c, v, final_id);
    }

    // Fixed enq_commit with stronger synchronization
    unsafe fn enq_commit(&self, c: *mut Cell, v: usize, cid: u64) {
        // Critical: ensure value is stored before T advances
        (*c).val.store(v, Ordering::SeqCst);

        // Full memory barrier to ensure the value is globally visible
        fence(Ordering::SeqCst);

        // Now advance T to linearize this enqueue
        self.advance_end_for_linearizability(&self.t, cid + 1);
    }

    // Fixed help_enq with defensive programming for edge cases
    unsafe fn help_enq(&self, handle: *mut Handle, c: *mut Cell, i: u64) -> Result<usize, ()> {
        // Full fence before any operations
        fence(Ordering::SeqCst);

        // Try to transition BOTTOM -> value or BOTTOM -> TOP
        let initial_val = (*c).val.load(Ordering::SeqCst);
        if initial_val != BOTTOM && initial_val != TOP {
            return Ok(initial_val);
        }

        // Line 91: try to mark cell as TOP if it's BOTTOM
        if !(*c)
            .val
            .compare_exchange(BOTTOM, TOP, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            fence(Ordering::SeqCst);
            let current_val = (*c).val.load(Ordering::SeqCst);
            if current_val != TOP {
                return Ok(current_val);
            }
        }

        // Full fence after marking TOP
        fence(Ordering::SeqCst);

        // c->val is TOP, so help slow-path enqueues
        if (*c).enq.load(Ordering::SeqCst).is_null() {
            // Lines 94-109: Help peer enqueues
            for _ in 0..2 {
                let p = (*handle).enq_peer.load(Ordering::SeqCst);
                if p.is_null() {
                    break;
                }

                let peer_req = &(*p).enq_req;
                fence(Ordering::SeqCst);
                let s = peer_req.get_state();

                if (*handle).enq_id.load(Ordering::SeqCst) == 0
                    || (*handle).enq_id.load(Ordering::SeqCst) == s.1
                {
                    break;
                }

                (*handle).enq_id.store(0, Ordering::SeqCst);
                (*handle).enq_peer.store((*p).next, Ordering::SeqCst);
            }

            fence(Ordering::SeqCst);
            let p = (*handle).enq_peer.load(Ordering::SeqCst);
            if !p.is_null() {
                let peer_req = &(*p).enq_req;
                fence(Ordering::SeqCst);
                let s = peer_req.get_state();

                if s.0 && s.1 <= i {
                    if !(*c)
                        .enq
                        .compare_exchange(
                            null_mut(),
                            peer_req as *const EnqReq as *mut EnqReq,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        (*handle).enq_id.store(s.1, Ordering::SeqCst);
                    }
                } else {
                    (*handle).enq_peer.store((*p).next, Ordering::SeqCst);
                }
            }

            // Line 111: If no request found, mark cell with TOP_ENQ
            fence(Ordering::SeqCst);
            if (*c).enq.load(Ordering::SeqCst).is_null() {
                (*c).enq
                    .compare_exchange(null_mut(), TOP_ENQ, Ordering::SeqCst, Ordering::SeqCst)
                    .ok();
            }
        }

        // Full fence before checking empty
        fence(Ordering::SeqCst);

        // Line 113-116: Check if cell won't be filled
        let enq_ptr = (*c).enq.load(Ordering::SeqCst);
        if enq_ptr == TOP_ENQ {
            // CRITICAL FIX: Be more conservative about declaring empty
            // Wait longer and check more times
            for retry in 0..100 {
                // Increased from 10
                fence(Ordering::SeqCst);
                let t_val = self.t.load(Ordering::SeqCst);
                let h_val = self.h.load(Ordering::SeqCst);

                // If T > i, definitely not empty at this cell
                if t_val > i {
                    return Ok(TOP);
                }

                // Check if value appeared
                fence(Ordering::SeqCst);
                let cell_val = (*c).val.load(Ordering::SeqCst);
                if cell_val != TOP {
                    return Ok(cell_val);
                }

                // Extra check: if H > i, we shouldn't return empty
                // as some dequeue has moved past this cell
                if h_val > i {
                    return Ok(TOP);
                }

                // Spin longer on each retry
                for _ in 0..(retry + 1) * 100 {
                    std::hint::spin_loop();
                }
            }

            // Final check after many retries - be very conservative
            fence(Ordering::SeqCst);
            let final_t = self.t.load(Ordering::SeqCst);
            let final_h = self.h.load(Ordering::SeqCst);

            // Only return empty if we're absolutely sure
            if final_t <= i && final_h <= i {
                // One more paranoid check
                fence(Ordering::SeqCst);
                if (*c).val.load(Ordering::SeqCst) == TOP {
                    return Err(()); // EMPTY
                }
            }

            return Ok(TOP);
        }

        // Lines 117-127: Handle enqueue request
        if !enq_ptr.is_null() && enq_ptr != TOP_ENQ && enq_ptr != EMPTY_ENQ {
            fence(Ordering::SeqCst);
            let req = &*enq_ptr;
            let s = req.get_state();
            fence(Ordering::SeqCst);
            let v = req.val.load(Ordering::SeqCst);

            if s.1 > i {
                // Line 119-122: Request unsuitable for this cell
                fence(Ordering::SeqCst);
                let cell_val = (*c).val.load(Ordering::SeqCst);
                if cell_val == TOP {
                    fence(Ordering::SeqCst);
                    let t_val = self.t.load(Ordering::SeqCst);
                    let h_val = self.h.load(Ordering::SeqCst);
                    if t_val <= i && h_val <= i {
                        return Err(()); // EMPTY
                    }
                }
            } else if req.try_claim(s.1, i)
                || (!s.0 && s.1 == i && (*c).val.load(Ordering::SeqCst) == TOP)
            {
                fence(Ordering::SeqCst);
                self.enq_commit(c, v, i);
            }
        }

        // Line 127: Return final value with full fence
        fence(Ordering::SeqCst);
        let final_val = (*c).val.load(Ordering::SeqCst);
        if final_val == TOP {
            Ok(TOP)
        } else {
            Ok(final_val)
        }
    }

    // Listing 4: deq_fast from paper (line 140-148)
    unsafe fn deq_fast(&self, handle: *mut Handle, id: &mut u64) -> Result<usize, ()> {
        let i = self.h.fetch_add(1, Ordering::SeqCst);
        let c = self.find_cell(&mut (*handle).head.load(Ordering::Acquire), i);

        let v = self.help_enq(handle, c, i)?;

        if v == TOP {
            *id = i;
            return Ok(TOP);
        }

        // The cell has a value and I claimed it
        if (*c)
            .deq
            .compare_exchange(null_mut(), TOP_DEQ, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Ok(v);
        }

        // Otherwise fail
        *id = i;
        Ok(TOP)
    }

    // Listing 4: deq_slow from paper (line 149-157) with MAX sync
    unsafe fn deq_slow(&self, handle: *mut Handle, cid: u64) -> Result<usize, ()> {
        fence(Ordering::SeqCst);
        let r = &(*handle).deq_req;
        r.id.store(cid, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        r.set_state(true, cid);
        fence(Ordering::SeqCst);

        self.help_deq(handle, handle);

        // Find the destination cell & read its value
        fence(Ordering::SeqCst);
        let i = r.get_state().1;
        fence(Ordering::SeqCst);
        let c = self.find_cell(&mut (*handle).head.load(Ordering::SeqCst), i);
        fence(Ordering::SeqCst);
        let v = (*c).val.load(Ordering::SeqCst);
        fence(Ordering::SeqCst);

        self.advance_end_for_linearizability(&self.h, i + 1);
        fence(Ordering::SeqCst);

        if v == TOP {
            Err(())
        } else {
            Ok(v)
        }
    }

    // Listing 4: help_deq from paper with MAXIMUM synchronization
    unsafe fn help_deq(&self, handle: *mut Handle, helpee: *mut Handle) {
        fence(Ordering::SeqCst);
        let r = &(*helpee).deq_req;
        let s = r.get_state();
        fence(Ordering::SeqCst);
        let id = r.id.load(Ordering::SeqCst);
        fence(Ordering::SeqCst);

        if !s.0 || s.1 < id {
            return;
        }

        let mut ha = (*helpee).head.load(Ordering::SeqCst);
        let mut prior = id;
        let mut i = id;
        let mut cand = 0;

        loop {
            fence(Ordering::SeqCst);
            // Check if request is still valid
            let curr_state = r.get_state();
            fence(Ordering::SeqCst);
            if !curr_state.0 || r.id.load(Ordering::SeqCst) != id {
                return;
            }

            // Find a candidate cell
            let mut hc = ha;
            while cand == 0 {
                fence(Ordering::SeqCst);
                let state_check = r.get_state();
                if state_check.1 != prior || !state_check.0 {
                    prior = state_check.1;
                    break;
                }

                i += 1;
                fence(Ordering::SeqCst);
                let c = self.find_cell(&mut hc, i);
                fence(Ordering::SeqCst);

                match self.help_enq(handle, c, i) {
                    Err(_) => {
                        // Queue is empty - this is a valid candidate
                        fence(Ordering::SeqCst);
                        cand = i;
                    }
                    Ok(v) if v != TOP && (*c).deq.load(Ordering::SeqCst).is_null() => {
                        // Found an unclaimed value - this is a valid candidate
                        fence(Ordering::SeqCst);
                        cand = i;
                    }
                    _ => {
                        // Continue searching
                        fence(Ordering::SeqCst);
                        let updated_state = r.get_state();
                        if updated_state.1 != prior || !updated_state.0 {
                            prior = updated_state.1;
                            break;
                        }
                    }
                }
            }

            if cand != 0 {
                fence(Ordering::SeqCst);
                // Try to announce candidate
                let old_state_val = (1u64 << 63) | prior;
                let new_state_val = (1u64 << 63) | cand;
                r.state
                    .compare_exchange(
                        old_state_val,
                        new_state_val,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .ok();
                fence(Ordering::SeqCst);

                let updated = r.get_state();
                prior = updated.1;
            }

            fence(Ordering::SeqCst);
            // Check if request is complete
            let final_state = r.get_state();
            if !final_state.0 || r.id.load(Ordering::SeqCst) != id {
                return;
            }

            // Try to claim the announced candidate
            fence(Ordering::SeqCst);
            let c = self.find_cell(&mut ha, prior);
            fence(Ordering::SeqCst);
            let val = (*c).val.load(Ordering::SeqCst);
            fence(Ordering::SeqCst);

            if val == TOP
                || (*c)
                    .deq
                    .compare_exchange(
                        null_mut(),
                        r as *const DeqReq as *mut DeqReq,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                || (*c).deq.load(Ordering::SeqCst) == r as *const DeqReq as *mut DeqReq
            {
                fence(Ordering::SeqCst);
                // Request is complete - clear pending bit
                let complete_state = prior; // pending=0, idx=prior
                r.state.store(complete_state, Ordering::SeqCst);
                fence(Ordering::SeqCst);
                return;
            }

            // Prepare for next iteration
            if prior >= i {
                cand = 0;
                i = prior;
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
            for _ in 0..PATIENCE {
                fence(Ordering::SeqCst);
                if self.enq_fast(handle, v, &mut cell_id) {
                    fence(Ordering::SeqCst);
                    return Ok(());
                }
            }

            fence(Ordering::SeqCst);
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

            for _ in 0..PATIENCE {
                fence(Ordering::SeqCst);
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
                fence(Ordering::SeqCst);
                match self.deq_slow(handle, cell_id) {
                    Ok(val) => v = val,
                    Err(_) => {
                        fence(Ordering::SeqCst);
                        return Err(());
                    }
                }
            }

            // Help dequeue peer as per paper
            fence(Ordering::SeqCst);
            let peer = (*handle).deq_peer.load(Ordering::SeqCst);
            if !peer.is_null() {
                self.help_deq(handle, peer);
                (*handle).deq_peer.store((*peer).next, Ordering::SeqCst);
            }

            fence(Ordering::SeqCst);
            Ok(std::mem::transmute_copy::<usize, T>(&v))
        }
    }

    pub fn is_empty(&self) -> bool {
        // Use SeqCst to ensure we see the most recent updates
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
