use std::alloc::{self, Layout};
use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr::{self, null_mut};
use std::sync::atomic::{fence, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::MpmcQueue;

const SEGMENT_SIZE: usize = 1024;
const PATIENCE: usize = 3;
const CACHE_LINE_SIZE: usize = 64;

const BOTTOM: usize = 0;
const TOP: usize = usize::MAX;
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
        let s = self.state.load(Ordering::Acquire);
        let pending = (s >> 63) != 0;
        let id = s & 0x7FFFFFFFFFFFFFFF;
        (pending, id)
    }

    fn set_state(&self, pending: bool, id: u64) {
        let s = ((pending as u64) << 63) | (id & 0x7FFFFFFFFFFFFFFF);
        self.state.store(s, Ordering::Release);
    }

    fn try_claim(&self, old_id: u64, new_id: u64) -> bool {
        let old = (1u64 << 63) | old_id;
        let new = new_id & 0x7FFFFFFFFFFFFFFF;
        self.state
            .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
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
        let s = self.state.load(Ordering::Acquire);
        let pending = (s >> 63) != 0;
        let idx = s & 0x7FFFFFFFFFFFFFFF;
        (pending, idx)
    }

    fn set_state(&self, pending: bool, idx: u64) {
        let s = ((pending as u64) << 63) | (idx & 0x7FFFFFFFFFFFFFFF);
        self.state.store(s, Ordering::Release);
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
        let layout = Layout::new::<Self>();
        let ptr = alloc::alloc_zeroed(layout) as *mut Self;
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }

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
    hazard: AtomicPtr<Segment>,
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
            hazard: AtomicPtr::new(null_mut()),
            _padding: [0; 64],
        }
    }
}

#[repr(C)]
pub struct YangCrummeyQueue<T: Send + Clone + 'static> {
    q: AtomicPtr<Segment>,
    t: AtomicU64,
    h: AtomicU64,
    i: AtomicUsize,
    handles: UnsafeCell<Vec<*mut Handle>>,
    is_shared_memory: bool,
    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for YangCrummeyQueue<T> {}
unsafe impl<T: Send + Clone> Sync for YangCrummeyQueue<T> {}

impl<T: Send + Clone + 'static> YangCrummeyQueue<T> {
    pub fn new(num_threads: usize) -> Self {
        unsafe {
            let seg0 = Segment::new(0);

            let mut handles = Vec::with_capacity(num_threads);
            for _ in 0..num_threads {
                let handle = Box::into_raw(Box::new(Handle::new()));
                (*handle).head.store(seg0, Ordering::Relaxed);
                (*handle).tail.store(seg0, Ordering::Relaxed);
                handles.push(handle);
            }

            for i in 0..num_threads {
                let curr = handles[i];
                let next = handles[(i + 1) % num_threads];
                (*curr).next = next;
                (*curr).enq_peer.store(next, Ordering::Relaxed);
                (*curr).deq_peer.store(next, Ordering::Relaxed);
            }

            Self {
                q: AtomicPtr::new(seg0),
                t: AtomicU64::new(0),
                h: AtomicU64::new(0),
                i: AtomicUsize::new(0),
                handles: UnsafeCell::new(handles),
                is_shared_memory: false,
                _phantom: std::marker::PhantomData,
            }
        }
    }

    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Increase items per thread and segments to handle scanning
        let items_per_thread = 300_000; // Increased from 100_000
        let max_items = items_per_thread * num_threads * 4; // 4x safety factor
        let num_segments = std::cmp::min((max_items + SEGMENT_SIZE - 1) / SEGMENT_SIZE, 16384); // Increased from 8192

        let queue_size = mem::size_of::<Self>();
        let handles_offset = (queue_size + 127) & !127;
        let segment_offset = handles_offset + num_threads * mem::size_of::<Handle>();
        let segment_offset = (segment_offset + 63) & !63;

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
                (*prev_seg).next.store(seg_ptr, Ordering::Relaxed);
            }
            prev_seg = seg_ptr;
        }

        let mut handles = Vec::with_capacity(num_threads);
        let handles_base = mem.add(handles_offset) as *mut Handle;

        for i in 0..num_threads {
            let handle_ptr = handles_base.add(i);
            ptr::write(handle_ptr, Handle::new());
            (*handle_ptr).tail.store(first_seg, Ordering::Relaxed);
            (*handle_ptr).head.store(first_seg, Ordering::Relaxed);
            handles.push(handle_ptr);
        }

        for i in 0..num_threads {
            let curr = handles[i];
            let next = handles[(i + 1) % num_threads];
            (*curr).next = next;
            (*curr).enq_peer.store(next, Ordering::Relaxed);
            (*curr).deq_peer.store(next, Ordering::Relaxed);
        }

        ptr::write(
            queue_ptr,
            Self {
                q: AtomicPtr::new(first_seg),
                t: AtomicU64::new(0),
                h: AtomicU64::new(0),
                i: AtomicUsize::new(0),
                handles: UnsafeCell::new(handles),
                is_shared_memory: true,
                _phantom: std::marker::PhantomData,
            },
        );

        &mut *queue_ptr
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let items_per_thread = 300_000;
        let max_items = items_per_thread * num_threads * 4;
        let num_segments = std::cmp::min((max_items + SEGMENT_SIZE - 1) / SEGMENT_SIZE, 16384);

        let queue_size = mem::size_of::<Self>();
        let handles_size = num_threads * mem::size_of::<Handle>();
        let segments_size = num_segments * mem::size_of::<Segment>();

        let total = queue_size + handles_size + segments_size + 8192;
        (total + 4095) & !4095
    }

    unsafe fn find_cell(&self, sp: &mut *mut Segment, cell_id: u64) -> *mut Cell {
        let seg_id = (cell_id / SEGMENT_SIZE as u64) as usize;
        let cell_idx = (cell_id % SEGMENT_SIZE as u64) as usize;

        let mut s = *sp;

        while (*s).id < seg_id {
            let next = (*s).next.load(Ordering::Acquire);
            if next.is_null() {
                if self.is_shared_memory {
                    panic!("Segment {} not found in pre-allocated segments!", seg_id);
                }

                let new_seg = Segment::new((*s).id + 1);
                match (*s).next.compare_exchange(
                    null_mut(),
                    new_seg,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        s = new_seg;
                    }
                    Err(_) => {
                        alloc::dealloc(new_seg as *mut u8, Layout::new::<Segment>());
                        s = (*s).next.load(Ordering::Acquire);
                    }
                }
            } else {
                s = next;
            }
        }

        *sp = s;
        &mut (*s).cells_mut()[cell_idx] as *mut Cell
    }

    unsafe fn advance_end(&self, e: &AtomicU64, cid: u64) {
        loop {
            let curr = e.load(Ordering::Acquire);
            if curr >= cid {
                break;
            }
            if e.compare_exchange(curr, cid, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }

    unsafe fn enq_fast(&self, handle: *mut Handle, v: usize, cid: &mut u64) -> bool {
        let i = self.t.fetch_add(1, Ordering::AcqRel);
        let c = self.find_cell(&mut (*handle).tail.load(Ordering::Acquire), i);

        match (*c)
            .val
            .compare_exchange(BOTTOM, v, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => true,
            Err(_) => {
                *cid = i;
                false
            }
        }
    }

    unsafe fn enq_slow(&self, handle: *mut Handle, v: usize, cell_id: u64) {
        let r = &(*handle).enq_req;
        r.val.store(v, Ordering::Release);
        r.set_state(true, cell_id);

        let mut tmp_tail = (*handle).tail.load(Ordering::Acquire);

        loop {
            let i = self.t.fetch_add(1, Ordering::AcqRel);
            let c = self.find_cell(&mut tmp_tail, i);

            let enq_ptr = (*c).enq.compare_exchange(
                null_mut(),
                r as *const EnqReq as *mut EnqReq,
                Ordering::AcqRel,
                Ordering::Acquire,
            );

            if enq_ptr.is_ok() && (*c).val.load(Ordering::Acquire) == BOTTOM {
                r.try_claim(cell_id, i);
                break;
            }

            let (pending, _) = r.get_state();
            if !pending {
                break;
            }
        }

        let (_, id) = r.get_state();
        let c = self.find_cell(&mut (*handle).tail.load(Ordering::Acquire), id);
        self.enq_commit(c, v, id);
    }

    unsafe fn enq_commit(&self, c: *mut Cell, v: usize, cid: u64) {
        assert!(
            v != TOP && v != BOTTOM,
            "Invalid value in enq_commit: {}",
            v
        );
        assert!(v < 1000000000, "Suspicious value in enq_commit: {}", v);

        self.advance_end(&self.t, cid + 1);
        fence(Ordering::SeqCst);

        (*c).val.store(v, Ordering::SeqCst);
        fence(Ordering::SeqCst);

        // Verify the write succeeded
        let stored = (*c).val.load(Ordering::SeqCst);
        assert_eq!(
            stored, v,
            "enq_commit failed: expected {}, got {}",
            v, stored
        );
    }

    unsafe fn help_enq(&self, handle: *mut Handle, c: *mut Cell, i: u64) -> Result<usize, ()> {
        // Strongest possible synchronization
        fence(Ordering::SeqCst);

        let val = (*c).val.load(Ordering::SeqCst);
        if val != BOTTOM && val != TOP {
            assert!(val < 1000000000, "Invalid value in cell: {}", val); // Sanity check
            return Ok(val);
        }

        match (*c)
            .val
            .compare_exchange(BOTTOM, TOP, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => {
                fence(Ordering::SeqCst);

                // Help enqueue with all synchronization
                if (*c).enq.load(Ordering::SeqCst).is_null() {
                    // Try harder to find an enqueue to help
                    for _ in 0..5 {
                        let p = (*handle).enq_peer.load(Ordering::SeqCst);
                        if p.is_null() {
                            break;
                        }

                        let peer_req = &(*p).enq_req;
                        let (pending, id) = peer_req.get_state();

                        if pending && id <= i {
                            if (*c)
                                .enq
                                .compare_exchange(
                                    null_mut(),
                                    peer_req as *const EnqReq as *mut EnqReq,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                )
                                .is_ok()
                            {
                                break;
                            }
                        }

                        (*handle).enq_peer.store((*p).next, Ordering::SeqCst);
                        fence(Ordering::SeqCst);
                    }

                    if (*c).enq.load(Ordering::SeqCst).is_null() {
                        (*c).enq.store(TOP_ENQ, Ordering::SeqCst);
                    }
                }

                fence(Ordering::SeqCst);
                let enq_ptr = (*c).enq.load(Ordering::SeqCst);

                if enq_ptr == TOP_ENQ {
                    fence(Ordering::SeqCst);
                    let t_final = self.t.load(Ordering::SeqCst);
                    let h_final = self.h.load(Ordering::SeqCst);

                    if t_final <= i && h_final >= t_final {
                        return Err(());
                    }
                    return Ok(TOP);
                }

                if !enq_ptr.is_null() && enq_ptr != TOP_ENQ && enq_ptr != EMPTY_ENQ {
                    let req = &*enq_ptr;
                    let (pending, req_id) = req.get_state();
                    let v = req.val.load(Ordering::SeqCst);

                    if v != BOTTOM && v != TOP && req_id <= i {
                        if pending {
                            req.try_claim(req_id, i);
                        }
                        // Force write the value
                        (*c).val.store(v, Ordering::SeqCst);
                        fence(Ordering::SeqCst);

                        // Verify it was written
                        let check = (*c).val.load(Ordering::SeqCst);
                        assert_eq!(
                            check, v,
                            "Value write failed: expected {}, got {}",
                            v, check
                        );
                    }
                }

                fence(Ordering::SeqCst);
                let final_val = (*c).val.load(Ordering::SeqCst);
                Ok(final_val)
            }
            Err(v) if v != TOP && v != BOTTOM => {
                assert!(v < 1000000000, "Invalid value from CAS: {}", v);
                Ok(v)
            }
            Err(_) => Ok(TOP),
        }
    }

    unsafe fn deq_fast(&self, handle: *mut Handle, id: &mut u64) -> Result<usize, ()> {
        let i = self.h.fetch_add(1, Ordering::AcqRel);
        let c = self.find_cell(&mut (*handle).head.load(Ordering::Acquire), i);

        match self.help_enq(handle, c, i) {
            Ok(TOP) => {
                *id = i;
                Ok(TOP)
            }
            Ok(v) => {
                let deq_ptr = (*c).deq.compare_exchange(
                    null_mut(),
                    TOP_DEQ,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );

                if deq_ptr.is_ok() {
                    Ok(v)
                } else {
                    *id = i;
                    Ok(TOP)
                }
            }
            Err(_) => {
                // Don't propagate error immediately - let slow path handle it
                *id = i;
                Ok(TOP) // Return TOP to indicate we should try slow path
            }
        }
    }

    unsafe fn deq_slow(&self, handle: *mut Handle, cid: u64) -> Result<usize, ()> {
        let r = &(*handle).deq_req;
        r.id.store(cid, Ordering::Release);
        r.set_state(true, cid);

        self.help_deq(handle, handle);

        let (_, idx) = r.get_state();
        let c = self.find_cell(&mut (*handle).head.load(Ordering::Acquire), idx);
        let v = (*c).val.load(Ordering::Acquire);

        self.advance_end(&self.h, idx + 1);

        if v == TOP {
            Err(())
        } else {
            Ok(v)
        }
    }

    unsafe fn help_deq(&self, handle: *mut Handle, helpee: *mut Handle) {
        let r = &(*helpee).deq_req;
        let (pending, idx) = r.get_state();
        let id = r.id.load(Ordering::Acquire);

        if !pending || idx < id {
            return;
        }

        let mut ha = (*helpee).head.load(Ordering::Acquire);
        (*handle).hazard.store(ha, Ordering::Release);
        fence(Ordering::SeqCst);

        let (_, mut prior) = r.get_state();
        let mut i = id;
        let mut cand = 0;

        loop {
            let mut hc = ha;
            while cand == 0 {
                let (p, curr_idx) = r.get_state();
                if curr_idx != prior || !p {
                    prior = curr_idx;
                    break;
                }

                i += 1;
                let c = self.find_cell(&mut hc, i);

                match self.help_enq(handle, c, i) {
                    Err(_) => cand = i,
                    Ok(v) if v != TOP && (*c).deq.load(Ordering::Acquire).is_null() => {
                        cand = i;
                    }
                    _ => {}
                }
            }

            if cand != 0 {
                let old_state = (1u64 << 63) | prior;
                let new_state = (1u64 << 63) | cand;
                r.state
                    .compare_exchange(old_state, new_state, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
                let (_, new_idx) = r.get_state();
                prior = new_idx;
            }

            let (pending, _) = r.get_state();
            if !pending || r.id.load(Ordering::Acquire) != id {
                (*handle).hazard.store(null_mut(), Ordering::Release);
                return;
            }

            let c = self.find_cell(&mut ha, prior);
            let val = (*c).val.load(Ordering::Acquire);

            if val == TOP
                || (*c)
                    .deq
                    .compare_exchange(
                        null_mut(),
                        r as *const DeqReq as *mut DeqReq,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                || (*c).deq.load(Ordering::Acquire) == r as *const DeqReq as *mut DeqReq
            {
                let old_state = (1u64 << 63) | prior;
                let new_state = prior;
                r.state
                    .compare_exchange(old_state, new_state, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
                (*handle).hazard.store(null_mut(), Ordering::Release);
                return;
            }

            if prior >= i {
                cand = 0;
                i = prior;
            }
        }
    }

    unsafe fn cleanup(&self, _handle: *mut Handle) {
        if self.is_shared_memory {
            return;
        }
    }

    pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), ()> {
        unsafe {
            let handles = &*self.handles.get();
            if thread_id >= handles.len() {
                return Err(());
            }

            let handle = handles[thread_id];
            (*handle)
                .hazard
                .store((*handle).tail.load(Ordering::Acquire), Ordering::Release);

            let v = std::mem::transmute_copy::<T, usize>(&item);
            std::mem::forget(item);

            let mut cell_id = 0;
            for _ in 0..PATIENCE {
                if self.enq_fast(handle, v, &mut cell_id) {
                    (*handle).hazard.store(null_mut(), Ordering::Release);
                    return Ok(());
                }
            }

            self.enq_slow(handle, v, cell_id);
            (*handle).hazard.store(null_mut(), Ordering::Release);
            Ok(())
        }
    }

    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            let handles = &*self.handles.get();
            if thread_id >= handles.len() {
                return Err(());
            }

            let handle = handles[thread_id];
            (*handle)
                .hazard
                .store((*handle).head.load(Ordering::Acquire), Ordering::Release);

            // Full memory barrier to ensure all prior operations are visible
            fence(Ordering::SeqCst);

            let mut v = TOP;
            let mut cell_id = 0;
            let mut total_attempts = 0;

            // Keep trying until we get a value or truly empty
            loop {
                total_attempts += 1;

                // Fast path attempts with synchronization
                for attempt in 0..PATIENCE {
                    fence(Ordering::SeqCst); // Ensure we see all enqueues

                    match self.deq_fast(handle, &mut cell_id) {
                        Ok(val) if val != TOP && val != BOTTOM => {
                            // Verify the value is valid
                            assert!(
                                val != TOP && val != BOTTOM,
                                "Fast path returned invalid value: {}",
                                val
                            );
                            v = val;
                            break;
                        }
                        Err(_) => {
                            // Yield to let enqueues complete
                            if attempt > 0 {
                                std::thread::yield_now();
                            }
                            fence(Ordering::SeqCst);
                        }
                        _ => {}
                    }
                }

                if v != TOP && v != BOTTOM {
                    break; // Got a value
                }

                // Try slow path with full synchronization
                fence(Ordering::SeqCst);

                match self.deq_slow(handle, cell_id) {
                    Ok(val) if val != TOP && val != BOTTOM => {
                        assert!(
                            val != TOP && val != BOTTOM,
                            "Slow path returned invalid value: {}",
                            val
                        );
                        v = val;
                        break;
                    }
                    _ => {
                        // Full synchronization before checking state
                        fence(Ordering::SeqCst);
                        std::thread::yield_now();
                        fence(Ordering::SeqCst);

                        let h_now = self.h.load(Ordering::SeqCst); // Use SeqCst
                        let t_now = self.t.load(Ordering::SeqCst); // Use SeqCst

                        if h_now >= t_now {
                            // Really empty
                            if total_attempts > 5 {
                                break;
                            }
                        } else {
                            // There MUST be elements, find them
                            v = self.force_find_value(handle, h_now, t_now);
                            if v != TOP && v != BOTTOM {
                                break;
                            }
                        }
                    }
                }

                // Don't spin too long
                if total_attempts > 20 {
                    break;
                }

                // Aggressive yielding
                std::thread::yield_now();
                fence(Ordering::SeqCst);
            }

            if v == TOP || v == BOTTOM {
                // Final check with strongest ordering
                fence(Ordering::SeqCst);
                let final_h = self.h.load(Ordering::SeqCst);
                let final_t = self.t.load(Ordering::SeqCst);

                assert!(
                    final_h >= final_t || (final_t - final_h) < 1000000,
                    "Queue state inconsistent: h={}, t={}",
                    final_h,
                    final_t
                );

                (*handle).hazard.store(null_mut(), Ordering::Release);
                self.cleanup(handle);
                return Err(());
            }

            // Verify we got a valid value
            assert!(
                v != TOP && v != BOTTOM,
                "About to return invalid value: {}",
                v
            );

            // Help peer dequeue
            let peer = (*handle).deq_peer.load(Ordering::Acquire);
            if !peer.is_null() {
                self.help_deq(handle, peer);
                (*handle).deq_peer.store((*peer).next, Ordering::Release);
            }

            (*handle).hazard.store(null_mut(), Ordering::Release);
            self.cleanup(handle);

            Ok(std::mem::transmute_copy::<usize, T>(&v))
        }
    }

    unsafe fn force_find_value(&self, handle: *mut Handle, start: u64, end: u64) -> usize {
        // Try up to 50 cells
        let scan_limit = 50;
        let scan_end = start.saturating_add(scan_limit).min(end);

        for i in start..scan_end {
            // Check segment bounds
            let seg_id = (i / SEGMENT_SIZE as u64) as usize;
            if seg_id >= 16384 {
                break;
            }

            let mut seg = (*handle).head.load(Ordering::Acquire);
            let c = self.find_cell(&mut seg, i);

            // Strongest synchronization
            fence(Ordering::SeqCst);

            // First force-help any pending enqueue
            let enq_ptr = (*c).enq.load(Ordering::SeqCst);
            if !enq_ptr.is_null() && enq_ptr != TOP_ENQ && enq_ptr != EMPTY_ENQ {
                let req = &*enq_ptr;
                let (pending, req_id) = req.get_state();

                if pending && req_id <= i {
                    let v = req.val.load(Ordering::SeqCst);
                    if v != BOTTOM && v != TOP {
                        // Force the enqueue to complete
                        req.try_claim(req_id, i);
                        (*c).val.store(v, Ordering::SeqCst);
                        fence(Ordering::SeqCst);
                    }
                }
            }

            // Check value with strongest ordering
            fence(Ordering::SeqCst);
            let val = (*c).val.load(Ordering::SeqCst);

            if val != BOTTOM && val != TOP {
                // Check if we can claim it
                let deq_ptr = (*c).deq.load(Ordering::SeqCst);
                if deq_ptr.is_null() {
                    if (*c)
                        .deq
                        .compare_exchange(null_mut(), TOP_DEQ, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        // Force H forward
                        loop {
                            let current_h = self.h.load(Ordering::SeqCst);
                            if current_h > i {
                                break;
                            }
                            if self
                                .h
                                .compare_exchange(
                                    current_h,
                                    i + 1,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                )
                                .is_ok()
                            {
                                break;
                            }
                        }

                        assert!(
                            val != TOP && val != BOTTOM,
                            "force_find_value found invalid value: {}",
                            val
                        );
                        return val;
                    }
                }
            }
        }

        TOP
    }

    unsafe fn comprehensive_scan(&self, handle: *mut Handle, start: u64, end: u64) -> usize {
        // Limit scan range to prevent segment overflow
        let scan_limit = 100; // Only scan 100 cells ahead
        let scan_end = start.saturating_add(scan_limit).min(end);

        for i in start..scan_end {
            let mut seg = (*handle).head.load(Ordering::Acquire);

            // Check if we're about to exceed segment bounds
            let seg_id = (i / SEGMENT_SIZE as u64) as usize;
            if seg_id >= 16384 {
                // Max segments
                break;
            }

            let c = self.find_cell(&mut seg, i);

            // Full fence to ensure we see all updates
            fence(Ordering::SeqCst);

            // First, help any pending enqueue
            let enq_ptr = (*c).enq.load(Ordering::Acquire);
            if !enq_ptr.is_null() && enq_ptr != TOP_ENQ && enq_ptr != EMPTY_ENQ {
                let req = &*enq_ptr;
                let (pending, req_id) = req.get_state();
                let v = req.val.load(Ordering::Acquire);

                if pending && req_id <= i && v != BOTTOM && v != TOP {
                    // Help complete the enqueue
                    if req.try_claim(req_id, i) {
                        (*c).val.store(v, Ordering::Release);
                        fence(Ordering::SeqCst);
                    }
                }
            }

            // Now check the value
            let val = (*c).val.load(Ordering::Acquire);
            if val != BOTTOM && val != TOP {
                // Check if already claimed
                let deq_ptr = (*c).deq.load(Ordering::Acquire);
                if deq_ptr.is_null() {
                    // Try to claim it
                    if (*c)
                        .deq
                        .compare_exchange(null_mut(), TOP_DEQ, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        // Update H to reflect this dequeue
                        loop {
                            let current_h = self.h.load(Ordering::Acquire);
                            if current_h > i {
                                break;
                            }
                            if self
                                .h
                                .compare_exchange(
                                    current_h,
                                    i + 1,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_ok()
                            {
                                break;
                            }
                        }
                        return val;
                    }
                }
            }
        }

        TOP
    }

    unsafe fn help_pending_enqueues(&self, handle: *mut Handle) {
        let h = self.h.load(Ordering::Acquire);
        let t = self.t.load(Ordering::Acquire);

        // Help complete enqueues for cells between h and t
        for i in h..h.saturating_add(10).min(t) {
            let mut seg = (*handle).head.load(Ordering::Acquire);
            let c = self.find_cell(&mut seg, i);

            // Try to help any pending enqueue at this cell
            let enq_ptr = (*c).enq.load(Ordering::Acquire);
            if !enq_ptr.is_null() && enq_ptr != TOP_ENQ && enq_ptr != EMPTY_ENQ {
                let req = &*enq_ptr;
                let (pending, req_id) = req.get_state();
                if pending && req_id <= i {
                    let v = req.val.load(Ordering::Acquire);
                    if v != BOTTOM && v != TOP {
                        (*c).val
                            .compare_exchange(TOP, v, Ordering::AcqRel, Ordering::Acquire)
                            .ok();
                    }
                }
            }
        }
    }

    unsafe fn scan_for_value(&self, handle: *mut Handle) -> usize {
        let h = self.h.load(Ordering::Acquire);
        let t = self.t.load(Ordering::Acquire);

        // Scan a range of cells for any available value
        for i in h..t.min(h + 100) {
            let mut seg = (*handle).head.load(Ordering::Acquire);
            let c = self.find_cell(&mut seg, i);

            let val = (*c).val.load(Ordering::Acquire);
            if val != BOTTOM && val != TOP {
                // Try to claim this value
                if (*c)
                    .deq
                    .compare_exchange(null_mut(), TOP_DEQ, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    // Successfully claimed, advance head
                    self.h
                        .compare_exchange(i, i + 1, Ordering::AcqRel, Ordering::Acquire)
                        .ok();
                    return val;
                }
            }
        }

        TOP
    }

    pub fn is_empty(&self) -> bool {
        self.h.load(Ordering::Acquire) >= self.t.load(Ordering::Acquire)
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

impl<T: Send + Clone> Drop for YangCrummeyQueue<T> {
    fn drop(&mut self) {
        if self.is_shared_memory {
            return;
        }

        unsafe {
            let mut current = self.q.load(Ordering::Relaxed);
            while !current.is_null() {
                let next = (*current).next.load(Ordering::Relaxed);

                let cells = (*current).cells();
                for i in 0..SEGMENT_SIZE {
                    let val = cells[i].val.load(Ordering::Relaxed);
                    if val != BOTTOM && val != TOP && val != 0 {
                        drop(Box::from_raw(val as *mut T));
                    }
                }

                alloc::dealloc(current as *mut u8, Layout::new::<Segment>());
                current = next;
            }

            let handles = &*self.handles.get();
            for &handle in handles.iter() {
                if !handle.is_null() {
                    drop(Box::from_raw(handle));
                }
            }
        }
    }
}
