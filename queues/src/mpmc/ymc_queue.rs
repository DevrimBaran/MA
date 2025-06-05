use std::alloc::{self, Layout};
use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr::{self, null_mut};
use std::sync::atomic::{fence, AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::MpmcQueue;

const SEGMENT_SIZE: usize = 1024;
const PATIENCE: usize = 3;
const CACHE_LINE_SIZE: usize = 64;

// Change BOTTOM to a value that won't conflict with actual data
// Using a high bit pattern that's unlikely to be a valid data value
const BOTTOM: usize = usize::MAX - 2; // Changed from 0
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

        let items_per_thread = 250_000;
        let max_items = items_per_thread * num_threads * 3;
        let num_segments = std::cmp::min((max_items + SEGMENT_SIZE - 1) / SEGMENT_SIZE, 8192);

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
        self.advance_end(&self.t, cid + 1);
        (*c).val.store(v, Ordering::Release);
    }

    unsafe fn help_enq(&self, handle: *mut Handle, c: *mut Cell, i: u64) -> Result<usize, ()> {
        // First, check if the cell already has a value (not BOTTOM or TOP)
        let val = (*c).val.load(Ordering::Acquire);
        if val != BOTTOM && val != TOP {
            return Ok(val);
        }

        // Try to mark the cell as unusable (TOP) if it's empty (BOTTOM)
        if let Err(val) =
            (*c).val
                .compare_exchange(BOTTOM, TOP, Ordering::AcqRel, Ordering::Acquire)
        {
            // If the cell already has a value (not BOTTOM or TOP), return it
            if val != TOP {
                return Ok(val);
            }
            // If the cell is already marked TOP, continue to check for enq request
        }

        // At this point, the cell is marked TOP (either by us or someone else)

        // Check if there's an enqueue request in the cell
        let enq_ptr = (*c).enq.load(Ordering::Acquire);
        if enq_ptr.is_null() {
            // No enqueue request yet, try to help peers
            let mut iterations = 0;
            loop {
                iterations += 1;
                if iterations > 2 {
                    break;
                }

                let p = (*handle).enq_peer.load(Ordering::Acquire);
                if p.is_null() {
                    break;
                }

                let peer_req = &(*p).enq_req;
                let (pending, id) = peer_req.get_state();

                let handle_enq_id = (*handle).enq_id.load(Ordering::Acquire);
                if handle_enq_id == 0 || handle_enq_id == id {
                    // Break if the peer's request is not pending or its id is too large
                    if !pending || id > i {
                        (*handle).enq_peer.store((*p).next, Ordering::Release);
                        (*handle).enq_id.store(0, Ordering::Release);
                        break;
                    }

                    // Try to reserve the cell for the peer's request
                    match (*c).enq.compare_exchange(
                        null_mut(),
                        peer_req as *const EnqReq as *mut EnqReq,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // Successfully reserved, move to the next peer
                            (*handle).enq_peer.store((*p).next, Ordering::Release);
                            break;
                        }
                        Err(_) => {
                            // Failed to reserve, remember this peer's request id
                            (*handle).enq_id.store(id, Ordering::Release);
                        }
                    }
                } else {
                    // Current peer has a new request or we've already helped this one
                    (*handle).enq_id.store(0, Ordering::Release);
                    (*handle).enq_peer.store((*p).next, Ordering::Release);
                }
            }

            // If still no enqueue request, mark it as TOP_ENQ
            if (*c).enq.load(Ordering::Acquire).is_null() {
                (*c).enq.store(TOP_ENQ, Ordering::Release);
            }
        }

        // Re-check the enqueue pointer, it might have changed
        let enq_ptr = (*c).enq.load(Ordering::Acquire);

        // If the cell is marked with TOP_ENQ, no value will be enqueued here
        if enq_ptr == TOP_ENQ {
            // Check if we can return EMPTY - only if T ≤ i
            if self.t.load(Ordering::Acquire) <= i {
                return Err(());
            }
            return Ok(TOP);
        }

        // If there's a valid enqueue request, try to help complete it
        if !enq_ptr.is_null() && enq_ptr != TOP_ENQ && enq_ptr != EMPTY_ENQ {
            let req = &*enq_ptr;
            let (pending, req_id) = req.get_state();
            let v = req.val.load(Ordering::Acquire);

            if req_id > i {
                // Request id is too large for this cell, check if we can return EMPTY
                if (*c).val.load(Ordering::Acquire) == TOP && self.t.load(Ordering::Acquire) <= i {
                    return Err(());
                }
            } else {
                // Try to claim the request for this cell
                if req.try_claim(req_id, i)
                    || (!pending && req_id == i && (*c).val.load(Ordering::Acquire) == TOP)
                {
                    // Successfully claimed or already claimed for this cell, commit the value
                    self.enq_commit(c, v, i);
                }
            }
        }

        // Final check of the cell's value
        let final_val = (*c).val.load(Ordering::Acquire);
        if final_val == TOP {
            // Check one more time if we should return EMPTY
            if self.t.load(Ordering::Acquire) <= i {
                return Err(());
            }
            Ok(TOP)
        } else {
            Ok(final_val)
        }
    }

    unsafe fn deq_fast(&self, handle: *mut Handle, id: &mut u64) -> Result<usize, ()> {
        let i = self.h.fetch_add(1, Ordering::AcqRel);
        let c = self.find_cell(&mut (*handle).head.load(Ordering::Acquire), i);

        match self.help_enq(handle, c, i)? {
            TOP => {
                *id = i;
                Ok(TOP)
            }
            v => {
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
        }
    }

    unsafe fn deq_slow(&self, handle: *mut Handle, cid: u64) -> Result<usize, ()> {
        let r = &(*handle).deq_req;
        r.id.store(cid, Ordering::Release);
        r.set_state(true, cid);

        self.help_deq(handle, handle);

        let (pending, idx) = r.get_state();
        // Verify the request completed
        if pending {
            // This shouldn't happen with a properly implemented help_deq
            return Err(());
        }

        let c = self.find_cell(&mut (*handle).head.load(Ordering::Acquire), idx);
        let v = (*c).val.load(Ordering::Acquire);

        self.advance_end(&self.h, idx + 1);

        if v == TOP {
            // Make one final check to ensure the queue is really empty
            if self.t.load(Ordering::Acquire) <= idx {
                return Err(());
            }
            return Ok(TOP); // Let the caller handle this special value
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
            // Re-check that the request is still pending before proceeding
            let (still_pending, _) = r.get_state();
            if !still_pending || r.id.load(Ordering::Acquire) != id {
                (*handle).hazard.store(null_mut(), Ordering::Release);
                return;
            }

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
                    Err(_) => {
                        // Queue is empty
                        cand = i;
                    }
                    Ok(v) if v != TOP && (*c).deq.load(Ordering::Acquire).is_null() => {
                        // Found a value that can be dequeued
                        cand = i;
                    }
                    _ => {}
                }
            }

            if cand != 0 {
                // Try to announce the candidate
                let old_state = (1u64 << 63) | prior;
                let new_state = (1u64 << 63) | cand;
                r.state
                    .compare_exchange(old_state, new_state, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
                let (_, new_idx) = r.get_state();
                prior = new_idx;
            }

            // Re-check that the request is still pending
            let (pending, _) = r.get_state();
            if !pending || r.id.load(Ordering::Acquire) != id {
                (*handle).hazard.store(null_mut(), Ordering::Release);
                return;
            }

            let c = self.find_cell(&mut ha, prior);
            let val = (*c).val.load(Ordering::Acquire);

            // Try to claim this cell for the dequeue request
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
                // Successfully claimed or another helper claimed it
                // Mark the request as complete
                let old_state = (1u64 << 63) | prior;
                let new_state = prior;
                r.state
                    .compare_exchange(old_state, new_state, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
                (*handle).hazard.store(null_mut(), Ordering::Release);
                return;
            }

            // Prepare for next iteration
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

            let mut v = TOP;
            let mut cell_id = 0;
            let mut empty_result = false;

            for _ in 0..PATIENCE {
                match self.deq_fast(handle, &mut cell_id) {
                    Ok(val) if val != TOP && val != BOTTOM => {
                        v = val;
                        break;
                    }
                    Err(_) => {
                        empty_result = true;
                        break;
                    }
                    _ => {}
                }
            }

            if empty_result {
                (*handle).hazard.store(null_mut(), Ordering::Release);
                self.cleanup(handle);
                return Err(());
            }

            if v == TOP || v == BOTTOM {
                match self.deq_slow(handle, cell_id) {
                    Ok(val) if val != TOP && val != BOTTOM => v = val,
                    Ok(TOP) => {
                        // Double-check if queue is really empty
                        if self.is_empty() {
                            (*handle).hazard.store(null_mut(), Ordering::Release);
                            self.cleanup(handle);
                            return Err(());
                        }
                        // Fall through to try again
                    }
                    _ => {
                        (*handle).hazard.store(null_mut(), Ordering::Release);
                        self.cleanup(handle);
                        return Err(());
                    }
                }
            }

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
                    if val != BOTTOM && val != TOP {
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
