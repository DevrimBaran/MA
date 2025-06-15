use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{
    fence, AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};

use crate::MpmcQueue;

// Constants from the paper
const MAX_PATIENCE: usize = 16;
const MAX_PATIENCE_DEQ: usize = 64;
const HELP_DELAY: usize = 8;
const FIN_BIT: u64 = 1 << 63;
const INC_BIT: u64 = 1 << 62;
const COUNTER_MASK: u64 = (1 << 62) - 1;

// Special index values - must fit in 30 bits
const IDX_BOTTOM: usize = 0x3FFFFFFE; // ⊥c
const IDX_EMPTY: usize = 0x3FFFFFFD; // ⊥

#[repr(C)]
#[derive(Clone, Copy)]
struct Entry {
    cycle: u32,
    is_safe: bool,
    enq: bool,
    index: usize,
}

impl Entry {
    const fn new() -> Self {
        Self {
            cycle: 0,
            is_safe: true,
            enq: true,
            index: IDX_EMPTY,
        }
    }
}

#[repr(C)]
struct EntryPair {
    note: AtomicU32,
    value: AtomicU64, // Packed Entry
}

impl EntryPair {
    const fn new() -> Self {
        Self {
            note: AtomicU32::new(u32::MAX),
            value: AtomicU64::new(0),
        }
    }

    const fn pack_entry(e: Entry) -> u64 {
        let mut packed = 0u64;
        packed |= (e.cycle as u64) << 32;
        packed |= (e.is_safe as u64) << 31;
        packed |= (e.enq as u64) << 30;
        packed |= (e.index as u64) & 0x3FFFFFFF;
        packed
    }

    const fn unpack_entry(packed: u64) -> Entry {
        Entry {
            cycle: (packed >> 32) as u32,
            is_safe: ((packed >> 31) & 1) != 0,
            enq: ((packed >> 30) & 1) != 0,
            index: (packed & 0x3FFFFFFF) as usize,
        }
    }
}

// Enhanced data entry with ready flag
#[repr(C)]
struct DataEntry<T> {
    value: UnsafeCell<Option<T>>,
    ready: AtomicBool, // Indicates data is ready to be consumed
}

impl<T> DataEntry<T> {
    const fn new() -> Self {
        Self {
            value: UnsafeCell::new(None),
            ready: AtomicBool::new(false),
        }
    }
}

#[repr(C)]
struct Phase2Rec {
    seq1: AtomicU32,
    local: AtomicU64,
    cnt: AtomicU64,
    seq2: AtomicU32,
}

impl Phase2Rec {
    const fn new() -> Self {
        Self {
            seq1: AtomicU32::new(1),
            local: AtomicU64::new(0),
            cnt: AtomicU64::new(0),
            seq2: AtomicU32::new(0),
        }
    }
}

#[repr(C)]
struct ThreadRecord {
    // Private fields
    next_check: AtomicUsize,
    next_tid: AtomicUsize,

    // Shared fields
    phase2: Phase2Rec,
    seq1: AtomicU32,
    enqueue: AtomicBool,
    pending: AtomicBool,
    local_tail: AtomicU64,
    init_tail: AtomicU64,
    local_head: AtomicU64,
    init_head: AtomicU64,
    index: AtomicUsize,
    seq2: AtomicU32,
    queue_id: AtomicUsize,
}

impl ThreadRecord {
    const fn new() -> Self {
        Self {
            next_check: AtomicUsize::new(HELP_DELAY),
            next_tid: AtomicUsize::new(0),
            phase2: Phase2Rec::new(),
            seq1: AtomicU32::new(1),
            enqueue: AtomicBool::new(false),
            pending: AtomicBool::new(false),
            local_tail: AtomicU64::new(0),
            init_tail: AtomicU64::new(0),
            local_head: AtomicU64::new(0),
            init_head: AtomicU64::new(0),
            index: AtomicUsize::new(0),
            seq2: AtomicU32::new(0),
            queue_id: AtomicUsize::new(0),
        }
    }
}

#[repr(C)]
struct GlobalPair {
    cnt: AtomicU64,
    ptr: AtomicU64,
}

#[repr(C)]
struct InnerWCQ {
    threshold: AtomicI32,
    tail: GlobalPair,
    head: GlobalPair,
    ring_size: usize,
    capacity: usize,
}

impl InnerWCQ {
    const fn new(ring_size: usize) -> Self {
        let capacity = ring_size * 2;
        Self {
            threshold: AtomicI32::new(-1),
            tail: GlobalPair {
                cnt: AtomicU64::new(capacity as u64),
                ptr: AtomicU64::new(0),
            },
            head: GlobalPair {
                cnt: AtomicU64::new(capacity as u64),
                ptr: AtomicU64::new(0),
            },
            ring_size,
            capacity,
        }
    }
}

#[repr(C)]
pub struct WCQueue<T: Send + Clone + 'static> {
    aq: InnerWCQ,
    fq: InnerWCQ,

    aq_entries_offset: usize,
    fq_entries_offset: usize,
    records_offset: usize,
    data_offset: usize,

    num_threads: usize,
    num_indices: usize,

    // Track total enqueued/dequeued for debugging
    total_enqueued: AtomicUsize,
    total_dequeued: AtomicUsize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for WCQueue<T> {}
unsafe impl<T: Send + Clone> Sync for WCQueue<T> {}

impl<T: Send + Clone + 'static> WCQueue<T> {
    #[inline]
    fn cache_remap(pos: usize, capacity: usize) -> usize {
        pos % capacity
    }

    unsafe fn get_entry(&self, _wq: &InnerWCQ, entries_offset: usize, idx: usize) -> &EntryPair {
        let base = self as *const _ as *const u8;
        let entries = base.add(entries_offset) as *const EntryPair;
        &*entries.add(idx)
    }

    unsafe fn get_entry_mut(
        &self,
        _wq: &InnerWCQ,
        entries_offset: usize,
        idx: usize,
    ) -> &mut EntryPair {
        let base = self as *const _ as *mut u8;
        let entries = base.add(entries_offset) as *mut EntryPair;
        &mut *entries.add(idx)
    }

    unsafe fn get_data(&self, idx: usize) -> &DataEntry<T> {
        let base = self as *const _ as *const u8;
        let data = base.add(self.data_offset) as *const DataEntry<T>;
        &*data.add(idx % self.num_indices)
    }

    unsafe fn get_record(&self, tid: usize) -> &ThreadRecord {
        let base = self as *const _ as *const u8;
        let records = base.add(self.records_offset) as *const ThreadRecord;
        &*records.add(tid % self.num_threads)
    }

    unsafe fn get_record_mut(&self, tid: usize) -> &mut ThreadRecord {
        let base = self as *const _ as *mut u8;
        let records = base.add(self.records_offset) as *mut ThreadRecord;
        &mut *records.add(tid % self.num_threads)
    }

    fn cycle(val: u64, ring_size: usize) -> u32 {
        (val / ring_size as u64) as u32
    }

    unsafe fn try_enq_inner(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        index: usize,
    ) -> Result<(), u64> {
        let tail = wq.tail.cnt.fetch_add(1, Ordering::SeqCst);
        let j = Self::cache_remap(tail as usize, wq.capacity);

        let entry = self.get_entry_mut(wq, entries_offset, j);
        let packed = entry.value.load(Ordering::SeqCst);
        let e = EntryPair::unpack_entry(packed);

        if e.cycle < Self::cycle(tail, wq.ring_size)
            && (e.is_safe || wq.head.cnt.load(Ordering::SeqCst) <= tail)
            && (e.index == IDX_EMPTY || e.index == IDX_BOTTOM)
        {
            let new_entry = Entry {
                cycle: Self::cycle(tail, wq.ring_size),
                is_safe: true,
                enq: true,
                index,
            };

            entry
                .value
                .store(EntryPair::pack_entry(new_entry), Ordering::SeqCst);

            if wq.threshold.load(Ordering::SeqCst) != (3 * wq.ring_size as i32 - 1) {
                wq.threshold
                    .store(3 * wq.ring_size as i32 - 1, Ordering::SeqCst);
            }

            Ok(())
        } else {
            Err(tail)
        }
    }

    unsafe fn try_deq_inner(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        index_out: &mut usize,
    ) -> Result<(), u64> {
        let head = wq.head.cnt.fetch_add(1, Ordering::SeqCst);
        let j = Self::cache_remap(head as usize, wq.capacity);

        let entry = self.get_entry_mut(wq, entries_offset, j);
        let packed = entry.value.load(Ordering::SeqCst);
        let e = EntryPair::unpack_entry(packed);

        if e.cycle == Self::cycle(head, wq.ring_size) {
            self.consume_inner(wq, entries_offset, head, j, &e);
            *index_out = e.index;
            return Ok(());
        }

        let mut new_val = Entry {
            cycle: e.cycle,
            is_safe: false,
            enq: e.enq,
            index: e.index,
        };

        if e.index == IDX_EMPTY || e.index == IDX_BOTTOM {
            new_val = Entry {
                cycle: Self::cycle(head, wq.ring_size),
                is_safe: e.is_safe,
                enq: true,
                index: IDX_EMPTY,
            };
        }

        if e.cycle < Self::cycle(head, wq.ring_size) {
            entry
                .value
                .store(EntryPair::pack_entry(new_val), Ordering::SeqCst);
        }

        let tail = wq.tail.cnt.load(Ordering::SeqCst);
        if tail <= head + 1 {
            self.catchup_inner(wq, tail, head + 1);

            if entries_offset == self.aq_entries_offset {
                wq.threshold.fetch_sub(1, Ordering::SeqCst);
            }

            *index_out = usize::MAX;
            return Ok(());
        }

        if entries_offset == self.aq_entries_offset {
            if wq.threshold.fetch_sub(1, Ordering::SeqCst) <= 0 {
                *index_out = usize::MAX;
                return Ok(());
            }
        }

        Err(head)
    }

    unsafe fn consume_inner(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        h: u64,
        j: usize,
        e: &Entry,
    ) {
        if !e.enq {
            self.finalize_request_inner(h);
        }

        let entry = self.get_entry_mut(wq, entries_offset, j);
        let packed = entry.value.load(Ordering::SeqCst);
        let mut e_mut = EntryPair::unpack_entry(packed);
        e_mut.index = IDX_BOTTOM;
        entry
            .value
            .store(EntryPair::pack_entry(e_mut), Ordering::SeqCst);
    }

    unsafe fn finalize_request_inner(&self, h: u64) {
        for i in 0..self.num_threads {
            let tid = (i + 1) % self.num_threads;
            let record = self.get_record(tid);

            let tail = record.local_tail.load(Ordering::SeqCst);
            if (tail & COUNTER_MASK) == h {
                record
                    .local_tail
                    .compare_exchange(tail, tail | FIN_BIT, Ordering::SeqCst, Ordering::SeqCst)
                    .ok();
                return;
            }

            let head = record.local_head.load(Ordering::SeqCst);
            if (head & COUNTER_MASK) == h {
                record
                    .local_head
                    .compare_exchange(head, head | FIN_BIT, Ordering::SeqCst, Ordering::SeqCst)
                    .ok();
                return;
            }
        }
    }

    unsafe fn catchup_inner(&self, wq: &InnerWCQ, mut tail: u64, head: u64) {
        let mut attempts = 0;
        while attempts < 1000 {
            match wq
                .tail
                .cnt
                .compare_exchange_weak(tail, head, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(new_tail) => {
                    tail = new_tail;
                    if tail >= head {
                        break;
                    }
                }
            }
            attempts += 1;
        }
    }

    // Fixed slow_faa with proper synchronization
    unsafe fn slow_faa(
        &self,
        global: &GlobalPair,
        local: &AtomicU64,
        v: &mut u64,
        thld: Option<&AtomicI32>,
        phase2: &Phase2Rec,
    ) -> bool {
        let mut attempts = 0;

        loop {
            attempts += 1;
            if attempts > 100 {
                return false;
            }

            let cnt = self.load_global_help_phase2(global, local, phase2);
            if cnt == u64::MAX {
                return false;
            }

            let old_v = *v;
            if local
                .compare_exchange(old_v, cnt | INC_BIT, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                *v = cnt | INC_BIT;
            } else {
                *v = local.load(Ordering::SeqCst);
                if *v & FIN_BIT != 0 {
                    return false;
                }
                if *v & INC_BIT == 0 {
                    return true;
                }
                continue;
            }

            self.prepare_phase2(phase2, local as *const _ as u64, cnt);

            // Try to increment global
            let mut global_attempts = 0;
            loop {
                global_attempts += 1;
                if global_attempts > 100 {
                    break;
                }

                let old_cnt = global.cnt.load(Ordering::SeqCst);
                if old_cnt == cnt {
                    if global
                        .cnt
                        .compare_exchange(cnt, cnt + 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        global
                            .ptr
                            .store(phase2 as *const _ as u64, Ordering::SeqCst);
                        break;
                    }
                } else if old_cnt > cnt {
                    break;
                }
                std::hint::spin_loop();
            }

            if let Some(threshold) = thld {
                threshold.fetch_sub(1, Ordering::SeqCst);
            }

            // Phase 2: clear INC bit
            local
                .compare_exchange(cnt | INC_BIT, cnt, Ordering::SeqCst, Ordering::SeqCst)
                .ok();

            // Clear phase2 pointer
            let mut clear_attempts = 0;
            loop {
                clear_attempts += 1;
                if clear_attempts > 100 {
                    break;
                }

                let ptr = global.ptr.load(Ordering::SeqCst);
                if ptr == phase2 as *const _ as u64 {
                    if global
                        .ptr
                        .compare_exchange(ptr, 0, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        break;
                    }
                } else {
                    break;
                }
            }

            *v = cnt;
            return true;
        }
    }

    unsafe fn load_global_help_phase2(
        &self,
        global: &GlobalPair,
        mylocal: &AtomicU64,
        _phase2: &Phase2Rec,
    ) -> u64 {
        let mut attempts = 0;

        loop {
            attempts += 1;
            if attempts > 100 {
                return global.cnt.load(Ordering::SeqCst);
            }

            if mylocal.load(Ordering::SeqCst) & FIN_BIT != 0 {
                return u64::MAX;
            }

            let gp_cnt = global.cnt.load(Ordering::SeqCst);
            let gp_ptr = global.ptr.load(Ordering::SeqCst);

            if gp_ptr == 0 {
                return gp_cnt;
            }

            // Help complete phase 2
            let phase2_ptr = gp_ptr as *const Phase2Rec;
            if phase2_ptr.is_null() {
                continue;
            }

            let phase2 = &*phase2_ptr;
            let seq = phase2.seq2.load(Ordering::SeqCst);
            let local_ptr = phase2.local.load(Ordering::SeqCst) as *const AtomicU64;
            let cnt = phase2.cnt.load(Ordering::SeqCst);

            if phase2.seq1.load(Ordering::SeqCst) == seq && !local_ptr.is_null() {
                let local = &*local_ptr;
                local
                    .compare_exchange(cnt | INC_BIT, cnt, Ordering::SeqCst, Ordering::SeqCst)
                    .ok();
            }

            // Clear phase2 pointer
            global
                .ptr
                .compare_exchange(gp_ptr, 0, Ordering::SeqCst, Ordering::SeqCst)
                .ok();

            return gp_cnt;
        }
    }

    unsafe fn prepare_phase2(&self, phase2: &Phase2Rec, local: u64, cnt: u64) {
        let seq = phase2.seq1.fetch_add(1, Ordering::SeqCst);
        phase2.local.store(local, Ordering::SeqCst);
        phase2.cnt.store(cnt, Ordering::SeqCst);
        phase2.seq2.store(seq, Ordering::SeqCst);
    }

    unsafe fn try_enq_slow(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        t: u64,
        index: usize,
        r: &ThreadRecord,
    ) -> bool {
        let j = Self::cache_remap(t as usize, wq.capacity);
        let entry = self.get_entry_mut(wq, entries_offset, j);

        let packed = entry.value.load(Ordering::SeqCst);
        let e = EntryPair::unpack_entry(packed);
        let note = entry.note.load(Ordering::SeqCst);

        if e.cycle < Self::cycle(t, wq.ring_size) && note < Self::cycle(t, wq.ring_size) {
            if !(e.is_safe || wq.head.cnt.load(Ordering::SeqCst) <= t)
                || (e.index != IDX_EMPTY && e.index != IDX_BOTTOM)
            {
                entry
                    .note
                    .store(Self::cycle(t, wq.ring_size), Ordering::SeqCst);
                return false;
            }

            // Produce entry with enq=0
            let new_entry = Entry {
                cycle: Self::cycle(t, wq.ring_size),
                is_safe: true,
                enq: false,
                index,
            };

            entry
                .value
                .store(EntryPair::pack_entry(new_entry), Ordering::SeqCst);

            // Finalize help request
            if r.local_tail
                .compare_exchange(t, t | FIN_BIT, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                // Set enq=1
                let mut final_entry = new_entry;
                final_entry.enq = true;
                entry
                    .value
                    .store(EntryPair::pack_entry(final_entry), Ordering::SeqCst);
            }

            if wq.threshold.load(Ordering::SeqCst) != (3 * wq.ring_size as i32 - 1) {
                wq.threshold
                    .store(3 * wq.ring_size as i32 - 1, Ordering::SeqCst);
            }

            return true;
        } else if e.cycle != Self::cycle(t, wq.ring_size) {
            return false;
        }

        true
    }

    unsafe fn enqueue_slow(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        mut tail: u64,
        index: usize,
        r: &ThreadRecord,
    ) {
        let mut attempts = 0;
        while attempts < 100 && self.slow_faa(&wq.tail, &r.local_tail, &mut tail, None, &r.phase2) {
            if self.try_enq_slow(wq, entries_offset, tail, index, r) {
                break;
            }
            attempts += 1;
        }
    }

    unsafe fn try_deq_slow(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        h: u64,
        r: &ThreadRecord,
    ) -> bool {
        let j = Self::cache_remap(h as usize, wq.capacity);
        let entry = self.get_entry_mut(wq, entries_offset, j);

        let packed = entry.value.load(Ordering::SeqCst);
        let e = EntryPair::unpack_entry(packed);

        if e.cycle == Self::cycle(h, wq.ring_size) && e.index != IDX_EMPTY {
            r.local_head
                .compare_exchange(h, h | FIN_BIT, Ordering::SeqCst, Ordering::SeqCst)
                .ok();
            return true;
        }

        let note = entry.note.load(Ordering::SeqCst);
        let mut val = Entry {
            cycle: Self::cycle(h, wq.ring_size),
            is_safe: e.is_safe,
            enq: true,
            index: IDX_EMPTY,
        };

        if e.index != IDX_EMPTY && e.index != IDX_BOTTOM {
            val = Entry {
                cycle: e.cycle,
                is_safe: false,
                enq: e.enq,
                index: e.index,
            };
        }

        if e.cycle < Self::cycle(h, wq.ring_size) && note < Self::cycle(h, wq.ring_size) {
            entry
                .note
                .store(Self::cycle(h, wq.ring_size), Ordering::SeqCst);
            entry
                .value
                .store(EntryPair::pack_entry(val), Ordering::SeqCst);
        }

        if e.cycle < Self::cycle(h, wq.ring_size) {
            entry
                .value
                .store(EntryPair::pack_entry(val), Ordering::SeqCst);
        }

        let tail = wq.tail.cnt.load(Ordering::SeqCst);
        if tail <= h + 1 {
            self.catchup_inner(wq, tail, h + 1);
            if wq.threshold.load(Ordering::SeqCst) < 0 {
                r.local_head
                    .compare_exchange(h, h | FIN_BIT, Ordering::SeqCst, Ordering::SeqCst)
                    .ok();
                return true;
            }
        }

        false
    }

    unsafe fn dequeue_slow(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        mut head: u64,
        r: &ThreadRecord,
    ) {
        let thld = &wq.threshold;
        let mut attempts = 0;
        while attempts < 100
            && self.slow_faa(&wq.head, &r.local_head, &mut head, Some(thld), &r.phase2)
        {
            if self.try_deq_slow(wq, entries_offset, head, r) {
                break;
            }
            attempts += 1;
        }
    }

    // Help operations
    unsafe fn help_threads(&self, tid: usize) {
        let r = self.get_record_mut(tid);

        let mut next_check = r.next_check.load(Ordering::SeqCst);
        next_check = next_check.saturating_sub(1);
        r.next_check.store(next_check, Ordering::SeqCst);

        if next_check != 0 {
            return;
        }

        let next_tid = r.next_tid.load(Ordering::SeqCst);
        let thr = self.get_record(next_tid);

        if thr.pending.load(Ordering::SeqCst) {
            if thr.enqueue.load(Ordering::SeqCst) {
                self.help_enqueue(thr);
            } else {
                self.help_dequeue(thr);
            }
        }

        r.next_check.store(HELP_DELAY, Ordering::SeqCst);
        r.next_tid
            .store((next_tid + 1) % self.num_threads, Ordering::SeqCst);
    }

    unsafe fn help_enqueue(&self, thr: &ThreadRecord) {
        let seq = thr.seq2.load(Ordering::SeqCst);
        let enqueue = thr.enqueue.load(Ordering::SeqCst);
        let idx = thr.index.load(Ordering::SeqCst);
        let tail = thr.init_tail.load(Ordering::SeqCst);
        let queue_id = thr.queue_id.load(Ordering::SeqCst);

        if enqueue && thr.seq1.load(Ordering::SeqCst) == seq {
            if queue_id == 0 {
                self.enqueue_slow(&self.aq, self.aq_entries_offset, tail, idx, thr);
            } else {
                self.enqueue_slow(&self.fq, self.fq_entries_offset, tail, idx, thr);
            }
        }
    }

    unsafe fn help_dequeue(&self, thr: &ThreadRecord) {
        let seq = thr.seq2.load(Ordering::SeqCst);
        let enqueue = thr.enqueue.load(Ordering::SeqCst);
        let head = thr.init_head.load(Ordering::SeqCst);
        let queue_id = thr.queue_id.load(Ordering::SeqCst);

        if !enqueue && thr.seq1.load(Ordering::SeqCst) == seq {
            if queue_id == 0 {
                self.dequeue_slow(&self.aq, self.aq_entries_offset, head, thr);
            } else {
                self.dequeue_slow(&self.fq, self.fq_entries_offset, head, thr);
            }
        }
    }

    // Inner queue operations
    unsafe fn enqueue_inner_wcq(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        index: usize,
        thread_id: usize,
        queue_id: usize,
    ) -> Result<(), ()> {
        self.help_threads(thread_id);

        // Update threshold when enqueueing to aq
        if queue_id == 0 && wq.threshold.load(Ordering::SeqCst) < 0 {
            wq.threshold
                .store(3 * wq.ring_size as i32 - 1, Ordering::SeqCst);
        }

        // Fast path
        let mut count = MAX_PATIENCE;
        while count > 0 {
            match self.try_enq_inner(wq, entries_offset, index) {
                Ok(()) => return Ok(()),
                Err(tail) => {
                    count -= 1;
                    if count == 0 {
                        // Slow path
                        let r = self.get_record_mut(thread_id);
                        let seq = r.seq1.load(Ordering::SeqCst);

                        r.local_tail.store(tail, Ordering::SeqCst);
                        r.init_tail.store(tail, Ordering::SeqCst);
                        r.index.store(index, Ordering::SeqCst);
                        r.enqueue.store(true, Ordering::SeqCst);
                        r.queue_id.store(queue_id, Ordering::SeqCst);
                        r.seq2.store(seq, Ordering::SeqCst);
                        r.pending.store(true, Ordering::SeqCst);

                        self.enqueue_slow(wq, entries_offset, tail, index, r);

                        r.pending.store(false, Ordering::SeqCst);
                        r.seq1.fetch_add(1, Ordering::SeqCst);

                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    unsafe fn dequeue_inner_wcq(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        thread_id: usize,
        queue_id: usize,
    ) -> Result<usize, ()> {
        // Don't check threshold for fq
        if queue_id == 0 && wq.threshold.load(Ordering::SeqCst) < 0 {
            return Err(());
        }

        self.help_threads(thread_id);

        // Fast path
        let mut count = MAX_PATIENCE_DEQ;
        let mut idx = 0;

        while count > 0 {
            match self.try_deq_inner(wq, entries_offset, &mut idx) {
                Ok(()) => {
                    if idx == usize::MAX {
                        return Err(());
                    }
                    return Ok(idx);
                }
                Err(head) => {
                    count -= 1;
                    if count == 0 {
                        // Slow path
                        let r = self.get_record_mut(thread_id);
                        let seq = r.seq1.load(Ordering::SeqCst);

                        r.local_head.store(head, Ordering::SeqCst);
                        r.init_head.store(head, Ordering::SeqCst);
                        r.enqueue.store(false, Ordering::SeqCst);
                        r.queue_id.store(queue_id, Ordering::SeqCst);
                        r.seq2.store(seq, Ordering::SeqCst);
                        r.pending.store(true, Ordering::SeqCst);

                        self.dequeue_slow(wq, entries_offset, head, r);

                        r.pending.store(false, Ordering::SeqCst);
                        r.seq1.fetch_add(1, Ordering::SeqCst);

                        // Get result
                        let h = r.local_head.load(Ordering::SeqCst) & COUNTER_MASK;
                        let j = Self::cache_remap(h as usize, wq.capacity);
                        let entry = self.get_entry(wq, entries_offset, j);
                        let packed = entry.value.load(Ordering::SeqCst);
                        let e = EntryPair::unpack_entry(packed);

                        if e.cycle == Self::cycle(h, wq.ring_size) && e.index != IDX_EMPTY {
                            self.consume_inner(wq, entries_offset, h, j, &e);
                            return Ok(e.index);
                        }

                        return Err(());
                    }
                }
            }
        }

        Err(())
    }

    // Fixed enqueue with ready flag
    pub fn enqueue(&self, value: T, thread_id: usize) -> Result<(), ()> {
        unsafe {
            // Get free index from fq
            match self.dequeue_inner_wcq(&self.fq, self.fq_entries_offset, thread_id, 1) {
                Ok(index) => {
                    if index >= self.num_indices {
                        return Err(());
                    }

                    let data_entry = self.get_data(index);

                    // Reset ready flag before storing
                    data_entry.ready.store(false, Ordering::SeqCst);
                    fence(Ordering::SeqCst);

                    // Store value
                    *data_entry.value.get() = Some(value);
                    fence(Ordering::SeqCst);

                    // Set ready flag after data is stored
                    data_entry.ready.store(true, Ordering::SeqCst);
                    fence(Ordering::SeqCst);

                    // Now enqueue the index
                    match self.enqueue_inner_wcq(
                        &self.aq,
                        self.aq_entries_offset,
                        index,
                        thread_id,
                        0,
                    ) {
                        Ok(()) => {
                            self.total_enqueued.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                        Err(()) => {
                            // Cleanup on failure
                            data_entry.ready.store(false, Ordering::SeqCst);
                            *data_entry.value.get() = None;
                            self.enqueue_inner_wcq(
                                &self.fq,
                                self.fq_entries_offset,
                                index,
                                thread_id,
                                1,
                            )
                            .ok();
                            Err(())
                        }
                    }
                }
                Err(()) => Err(()),
            }
        }
    }

    // Fixed dequeue with ready flag
    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            // Dequeue index from aq
            match self.dequeue_inner_wcq(&self.aq, self.aq_entries_offset, thread_id, 0) {
                Ok(index) => {
                    if index >= self.num_indices {
                        // Invalid index - this should never happen
                        eprintln!("ERROR: Invalid index {} dequeued", index);
                        return Err(());
                    }

                    let data_entry = self.get_data(index);

                    // Wait for data to be ready with better timeout
                    let mut spin_count = 0;
                    while !data_entry.ready.load(Ordering::SeqCst) {
                        spin_count += 1;

                        if spin_count % 100_000 == 0 {
                            // Check if the enqueuer might have crashed
                            fence(Ordering::SeqCst);

                            // Re-check ready flag after fence
                            if data_entry.ready.load(Ordering::SeqCst) {
                                break;
                            }
                        }

                        if spin_count > 10_000_000 {
                            // Data not ready after very long wait
                            eprintln!(
                                "ERROR: Data not ready for index {} after {} spins",
                                index, spin_count
                            );

                            // Return index to fq to prevent leak
                            self.enqueue_inner_wcq(
                                &self.fq,
                                self.fq_entries_offset,
                                index,
                                thread_id,
                                1,
                            )
                            .ok();

                            return Err(());
                        }
                        std::hint::spin_loop();
                    }

                    fence(Ordering::SeqCst);

                    // Take value
                    let value = (*data_entry.value.get()).take();

                    // Clear ready flag
                    data_entry.ready.store(false, Ordering::SeqCst);
                    fence(Ordering::SeqCst);

                    // Return index to fq
                    if self
                        .enqueue_inner_wcq(&self.fq, self.fq_entries_offset, index, thread_id, 1)
                        .is_err()
                    {
                        eprintln!("ERROR: Failed to return index {} to fq", index);
                    }

                    match value {
                        Some(v) => {
                            self.total_dequeued.fetch_add(1, Ordering::SeqCst);
                            Ok(v)
                        }
                        None => {
                            eprintln!("ERROR: Data was None for index {}", index);
                            Err(())
                        }
                    }
                }
                Err(()) => Err(()),
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        // First help all pending operations
        for _ in 0..2 {
            for tid in 0..self.num_threads {
                unsafe {
                    self.help_threads(tid);
                }
            }
        }

        fence(Ordering::SeqCst);

        // Check if aq is empty
        let aq_head = self.aq.head.cnt.load(Ordering::SeqCst);
        let aq_tail = self.aq.tail.cnt.load(Ordering::SeqCst);

        // Also check threshold
        let aq_threshold = self.aq.threshold.load(Ordering::SeqCst);

        if aq_head < aq_tail {
            return false;
        }

        // Check for any pending enqueues to aq
        for i in 0..self.num_threads {
            let record = unsafe { self.get_record(i) };
            if record.pending.load(Ordering::SeqCst) {
                let is_enq = record.enqueue.load(Ordering::SeqCst);
                let queue_id = record.queue_id.load(Ordering::SeqCst);

                if is_enq && queue_id == 0 {
                    return false; // There's a pending enqueue to aq
                }
            }
        }

        // Queue is empty if aq is empty (head >= tail) and threshold < 0
        aq_head >= aq_tail && aq_threshold < 0
    }

    pub fn is_full(&self) -> bool {
        // Check if free queue is empty by comparing head and tail
        let fq_head = self.fq.head.cnt.load(Ordering::SeqCst);
        let fq_tail = self.fq.tail.cnt.load(Ordering::SeqCst);

        // Also check threshold as a secondary check
        let fq_threshold = self.fq.threshold.load(Ordering::SeqCst);

        // Queue is full if free queue is empty (head >= tail and threshold < 0)
        fq_head >= fq_tail && fq_threshold < 0
    }

    // Memory layout
    fn layout(num_threads: usize, num_indices: usize) -> (Layout, [usize; 4]) {
        let ring_size = num_indices.next_power_of_two();
        let capacity = ring_size * 2;

        let root = Layout::new::<Self>();
        let (l_aq_entries, o_aq_entries) = root
            .extend(Layout::array::<EntryPair>(capacity).unwrap())
            .unwrap();
        let (l_fq_entries, o_fq_entries) = l_aq_entries
            .extend(Layout::array::<EntryPair>(capacity).unwrap())
            .unwrap();
        let (l_records, o_records) = l_fq_entries
            .extend(Layout::array::<ThreadRecord>(num_threads).unwrap())
            .unwrap();
        let (l_final, o_data) = l_records
            .extend(Layout::array::<DataEntry<T>>(num_indices).unwrap())
            .unwrap();

        (
            l_final.pad_to_align(),
            [o_aq_entries, o_fq_entries, o_records, o_data],
        )
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let num_indices = 65536;
        Self::layout(num_threads, num_indices).0.size()
    }

    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let num_indices = 65536;
        let ring_size = num_indices;
        let (_layout, offsets) = Self::layout(num_threads, num_indices);

        let self_ptr = mem as *mut Self;

        ptr::write(
            self_ptr,
            Self {
                aq: InnerWCQ::new(ring_size),
                fq: InnerWCQ::new(ring_size),
                aq_entries_offset: offsets[0],
                fq_entries_offset: offsets[1],
                records_offset: offsets[2],
                data_offset: offsets[3],
                num_threads,
                num_indices,
                total_enqueued: AtomicUsize::new(0),
                total_dequeued: AtomicUsize::new(0),
                _phantom: std::marker::PhantomData,
            },
        );

        let queue = &mut *self_ptr;

        // Initialize aq entries
        let aq_entries = mem.add(offsets[0]) as *mut EntryPair;
        for i in 0..queue.aq.capacity {
            let entry_pair = EntryPair {
                note: AtomicU32::new(u32::MAX),
                value: AtomicU64::new(EntryPair::pack_entry(Entry::new())),
            };
            ptr::write(aq_entries.add(i), entry_pair);
        }

        // Initialize aq queue as empty
        queue.aq.threshold.store(-1, Ordering::SeqCst);

        // Initialize fq entries
        let fq_entries = mem.add(offsets[1]) as *mut EntryPair;
        for i in 0..queue.fq.capacity {
            let entry_pair = EntryPair {
                note: AtomicU32::new(u32::MAX),
                value: AtomicU64::new(EntryPair::pack_entry(Entry::new())),
            };
            ptr::write(fq_entries.add(i), entry_pair);
        }

        // Initialize free queue with all indices
        queue
            .fq
            .head
            .cnt
            .store(queue.fq.capacity as u64, Ordering::SeqCst);
        queue
            .fq
            .tail
            .cnt
            .store((queue.fq.capacity + num_indices) as u64, Ordering::SeqCst);

        // Pre-populate fq with all indices
        for i in 0..num_indices {
            let tail_pos = (queue.fq.capacity + i) as u64;
            let j = Self::cache_remap(tail_pos as usize, queue.fq.capacity);
            let entry = &*fq_entries.add(j);
            let e = Entry {
                cycle: Self::cycle(tail_pos, ring_size),
                is_safe: true,
                enq: true,
                index: i,
            };
            entry
                .value
                .store(EntryPair::pack_entry(e), Ordering::SeqCst);
        }

        // Set threshold after populating
        queue
            .fq
            .threshold
            .store(3 * ring_size as i32 - 1, Ordering::SeqCst);

        // Initialize thread records
        let records = mem.add(offsets[2]) as *mut ThreadRecord;
        for i in 0..num_threads {
            ptr::write(records.add(i), ThreadRecord::new());
            let record = &mut *records.add(i);
            record.next_tid.store(i, Ordering::SeqCst);
        }

        // Initialize data storage
        let data = mem.add(offsets[3]) as *mut DataEntry<T>;
        for i in 0..num_indices {
            ptr::write(data.add(i), DataEntry::new());
        }

        fence(Ordering::SeqCst);

        queue
    }

    pub fn debug_state(&self) {
        println!("=== Queue State ===");
        println!(
            "AQ Head: {}, Tail: {}, Threshold: {}",
            self.aq.head.cnt.load(Ordering::SeqCst),
            self.aq.tail.cnt.load(Ordering::SeqCst),
            self.aq.threshold.load(Ordering::SeqCst)
        );
        println!(
            "FQ Head: {}, Tail: {}, Threshold: {}",
            self.fq.head.cnt.load(Ordering::SeqCst),
            self.fq.tail.cnt.load(Ordering::SeqCst),
            self.fq.threshold.load(Ordering::SeqCst)
        );
        println!(
            "Total enqueued: {}, dequeued: {}",
            self.total_enqueued.load(Ordering::SeqCst),
            self.total_dequeued.load(Ordering::SeqCst)
        );
    }
}

// Implement MpmcQueue trait
impl<T: Send + Clone + 'static> MpmcQueue<T> for WCQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, item: T, thread_id: usize) -> Result<(), Self::PushError> {
        self.enqueue(item, thread_id)
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
