use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{
    fence, AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};

use crate::MpmcQueue;

// Constants from the paper
const MAX_PATIENCE: usize = 16; // For enqueue
const MAX_PATIENCE_DEQ: usize = 64; // For dequeue
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
            value: AtomicU64::new(0), // Will be initialized properly later
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
    queue_id: AtomicUsize, // 0 for aq, 1 for fq
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
    ptr: AtomicU64, // Phase2Rec pointer stored as u64
}

// Inner wCQ structure for indices
#[repr(C)]
struct InnerWCQ {
    threshold: AtomicI32,
    tail: GlobalPair,
    head: GlobalPair,

    // Configuration
    ring_size: usize,
    capacity: usize, // 2 * ring_size as per SCQ
}

impl InnerWCQ {
    const fn new(ring_size: usize) -> Self {
        let capacity = ring_size * 2;
        Self {
            threshold: AtomicI32::new(-1),
            tail: GlobalPair {
                cnt: AtomicU64::new(capacity as u64), // Start at 2n as per SCQ
                ptr: AtomicU64::new(0),
            },
            head: GlobalPair {
                cnt: AtomicU64::new(capacity as u64), // Start at 2n as per SCQ
                ptr: AtomicU64::new(0),
            },
            ring_size,
            capacity,
        }
    }
}

#[repr(C)]
pub struct WCQueue<T: Send + Clone + 'static> {
    // Two wCQ instances for available and allocated indices
    aq: InnerWCQ, // allocated queue
    fq: InnerWCQ, // free queue

    // Offsets into shared memory
    aq_entries_offset: usize,
    fq_entries_offset: usize,
    records_offset: usize,
    data_offset: usize,

    // Configuration
    num_threads: usize,
    num_indices: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for WCQueue<T> {}
unsafe impl<T: Send + Clone> Sync for WCQueue<T> {}

impl<T: Send + Clone + 'static> WCQueue<T> {
    // Cache remap function from SCQ
    #[inline]
    fn cache_remap(pos: usize, capacity: usize) -> usize {
        // Simple modulo-based remapping to avoid complex bit operations
        pos % capacity
    }

    unsafe fn get_entry(&self, wq: &InnerWCQ, entries_offset: usize, idx: usize) -> &EntryPair {
        let base = self as *const _ as *const u8;
        let entries = base.add(entries_offset) as *const EntryPair;
        &*entries.add(idx)
    }

    unsafe fn get_entry_mut(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        idx: usize,
    ) -> &mut EntryPair {
        let base = self as *const _ as *mut u8;
        let entries = base.add(entries_offset) as *mut EntryPair;
        &mut *entries.add(idx)
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

    unsafe fn get_data(&self, idx: usize) -> &UnsafeCell<Option<T>> {
        let base = self as *const _ as *const u8;
        let data = base.add(self.data_offset) as *const UnsafeCell<Option<T>>;
        &*data.add(idx % self.num_indices)
    }

    fn cycle(val: u64, ring_size: usize) -> u32 {
        (val / ring_size as u64) as u32
    }

    // Fast path operations
    unsafe fn try_enq_inner(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        index: usize,
    ) -> Result<(), u64> {
        let tail = wq.tail.cnt.fetch_add(1, Ordering::AcqRel);
        let j = Self::cache_remap(tail as usize, wq.capacity);

        let entry = self.get_entry_mut(wq, entries_offset, j);
        let packed = entry.value.load(Ordering::Acquire);
        let e = EntryPair::unpack_entry(packed);

        // Debug for first few operations
        if tail < 5 && entries_offset == self.aq_entries_offset {
            eprintln!("AQ enq: tail={}, j={}, cycle={}, expected_cycle={}, index={}, attempting to store index={}", 
                tail, j, e.cycle, Self::cycle(tail, wq.ring_size), e.index, index);
        }

        if e.cycle <= Self::cycle(tail, wq.ring_size)
            && (e.is_safe || wq.head.cnt.load(Ordering::Acquire) <= tail)
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
                .store(EntryPair::pack_entry(new_entry), Ordering::Release);

            if wq.threshold.load(Ordering::Acquire) != (3 * wq.ring_size as i32 - 1) {
                wq.threshold
                    .store(3 * wq.ring_size as i32 - 1, Ordering::Release);
            }

            if tail < 5 && entries_offset == self.aq_entries_offset {
                eprintln!("AQ enq: SUCCESS storing index {} at position {}", index, j);
            }

            Ok(())
        } else {
            if tail < 5 && entries_offset == self.aq_entries_offset {
                eprintln!("AQ enq: FAILED - cycle check: {} <= {}, is_safe: {}, index check: {} == {} or {}", 
                    e.cycle, Self::cycle(tail, wq.ring_size), e.is_safe, e.index, IDX_EMPTY, IDX_BOTTOM);
            }
            Err(tail)
        }
    }

    unsafe fn try_deq_inner(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        index_out: &mut usize,
    ) -> Result<(), u64> {
        let head = wq.head.cnt.fetch_add(1, Ordering::AcqRel);
        let j = Self::cache_remap(head as usize, wq.capacity);

        let entry = self.get_entry_mut(wq, entries_offset, j);
        let packed = entry.value.load(Ordering::Acquire);
        let e = EntryPair::unpack_entry(packed);

        // Debug for first few operations
        if head < 5 && entries_offset == self.fq_entries_offset {
            eprintln!(
                "FQ deq: head={}, j={}, cycle={}, expected_cycle={}, index={}",
                head,
                j,
                e.cycle,
                Self::cycle(head, wq.ring_size),
                e.index
            );
        }

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
                .store(EntryPair::pack_entry(new_val), Ordering::Release);
        }

        let tail = wq.tail.cnt.load(Ordering::Acquire);
        if tail <= head + 1 {
            self.catchup_inner(wq, tail, head + 1);

            // Only decrement threshold for aq, not fq
            if entries_offset == self.aq_entries_offset {
                wq.threshold.fetch_sub(1, Ordering::AcqRel);
            }

            *index_out = usize::MAX; // Empty
            return Ok(());
        }

        // Only check/decrement threshold for aq
        if entries_offset == self.aq_entries_offset {
            if wq.threshold.fetch_sub(1, Ordering::AcqRel) <= 0 {
                *index_out = usize::MAX; // Empty
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
        let packed = entry.value.load(Ordering::Acquire);
        let mut e_mut = EntryPair::unpack_entry(packed);
        e_mut.index = IDX_BOTTOM;
        entry
            .value
            .store(EntryPair::pack_entry(e_mut), Ordering::Release);
    }

    unsafe fn finalize_request_inner(&self, h: u64) {
        for i in 0..self.num_threads {
            let tid = (i + 1) % self.num_threads;
            let record = self.get_record(tid);

            let tail = record.local_tail.load(Ordering::Acquire);
            if (tail & COUNTER_MASK) == h {
                record
                    .local_tail
                    .compare_exchange(tail, tail | FIN_BIT, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
                return;
            }

            let head = record.local_head.load(Ordering::Acquire);
            if (head & COUNTER_MASK) == h {
                record
                    .local_head
                    .compare_exchange(head, head | FIN_BIT, Ordering::AcqRel, Ordering::Acquire)
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
                .compare_exchange_weak(tail, head, Ordering::AcqRel, Ordering::Acquire)
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

    // Slow path operations
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
                return false; // Bounded to prevent infinite loops
            }

            let cnt = self.load_global_help_phase2(global, local, phase2);
            if cnt == u64::MAX {
                return false;
            }

            let old_v = *v;
            if local
                .compare_exchange(old_v, cnt | INC_BIT, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                *v = cnt | INC_BIT;
            } else {
                *v = local.load(Ordering::Acquire);
                if *v & FIN_BIT != 0 {
                    return false;
                }
                if *v & INC_BIT == 0 {
                    return true;
                }
                continue;
            }

            self.prepare_phase2(phase2, local as *const _ as u64, cnt);

            // Try to increment global with phase2 pointer
            let mut global_attempts = 0;
            loop {
                global_attempts += 1;
                if global_attempts > 100 {
                    break;
                }

                let old_cnt = global.cnt.load(Ordering::Acquire);

                if old_cnt == cnt {
                    if global
                        .cnt
                        .compare_exchange(cnt, cnt + 1, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        global
                            .ptr
                            .store(phase2 as *const _ as u64, Ordering::Release);
                        break;
                    }
                } else if old_cnt > cnt {
                    break;
                }
                std::hint::spin_loop();
            }

            if let Some(threshold) = thld {
                threshold.fetch_sub(1, Ordering::AcqRel);
            }

            // Phase 2: clear INC bit
            local
                .compare_exchange(cnt | INC_BIT, cnt, Ordering::AcqRel, Ordering::Acquire)
                .ok();

            // Clear phase2 pointer
            let mut clear_attempts = 0;
            loop {
                clear_attempts += 1;
                if clear_attempts > 100 {
                    break;
                }

                let ptr = global.ptr.load(Ordering::Acquire);
                if ptr == phase2 as *const _ as u64 {
                    if global
                        .ptr
                        .compare_exchange(ptr, 0, Ordering::AcqRel, Ordering::Acquire)
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
                return global.cnt.load(Ordering::Acquire);
            }

            if mylocal.load(Ordering::Acquire) & FIN_BIT != 0 {
                return u64::MAX;
            }

            let gp_cnt = global.cnt.load(Ordering::Acquire);
            let gp_ptr = global.ptr.load(Ordering::Acquire);

            if gp_ptr == 0 {
                return gp_cnt;
            }

            // Help complete phase 2
            let phase2_ptr = gp_ptr as *const Phase2Rec;
            if phase2_ptr.is_null() {
                continue;
            }

            let phase2 = &*phase2_ptr;
            let seq = phase2.seq2.load(Ordering::Acquire);
            let local_ptr = phase2.local.load(Ordering::Acquire) as *const AtomicU64;
            let cnt = phase2.cnt.load(Ordering::Acquire);

            // Try to help
            if phase2.seq1.load(Ordering::Acquire) == seq && !local_ptr.is_null() {
                let local = &*local_ptr;
                local
                    .compare_exchange(cnt | INC_BIT, cnt, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
            }

            // Clear phase2 pointer
            global
                .ptr
                .compare_exchange(gp_ptr, 0, Ordering::AcqRel, Ordering::Acquire)
                .ok();

            return gp_cnt;
        }
    }

    unsafe fn prepare_phase2(&self, phase2: &Phase2Rec, local: u64, cnt: u64) {
        let seq = phase2.seq1.load(Ordering::Acquire);
        phase2.local.store(local, Ordering::Release);
        phase2.cnt.store(cnt, Ordering::Release);
        phase2.seq2.store(seq, Ordering::Release);
        phase2.seq1.store(seq + 1, Ordering::Release);
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

        let packed = entry.value.load(Ordering::Acquire);
        let e = EntryPair::unpack_entry(packed);
        let note = entry.note.load(Ordering::Acquire);

        if e.cycle < Self::cycle(t, wq.ring_size) && note < Self::cycle(t, wq.ring_size) {
            if !(e.is_safe || wq.head.cnt.load(Ordering::Acquire) <= t)
                || (e.index != IDX_EMPTY && e.index != IDX_BOTTOM)
            {
                // Avert helper enqueuers
                entry
                    .note
                    .store(Self::cycle(t, wq.ring_size), Ordering::Release);
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
                .store(EntryPair::pack_entry(new_entry), Ordering::Release);

            // Finalize help request
            if r.local_tail
                .compare_exchange(t, t | FIN_BIT, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // Set enq=1
                let mut final_entry = new_entry;
                final_entry.enq = true;
                entry
                    .value
                    .store(EntryPair::pack_entry(final_entry), Ordering::Release);
            }

            if wq.threshold.load(Ordering::Acquire) != (3 * wq.ring_size as i32 - 1) {
                wq.threshold
                    .store(3 * wq.ring_size as i32 - 1, Ordering::Release);
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

        let packed = entry.value.load(Ordering::Acquire);
        let e = EntryPair::unpack_entry(packed);

        if e.cycle == Self::cycle(h, wq.ring_size) && e.index != IDX_EMPTY {
            r.local_head
                .compare_exchange(h, h | FIN_BIT, Ordering::AcqRel, Ordering::Acquire)
                .ok();
            return true;
        }

        let note = entry.note.load(Ordering::Acquire);
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
                .store(Self::cycle(h, wq.ring_size), Ordering::Release);
            entry
                .value
                .store(EntryPair::pack_entry(val), Ordering::Release);
        }

        if e.cycle < Self::cycle(h, wq.ring_size) {
            entry
                .value
                .store(EntryPair::pack_entry(val), Ordering::Release);
        }

        let tail = wq.tail.cnt.load(Ordering::Acquire);
        if tail <= h + 1 {
            self.catchup_inner(wq, tail, h + 1);
            if wq.threshold.load(Ordering::Acquire) < 0 {
                r.local_head
                    .compare_exchange(h, h | FIN_BIT, Ordering::AcqRel, Ordering::Acquire)
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

        let mut next_check = r.next_check.load(Ordering::Acquire);
        next_check = next_check.saturating_sub(1);
        r.next_check.store(next_check, Ordering::Release);

        if next_check != 0 {
            return;
        }

        let next_tid = r.next_tid.load(Ordering::Acquire);
        let thr = self.get_record(next_tid);

        if thr.pending.load(Ordering::Acquire) {
            if thr.enqueue.load(Ordering::Acquire) {
                self.help_enqueue(thr);
            } else {
                self.help_dequeue(thr);
            }
        }

        r.next_check.store(HELP_DELAY, Ordering::Release);
        r.next_tid
            .store((next_tid + 1) % self.num_threads, Ordering::Release);
    }

    unsafe fn help_enqueue(&self, thr: &ThreadRecord) {
        let seq = thr.seq2.load(Ordering::Acquire);
        let enqueue = thr.enqueue.load(Ordering::Acquire);
        let idx = thr.index.load(Ordering::Acquire);
        let tail = thr.init_tail.load(Ordering::Acquire);
        let queue_id = thr.queue_id.load(Ordering::Acquire);

        if enqueue && thr.seq1.load(Ordering::Acquire) == seq {
            // Determine which queue based on queue_id
            if queue_id == 0 {
                self.enqueue_slow(&self.aq, self.aq_entries_offset, tail, idx, thr);
            } else {
                self.enqueue_slow(&self.fq, self.fq_entries_offset, tail, idx, thr);
            }
        }
    }

    unsafe fn help_dequeue(&self, thr: &ThreadRecord) {
        let seq = thr.seq2.load(Ordering::Acquire);
        let enqueue = thr.enqueue.load(Ordering::Acquire);
        let head = thr.init_head.load(Ordering::Acquire);
        let queue_id = thr.queue_id.load(Ordering::Acquire);

        if !enqueue && thr.seq1.load(Ordering::Acquire) == seq {
            // Determine which queue based on queue_id
            if queue_id == 0 {
                self.dequeue_slow(&self.aq, self.aq_entries_offset, head, thr);
            } else {
                self.dequeue_slow(&self.fq, self.fq_entries_offset, head, thr);
            }
        }
    }

    // Inner queue operations with wait-free slow path
    unsafe fn enqueue_inner_wcq(
        &self,
        wq: &InnerWCQ,
        entries_offset: usize,
        index: usize,
        thread_id: usize,
        queue_id: usize,
    ) -> Result<(), ()> {
        self.help_threads(thread_id);

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
                        let seq = r.seq1.load(Ordering::Acquire);

                        r.local_tail.store(tail, Ordering::Release);
                        r.init_tail.store(tail, Ordering::Release);
                        r.index.store(index, Ordering::Release);
                        r.enqueue.store(true, Ordering::Release);
                        r.queue_id.store(queue_id, Ordering::Release);
                        r.seq2.store(seq, Ordering::Release);
                        r.pending.store(true, Ordering::Release);

                        self.enqueue_slow(wq, entries_offset, tail, index, r);

                        r.pending.store(false, Ordering::Release);
                        r.seq1.fetch_add(1, Ordering::AcqRel);

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
        // Don't check threshold for fq - it should have items
        if queue_id == 0 && wq.threshold.load(Ordering::Acquire) < 0 {
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
                        let seq = r.seq1.load(Ordering::Acquire);

                        r.local_head.store(head, Ordering::Release);
                        r.init_head.store(head, Ordering::Release);
                        r.enqueue.store(false, Ordering::Release);
                        r.queue_id.store(queue_id, Ordering::Release);
                        r.seq2.store(seq, Ordering::Release);
                        r.pending.store(true, Ordering::Release);

                        self.dequeue_slow(wq, entries_offset, head, r);

                        r.pending.store(false, Ordering::Release);
                        r.seq1.fetch_add(1, Ordering::AcqRel);

                        // Get result
                        let h = r.local_head.load(Ordering::Acquire) & COUNTER_MASK;
                        let j = Self::cache_remap(h as usize, wq.capacity);
                        let entry = self.get_entry(wq, entries_offset, j);
                        let packed = entry.value.load(Ordering::Acquire);
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

    // Public interface with indirection
    pub fn enqueue(&self, value: T, thread_id: usize) -> Result<(), ()> {
        unsafe {
            // Get free index from fq
            match self.dequeue_inner_wcq(&self.fq, self.fq_entries_offset, thread_id, 1) {
                Ok(index) => {
                    if index >= self.num_indices {
                        eprintln!("WCQ: Got invalid index {} from fq", index);
                        return Err(());
                    }

                    // Debug
                    if index < 5 {
                        eprintln!("WCQ enqueue: got index {} from fq", index);
                    }

                    // Store value
                    let data_cell = self.get_data(index);
                    *data_cell.get() = Some(value);
                    fence(Ordering::Release);

                    // Enqueue index to aq
                    match self.enqueue_inner_wcq(
                        &self.aq,
                        self.aq_entries_offset,
                        index,
                        thread_id,
                        0,
                    ) {
                        Ok(()) => {
                            if index < 5 {
                                eprintln!(
                                    "WCQ enqueue: successfully enqueued index {} to aq",
                                    index
                                );
                            }
                            Ok(())
                        }
                        Err(()) => {
                            eprintln!("WCQ enqueue: failed to enqueue index {} to aq", index);
                            // Return index to fq on failure
                            *data_cell.get() = None;
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
                Err(()) => {
                    eprintln!("WCQ: Failed to get index from fq");
                    Err(())
                }
            }
        }
    }

    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            // Dequeue index from aq
            match self.dequeue_inner_wcq(&self.aq, self.aq_entries_offset, thread_id, 0) {
                Ok(index) => {
                    if index < 5 {
                        eprintln!("WCQ dequeue: got index {} from aq", index);
                    }

                    // Get value
                    let data_cell = self.get_data(index);
                    let value = (*data_cell.get()).take();
                    fence(Ordering::Acquire);

                    // Return index to fq
                    self.enqueue_inner_wcq(&self.fq, self.fq_entries_offset, index, thread_id, 1)
                        .ok();

                    value.ok_or(())
                }
                Err(()) => {
                    eprintln!("WCQ dequeue: failed to get index from aq");
                    Err(())
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        // Check if aq has any items (threshold < 0 means empty)
        let aq_head = self.aq.head.cnt.load(Ordering::Acquire);
        let aq_tail = self.aq.tail.cnt.load(Ordering::Acquire);
        aq_head >= aq_tail
    }

    pub fn is_full(&self) -> bool {
        self.fq.threshold.load(Ordering::Acquire) < 0
    }

    // Memory layout calculation
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
            .extend(Layout::array::<UnsafeCell<Option<T>>>(num_indices).unwrap())
            .unwrap();

        (
            l_final.pad_to_align(),
            [o_aq_entries, o_fq_entries, o_records, o_data],
        )
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let num_indices = 65536; // 2^16 as mentioned in paper - fits in 30 bits
        Self::layout(num_threads, num_indices).0.size()
    }

    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let num_indices = 65536; // Must be less than 0x3FFFFFFD
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

        // Debug IDX values
        eprintln!("IDX_EMPTY = {}, IDX_BOTTOM = {}", IDX_EMPTY, IDX_BOTTOM);

        // Initialize aq queue as empty
        queue.aq.threshold.store(-1, Ordering::Release);
        // Already initialized to 2n in new()

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
        // Start head at 2n and tail at 2n + num_indices
        queue
            .fq
            .head
            .cnt
            .store(queue.fq.capacity as u64, Ordering::Release);
        queue
            .fq
            .tail
            .cnt
            .store((queue.fq.capacity + num_indices) as u64, Ordering::Release);

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
                .store(EntryPair::pack_entry(e), Ordering::Release);

            // Debug first few entries
            if i < 5 {
                eprintln!("FQ init: i={}, j={}, cycle={}", i, j, e.cycle);
            }
        }

        // Set threshold after populating
        queue
            .fq
            .threshold
            .store(3 * ring_size as i32 - 1, Ordering::Release);

        // Initialize thread records
        let records = mem.add(offsets[2]) as *mut ThreadRecord;
        for i in 0..num_threads {
            ptr::write(records.add(i), ThreadRecord::new());
            let record = &mut *records.add(i);
            record.next_tid.store(i, Ordering::Release);
        }

        // Initialize data storage
        let data = mem.add(offsets[3]) as *mut UnsafeCell<Option<T>>;
        for i in 0..num_indices {
            ptr::write(data.add(i), UnsafeCell::new(None));
        }

        fence(Ordering::SeqCst);

        queue
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
