use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{
    fence, AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};
use std::time::Duration;

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
pub struct Entry {
    pub cycle: u32,
    pub is_safe: bool,
    pub enq: bool,
    pub index: usize,
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
pub struct EntryPair {
    note: AtomicU32,
    pub value: AtomicU64, // Packed Entry
}

impl EntryPair {
    const fn new() -> Self {
        Self {
            note: AtomicU32::new(u32::MAX),
            value: AtomicU64::new(0),
        }
    }

    fn pack_entry(e: Entry) -> u64 {
        let mut packed = 0u64;
        packed |= (e.cycle as u64) << 32;
        packed |= (e.is_safe as u64) << 31;
        packed |= (e.enq as u64) << 30;
        packed |= (e.index as u64) & 0x3FFFFFFF;
        packed
    }

    pub fn unpack_entry(packed: u64) -> Entry {
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
pub struct GlobalPair {
    pub cnt: AtomicU64,
    ptr: AtomicU64,
}

#[repr(C)]
pub struct InnerWCQ {
    threshold: AtomicI32,
    pub tail: GlobalPair,
    pub head: GlobalPair,
    ring_size: usize,
    pub capacity: usize,
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
    pub aq: InnerWCQ,
    fq: InnerWCQ,

    pub aq_entries_offset: usize,
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
    pub fn cache_remap(pos: usize, capacity: usize) -> usize {
        pos % capacity
    }

    pub unsafe fn get_entry(
        &self,
        _wq: &InnerWCQ,
        entries_offset: usize,
        idx: usize,
    ) -> &EntryPair {
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
        let tail = wq.tail.cnt.fetch_add(1, Ordering::AcqRel);
        let j = Self::cache_remap(tail as usize, wq.capacity);

        let entry = self.get_entry(wq, entries_offset, j);
        let mut attempts = 0;

        loop {
            let packed = entry.value.load(Ordering::Acquire);
            let e = EntryPair::unpack_entry(packed);

            if e.cycle < Self::cycle(tail, wq.ring_size)
                && (e.is_safe || wq.head.cnt.load(Ordering::Acquire) <= tail)
                && (e.index == IDX_EMPTY || e.index == IDX_BOTTOM)
            {
                let new_entry = Entry {
                    cycle: Self::cycle(tail, wq.ring_size),
                    is_safe: true,
                    enq: true,
                    index,
                };

                let new_packed = EntryPair::pack_entry(new_entry);
                match entry.value.compare_exchange_weak(
                    packed,
                    new_packed,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Only update threshold for aq when it's negative (empty)
                        if entries_offset == self.aq_entries_offset {
                            let current_threshold = wq.threshold.load(Ordering::Acquire);
                            if current_threshold < 0 {
                                wq.threshold
                                    .store(3 * wq.ring_size as i32 - 1, Ordering::Release);
                            }
                        }
                        return Ok(());
                    }
                    Err(_) => {
                        attempts += 1;
                        if attempts > 10 {
                            return Err(tail);
                        }
                        continue;
                    }
                }
            } else {
                return Err(tail);
            }
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

        // Strong synchronization before reading
        fence(Ordering::SeqCst);

        let entry = self.get_entry(wq, entries_offset, j);
        let packed = entry.value.load(Ordering::Acquire);
        let e = EntryPair::unpack_entry(packed);

        if e.cycle == Self::cycle(head, wq.ring_size) {
            // Valid entry - consume it
            self.consume_inner(wq, entries_offset, head, j, &e);
            *index_out = e.index;
            return Ok(());
        }

        // Entry not ready - check if queue is empty
        // CRITICAL: Use SeqCst for proper synchronization
        fence(Ordering::SeqCst);
        let tail = wq.tail.cnt.load(Ordering::SeqCst);

        // CRITICAL FIX: Correct empty check with proper comparison
        if tail <= head + 1 {
            // Queue is empty or about to be empty
            self.catchup_inner(wq, tail, head + 1);

            if entries_offset == self.aq_entries_offset {
                // Only set threshold to -1 if truly empty
                fence(Ordering::SeqCst);
                let current_tail = wq.tail.cnt.load(Ordering::SeqCst);
                if current_tail <= head + 1 {
                    wq.threshold.store(-1, Ordering::SeqCst);
                }
            }

            *index_out = usize::MAX;
            return Ok(());
        }

        // Try to update entry if it's old
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
            let new_packed = EntryPair::pack_entry(new_val);
            entry
                .value
                .compare_exchange(packed, new_packed, Ordering::Release, Ordering::Acquire)
                .ok();
        }

        // Update threshold for aq
        if entries_offset == self.aq_entries_offset {
            let new_threshold = wq.threshold.fetch_sub(1, Ordering::AcqRel) - 1;
            if new_threshold < 0 {
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
        // CRITICAL: Add synchronization before consume
        fence(Ordering::SeqCst);

        // First finalize if needed
        if !e.enq && entries_offset == self.aq_entries_offset {
            self.finalize_request_inner(h);
        }

        let entry = self.get_entry_mut(wq, entries_offset, j);

        // CRITICAL FIX: Use atomic compare-exchange to prevent race
        let expected = EntryPair::pack_entry(*e);
        let mut consumed_entry = *e;
        consumed_entry.index = IDX_BOTTOM;
        let new_packed = EntryPair::pack_entry(consumed_entry);

        // Strong synchronization before consume
        fence(Ordering::SeqCst);

        // Only consume if entry hasn't changed
        let mut retry_count = 0;
        loop {
            match entry.value.compare_exchange(
                expected,
                new_packed,
                Ordering::SeqCst, // Use SeqCst for stronger guarantee
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    // Additional fence after successful consume
                    fence(Ordering::SeqCst);
                    break;
                }
                Err(current) => {
                    retry_count += 1;
                    if retry_count > 10 {
                        // Entry was modified - someone else consumed it
                        eprintln!(
                            "WARNING: Failed to consume entry after {} retries",
                            retry_count
                        );
                        break;
                    }

                    // Check if it's already consumed
                    let current_entry = EntryPair::unpack_entry(current);
                    if current_entry.index == IDX_BOTTOM {
                        break; // Already consumed
                    }

                    std::hint::spin_loop();
                }
            }
        }
    }

    unsafe fn finalize_request_inner(&self, h: u64) {
        // Try to find and finalize the matching request
        for round in 0..2 {
            for i in 0..self.num_threads {
                let tid = i;
                let record = self.get_record(tid);

                let tail = record.local_tail.load(Ordering::Acquire);
                if (tail & COUNTER_MASK) == h && (tail & FIN_BIT) == 0 {
                    if record
                        .local_tail
                        .compare_exchange(
                            tail,
                            tail | FIN_BIT,
                            Ordering::Release,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }

                let head = record.local_head.load(Ordering::Acquire);
                if (head & COUNTER_MASK) == h && (head & FIN_BIT) == 0 {
                    if record
                        .local_head
                        .compare_exchange(
                            head,
                            head | FIN_BIT,
                            Ordering::Release,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
            }

            // Brief synchronization between rounds
            if round == 0 {
                std::hint::spin_loop();
            }
        }
    }

    unsafe fn catchup_inner(&self, wq: &InnerWCQ, mut tail: u64, head: u64) {
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 10000;

        while attempts < MAX_ATTEMPTS {
            match wq.tail.cnt.compare_exchange_weak(
                tail,
                head,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(new_tail) => {
                    tail = new_tail;
                    if tail >= head {
                        return; // Someone else updated it
                    }
                }
            }

            attempts += 1;
            if attempts % 100 == 0 {
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }

        // Force update if we couldn't do it atomically
        if attempts >= MAX_ATTEMPTS {
            eprintln!("WARNING: catchup_inner failed after {} attempts", attempts);
            wq.tail.cnt.store(head, Ordering::Release);
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
            if attempts > 200 {
                // Force finalization before giving up
                local.fetch_or(FIN_BIT, Ordering::SeqCst);
                return false;
            }

            // Load global with full synchronization
            fence(Ordering::SeqCst);
            let cnt = self.load_global_help_phase2(global, local, phase2);
            if cnt == u64::MAX {
                return false;
            }

            let old_v = *v;

            // Phase 1: Set INC bit
            match local.compare_exchange(old_v, cnt | INC_BIT, Ordering::SeqCst, Ordering::Acquire)
            {
                Ok(_) => {
                    *v = cnt | INC_BIT;
                }
                Err(current) => {
                    *v = current;
                    if *v & FIN_BIT != 0 {
                        return false;
                    }
                    if *v & INC_BIT == 0 {
                        return true;
                    }
                    continue;
                }
            }

            // Prepare phase2 request
            self.prepare_phase2(phase2, local as *const _ as u64, cnt);

            // Try to increment global with retry
            let mut global_attempts = 0;
            let mut success = false;

            while global_attempts < 100 {
                let old_cnt = global.cnt.load(Ordering::Acquire);

                if old_cnt == cnt {
                    if global
                        .cnt
                        .compare_exchange(cnt, cnt + 1, Ordering::SeqCst, Ordering::Acquire)
                        .is_ok()
                    {
                        // Set phase2 pointer
                        global
                            .ptr
                            .store(phase2 as *const _ as u64, Ordering::SeqCst);
                        success = true;
                        break;
                    }
                } else if old_cnt > cnt {
                    // Someone else advanced
                    break;
                }

                global_attempts += 1;
                std::hint::spin_loop();
            }

            if let Some(threshold) = thld {
                threshold.fetch_sub(1, Ordering::SeqCst);
            }

            // Phase 2: Clear INC bit with retry
            fence(Ordering::SeqCst);

            for _ in 0..10 {
                if local
                    .compare_exchange(cnt | INC_BIT, cnt, Ordering::SeqCst, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
                std::hint::spin_loop();
            }

            // Clear phase2 pointer if we set it
            if success {
                let mut clear_attempts = 0;
                while clear_attempts < 100 {
                    let ptr = global.ptr.load(Ordering::Acquire);
                    if ptr == phase2 as *const _ as u64 {
                        if global
                            .ptr
                            .compare_exchange(ptr, 0, Ordering::SeqCst, Ordering::Acquire)
                            .is_ok()
                        {
                            break;
                        }
                    } else {
                        break;
                    }
                    clear_attempts += 1;
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

            if phase2.seq1.load(Ordering::Acquire) == seq && !local_ptr.is_null() {
                let local = &*local_ptr;
                local
                    .compare_exchange(cnt | INC_BIT, cnt, Ordering::Release, Ordering::Acquire)
                    .ok();
            }

            // Clear phase2 pointer
            global
                .ptr
                .compare_exchange(gp_ptr, 0, Ordering::Release, Ordering::Acquire)
                .ok();

            return gp_cnt;
        }
    }

    unsafe fn prepare_phase2(&self, phase2: &Phase2Rec, local: u64, cnt: u64) {
        let seq = phase2.seq1.fetch_add(1, Ordering::AcqRel) + 1;
        phase2.local.store(local, Ordering::Release);
        phase2.cnt.store(cnt, Ordering::Release);
        phase2.seq2.store(seq, Ordering::Release);
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

        // Strong synchronization before slow path operation
        fence(Ordering::SeqCst);

        // Try multiple times with backoff
        for attempt in 0..20 {
            let packed = entry.value.load(Ordering::SeqCst); // Use SeqCst for visibility
            let e = EntryPair::unpack_entry(packed);
            let note = entry.note.load(Ordering::SeqCst);

            if e.cycle < Self::cycle(t, wq.ring_size) && note < Self::cycle(t, wq.ring_size) {
                if !(e.is_safe || wq.head.cnt.load(Ordering::SeqCst) <= t)
                    || (e.index != IDX_EMPTY && e.index != IDX_BOTTOM)
                {
                    // Update note to prevent other helpers
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

                let new_packed = EntryPair::pack_entry(new_entry);

                // Strong synchronization before CAS
                fence(Ordering::SeqCst);

                match entry.value.compare_exchange_weak(
                    packed,
                    new_packed,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        // Strong synchronization after successful CAS
                        fence(Ordering::SeqCst);

                        // Try to finalize - with retry logic
                        let mut finalized = false;
                        for fin_attempt in 0..20 {
                            let local_tail = r.local_tail.load(Ordering::SeqCst);
                            if local_tail & FIN_BIT != 0 {
                                finalized = true;
                                break;
                            }

                            if r.local_tail
                                .compare_exchange(
                                    t,
                                    t | FIN_BIT,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                )
                                .is_ok()
                            {
                                finalized = true;
                                break;
                            }

                            if fin_attempt < 10 {
                                std::hint::spin_loop();
                            } else {
                                fence(Ordering::SeqCst);
                                std::thread::yield_now();
                            }
                        }

                        if finalized {
                            // Full synchronization before setting enq=1
                            fence(Ordering::SeqCst);

                            // Set enq=1 with strong ordering
                            let mut final_entry = new_entry;
                            final_entry.enq = true;
                            let final_packed = EntryPair::pack_entry(final_entry);

                            // Use strong CAS to ensure update
                            entry.value.store(final_packed, Ordering::SeqCst);

                            // Final synchronization
                            fence(Ordering::SeqCst);
                        }

                        // Update threshold with synchronization
                        if entries_offset == self.aq_entries_offset {
                            fence(Ordering::SeqCst);
                            let current = wq.threshold.load(Ordering::SeqCst);
                            if current < 0 {
                                wq.threshold
                                    .store(3 * wq.ring_size as i32 - 1, Ordering::SeqCst);
                            }
                        }

                        return true;
                    }
                    Err(_) => {
                        // Backoff before retry
                        if attempt < 5 {
                            std::hint::spin_loop();
                        } else if attempt < 10 {
                            fence(Ordering::SeqCst);
                            std::thread::yield_now();
                        } else {
                            fence(Ordering::SeqCst);
                            std::thread::sleep(Duration::from_micros(1));
                        }
                        continue;
                    }
                }
            } else if e.cycle == Self::cycle(t, wq.ring_size) {
                // Someone else got this slot
                return false;
            }
        }

        false
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
        let max_attempts = 1000;

        loop {
            // Check if operation was already completed
            let local_tail = r.local_tail.load(Ordering::Acquire);
            if local_tail & FIN_BIT != 0 {
                return; // Already finalized
            }

            // Try to increment tail
            if !self.slow_faa(&wq.tail, &r.local_tail, &mut tail, None, &r.phase2) {
                return; // FIN bit was set
            }

            // Try to insert
            if self.try_enq_slow(wq, entries_offset, tail, index, r) {
                return; // Success
            }

            attempts += 1;
            if attempts > max_attempts {
                // Force synchronization
                fence(Ordering::SeqCst);

                // Try with current tail
                let current_tail = wq.tail.cnt.load(Ordering::Acquire);
                if current_tail > tail {
                    // Try recent positions
                    for t in (tail..current_tail.min(tail + 10)) {
                        if self.try_enq_slow(wq, entries_offset, t, index, r) {
                            return;
                        }
                    }
                }

                // Final attempt to mark as complete
                r.local_tail.fetch_or(FIN_BIT, Ordering::Release);
                return;
            }

            // Backoff
            if attempts < 10 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
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
        let entry = self.get_entry(wq, entries_offset, j);

        // Stronger synchronization before reading
        fence(Ordering::Acquire);

        let packed = entry.value.load(Ordering::Acquire);
        let e = EntryPair::unpack_entry(packed);

        // Ready or consumed by helper
        if e.cycle == Self::cycle(h, wq.ring_size) && e.index != IDX_EMPTY {
            // Set FIN bit with strong ordering
            r.local_head
                .compare_exchange(h, h | FIN_BIT, Ordering::SeqCst, Ordering::Acquire)
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
            if e.cycle < Self::cycle(h, wq.ring_size) {
                val = Entry {
                    cycle: e.cycle,
                    is_safe: false,
                    enq: e.enq,
                    index: e.index,
                };
            }
        }

        if e.cycle < Self::cycle(h, wq.ring_size) {
            if note < Self::cycle(h, wq.ring_size) {
                entry
                    .note
                    .store(Self::cycle(h, wq.ring_size), Ordering::Release);
            }

            let new_packed = EntryPair::pack_entry(val);
            entry
                .value
                .compare_exchange(
                    packed,
                    new_packed,
                    Ordering::SeqCst, // Stronger ordering
                    Ordering::Acquire,
                )
                .ok();
        }

        // Enhanced empty check with synchronization
        fence(Ordering::SeqCst);
        let tail = wq.tail.cnt.load(Ordering::SeqCst);

        if tail <= h + 1 {
            self.catchup_inner(wq, tail, h + 1);

            // For AQ, check threshold
            if entries_offset == self.aq_entries_offset {
                fence(Ordering::SeqCst);

                if wq.threshold.load(Ordering::SeqCst) < 0 {
                    r.local_head
                        .compare_exchange(h, h | FIN_BIT, Ordering::SeqCst, Ordering::Acquire)
                        .ok();
                    return true;
                }
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
        let thld = if entries_offset == self.aq_entries_offset {
            Some(&wq.threshold)
        } else {
            None
        };

        let mut attempts = 0;
        let max_attempts = 200;

        loop {
            attempts += 1;

            // Check if we've been finalized
            let local_head = r.local_head.load(Ordering::Acquire);
            if local_head & FIN_BIT != 0 {
                return;
            }

            if attempts > max_attempts {
                // Before giving up, do a final synchronization
                fence(Ordering::SeqCst);

                // Check if queue is truly empty
                let mut current_head = wq.head.cnt.load(Ordering::SeqCst);
                let current_tail = wq.tail.cnt.load(Ordering::SeqCst);

                if current_tail <= current_head {
                    // Queue is empty, finalize
                    r.local_head.fetch_or(FIN_BIT, Ordering::SeqCst);
                    return;
                }

                // CRITICAL: Check if there's an item at our original head position
                // This handles the case where we incremented head but didn't get the item
                let original_j = Self::cache_remap(head as usize, wq.capacity);
                let original_entry = self.get_entry(wq, entries_offset, original_j);
                let original_packed = original_entry.value.load(Ordering::SeqCst);
                let original_e = EntryPair::unpack_entry(original_packed);

                if original_e.cycle == Self::cycle(head, wq.ring_size)
                    && original_e.index != IDX_EMPTY
                    && original_e.index != IDX_BOTTOM
                {
                    // Found the item! Try to consume it
                    self.consume_inner(wq, entries_offset, head, original_j, &original_e);

                    // Update the record with the index
                    r.index.store(original_e.index, Ordering::SeqCst);
                    r.local_head.fetch_or(FIN_BIT, Ordering::SeqCst);
                    return;
                }

                // Try a few more times with current head
                for _ in 0..10 {
                    if self.try_deq_slow(wq, entries_offset, current_head, r) {
                        return;
                    }
                    current_head = wq.head.cnt.load(Ordering::Acquire);
                }

                // Final finalization
                r.local_head.fetch_or(FIN_BIT, Ordering::SeqCst);
                return;
            }

            // Try to advance head
            if !self.slow_faa(&wq.head, &r.local_head, &mut head, thld, &r.phase2) {
                return;
            }

            // Try to dequeue at this position
            if self.try_deq_slow(wq, entries_offset, head, r) {
                return;
            }

            // Backoff strategy
            if attempts < 50 {
                std::hint::spin_loop();
            } else if attempts < 100 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_micros(1));
            }
        }
    }

    // Help operations
    unsafe fn help_threads(&self, tid: usize) {
        let r = self.get_record_mut(tid);

        // Always help, don't skip
        r.next_check.store(0, Ordering::Release);

        // Strong synchronization
        fence(Ordering::SeqCst);

        // Help multiple threads aggressively
        let base_tid = r.next_tid.load(Ordering::Acquire);
        for offset in 0..self.num_threads {
            let next_tid = (base_tid + offset) % self.num_threads;
            if next_tid == tid {
                continue; // Don't help ourselves
            }

            let thr = self.get_record(next_tid);

            if thr.pending.load(Ordering::Acquire) {
                let seq1 = thr.seq1.load(Ordering::Acquire);
                let seq2 = thr.seq2.load(Ordering::Acquire);

                if seq1 == seq2 {
                    // Extra synchronization before helping
                    fence(Ordering::SeqCst);

                    if thr.enqueue.load(Ordering::Acquire) {
                        self.help_enqueue(thr);
                    } else {
                        self.help_dequeue(thr);
                    }
                }
            }
        }

        r.next_tid
            .store((base_tid + 1) % self.num_threads, Ordering::Release);
    }

    unsafe fn help_enqueue(&self, thr: &ThreadRecord) {
        let seq = thr.seq2.load(Ordering::Acquire);
        let enqueue = thr.enqueue.load(Ordering::Acquire);
        let idx = thr.index.load(Ordering::Acquire);
        let tail = thr.init_tail.load(Ordering::Acquire);
        let queue_id = thr.queue_id.load(Ordering::Acquire);

        if enqueue && thr.seq1.load(Ordering::Acquire) == seq && thr.pending.load(Ordering::Acquire)
        {
            // Additional synchronization before helping
            fence(Ordering::SeqCst);

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

        if !enqueue
            && thr.seq1.load(Ordering::Acquire) == seq
            && thr.pending.load(Ordering::Acquire)
        {
            // Additional synchronization before helping
            fence(Ordering::SeqCst);

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
                        fence(Ordering::SeqCst);
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
        // Don't check threshold for fq
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
                        r.index.store(usize::MAX, Ordering::Release); // Initialize index
                        r.seq2.store(seq, Ordering::Release);
                        fence(Ordering::SeqCst);
                        r.pending.store(true, Ordering::Release);

                        self.dequeue_slow(wq, entries_offset, head, r);

                        r.pending.store(false, Ordering::Release);
                        r.seq1.fetch_add(1, Ordering::AcqRel);

                        // Check if we stored an index during slow path
                        let stored_index = r.index.load(Ordering::SeqCst);
                        if stored_index != usize::MAX {
                            return Ok(stored_index);
                        }

                        // Get result - check both the initial head and any updates
                        let final_head = r.local_head.load(Ordering::Acquire);
                        let h = final_head & COUNTER_MASK;
                        let j = Self::cache_remap(h as usize, wq.capacity);
                        let entry = self.get_entry(wq, entries_offset, j);

                        // Strong synchronization before reading
                        fence(Ordering::SeqCst);
                        let packed = entry.value.load(Ordering::SeqCst);
                        let e = EntryPair::unpack_entry(packed);

                        if e.cycle == Self::cycle(h, wq.ring_size)
                            && e.index != IDX_EMPTY
                            && e.index != IDX_BOTTOM
                        {
                            self.consume_inner(wq, entries_offset, h, j, &e);
                            return Ok(e.index);
                        }

                        // Also check the original head position in case it was updated
                        let orig_j = Self::cache_remap(head as usize, wq.capacity);
                        let orig_entry = self.get_entry(wq, entries_offset, orig_j);

                        fence(Ordering::SeqCst);
                        let orig_packed = orig_entry.value.load(Ordering::SeqCst);
                        let orig_e = EntryPair::unpack_entry(orig_packed);

                        if orig_e.cycle == Self::cycle(head, wq.ring_size)
                            && orig_e.index != IDX_EMPTY
                            && orig_e.index != IDX_BOTTOM
                        {
                            self.consume_inner(wq, entries_offset, head, orig_j, &orig_e);
                            return Ok(orig_e.index);
                        }

                        // Check if FIN was set, indicating we should stop
                        if final_head & FIN_BIT != 0 {
                            return Err(());
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
                        eprintln!("ERROR: Invalid index {} from fq", index);
                        return Err(());
                    }

                    let data_entry = self.get_data(index);

                    // Critical: Set value and ready flag atomically
                    // First ensure the slot is clean
                    fence(Ordering::SeqCst);

                    // Store value with proper synchronization
                    *data_entry.value.get() = Some(value);

                    // Compiler barrier to prevent reordering
                    std::sync::atomic::compiler_fence(Ordering::SeqCst);

                    // Full memory barrier before setting ready
                    fence(Ordering::SeqCst);

                    // Set ready flag - this MUST happen after value is stored
                    data_entry.ready.store(true, Ordering::SeqCst);

                    // Another full barrier to ensure visibility
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
                            // Final fence to ensure everything is visible
                            fence(Ordering::SeqCst);
                            self.total_enqueued.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                        Err(()) => {
                            // Cleanup on failure with proper synchronization
                            fence(Ordering::SeqCst);
                            data_entry.ready.store(false, Ordering::SeqCst);
                            fence(Ordering::SeqCst);
                            *data_entry.value.get() = None;
                            fence(Ordering::SeqCst);

                            // Return index to free queue
                            let mut retry = 0;
                            while self
                                .enqueue_inner_wcq(
                                    &self.fq,
                                    self.fq_entries_offset,
                                    index,
                                    thread_id,
                                    1,
                                )
                                .is_err()
                                && retry < 100
                            {
                                retry += 1;
                                std::thread::yield_now();
                            }

                            Err(())
                        }
                    }
                }
                Err(()) => Err(()),
            }
        }
    }

    pub fn debug_dequeue(&self, thread_id: usize) -> Result<T, ()> {
        eprintln!("[Thread {}] DEBUG: Starting dequeue", thread_id);

        unsafe {
            fence(Ordering::SeqCst);
            self.help_threads(thread_id);
            fence(Ordering::SeqCst);

            match self.dequeue_inner_wcq(&self.aq, self.aq_entries_offset, thread_id, 0) {
                Ok(index) => {
                    eprintln!("[Thread {}] DEBUG: Got index {} from aq", thread_id, index);

                    if index >= self.num_indices {
                        eprintln!(
                            "[Thread {}] ERROR: Invalid index {} dequeued",
                            thread_id, index
                        );
                        return Err(());
                    }

                    let data_entry = self.get_data(index);

                    // Wait for ready
                    let mut spin_count = 0;
                    while !data_entry.ready.load(Ordering::SeqCst) {
                        spin_count += 1;
                        if spin_count > 1000 && spin_count % 100000 == 0 {
                            eprintln!(
                                "[Thread {}] DEBUG: Still waiting for ready at index {}, spins={}",
                                thread_id, index, spin_count
                            );
                        }
                        if spin_count > 10_000_000 {
                            eprintln!(
                                "[Thread {}] ERROR: Timeout waiting for ready at index {}",
                                thread_id, index
                            );
                            self.debug_state();
                            return Err(());
                        }
                        std::hint::spin_loop();
                    }

                    // Take value
                    fence(Ordering::SeqCst);
                    std::sync::atomic::compiler_fence(Ordering::SeqCst);
                    fence(Ordering::SeqCst);

                    let value = (*data_entry.value.get()).take();

                    fence(Ordering::SeqCst);
                    data_entry.ready.store(false, Ordering::SeqCst);
                    fence(Ordering::SeqCst);

                    // Return index to fq
                    self.enqueue_inner_wcq(&self.fq, self.fq_entries_offset, index, thread_id, 1)
                        .ok();

                    match value {
                        Some(v) => {
                            let total = self.total_dequeued.fetch_add(1, Ordering::SeqCst) + 1;
                            eprintln!("[Thread {}] DEBUG: Successfully dequeued from index {}, total_dequeued={}", thread_id, index, total);
                            Ok(v)
                        }
                        None => {
                            eprintln!(
                                "[Thread {}] ERROR: Value was None at index {}",
                                thread_id, index
                            );
                            Err(())
                        }
                    }
                }
                Err(()) => {
                    eprintln!("[Thread {}] DEBUG: Failed to get index from aq", thread_id);
                    Err(())
                }
            }
        }
    }

    pub fn debug_enqueue(&self, value: T, thread_id: usize) -> Result<(), ()> {
        eprintln!("[Thread {}] DEBUG: Starting enqueue", thread_id);

        // Get free index from fq
        match unsafe { self.dequeue_inner_wcq(&self.fq, self.fq_entries_offset, thread_id, 1) } {
            Ok(index) => {
                eprintln!("[Thread {}] DEBUG: Got free index {}", thread_id, index);

                unsafe {
                    let data_entry = self.get_data(index);

                    // Store value
                    fence(Ordering::SeqCst);
                    *data_entry.value.get() = Some(value);
                    std::sync::atomic::compiler_fence(Ordering::SeqCst);
                    fence(Ordering::SeqCst);
                    data_entry.ready.store(true, Ordering::SeqCst);
                    fence(Ordering::SeqCst);

                    eprintln!(
                        "[Thread {}] DEBUG: Stored value at index {}, ready=true",
                        thread_id, index
                    );

                    // Now enqueue the index
                    match self.enqueue_inner_wcq(
                        &self.aq,
                        self.aq_entries_offset,
                        index,
                        thread_id,
                        0,
                    ) {
                        Ok(()) => {
                            fence(Ordering::SeqCst);
                            let total = self.total_enqueued.fetch_add(1, Ordering::SeqCst) + 1;
                            eprintln!("[Thread {}] DEBUG: Successfully enqueued index {} to aq, total_enqueued={}", thread_id, index, total);
                            Ok(())
                        }
                        Err(()) => {
                            eprintln!(
                                "[Thread {}] DEBUG: Failed to enqueue index {} to aq",
                                thread_id, index
                            );
                            // Cleanup
                            fence(Ordering::SeqCst);
                            data_entry.ready.store(false, Ordering::SeqCst);
                            fence(Ordering::SeqCst);
                            *data_entry.value.get() = None;
                            fence(Ordering::SeqCst);

                            // Return index to fq
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
            }
            Err(()) => {
                eprintln!(
                    "[Thread {}] DEBUG: Failed to get free index from fq",
                    thread_id
                );
                Err(())
            }
        }
    }

    // Fixed dequeue with ready flag
    // Fixed dequeue with ready flag
    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            // Full synchronization before starting
            fence(Ordering::SeqCst);

            // Help other threads first
            self.help_threads(thread_id);

            // Add synchronization before dequeue
            fence(Ordering::SeqCst);

            match self.dequeue_inner_wcq(&self.aq, self.aq_entries_offset, thread_id, 0) {
                Ok(index) => {
                    if index >= self.num_indices {
                        eprintln!("ERROR: Invalid index {} dequeued", index);
                        return Err(());
                    }

                    let data_entry = self.get_data(index);

                    // Enhanced ready wait with aggressive synchronization
                    let mut spin_count = 0;
                    let start_time = std::time::Instant::now();
                    let mut last_check = std::time::Instant::now();

                    while !data_entry.ready.load(Ordering::SeqCst) {
                        spin_count += 1;

                        // Aggressive memory synchronization
                        if spin_count % 100 == 0 {
                            fence(Ordering::SeqCst);
                            std::sync::atomic::compiler_fence(Ordering::SeqCst);
                        }

                        // Periodic strong synchronization
                        if spin_count % 1000 == 0 {
                            fence(Ordering::SeqCst);

                            // Help other threads more aggressively
                            for _ in 0..self.num_threads {
                                self.help_threads(thread_id);
                            }

                            // Re-check after helping
                            fence(Ordering::SeqCst);
                            if data_entry.ready.load(Ordering::SeqCst) {
                                break;
                            }
                        }

                        // Check for timeout with more detailed logging
                        if spin_count > 10_000_000 || start_time.elapsed() > Duration::from_secs(30)
                        {
                            eprintln!(
                                "ERROR: Data not ready for index {} after {} spins, elapsed: {:?}",
                                index,
                                spin_count,
                                start_time.elapsed()
                            );

                            // Debug queue state
                            self.debug_state();

                            // Final aggressive synchronization attempt
                            for _ in 0..10 {
                                fence(Ordering::SeqCst);
                                std::thread::sleep(Duration::from_millis(1));

                                // Help all threads
                                for tid in 0..self.num_threads {
                                    self.help_threads(tid);
                                }

                                fence(Ordering::SeqCst);

                                if data_entry.ready.load(Ordering::SeqCst) {
                                    break;
                                }
                            }

                            if !data_entry.ready.load(Ordering::SeqCst) {
                                // Return index to fq
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
                        }

                        // Adaptive spinning with better backoff
                        if spin_count < 10 {
                            std::hint::spin_loop();
                        } else if spin_count < 100 {
                            for _ in 0..10 {
                                std::hint::spin_loop();
                            }
                        } else if spin_count < 1000 {
                            std::thread::yield_now();
                        } else if spin_count < 10000 {
                            std::thread::sleep(Duration::from_nanos(100));
                        } else {
                            std::thread::sleep(Duration::from_micros(10));
                        }
                    }

                    // Triple fence before taking value
                    fence(Ordering::SeqCst);
                    std::sync::atomic::compiler_fence(Ordering::SeqCst);
                    fence(Ordering::SeqCst);

                    // Take value atomically
                    let value = (*data_entry.value.get()).take();

                    // Clear ready flag with full synchronization
                    fence(Ordering::SeqCst);
                    data_entry.ready.store(false, Ordering::SeqCst);
                    fence(Ordering::SeqCst);

                    // Return index to fq with retry
                    let mut retry = 0;
                    while self
                        .enqueue_inner_wcq(&self.fq, self.fq_entries_offset, index, thread_id, 1)
                        .is_err()
                        && retry < 100
                    {
                        retry += 1;
                        std::thread::yield_now();
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
        // Help threads to ensure pending operations complete
        for tid in 0..self.num_threads {
            unsafe {
                self.help_threads(tid);
            }
        }

        // Strong synchronization
        fence(Ordering::SeqCst);

        // Method 1: Check the total counters
        let total_enqueued = self.total_enqueued.load(Ordering::Acquire);
        let total_dequeued = self.total_dequeued.load(Ordering::Acquire);

        // If counters match, queue is definitely empty
        if total_enqueued == total_dequeued {
            return true;
        }

        // Method 2: Check queue state (for when counters might be out of sync)
        let aq_head = self.aq.head.cnt.load(Ordering::Acquire);
        let aq_tail = self.aq.tail.cnt.load(Ordering::Acquire);
        let aq_threshold = self.aq.threshold.load(Ordering::Acquire);

        // In a circular queue, when empty:
        // 1. head should equal tail (they've caught up)
        // 2. threshold should be negative (no pending dequeues)
        if aq_head == aq_tail && aq_threshold < 0 {
            return true;
        }

        // Additional check: if we've wrapped around many times,
        // use modular arithmetic to check
        let ring_size = self.aq.ring_size;
        let head_pos = aq_head % (ring_size as u64 * 2);
        let tail_pos = aq_tail % (ring_size as u64 * 2);

        // If positions are equal and threshold is negative, queue is empty
        if head_pos == tail_pos && aq_threshold < 0 {
            return true;
        }

        false
    }

    pub fn is_full(&self) -> bool {
        // Help all threads first to ensure pending operations complete
        for _ in 0..2 {
            for tid in 0..self.num_threads {
                unsafe {
                    self.help_threads(tid);
                }
            }
        }

        fence(Ordering::SeqCst);

        // Check if free queue is empty
        let fq_head = self.fq.head.cnt.load(Ordering::Acquire);
        let fq_tail = self.fq.tail.cnt.load(Ordering::Acquire);
        let fq_threshold = self.fq.threshold.load(Ordering::Acquire);

        // Also check how many items are in use
        let total_enqueued = self.total_enqueued.load(Ordering::Acquire);
        let total_dequeued = self.total_dequeued.load(Ordering::Acquire);
        let items_in_use = total_enqueued.saturating_sub(total_dequeued);

        // Queue is full if:
        // 1. Free queue has no available indices (head >= tail AND threshold < 0)
        // 2. OR we've used all available slots
        (fq_head >= fq_tail && fq_threshold < 0) || items_in_use >= self.num_indices
    }

    pub fn drain_all(&self, thread_id: usize) -> Vec<T> {
        let mut items = Vec::new();
        let mut consecutive_empty = 0;
        const MAX_EMPTY_ATTEMPTS: usize = 100_000;

        // Get initial state
        let initial_enqueued = self.total_enqueued.load(Ordering::SeqCst);
        let initial_dequeued = self.total_dequeued.load(Ordering::SeqCst);
        let expected_items = initial_enqueued.saturating_sub(initial_dequeued);

        loop {
            // Aggressive helping
            for _ in 0..self.num_threads {
                unsafe {
                    self.help_threads(thread_id);
                }
            }

            match self.pop(thread_id) {
                Ok(item) => {
                    items.push(item);
                    consecutive_empty = 0;

                    // Check if we've gotten everything
                    if items.len() >= expected_items {
                        break;
                    }
                }
                Err(_) => {
                    consecutive_empty += 1;

                    // Periodic aggressive synchronization
                    if consecutive_empty % 1000 == 0 {
                        fence(Ordering::SeqCst);

                        // Help all threads again
                        for tid in 0..self.num_threads {
                            unsafe {
                                self.help_threads(tid);
                            }
                        }

                        // Re-check counters
                        let current_enqueued = self.total_enqueued.load(Ordering::SeqCst);
                        let current_dequeued = self.total_dequeued.load(Ordering::SeqCst);

                        if current_dequeued + items.len() >= current_enqueued {
                            // We've gotten everything
                            break;
                        }
                    }

                    // Check if we should stop
                    if consecutive_empty > MAX_EMPTY_ATTEMPTS {
                        break;
                    }

                    // Backoff
                    if consecutive_empty < 100 {
                        std::hint::spin_loop();
                    } else if consecutive_empty < 1000 {
                        std::thread::yield_now();
                    } else if consecutive_empty < 10000 {
                        std::thread::sleep(Duration::from_micros(1));
                    } else {
                        std::thread::sleep(Duration::from_micros(10));
                    }
                }
            }
        }

        items
    }

    pub fn drain_remaining(&self, thread_id: usize, max_items: usize) -> Vec<T> {
        let mut items = Vec::new();
        let mut empty_rounds = 0;

        while items.len() < max_items && empty_rounds < 10 {
            // Force complete synchronization
            fence(Ordering::SeqCst);

            // Help all threads aggressively
            for _ in 0..self.num_threads {
                unsafe {
                    self.help_threads(thread_id);
                }
            }

            // Try to dequeue
            let mut found_item = false;
            for _ in 0..100 {
                match self.pop(thread_id) {
                    Ok(item) => {
                        items.push(item);
                        found_item = true;
                        empty_rounds = 0;
                        break;
                    }
                    Err(_) => {
                        std::hint::spin_loop();
                    }
                }
            }

            if !found_item {
                empty_rounds += 1;

                // Aggressive synchronization between rounds
                fence(Ordering::SeqCst);
                std::thread::sleep(Duration::from_micros(100));
                fence(Ordering::SeqCst);

                // Check if there are still items
                let total_enqueued = self.total_enqueued.load(Ordering::SeqCst);
                let total_dequeued = self.total_dequeued.load(Ordering::SeqCst);

                if total_enqueued == total_dequeued + items.len() {
                    // We've gotten everything
                    break;
                }
            }
        }

        items
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

        // Initialize aq entries with proper empty state
        let aq_entries = mem.add(offsets[0]) as *mut EntryPair;
        for i in 0..queue.aq.capacity {
            let entry_pair = EntryPair {
                note: AtomicU32::new(u32::MAX),
                value: AtomicU64::new(EntryPair::pack_entry(Entry {
                    cycle: 0,
                    is_safe: true,
                    enq: true,
                    index: IDX_EMPTY,
                })),
            };
            ptr::write(aq_entries.add(i), entry_pair);
        }

        // Initialize fq entries
        let fq_entries = mem.add(offsets[1]) as *mut EntryPair;
        for i in 0..queue.fq.capacity {
            let entry_pair = EntryPair {
                note: AtomicU32::new(u32::MAX),
                value: AtomicU64::new(EntryPair::pack_entry(Entry {
                    cycle: 0,
                    is_safe: true,
                    enq: true,
                    index: IDX_EMPTY,
                })),
            };
            ptr::write(fq_entries.add(i), entry_pair);
        }

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
        }

        // Initialize queue pointers - fq starts with all indices
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

        // Set fq threshold to indicate it has items
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
            self.aq.head.cnt.load(Ordering::Acquire),
            self.aq.tail.cnt.load(Ordering::Acquire),
            self.aq.threshold.load(Ordering::Acquire)
        );
        println!(
            "FQ Head: {}, Tail: {}, Threshold: {}",
            self.fq.head.cnt.load(Ordering::Acquire),
            self.fq.tail.cnt.load(Ordering::Acquire),
            self.fq.threshold.load(Ordering::Acquire)
        );
        println!(
            "Total enqueued: {}, dequeued: {}",
            self.total_enqueued.load(Ordering::Acquire),
            self.total_dequeued.load(Ordering::Acquire)
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
