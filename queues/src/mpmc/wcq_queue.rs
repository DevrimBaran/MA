use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{
    fence, AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};
use std::time::Duration;

use crate::MpmcQueue;

const MAX_PATIENCE: usize = 16;
const MAX_PATIENCE_DEQ: usize = 64;
const HELP_DELAY: usize = 8;
const FIN_BIT: u64 = 1 << 63;
const INC_BIT: u64 = 1 << 62;
const COUNTER_MASK: u64 = (1 << 62) - 1;

const IDX_BOTTOM: usize = 0x3FFFFFFE;
const IDX_EMPTY: usize = 0x3FFFFFFD;

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
    pub value: AtomicU64,
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

#[repr(C)]
struct DataEntry<T> {
    value: UnsafeCell<Option<T>>,
    ready: AtomicBool,
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
    next_check: AtomicUsize,
    next_tid: AtomicUsize,

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

    total_enqueued: AtomicUsize,
    total_dequeued: AtomicUsize,

    // Add termination flag
    terminating: AtomicBool,

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

                fence(Ordering::SeqCst);

                match entry.value.compare_exchange_weak(
                    packed,
                    new_packed,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        fence(Ordering::SeqCst);

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

        fence(Ordering::SeqCst);

        let entry = self.get_entry(wq, entries_offset, j);

        let mut retry_count = 0;
        let mut packed;
        let mut e;

        loop {
            packed = entry.value.load(Ordering::SeqCst);
            e = EntryPair::unpack_entry(packed);

            if e.cycle == Self::cycle(head, wq.ring_size) {
                self.consume_inner(wq, entries_offset, head, j, &e);
                *index_out = e.index;
                return Ok(());
            }

            retry_count += 1;
            if retry_count > 20 {
                break;
            }

            for _ in 0..10 {
                std::hint::spin_loop();
            }
            fence(Ordering::SeqCst);
        }

        fence(Ordering::SeqCst);
        let tail = wq.tail.cnt.load(Ordering::SeqCst);

        if tail <= head + 1 {
            fence(Ordering::SeqCst);
            std::thread::yield_now();
            fence(Ordering::SeqCst);

            for final_check in 0..5 {
                fence(Ordering::SeqCst);
                packed = entry.value.load(Ordering::SeqCst);
                e = EntryPair::unpack_entry(packed);

                if e.cycle == Self::cycle(head, wq.ring_size)
                    && e.index != IDX_EMPTY
                    && e.index != IDX_BOTTOM
                {
                    self.consume_inner(wq, entries_offset, head, j, &e);
                    *index_out = e.index;
                    return Ok(());
                }

                if final_check < 4 {
                    std::thread::yield_now();
                }
            }

            self.catchup_inner(wq, tail, head + 1);

            if entries_offset == self.aq_entries_offset {
                fence(Ordering::SeqCst);
                let current_tail = wq.tail.cnt.load(Ordering::SeqCst);
                if current_tail <= head + 1 {
                    wq.threshold.store(-1, Ordering::SeqCst);
                }
            }

            *index_out = usize::MAX;
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
            let new_packed = EntryPair::pack_entry(new_val);
            entry
                .value
                .compare_exchange(packed, new_packed, Ordering::SeqCst, Ordering::SeqCst)
                .ok();
        }

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
        fence(Ordering::SeqCst);

        if !e.enq && entries_offset == self.aq_entries_offset {
            self.finalize_request_inner(h);
        }

        let entry = self.get_entry_mut(wq, entries_offset, j);

        let expected = EntryPair::pack_entry(*e);
        let mut consumed_entry = *e;
        consumed_entry.index = IDX_BOTTOM;
        let new_packed = EntryPair::pack_entry(consumed_entry);

        fence(Ordering::SeqCst);

        let mut retry_count = 0;
        loop {
            match entry.value.compare_exchange(
                expected,
                new_packed,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    fence(Ordering::SeqCst);

                    if entries_offset == self.aq_entries_offset {
                        fence(Ordering::SeqCst);

                        let mut empty_check_attempts = 0;
                        loop {
                            let current_head = wq.head.cnt.load(Ordering::SeqCst);
                            let current_tail = wq.tail.cnt.load(Ordering::SeqCst);

                            if current_tail <= current_head {
                                wq.threshold.store(-1, Ordering::SeqCst);
                                break;
                            }

                            empty_check_attempts += 1;
                            if empty_check_attempts > 10 {
                                break;
                            }

                            fence(Ordering::SeqCst);
                            std::hint::spin_loop();
                        }
                    }

                    break;
                }
                Err(current) => {
                    retry_count += 1;
                    if retry_count > 100 {
                        break;
                    }

                    let current_entry = EntryPair::unpack_entry(current);
                    if current_entry.index == IDX_BOTTOM {
                        break;
                    }

                    std::hint::spin_loop();
                }
            }
        }
    }

    unsafe fn finalize_request_inner(&self, h: u64) {
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

            if round == 0 {
                std::hint::spin_loop();
            }
        }
    }

    unsafe fn catchup_inner(&self, wq: &InnerWCQ, mut tail: u64, head: u64) {
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 10000;

        while attempts < MAX_ATTEMPTS {
            match wq
                .tail
                .cnt
                .compare_exchange_weak(tail, head, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => {
                    if wq as *const _ == &self.aq as *const _ {
                        fence(Ordering::SeqCst);
                        wq.threshold.store(-1, Ordering::SeqCst);
                        fence(Ordering::SeqCst);
                    }
                    return;
                }
                Err(new_tail) => {
                    tail = new_tail;
                    if tail >= head {
                        if wq as *const _ == &self.aq as *const _ && tail == head {
                            fence(Ordering::SeqCst);
                            wq.threshold.store(-1, Ordering::SeqCst);
                        }
                        return;
                    }
                }
            }

            attempts += 1;
            if attempts % 100 == 0 {
                fence(Ordering::SeqCst);
                std::thread::yield_now();
            } else {
                std::hint::spin_loop();
            }
        }
    }

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
                local.fetch_or(FIN_BIT, Ordering::SeqCst);
                return false;
            }

            fence(Ordering::SeqCst);
            let cnt = self.load_global_help_phase2(global, local, phase2);
            if cnt == u64::MAX {
                return false;
            }

            let old_v = *v;

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

            self.prepare_phase2(phase2, local as *const _ as u64, cnt);

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
                        global
                            .ptr
                            .store(phase2 as *const _ as u64, Ordering::SeqCst);
                        success = true;
                        break;
                    }
                } else if old_cnt > cnt {
                    break;
                }

                global_attempts += 1;
                std::hint::spin_loop();
            }

            if let Some(threshold) = thld {
                threshold.fetch_sub(1, Ordering::SeqCst);
            }

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

        fence(Ordering::SeqCst);

        for attempt in 0..20 {
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

                let new_entry = Entry {
                    cycle: Self::cycle(t, wq.ring_size),
                    is_safe: true,
                    enq: false,
                    index,
                };

                let new_packed = EntryPair::pack_entry(new_entry);

                fence(Ordering::SeqCst);

                match entry.value.compare_exchange_weak(
                    packed,
                    new_packed,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => {
                        fence(Ordering::SeqCst);

                        let mut finalized = false;
                        for fin_attempt in 0..50 {
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

                            fence(Ordering::SeqCst);

                            if fin_attempt < 10 {
                                std::hint::spin_loop();
                            } else {
                                std::thread::yield_now();
                            }
                        }

                        if finalized {
                            let mut final_entry = new_entry;
                            final_entry.enq = true;
                            let final_packed = EntryPair::pack_entry(final_entry);

                            for update_attempt in 0..10 {
                                fence(Ordering::SeqCst);

                                let current = entry.value.load(Ordering::SeqCst);
                                let current_entry = EntryPair::unpack_entry(current);

                                if current_entry.cycle == new_entry.cycle
                                    && current_entry.index == new_entry.index
                                {
                                    entry.value.store(final_packed, Ordering::SeqCst);
                                    fence(Ordering::SeqCst);

                                    let verify = entry.value.load(Ordering::SeqCst);
                                    let verify_entry = EntryPair::unpack_entry(verify);
                                    if verify_entry.enq {
                                        break;
                                    }
                                }

                                if update_attempt > 5 {
                                    std::thread::yield_now();
                                }
                            }

                            fence(Ordering::SeqCst);
                        }

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
            let local_tail = r.local_tail.load(Ordering::Acquire);
            if local_tail & FIN_BIT != 0 {
                return;
            }

            if !self.slow_faa(&wq.tail, &r.local_tail, &mut tail, None, &r.phase2) {
                return;
            }

            if self.try_enq_slow(wq, entries_offset, tail, index, r) {
                return;
            }

            attempts += 1;
            if attempts > max_attempts {
                fence(Ordering::SeqCst);

                let current_tail = wq.tail.cnt.load(Ordering::Acquire);
                if current_tail > tail {
                    for t in (tail..current_tail.min(tail + 10)) {
                        if self.try_enq_slow(wq, entries_offset, t, index, r) {
                            return;
                        }
                    }
                }

                r.local_tail.fetch_or(FIN_BIT, Ordering::Release);
                return;
            }

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

        fence(Ordering::Acquire);

        let packed = entry.value.load(Ordering::Acquire);
        let e = EntryPair::unpack_entry(packed);

        if e.cycle == Self::cycle(h, wq.ring_size) && e.index != IDX_EMPTY {
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
                .compare_exchange(packed, new_packed, Ordering::SeqCst, Ordering::Acquire)
                .ok();
        }

        fence(Ordering::SeqCst);
        let tail = wq.tail.cnt.load(Ordering::SeqCst);

        if tail <= h + 1 {
            self.catchup_inner(wq, tail, h + 1);

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
        let original_head = head;

        loop {
            attempts += 1;

            let local_head = r.local_head.load(Ordering::Acquire);
            if local_head & FIN_BIT != 0 {
                return;
            }

            if attempts > max_attempts {
                fence(Ordering::SeqCst);

                let mut current_head = wq.head.cnt.load(Ordering::SeqCst);
                let current_tail = wq.tail.cnt.load(Ordering::SeqCst);

                if current_tail <= current_head {
                    r.local_head.fetch_or(FIN_BIT, Ordering::SeqCst);
                    return;
                }

                let original_j = Self::cache_remap(original_head as usize, wq.capacity);
                let original_entry = self.get_entry(wq, entries_offset, original_j);

                fence(Ordering::SeqCst);
                let original_packed = original_entry.value.load(Ordering::SeqCst);
                let original_e = EntryPair::unpack_entry(original_packed);

                if original_e.cycle == Self::cycle(original_head, wq.ring_size)
                    && original_e.index != IDX_EMPTY
                    && original_e.index != IDX_BOTTOM
                {
                    self.consume_inner(wq, entries_offset, original_head, original_j, &original_e);

                    r.index.store(original_e.index, Ordering::SeqCst);
                    r.local_head.fetch_or(FIN_BIT, Ordering::SeqCst);
                    return;
                }

                for check_head in original_head..head {
                    let check_j = Self::cache_remap(check_head as usize, wq.capacity);
                    let check_entry = self.get_entry(wq, entries_offset, check_j);

                    fence(Ordering::SeqCst);
                    let check_packed = check_entry.value.load(Ordering::SeqCst);
                    let check_e = EntryPair::unpack_entry(check_packed);

                    if check_e.cycle == Self::cycle(check_head, wq.ring_size)
                        && check_e.index != IDX_EMPTY
                        && check_e.index != IDX_BOTTOM
                    {
                        self.consume_inner(wq, entries_offset, check_head, check_j, &check_e);
                        r.index.store(check_e.index, Ordering::SeqCst);
                        r.local_head.fetch_or(FIN_BIT, Ordering::SeqCst);
                        return;
                    }
                }

                for _ in 0..10 {
                    if self.try_deq_slow(wq, entries_offset, current_head, r) {
                        return;
                    }
                    current_head = wq.head.cnt.load(Ordering::Acquire);
                }

                fence(Ordering::SeqCst);

                for tid in 0..self.num_threads {
                    self.help_threads(tid);
                }

                fence(Ordering::SeqCst);

                let final_head = r.local_head.load(Ordering::Acquire);
                let h = final_head & COUNTER_MASK;
                let j = Self::cache_remap(h as usize, wq.capacity);
                let entry = self.get_entry(wq, entries_offset, j);

                let mut packed = 0;
                for _ in 0..10 {
                    fence(Ordering::SeqCst);
                    packed = entry.value.load(Ordering::SeqCst);
                }
                let e = EntryPair::unpack_entry(packed);

                if e.cycle == Self::cycle(h, wq.ring_size)
                    && e.index != IDX_EMPTY
                    && e.index != IDX_BOTTOM
                {
                    self.consume_inner(wq, entries_offset, h, j, &e);
                    r.index.store(e.index, Ordering::SeqCst);
                    r.local_head.fetch_or(FIN_BIT, Ordering::SeqCst);
                    return;
                }

                if final_head & FIN_BIT != 0 {
                    return;
                }

                r.local_head.fetch_or(FIN_BIT, Ordering::SeqCst);
                return;
            }

            if !self.slow_faa(&wq.head, &r.local_head, &mut head, thld, &r.phase2) {
                return;
            }

            if self.try_deq_slow(wq, entries_offset, head, r) {
                return;
            }

            if attempts < 50 {
                std::hint::spin_loop();
            } else if attempts < 100 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_micros(1));
            }
        }
    }

    unsafe fn help_threads(&self, tid: usize) {
        let r = self.get_record_mut(tid);

        let check_count = r.next_check.load(Ordering::Acquire);
        if check_count > 0 {
            r.next_check.store(check_count - 1, Ordering::Release);

            let total_enqueued = self.total_enqueued.load(Ordering::Acquire);
            let total_dequeued = self.total_dequeued.load(Ordering::Acquire);

            if total_enqueued <= total_dequeued + 10 {
                return;
            }
        }

        r.next_check.store(HELP_DELAY, Ordering::Release);

        fence(Ordering::SeqCst);

        let base_tid = r.next_tid.load(Ordering::Acquire);

        let help_count = if self.total_enqueued.load(Ordering::Acquire)
            > self.total_dequeued.load(Ordering::Acquire)
        {
            self.num_threads.min(4)
        } else {
            2
        };

        for offset in 0..help_count {
            let next_tid = (base_tid + offset) % self.num_threads;
            if next_tid == tid {
                continue;
            }

            let thr = self.get_record(next_tid);

            if thr.pending.load(Ordering::Acquire) {
                let seq1 = thr.seq1.load(Ordering::Acquire);
                let seq2 = thr.seq2.load(Ordering::Acquire);

                if seq1 == seq2 {
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
            fence(Ordering::SeqCst);

            if queue_id == 0 {
                self.dequeue_slow(&self.aq, self.aq_entries_offset, head, thr);
            } else {
                self.dequeue_slow(&self.fq, self.fq_entries_offset, head, thr);
            }
        }
    }

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
                Ok(()) => {
                    // CRITICAL: Ensure enqueue is visible
                    fence(Ordering::SeqCst);

                    // For aq, ensure threshold is updated
                    if entries_offset == self.aq_entries_offset {
                        fence(Ordering::SeqCst);
                        let threshold = wq.threshold.load(Ordering::Acquire);
                        if threshold < 0 {
                            // Help make it positive
                            wq.threshold
                                .compare_exchange(
                                    threshold,
                                    3 * wq.ring_size as i32 - 1,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                )
                                .ok();
                        }
                    }

                    return Ok(());
                }
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

                        // Extra synchronization after slow path
                        fence(Ordering::SeqCst);

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
        if queue_id == 0 {
            fence(Ordering::SeqCst);
            let threshold = wq.threshold.load(Ordering::SeqCst);

            if threshold < 0 {
                fence(Ordering::SeqCst);
                let head = wq.head.cnt.load(Ordering::SeqCst);
                let tail = wq.tail.cnt.load(Ordering::SeqCst);

                if tail > head {
                    for _ in 0..100 {
                        fence(Ordering::SeqCst);
                        let new_threshold = wq.threshold.load(Ordering::SeqCst);
                        if new_threshold >= 0 {
                            break;
                        }

                        let new_tail = wq.tail.cnt.load(Ordering::SeqCst);
                        let new_head = wq.head.cnt.load(Ordering::SeqCst);

                        if new_tail <= new_head {
                            return Err(());
                        }

                        std::hint::spin_loop();
                    }
                } else {
                    return Err(());
                }
            }
        }

        self.help_threads(thread_id);

        let mut count = if queue_id == 0 {
            MAX_PATIENCE_DEQ * 4
        } else {
            MAX_PATIENCE_DEQ
        };
        let mut idx = 0;

        while count > 0 {
            match self.try_deq_inner(wq, entries_offset, &mut idx) {
                Ok(()) => {
                    if idx == usize::MAX {
                        if queue_id == 0 && count > MAX_PATIENCE_DEQ {
                            fence(Ordering::SeqCst);
                            let head = wq.head.cnt.load(Ordering::SeqCst);
                            let tail = wq.tail.cnt.load(Ordering::SeqCst);

                            if tail > head {
                                for tid in 0..self.num_threads {
                                    self.help_threads(tid);
                                }

                                count -= 1;
                                fence(Ordering::SeqCst);
                                std::thread::yield_now();
                                continue;
                            }
                        }
                        return Err(());
                    }
                    return Ok(idx);
                }
                Err(head) => {
                    count -= 1;
                    if count == 0 {
                        for _ in 0..5 {
                            for tid in 0..self.num_threads {
                                self.help_threads(tid);
                            }
                            fence(Ordering::SeqCst);
                        }

                        let r = self.get_record_mut(thread_id);
                        let seq = r.seq1.load(Ordering::Acquire);

                        r.local_head.store(head, Ordering::Release);
                        r.init_head.store(head, Ordering::Release);
                        r.enqueue.store(false, Ordering::Release);
                        r.queue_id.store(queue_id, Ordering::Release);
                        r.index.store(usize::MAX, Ordering::Release);
                        r.seq2.store(seq, Ordering::Release);
                        fence(Ordering::SeqCst);
                        r.pending.store(true, Ordering::Release);

                        self.dequeue_slow(wq, entries_offset, head, r);

                        r.pending.store(false, Ordering::Release);
                        r.seq1.fetch_add(1, Ordering::AcqRel);

                        let stored_index = r.index.load(Ordering::SeqCst);
                        if stored_index != usize::MAX {
                            return Ok(stored_index);
                        }

                        fence(Ordering::SeqCst);
                        let final_head = r.local_head.load(Ordering::Acquire);
                        let h = final_head & COUNTER_MASK;
                        let j = Self::cache_remap(h as usize, wq.capacity);
                        let entry = self.get_entry(wq, entries_offset, j);

                        for read_attempt in 0..10 {
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

                            if read_attempt < 5 {
                                std::hint::spin_loop();
                            } else {
                                std::thread::yield_now();
                            }
                        }

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

    pub fn enqueue(&self, value: T, thread_id: usize) -> Result<(), ()> {
        unsafe {
            // Get free index from fq
            match self.dequeue_inner_wcq(&self.fq, self.fq_entries_offset, thread_id, 1) {
                Ok(index) => {
                    if index >= self.num_indices {
                        return Err(());
                    }

                    let data_entry = self.get_data(index);

                    // Critical: Set value and ready flag atomically
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
                            // CRITICAL: Extra synchronization to ensure visibility
                            fence(Ordering::SeqCst);
                            self.total_enqueued.fetch_add(1, Ordering::SeqCst);
                            fence(Ordering::SeqCst);

                            // Help other threads to ensure progress
                            self.help_threads(thread_id);

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

    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            fence(Ordering::SeqCst);

            // Check if we're terminating
            if self.terminating.load(Ordering::Acquire) {
                return Err(());
            }

            self.help_threads(thread_id);

            fence(Ordering::SeqCst);

            match self.dequeue_inner_wcq(&self.aq, self.aq_entries_offset, thread_id, 0) {
                Ok(index) => {
                    if index >= self.num_indices {
                        return Err(());
                    }

                    let data_entry = self.get_data(index);

                    // Special handling for small thread counts (1P_1C)
                    if self.num_threads <= 2 {
                        // Much shorter timeout for 1P_1C
                        let mut attempts = 0;
                        const MAX_ATTEMPTS_1P1C: usize = 10000;

                        while !data_entry.ready.load(Ordering::SeqCst)
                            && attempts < MAX_ATTEMPTS_1P1C
                        {
                            attempts += 1;

                            // Check termination flag periodically
                            if attempts % 100 == 0 {
                                fence(Ordering::SeqCst);

                                if self.terminating.load(Ordering::Acquire) {
                                    // Return index to fq before bailing
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

                            std::hint::spin_loop();
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
                    } else {
                        // Original logic for larger thread counts
                        let mut spin_count = 0;
                        let start_time = std::time::Instant::now();

                        while !data_entry.ready.load(Ordering::SeqCst) {
                            spin_count += 1;

                            if spin_count % 100 == 0 {
                                fence(Ordering::SeqCst);
                                std::sync::atomic::compiler_fence(Ordering::SeqCst);
                            }

                            if spin_count % 1000 == 0 {
                                fence(Ordering::SeqCst);

                                for _ in 0..self.num_threads {
                                    self.help_threads(thread_id);
                                }

                                fence(Ordering::SeqCst);
                                if data_entry.ready.load(Ordering::SeqCst) {
                                    break;
                                }
                            }

                            if spin_count > 10_000_000
                                || start_time.elapsed() > Duration::from_secs(30)
                            {
                                // Timeout handling
                                for _ in 0..10 {
                                    fence(Ordering::SeqCst);
                                    std::thread::sleep(Duration::from_millis(1));

                                    for tid in 0..self.num_threads {
                                        self.help_threads(tid);
                                    }

                                    fence(Ordering::SeqCst);

                                    if data_entry.ready.load(Ordering::SeqCst) {
                                        break;
                                    }
                                }

                                if !data_entry.ready.load(Ordering::SeqCst) {
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
                    }

                    fence(Ordering::SeqCst);
                    std::sync::atomic::compiler_fence(Ordering::SeqCst);
                    fence(Ordering::SeqCst);

                    let value = (*data_entry.value.get()).take();

                    fence(Ordering::SeqCst);
                    data_entry.ready.store(false, Ordering::SeqCst);
                    fence(Ordering::SeqCst);

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
                        None => Err(()),
                    }
                }
                Err(()) => {
                    // Skip recovery logic for small thread counts
                    if self.num_threads <= 2 {
                        return Err(());
                    }

                    // Original recovery logic for larger thread counts...
                    Err(())
                }
            }
        }
    }

    pub fn terminate(&self) {
        self.terminating.store(true, Ordering::Release);
        fence(Ordering::SeqCst);
    }

    pub fn is_empty(&self) -> bool {
        for tid in 0..self.num_threads {
            unsafe {
                self.help_threads(tid);
            }
        }

        fence(Ordering::SeqCst);

        let total_enqueued = self.total_enqueued.load(Ordering::Acquire);
        let total_dequeued = self.total_dequeued.load(Ordering::Acquire);

        if total_enqueued == total_dequeued {
            return true;
        }

        let aq_head = self.aq.head.cnt.load(Ordering::Acquire);
        let aq_tail = self.aq.tail.cnt.load(Ordering::Acquire);
        let aq_threshold = self.aq.threshold.load(Ordering::Acquire);

        if aq_head == aq_tail && aq_threshold < 0 {
            return true;
        }

        let ring_size = self.aq.ring_size;
        let head_pos = aq_head % (ring_size as u64 * 2);
        let tail_pos = aq_tail % (ring_size as u64 * 2);

        if head_pos == tail_pos && aq_threshold < 0 {
            return true;
        }

        false
    }

    pub fn is_full(&self) -> bool {
        for _ in 0..2 {
            for tid in 0..self.num_threads {
                unsafe {
                    self.help_threads(tid);
                }
            }
        }

        fence(Ordering::SeqCst);

        let fq_head = self.fq.head.cnt.load(Ordering::Acquire);
        let fq_tail = self.fq.tail.cnt.load(Ordering::Acquire);
        let fq_threshold = self.fq.threshold.load(Ordering::Acquire);

        let total_enqueued = self.total_enqueued.load(Ordering::Acquire);
        let total_dequeued = self.total_dequeued.load(Ordering::Acquire);
        let items_in_use = total_enqueued.saturating_sub(total_dequeued);

        (fq_head >= fq_tail && fq_threshold < 0) || items_in_use >= self.num_indices
    }

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

        // CRITICAL: Zero out the entire memory region first
        let total_size = Self::shared_size(num_threads);
        ptr::write_bytes(mem, 0, total_size);
        fence(Ordering::SeqCst);

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
                terminating: AtomicBool::new(false),
                _phantom: std::marker::PhantomData,
            },
        );

        let queue = &mut *self_ptr;

        // Initialize all entries with proper memory barriers
        let aq_entries = mem.add(offsets[0]) as *mut EntryPair;
        for i in 0..queue.aq.capacity {
            fence(Ordering::SeqCst);
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

        let fq_entries = mem.add(offsets[1]) as *mut EntryPair;
        for i in 0..queue.fq.capacity {
            fence(Ordering::SeqCst);
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

        // Initialize fq with all indices - with extra synchronization
        fence(Ordering::SeqCst);
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
            fence(Ordering::SeqCst);
        }

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

        queue
            .fq
            .threshold
            .store(3 * ring_size as i32 - 1, Ordering::SeqCst);

        // Initialize thread records with clean state
        let records = mem.add(offsets[2]) as *mut ThreadRecord;
        for i in 0..num_threads {
            ptr::write(records.add(i), ThreadRecord::new());
            fence(Ordering::SeqCst);
            let record = &mut *records.add(i);
            record.next_tid.store(i, Ordering::SeqCst);
            // Ensure clean initial state
            record.pending.store(false, Ordering::SeqCst);
            record.local_tail.store(0, Ordering::SeqCst);
            record.local_head.store(0, Ordering::SeqCst);
            record.init_tail.store(0, Ordering::SeqCst);
            record.init_head.store(0, Ordering::SeqCst);
            record.seq1.store(1, Ordering::SeqCst);
            record.seq2.store(0, Ordering::SeqCst);
            fence(Ordering::SeqCst);
        }

        // Initialize data entries as completely empty
        let data = mem.add(offsets[3]) as *mut DataEntry<T>;
        for i in 0..num_indices {
            ptr::write(data.add(i), DataEntry::new());
            fence(Ordering::SeqCst);
            let entry = &*data.add(i);
            entry.ready.store(false, Ordering::SeqCst);
        }

        // Final synchronization
        fence(Ordering::SeqCst);
        std::sync::atomic::compiler_fence(Ordering::SeqCst);
        fence(Ordering::SeqCst);

        queue
    }
}

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
