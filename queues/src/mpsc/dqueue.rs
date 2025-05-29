use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::MpscQueue;

#[cfg(miri)]
pub const L_LOCAL_BUFFER_CAPACITY: usize = 32;
#[cfg(not(miri))]
pub const L_LOCAL_BUFFER_CAPACITY: usize = 131072;

#[cfg(miri)]
pub const N_SEGMENT_CAPACITY: usize = 64;
#[cfg(not(miri))]
pub const N_SEGMENT_CAPACITY: usize = 262144;

#[repr(C, align(64))]
struct Segment<T> {
    id: u64,
    cells: *mut UnsafeCell<MaybeUninit<Option<T>>>,
    next: AtomicPtr<Segment<T>>,
    next_free: AtomicPtr<Segment<T>>,
}

#[repr(C)]
struct Request<T> {
    val: MaybeUninit<T>,
    cid: u64,
}

#[repr(C, align(64))]
pub struct Producer<T> {
    local_buffer: UnsafeCell<[MaybeUninit<Request<T>>; L_LOCAL_BUFFER_CAPACITY]>,
    local_head: AtomicUsize,
    local_tail: AtomicUsize,
    pseg: AtomicPtr<Segment<T>>,
}

impl<T> Producer<T> {
    unsafe fn init_in_place(producer_ptr: *mut Self, initial_segment: *mut Segment<T>) {
        let buffer_ptr = (*producer_ptr).local_buffer.get() as *mut MaybeUninit<Request<T>>;
        for i in 0..L_LOCAL_BUFFER_CAPACITY {
            ptr::write(buffer_ptr.add(i), MaybeUninit::uninit());
        }
        ptr::addr_of_mut!((*producer_ptr).local_head).write(AtomicUsize::new(0));
        ptr::addr_of_mut!((*producer_ptr).local_tail).write(AtomicUsize::new(0));
        ptr::addr_of_mut!((*producer_ptr).pseg).write(AtomicPtr::new(initial_segment));
    }

    #[inline(always)]
    fn local_wrap(i: usize) -> usize {
        i % L_LOCAL_BUFFER_CAPACITY
    }

    #[inline(always)]
    fn local_next(i: usize) -> usize {
        Self::local_wrap(i + 1)
    }
}

#[repr(C, align(64))]
pub struct DQueue<T: Send + Clone + 'static> {
    q_head: AtomicU64,
    q_tail: AtomicU64,
    qseg: AtomicPtr<Segment<T>>,
    producers_array: *mut Producer<T>,
    num_producers: usize,
    segment_pool_metadata: *mut Segment<T>,
    segment_cells_pool: *mut UnsafeCell<MaybeUninit<Option<T>>>,
    segment_pool_capacity: usize,
    next_free_segment_idx: AtomicUsize,
    free_segment_list_head: AtomicPtr<Segment<T>>,
    cseg: AtomicPtr<Segment<T>>,
}

unsafe impl<T: Send + Clone + 'static> Send for DQueue<T> {}
unsafe impl<T: Send + Clone + 'static> Sync for DQueue<T> {}

impl<T: Send + Clone + 'static> DQueue<T> {
    pub fn shared_size(num_producers: usize, segment_pool_capacity: usize) -> usize {
        let self_align = mem::align_of::<Self>();
        let producers_align = mem::align_of::<Producer<T>>();
        let segment_meta_align = mem::align_of::<Segment<T>>();
        let cell_align = mem::align_of::<UnsafeCell<MaybeUninit<Option<T>>>>();

        let mut total_size = 0;

        // Align for Self
        total_size = (total_size + self_align - 1) & !(self_align - 1);
        total_size += mem::size_of::<Self>();

        // Align for producers array
        if num_producers > 0 {
            total_size = (total_size + producers_align - 1) & !(producers_align - 1);
            total_size += num_producers * mem::size_of::<Producer<T>>();
        }

        // Align for segment metadata array
        total_size = (total_size + segment_meta_align - 1) & !(segment_meta_align - 1);
        total_size += segment_pool_capacity * mem::size_of::<Segment<T>>();

        // Align for cells array
        total_size = (total_size + cell_align - 1) & !(cell_align - 1);
        total_size += segment_pool_capacity
            * N_SEGMENT_CAPACITY
            * mem::size_of::<UnsafeCell<MaybeUninit<Option<T>>>>();

        // Ensure overall alignment to cache line
        (total_size + 63) & !63
    }

    pub unsafe fn init_in_shared(
        mem_ptr: *mut u8,
        num_producers: usize,
        segment_pool_capacity: usize,
    ) -> &'static mut Self {
        let mut current_offset = 0usize;

        let align_and_advance = |co: &mut usize, size: usize, align: usize| -> *mut u8 {
            *co = (*co + align - 1) & !(align - 1);
            let ptr = mem_ptr.add(*co);
            *co += size;
            ptr
        };

        // Ensure Self is aligned to 64 bytes
        let q_ptr = align_and_advance(&mut current_offset, mem::size_of::<Self>(), 64) as *mut Self;

        // Ensure Producer array is aligned to 64 bytes (since Producer is repr(C, align(64)))
        let p_arr_ptr = if num_producers > 0 {
            align_and_advance(
                &mut current_offset,
                num_producers * mem::size_of::<Producer<T>>(),
                64,
            ) as *mut Producer<T>
        } else {
            ptr::null_mut()
        };
        let seg_pool_meta_ptr = align_and_advance(
            &mut current_offset,
            segment_pool_capacity * mem::size_of::<Segment<T>>(),
            mem::align_of::<Segment<T>>(),
        ) as *mut Segment<T>;
        let seg_cells_pool_ptr = align_and_advance(
            &mut current_offset,
            segment_pool_capacity
                * N_SEGMENT_CAPACITY
                * mem::size_of::<UnsafeCell<MaybeUninit<Option<T>>>>(),
            mem::align_of::<UnsafeCell<MaybeUninit<Option<T>>>>(),
        ) as *mut UnsafeCell<MaybeUninit<Option<T>>>;

        ptr::addr_of_mut!((*q_ptr).q_head).write(AtomicU64::new(0));
        ptr::addr_of_mut!((*q_ptr).q_tail).write(AtomicU64::new(0));
        ptr::addr_of_mut!((*q_ptr).producers_array).write(p_arr_ptr);
        ptr::addr_of_mut!((*q_ptr).num_producers).write(num_producers);
        ptr::addr_of_mut!((*q_ptr).segment_pool_metadata).write(seg_pool_meta_ptr);
        ptr::addr_of_mut!((*q_ptr).segment_cells_pool).write(seg_cells_pool_ptr);
        ptr::addr_of_mut!((*q_ptr).segment_pool_capacity).write(segment_pool_capacity);
        ptr::addr_of_mut!((*q_ptr).next_free_segment_idx).write(AtomicUsize::new(0));
        ptr::addr_of_mut!((*q_ptr).free_segment_list_head).write(AtomicPtr::new(ptr::null_mut()));

        let self_ref_for_alloc = &*q_ptr;
        let init_seg_ptr = self_ref_for_alloc.alloc_segment_from_pool_raw(0, true);
        if init_seg_ptr.is_null() {
            panic!("DQueue: Failed to allocate initial segment.");
        }

        ptr::addr_of_mut!((*q_ptr).qseg).write(AtomicPtr::new(init_seg_ptr));
        ptr::addr_of_mut!((*q_ptr).cseg).write(AtomicPtr::new(init_seg_ptr));
        if num_producers > 0 && !p_arr_ptr.is_null() {
            for i in 0..num_producers {
                Producer::init_in_place(p_arr_ptr.add(i), init_seg_ptr);
            }
        }
        &mut *q_ptr
    }

    unsafe fn alloc_segment_from_pool_raw(&self, seg_id: u64, is_initial: bool) -> *mut Segment<T> {
        let mut head = self.free_segment_list_head.load(Ordering::Acquire);
        while !head.is_null() {
            let next_free_seg = (*head).next_free.load(Ordering::Relaxed);
            match self.free_segment_list_head.compare_exchange_weak(
                head,
                next_free_seg,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(reused_seg_ptr) => {
                    (*reused_seg_ptr).id = seg_id;
                    let original_idx = (reused_seg_ptr as usize
                        - self.segment_pool_metadata as usize)
                        / mem::size_of::<Segment<T>>();
                    (*reused_seg_ptr).cells = self
                        .segment_cells_pool
                        .add(original_idx * N_SEGMENT_CAPACITY);
                    (*reused_seg_ptr)
                        .next
                        .store(ptr::null_mut(), Ordering::Relaxed);
                    (*reused_seg_ptr)
                        .next_free
                        .store(ptr::null_mut(), Ordering::Relaxed);
                    for i in 0..N_SEGMENT_CAPACITY {
                        ptr::write(
                            (*((*reused_seg_ptr).cells.add(i))).get(),
                            MaybeUninit::new(None),
                        );
                    }
                    return reused_seg_ptr;
                }
                Err(new_head) => head = new_head,
            }
        }
        let mut idx;
        if is_initial {
            match self.next_free_segment_idx.compare_exchange(
                0,
                1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(initial_idx_val) => idx = initial_idx_val,
                Err(_current_if_not_zero) => {
                    idx = self.next_free_segment_idx.fetch_add(1, Ordering::Relaxed)
                }
            }
        } else {
            idx = self.next_free_segment_idx.fetch_add(1, Ordering::Relaxed);
        }
        if idx >= self.segment_pool_capacity {
            self.next_free_segment_idx.fetch_sub(1, Ordering::Relaxed);
            return ptr::null_mut();
        }
        let seg_meta_ptr = self.segment_pool_metadata.add(idx);
        let cells_start_ptr = self.segment_cells_pool.add(idx * N_SEGMENT_CAPACITY);
        ptr::addr_of_mut!((*seg_meta_ptr).id).write(seg_id);
        ptr::addr_of_mut!((*seg_meta_ptr).cells).write(cells_start_ptr);
        ptr::addr_of_mut!((*seg_meta_ptr).next).write(AtomicPtr::new(ptr::null_mut()));
        ptr::addr_of_mut!((*seg_meta_ptr).next_free).write(AtomicPtr::new(ptr::null_mut()));
        for i in 0..N_SEGMENT_CAPACITY {
            ptr::write((*(cells_start_ptr.add(i))).get(), MaybeUninit::new(None));
        }
        seg_meta_ptr
    }

    unsafe fn release_segment_to_pool(&self, seg_to_free: *mut Segment<T>) {
        if seg_to_free.is_null() {
            return;
        }
        let mut head = self.free_segment_list_head.load(Ordering::Acquire);
        loop {
            (*seg_to_free).next_free.store(head, Ordering::Relaxed);
            match self.free_segment_list_head.compare_exchange_weak(
                head,
                seg_to_free,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(new_head) => head = new_head,
            }
        }
    }

    unsafe fn new_segment(&self, id: u64) -> *mut Segment<T> {
        self.alloc_segment_from_pool_raw(id, false)
    }

    unsafe fn find_segment(&self, sp_cache: *mut Segment<T>, target_cid: u64) -> *mut Segment<T> {
        let target_segment_id = target_cid / N_SEGMENT_CAPACITY as u64;
        let mut current_seg_ptr = if !sp_cache.is_null() && (*sp_cache).id <= target_segment_id {
            sp_cache
        } else {
            self.qseg.load(Ordering::Acquire)
        };
        if current_seg_ptr.is_null() {
            return ptr::null_mut();
        }
        let mut loop_count = 0;
        let max_loops = self.segment_pool_capacity + self.num_producers + 20;
        while (*current_seg_ptr).id < target_segment_id {
            loop_count += 1;
            if loop_count > max_loops {
                return ptr::null_mut();
            }
            let mut next_ptr = (*current_seg_ptr).next.load(Ordering::Acquire);
            if next_ptr.is_null() {
                let next_expected_id = (*current_seg_ptr).id + 1;
                if next_expected_id <= target_segment_id {
                    let new_seg_ptr = self.new_segment(next_expected_id);
                    if new_seg_ptr.is_null() {
                        next_ptr = (*current_seg_ptr).next.load(Ordering::Acquire);
                        if next_ptr.is_null() {
                            return ptr::null_mut();
                        }
                    } else {
                        match (*current_seg_ptr).next.compare_exchange(
                            ptr::null_mut(),
                            new_seg_ptr,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => next_ptr = new_seg_ptr,
                            Err(existing_next) => {
                                self.release_segment_to_pool(new_seg_ptr);
                                next_ptr = existing_next;
                            }
                        }
                    }
                } else {
                    current_seg_ptr = self.qseg.load(Ordering::Acquire);
                    if current_seg_ptr.is_null() || (*current_seg_ptr).id > target_segment_id {
                        return ptr::null_mut();
                    }
                    continue;
                }
            }
            current_seg_ptr = next_ptr;
            if current_seg_ptr.is_null() {
                return ptr::null_mut();
            }
        }
        if (*current_seg_ptr).id == target_segment_id {
            current_seg_ptr
        } else {
            ptr::null_mut()
        }
    }

    pub unsafe fn run_gc(&self) {
        let q_head_snapshot = self.q_head.load(Ordering::Acquire);
        let consumer_cached_cseg_ptr = self.cseg.load(Ordering::Acquire);

        if consumer_cached_cseg_ptr.is_null() {
            return;
        }

        let mut min_producer_referenced_seg_id = u64::MAX;
        if self.num_producers > 0 {
            for i in 0..self.num_producers {
                let p_struct = self.producers_array.add(i);
                let p_cached_seg = (*p_struct).pseg.load(Ordering::Acquire);
                if !p_cached_seg.is_null() {
                    min_producer_referenced_seg_id =
                        min_producer_referenced_seg_id.min((*p_cached_seg).id);
                }
                let local_h = (*p_struct).local_head.load(Ordering::Relaxed);
                let local_t = (*p_struct).local_tail.load(Ordering::Relaxed);
                let local_buf_ptr = (*p_struct).local_buffer.get();
                let mut current_local_idx = local_h;
                while current_local_idx != local_t {
                    let req_ptr = (*(local_buf_ptr as *const MaybeUninit<Request<T>>)
                        .add(current_local_idx))
                    .as_ptr();
                    min_producer_referenced_seg_id = min_producer_referenced_seg_id
                        .min((*req_ptr).cid / N_SEGMENT_CAPACITY as u64);
                    current_local_idx = Producer::<T>::local_next(current_local_idx);
                }
            }
        } else {
            min_producer_referenced_seg_id = (*consumer_cached_cseg_ptr).id;
        }

        let safe_seg_id = min_producer_referenced_seg_id;

        loop {
            let current_q_seg_val = self.qseg.load(Ordering::Acquire);
            if current_q_seg_val.is_null()
                || (*current_q_seg_val).id >= safe_seg_id
                || current_q_seg_val == consumer_cached_cseg_ptr
            {
                break;
            }
            let end_of_current_q_seg_cid =
                ((*current_q_seg_val).id + 1) * (N_SEGMENT_CAPACITY as u64);
            if q_head_snapshot >= end_of_current_q_seg_cid {
                let next_q_seg_candidate = (*current_q_seg_val).next.load(Ordering::Acquire);
                if next_q_seg_candidate.is_null() {
                    break;
                }
                if self
                    .qseg
                    .compare_exchange(
                        current_q_seg_val,
                        next_q_seg_candidate,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.release_segment_to_pool(current_q_seg_val);
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        let mut prev_seg_ptr = self.qseg.load(Ordering::Acquire);
        if prev_seg_ptr.is_null() {
            return;
        }
        let mut current_seg_to_check_ptr = (*prev_seg_ptr).next.load(Ordering::Acquire);

        while !current_seg_to_check_ptr.is_null()
            && (*current_seg_to_check_ptr).id < (*consumer_cached_cseg_ptr).id
        {
            let current_seg_id = (*current_seg_to_check_ptr).id;
            let mut is_safe_to_reclaim = true;

            if current_seg_id < safe_seg_id {
                is_safe_to_reclaim = false;
            } else {
                for i in 0..self.num_producers {
                    let p_struct = self.producers_array.add(i);
                    let local_h = (*p_struct).local_head.load(Ordering::Relaxed);
                    let local_t = (*p_struct).local_tail.load(Ordering::Relaxed);
                    let local_buf_ptr = (*p_struct).local_buffer.get();
                    let mut current_local_idx = local_h;
                    let mut producer_needs_this_segment = false;
                    while current_local_idx != local_t {
                        let req_ptr = (*(local_buf_ptr as *const MaybeUninit<Request<T>>)
                            .add(current_local_idx))
                        .as_ptr();
                        if (*req_ptr).cid / N_SEGMENT_CAPACITY as u64 == current_seg_id {
                            producer_needs_this_segment = true;
                            break;
                        }
                        current_local_idx = Producer::<T>::local_next(current_local_idx);
                    }
                    if producer_needs_this_segment {
                        is_safe_to_reclaim = false;
                        break;
                    }
                }
            }

            let next_seg_in_list = (*current_seg_to_check_ptr).next.load(Ordering::Acquire);

            if is_safe_to_reclaim {
                if (*prev_seg_ptr)
                    .next
                    .compare_exchange(
                        current_seg_to_check_ptr,
                        next_seg_in_list,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.release_segment_to_pool(current_seg_to_check_ptr);
                    current_seg_to_check_ptr = next_seg_in_list;
                } else {
                    prev_seg_ptr = (*prev_seg_ptr).next.load(Ordering::Acquire);
                    if prev_seg_ptr.is_null() {
                        break;
                    }
                    current_seg_to_check_ptr = (*prev_seg_ptr).next.load(Ordering::Acquire);
                }
            } else {
                prev_seg_ptr = current_seg_to_check_ptr;
                current_seg_to_check_ptr = next_seg_in_list;
            }
        }
    }

    pub unsafe fn dump_local_buffer(&self, producer_idx: usize) {
        if self.num_producers == 0 || producer_idx >= self.num_producers {
            return;
        }
        let p_struct_ptr = self.producers_array.add(producer_idx);
        let local_head_atomic = &(*p_struct_ptr).local_head;
        let local_tail_val = (*p_struct_ptr).local_tail.load(Ordering::Relaxed);
        let local_buf_ptr = (*p_struct_ptr).local_buffer.get();
        let pseg_atomic = &(*p_struct_ptr).pseg;
        let mut current_local_h = local_head_atomic.load(Ordering::Relaxed);
        if current_local_h == local_tail_val {
            return;
        }
        let mut producer_cached_pseg = pseg_atomic.load(Ordering::Acquire);
        let mut iter_count = 0;
        let max_local_iters = L_LOCAL_BUFFER_CAPACITY + 5;
        while current_local_h != local_tail_val {
            iter_count += 1;
            if iter_count > max_local_iters {
                break;
            }
            let req_mu_ptr = (local_buf_ptr as *const MaybeUninit<Request<T>>).add(current_local_h);
            let req_ptr = (*req_mu_ptr).as_ptr();
            let cid = (*req_ptr).cid;
            let target_seg_id = cid / N_SEGMENT_CAPACITY as u64;
            let target_seg_ptr =
                if !producer_cached_pseg.is_null() && (*producer_cached_pseg).id == target_seg_id {
                    producer_cached_pseg
                } else {
                    self.find_segment(producer_cached_pseg, cid)
                };
            if target_seg_ptr.is_null() {
                break;
            }
            if producer_cached_pseg != target_seg_ptr {
                pseg_atomic.store(target_seg_ptr, Ordering::Release);
                producer_cached_pseg = target_seg_ptr;
            }
            let cell_idx = (cid % N_SEGMENT_CAPACITY as u64) as usize;
            let cell_ptr = (*target_seg_ptr).cells.add(cell_idx);
            let val_to_write = ptr::read(&(*req_ptr).val).assume_init();
            ptr::write((*cell_ptr).get(), MaybeUninit::new(Some(val_to_write)));
            current_local_h = Producer::<T>::local_next(current_local_h);
        }
        local_head_atomic.store(current_local_h, Ordering::Release);
    }

    unsafe fn help_enqueue(&self) {
        if self.num_producers == 0 {
            return;
        }
        for i in 0..self.num_producers {
            let p_struct_ptr = self.producers_array.add(i);
            let local_head_atomic = &(*p_struct_ptr).local_head;
            let local_tail_val = (*p_struct_ptr).local_tail.load(Ordering::Acquire);
            let local_buf_ptr = (*p_struct_ptr).local_buffer.get();
            let pseg_atomic = &(*p_struct_ptr).pseg;
            let mut current_local_h = local_head_atomic.load(Ordering::Acquire);
            if current_local_h == local_tail_val {
                continue;
            }
            let mut producer_cached_pseg_hint = pseg_atomic.load(Ordering::Acquire);
            let mut iter_count = 0;
            let max_local_iters = L_LOCAL_BUFFER_CAPACITY + 5;
            while current_local_h != local_tail_val {
                iter_count += 1;
                if iter_count > max_local_iters {
                    break;
                }
                let req_mu_ptr =
                    (local_buf_ptr as *const MaybeUninit<Request<T>>).add(current_local_h);
                let req_ptr = (*req_mu_ptr).as_ptr();
                let cid = (*req_ptr).cid;
                let val_clone = (*(*req_ptr).val.as_ptr()).clone();
                let target_seg_id = cid / N_SEGMENT_CAPACITY as u64;
                let target_seg_ptr = if !producer_cached_pseg_hint.is_null()
                    && (*producer_cached_pseg_hint).id == target_seg_id
                {
                    producer_cached_pseg_hint
                } else {
                    let helper_hint = self.cseg.load(Ordering::Acquire);
                    let hint_to_use =
                        if !helper_hint.is_null() && (*helper_hint).id <= target_seg_id {
                            helper_hint
                        } else {
                            producer_cached_pseg_hint
                        };
                    self.find_segment(hint_to_use, cid)
                };
                if target_seg_ptr.is_null() {
                    break;
                }
                producer_cached_pseg_hint = target_seg_ptr;
                let cell_idx = (cid % N_SEGMENT_CAPACITY as u64) as usize;
                let cell_ptr = (*target_seg_ptr).cells.add(cell_idx);
                let option_ptr_in_cell = (*cell_ptr).get();
                if (*option_ptr_in_cell).as_ptr().is_null()
                    || (*option_ptr_in_cell).assume_init_ref().is_none()
                {
                    ptr::write(option_ptr_in_cell, MaybeUninit::new(Some(val_clone)));
                }
                current_local_h = Producer::<T>::local_next(current_local_h);
            }
        }
    }

    pub fn enqueue(&self, producer_idx: usize, item: T) -> Result<(), ()> {
        if self.num_producers == 0 || producer_idx >= self.num_producers {
            return Err(());
        }
        unsafe {
            let p_struct = self.producers_array.add(producer_idx);
            let local_head = &(*p_struct).local_head;
            let local_tail = &(*p_struct).local_tail;
            let local_buf_ptr = (*p_struct).local_buffer.get();
            let current_local_t_val = local_tail.load(Ordering::Relaxed);
            if Producer::<T>::local_next(current_local_t_val) == local_head.load(Ordering::Acquire)
            {
                self.dump_local_buffer(producer_idx);
                if Producer::<T>::local_next(local_tail.load(Ordering::Relaxed))
                    == local_head.load(Ordering::Acquire)
                {
                    return Err(());
                }
            }
            let cid = self.q_tail.fetch_add(1, Ordering::AcqRel);
            let tail_idx_for_write = local_tail.load(Ordering::Relaxed);
            let req_slot_ptr =
                (local_buf_ptr as *mut MaybeUninit<Request<T>>).add(tail_idx_for_write);
            ptr::write(
                req_slot_ptr,
                MaybeUninit::new(Request {
                    val: MaybeUninit::new(item),
                    cid,
                }),
            );
            local_tail.store(
                Producer::<T>::local_next(tail_idx_for_write),
                Ordering::Release,
            );
        }
        Ok(())
    }

    pub fn dequeue(&self) -> Option<T> {
        unsafe {
            let head_val = self.q_head.load(Ordering::Acquire);
            let mut q_tail_snapshot = self.q_tail.load(Ordering::Acquire);
            if head_val >= q_tail_snapshot {
                let mut producer_has_items = false;
                if self.num_producers > 0 {
                    for i in 0..self.num_producers {
                        let p_struct = self.producers_array.add(i);
                        if (*p_struct).local_head.load(Ordering::Relaxed)
                            != (*p_struct).local_tail.load(Ordering::Relaxed)
                        {
                            producer_has_items = true;
                            break;
                        }
                    }
                }
                if !producer_has_items {
                    q_tail_snapshot = self.q_tail.load(Ordering::Acquire);
                    if head_val >= q_tail_snapshot {
                        return None;
                    }
                }
            }
            let consumer_cached_cseg = self.cseg.load(Ordering::Acquire);
            let mut seg = self.find_segment(consumer_cached_cseg, head_val);
            if seg.is_null() {
                self.help_enqueue();
                q_tail_snapshot = self.q_tail.load(Ordering::Acquire);
                let current_head_val_after_help = self.q_head.load(Ordering::Acquire);
                if current_head_val_after_help >= q_tail_snapshot {
                    return None;
                }
                seg = self.find_segment(
                    self.cseg.load(Ordering::Acquire),
                    current_head_val_after_help,
                );
                if seg.is_null() {
                    return None;
                }

                let cell_idx_retry =
                    (current_head_val_after_help % N_SEGMENT_CAPACITY as u64) as usize;
                let cell_ptr_retry = (*seg).cells.add(cell_idx_retry);
                let item_mu_opt_ptr_retry = (*cell_ptr_retry).get();
                let item_opt_retry = ptr::read(item_mu_opt_ptr_retry).assume_init();

                return match item_opt_retry {
                    Some(item_val_retry) => {
                        ptr::write(item_mu_opt_ptr_retry, MaybeUninit::new(None));
                        self.q_head
                            .store(current_head_val_after_help + 1, Ordering::Release);
                        Some(item_val_retry)
                    }
                    None => {
                        ptr::write(item_mu_opt_ptr_retry, MaybeUninit::new(None));
                        None
                    }
                };
            }
            if seg != consumer_cached_cseg {
                self.cseg.store(seg, Ordering::Release);
            }
            let cell_idx = (head_val % N_SEGMENT_CAPACITY as u64) as usize;
            let cell_ptr = (*seg).cells.add(cell_idx);
            let item_mu_opt_ptr = (*cell_ptr).get();
            let item_opt = ptr::read(item_mu_opt_ptr).assume_init();
            match item_opt {
                Some(item_val) => {
                    ptr::write(item_mu_opt_ptr, MaybeUninit::new(None));
                    self.q_head.store(head_val + 1, Ordering::Release);
                    Some(item_val)
                }
                None => {
                    ptr::write(item_mu_opt_ptr, MaybeUninit::new(None));
                    let tail_now = self.q_tail.load(Ordering::Acquire);
                    if head_val >= tail_now {
                        let mut producer_has_items = false;
                        if self.num_producers > 0 {
                            for i in 0..self.num_producers {
                                let p_struct = self.producers_array.add(i);
                                if (*p_struct).local_head.load(Ordering::Relaxed)
                                    != (*p_struct).local_tail.load(Ordering::Relaxed)
                                {
                                    producer_has_items = true;
                                    break;
                                }
                            }
                        }
                        if !producer_has_items {
                            return None;
                        }
                    }
                    self.help_enqueue();
                    let item_after_help_mu_opt_ptr = (*(*seg).cells.add(cell_idx)).get();
                    let item_opt_after_help = ptr::read(item_after_help_mu_opt_ptr).assume_init();
                    match item_opt_after_help {
                        Some(item_val_after_help) => {
                            ptr::write(item_after_help_mu_opt_ptr, MaybeUninit::new(None));
                            self.q_head.store(head_val + 1, Ordering::Release);
                            Some(item_val_after_help)
                        }
                        None => {
                            ptr::write(item_after_help_mu_opt_ptr, MaybeUninit::new(None));
                            None
                        }
                    }
                }
            }
        }
    }
}

impl<T: Send + Clone + 'static> MpscQueue<T> for DQueue<T> {
    type PushError = ();
    type PopError = ();
    fn push(&self, _item: T) -> Result<(), Self::PushError> {
        panic!("DQueue::push on MpscQueue trait. Use DQueue::enqueue(producer_id, item) or BenchMpscQueue::bench_push.");
    }
    fn pop(&self) -> Result<T, Self::PopError> {
        self.dequeue().ok_or(())
    }
    fn is_empty(&self) -> bool {
        let head = self.q_head.load(Ordering::Acquire);
        let tail = self.q_tail.load(Ordering::Acquire);
        if head >= tail {
            if self.num_producers > 0 {
                for i in 0..self.num_producers {
                    let p = unsafe { self.producers_array.add(i) };
                    unsafe {
                        if (*p).local_head.load(Ordering::Relaxed)
                            != (*p).local_tail.load(Ordering::Relaxed)
                        {
                            return false; // Local buffer has items!
                        }
                    }
                }
            }
            return true;
        }
        false
    }
    fn is_full(&self) -> bool {
        false
    }
}
