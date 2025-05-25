use crate::spsc::LamportQueue;
use crate::SpscQueue;
use nix::libc;
use std::{
    cell::UnsafeCell,
    mem::{self, ManuallyDrop, MaybeUninit},
    ptr,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize, Ordering},
};

const BUF_CAP: usize = 65536;
const POOL_CAP: usize = 32;
const BOTH_READY: u32 = 2;
const MAX_SEGMENTS: usize = 64;

#[repr(C, align(128))]
struct RingSlot<T: Send + 'static> {
    segment_ptr: UnsafeCell<*mut LamportQueue<T>>,
    segment_len: AtomicUsize,
    flag: AtomicU32,
    initialized: AtomicBool,
    _padding: [u8; 64],
}

#[repr(C)]
struct SegmentNode<T: Send + 'static> {
    segment: *mut LamportQueue<T>,
    next: AtomicPtr<SegmentNode<T>>,
}

#[repr(C, align(128))]
pub struct UnboundedQueue<T: Send + 'static> {
    pub write_segment: UnsafeCell<*mut LamportQueue<T>>,
    _padding1: [u8; 64],

    pub read_segment: UnsafeCell<*mut LamportQueue<T>>,
    _padding2: [u8; 64],

    segments_head: AtomicPtr<SegmentNode<T>>,
    segments_tail: UnsafeCell<*mut SegmentNode<T>>,

    pub segment_mmap_size: AtomicUsize,
    ring_slot_cache: UnsafeCell<[MaybeUninit<RingSlot<T>>; POOL_CAP]>,
    cache_head: AtomicUsize,
    cache_tail: AtomicUsize,
    transition_item: UnsafeCell<Option<T>>,
    segment_count: AtomicUsize,
    initialized: AtomicBool,
}

unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

impl<T: Send + 'static> UnboundedQueue<T> {
    pub unsafe fn _allocate_segment(&self) -> Option<*mut LamportQueue<T>> {
        let current_count = self.segment_count.fetch_add(1, Ordering::Relaxed);
        if current_count >= MAX_SEGMENTS {
            self.segment_count.fetch_sub(1, Ordering::Relaxed);
            return None;
        }

        let size_to_mmap = LamportQueue::<T>::shared_size(BUF_CAP);
        if size_to_mmap == 0 {
            self.segment_count.fetch_sub(1, Ordering::Relaxed);
            return None;
        }

        let ptr = libc::mmap(
            ptr::null_mut(),
            size_to_mmap,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        );

        if ptr == libc::MAP_FAILED {
            self.segment_count.fetch_sub(1, Ordering::Relaxed);
            let err = std::io::Error::last_os_error();
            eprintln!("uSPSC: mmap failed in _allocate_segment: {}", err);
            return None;
        }

        self.segment_mmap_size
            .store(size_to_mmap, Ordering::Release);

        let queue_ptr = LamportQueue::init_in_shared(ptr as *mut u8, BUF_CAP);

        let node_ptr = Box::into_raw(Box::new(SegmentNode {
            segment: queue_ptr,
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        let prev_tail = *self.segments_tail.get();
        if !prev_tail.is_null() {
            (*prev_tail).next.store(node_ptr, Ordering::Release);
        } else {
            self.segments_head.store(node_ptr, Ordering::Release);
        }
        *self.segments_tail.get() = node_ptr;

        Some(queue_ptr)
    }

    pub unsafe fn _deallocate_segment(&self, segment_ptr: *mut LamportQueue<T>) {
        if segment_ptr.is_null() {
            return;
        }

        let size_to_munmap = self.segment_mmap_size.load(Ordering::Acquire);
        if size_to_munmap == 0 {
            eprintln!(
                "uSPSC: Warning - _deallocate_segment called with size 0 for segment {:p}",
                segment_ptr
            );
            return;
        }

        let segment = &mut *segment_ptr;
        if mem::needs_drop::<T>() {
            let head_idx = segment.head.load(Ordering::Acquire);
            let tail_idx = segment.tail.load(Ordering::Acquire);
            let mask = segment.mask;

            let buf_ref = &mut segment.buf;

            let mut current_idx = head_idx;
            while current_idx != tail_idx {
                let slot_idx = current_idx & mask;
                if slot_idx < buf_ref.len() {
                    let cell_ref = &buf_ref[slot_idx];
                    let option_ref = &mut *cell_ref.get();
                    if let Some(item) = option_ref.take() {
                        drop(item);
                    }
                }
                current_idx = current_idx.wrapping_add(1);
            }
        }

        let md_box = ptr::read(&segment.buf);
        let _ = ManuallyDrop::into_inner(md_box);

        let result = libc::munmap(segment_ptr as *mut libc::c_void, size_to_munmap);
        if result != 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("uSPSC: Error in munmap: {}", err);
        } else {
            self.segment_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn ensure_initialized(&self) -> bool {
        if !self.initialized.load(Ordering::Acquire) {
            return false;
        }

        unsafe {
            let write_ptr = *self.write_segment.get();
            let read_ptr = *self.read_segment.get();

            if write_ptr.is_null() || read_ptr.is_null() {
                return false;
            }
        }

        true
    }

    fn get_new_ring_from_pool_or_alloc(&self) -> Option<*mut LamportQueue<T>> {
        let cache_h = self.cache_head.load(Ordering::Acquire);
        let cache_t = self.cache_tail.load(Ordering::Acquire);

        if cache_h != cache_t {
            let slot_idx = cache_h % POOL_CAP;
            let ring_slots_ptr = self.ring_slot_cache.get();

            let slot_ref = unsafe {
                let slot_ptr = (*ring_slots_ptr).as_ptr().add(slot_idx);
                (*slot_ptr).assume_init_ref()
            };

            if slot_ref.initialized.load(Ordering::Acquire)
                && slot_ref.flag.load(Ordering::Acquire) == BOTH_READY
            {
                if self
                    .cache_head
                    .compare_exchange(
                        cache_h,
                        cache_h.wrapping_add(1),
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    let segment_ptr = unsafe { *slot_ref.segment_ptr.get() };

                    if !segment_ptr.is_null() {
                        unsafe {
                            let slot_mut_ptr = (*ring_slots_ptr).as_mut_ptr().add(slot_idx);
                            (*(*slot_mut_ptr).assume_init_mut())
                                .initialized
                                .store(false, Ordering::Release);
                        }

                        unsafe {
                            let segment = &mut *segment_ptr;
                            segment.head.store(0, Ordering::Release);
                            segment.tail.store(0, Ordering::Release);
                        }
                        return Some(segment_ptr);
                    }
                }
            }
        }

        unsafe { self._allocate_segment() }
    }

    fn get_next_segment(&self) -> Result<*mut LamportQueue<T>, ()> {
        let producer_segment = unsafe { *self.write_segment.get() };
        let consumer_segment = unsafe { *self.read_segment.get() };

        if producer_segment.is_null() {
            return Err(());
        }

        if consumer_segment == producer_segment {
            return Err(());
        }

        unsafe {
            let mut current = self.segments_head.load(Ordering::Acquire);

            while !current.is_null() {
                if (*current).segment == consumer_segment {
                    let next_node = (*current).next.load(Ordering::Acquire);
                    if !next_node.is_null() {
                        return Ok((*next_node).segment);
                    }
                    break;
                }
                current = (*current).next.load(Ordering::Acquire);
            }
        }

        Ok(producer_segment)
    }

    fn recycle_ring_to_pool_or_dealloc(&self, segment_to_recycle: *mut LamportQueue<T>) {
        if segment_to_recycle.is_null() {
            return;
        }

        unsafe {
            let segment = &mut *segment_to_recycle;
            segment.head.store(0, Ordering::Release);
            segment.tail.store(0, Ordering::Release);
        }

        let cache_t = self.cache_tail.load(Ordering::Relaxed);
        let cache_h = self.cache_head.load(Ordering::Acquire);
        let cache_count = cache_t.wrapping_sub(cache_h);

        if cache_count < POOL_CAP - 1 {
            let slot_idx = cache_t % POOL_CAP;
            let ring_slots_ptr = self.ring_slot_cache.get();

            let slot_ref = unsafe {
                let slot_ptr = (*ring_slots_ptr).as_mut_ptr().add(slot_idx);
                (*slot_ptr).assume_init_mut()
            };

            unsafe {
                *slot_ref.segment_ptr.get() = segment_to_recycle;
            }
            slot_ref.segment_len.store(
                self.segment_mmap_size.load(Ordering::Acquire),
                Ordering::Release,
            );
            slot_ref.flag.store(BOTH_READY, Ordering::Release);

            slot_ref.initialized.store(true, Ordering::Release);
            self.cache_tail
                .store(cache_t.wrapping_add(1), Ordering::Release);
        } else {
            unsafe {
                let slot_idx = cache_t % POOL_CAP;
                let ring_slots_ptr = self.ring_slot_cache.get();

                let slot_ref = {
                    let slot_ptr = (*ring_slots_ptr).as_mut_ptr().add(slot_idx);
                    (*slot_ptr).assume_init_mut()
                };

                *slot_ref.segment_ptr.get() = segment_to_recycle;
                slot_ref.segment_len.store(
                    self.segment_mmap_size.load(Ordering::Acquire),
                    Ordering::Release,
                );
                slot_ref.flag.store(BOTH_READY, Ordering::Release);

                slot_ref.initialized.store(true, Ordering::Release);
                self.cache_tail
                    .store(cache_t.wrapping_add(1), Ordering::Release);
            }
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for UnboundedQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        if !self.ensure_initialized() {
            return Err(());
        }

        let current_producer_segment = unsafe { *self.write_segment.get() };
        if current_producer_segment.is_null() {
            return Err(());
        }

        unsafe {
            let transition_ref = &mut *self.transition_item.get();

            if let Some(pending) = transition_ref.take() {
                let segment = &*current_producer_segment;

                let tail = segment.tail.load(Ordering::Acquire);
                let next = tail + 1;
                let head = segment.head.load(Ordering::Acquire);

                if next == head + segment.mask + 1 {
                    *transition_ref = Some(pending);

                    let new_segment = match self.get_new_ring_from_pool_or_alloc() {
                        Some(segment) => segment,
                        None => {
                            *transition_ref = Some(item);
                            return Ok(());
                        }
                    };

                    *self.write_segment.get() = new_segment;
                    std::sync::atomic::fence(Ordering::Release);

                    let new_segment = &*new_segment;

                    if let Some(pending) = transition_ref.take() {
                        if new_segment.tail.load(Ordering::Acquire)
                            < new_segment.head.load(Ordering::Acquire) + new_segment.mask
                        {
                            let slot = new_segment.idx(new_segment.tail.load(Ordering::Relaxed));
                            *new_segment.buf[slot].get() = Some(pending);
                            new_segment.tail.store(
                                new_segment.tail.load(Ordering::Relaxed) + 1,
                                Ordering::Release,
                            );
                        } else {
                            *transition_ref = Some(pending);
                        }
                    }

                    if let Some(pending) = transition_ref.take() {
                        *transition_ref = Some(item);

                        *transition_ref = Some(pending);
                        return Ok(());
                    } else {
                        if new_segment.tail.load(Ordering::Acquire)
                            < new_segment.head.load(Ordering::Acquire) + new_segment.mask
                        {
                            let slot = new_segment.idx(new_segment.tail.load(Ordering::Relaxed));
                            *new_segment.buf[slot].get() = Some(item);
                            new_segment.tail.store(
                                new_segment.tail.load(Ordering::Relaxed) + 1,
                                Ordering::Release,
                            );
                            return Ok(());
                        } else {
                            *transition_ref = Some(item);
                            return Ok(());
                        }
                    }
                } else {
                    let slot = segment.idx(tail);
                    *segment.buf[slot].get() = Some(pending);
                    segment.tail.store(next, Ordering::Release);
                }
            }

            let segment = &*current_producer_segment;

            let tail = segment.tail.load(Ordering::Acquire);
            let next = tail + 1;
            let head = segment.head.load(Ordering::Acquire);

            if next == head + segment.mask + 1 {
                let new_segment = match self.get_new_ring_from_pool_or_alloc() {
                    Some(segment) => segment,
                    None => {
                        *transition_ref = Some(item);
                        return Ok(());
                    }
                };

                *self.write_segment.get() = new_segment;
                std::sync::atomic::fence(Ordering::Release);

                let new_segment = &*new_segment;

                if new_segment.tail.load(Ordering::Acquire)
                    < new_segment.head.load(Ordering::Acquire) + new_segment.mask
                {
                    let slot = new_segment.idx(new_segment.tail.load(Ordering::Relaxed));
                    *new_segment.buf[slot].get() = Some(item);
                    new_segment.tail.store(
                        new_segment.tail.load(Ordering::Relaxed) + 1,
                        Ordering::Release,
                    );
                    return Ok(());
                } else {
                    *transition_ref = Some(item);
                    return Ok(());
                }
            } else {
                let slot = segment.idx(tail);
                *segment.buf[slot].get() = Some(item);
                segment.tail.store(next, Ordering::Release);
                return Ok(());
            }
        }
    }

    fn pop(&self) -> Result<T, Self::PopError> {
        if !self.ensure_initialized() {
            return Err(());
        }

        let current_consumer_segment = unsafe { *self.read_segment.get() };
        if current_consumer_segment.is_null() {
            return Err(());
        }

        match unsafe { (*current_consumer_segment).pop() } {
            Ok(item) => return Ok(item),
            Err(_) => {
                std::sync::atomic::fence(Ordering::Acquire);

                let current_producer_segment = unsafe { *self.write_segment.get() };

                if current_consumer_segment == current_producer_segment {
                    return Err(());
                }

                let is_empty = unsafe { (*current_consumer_segment).empty() };
                if is_empty {
                    let segment_to_recycle = current_consumer_segment;

                    match self.get_next_segment() {
                        Ok(next_segment) => {
                            if next_segment.is_null() {
                                return Err(());
                            }

                            unsafe {
                                *self.read_segment.get() = next_segment;
                            }

                            std::sync::atomic::fence(Ordering::Release);

                            self.recycle_ring_to_pool_or_dealloc(segment_to_recycle);

                            unsafe { (*next_segment).pop() }
                        }
                        Err(_) => Err(()),
                    }
                } else {
                    unsafe { (*current_consumer_segment).pop() }
                }
            }
        }
    }

    #[inline]
    fn available(&self) -> bool {
        if !self.ensure_initialized() {
            return false;
        }

        let write_ptr = unsafe { *self.write_segment.get() };
        if write_ptr.is_null() {
            return false;
        }

        let current_has_space = unsafe { (*write_ptr).available() };
        let cache_has_space =
            self.cache_head.load(Ordering::Relaxed) != self.cache_tail.load(Ordering::Acquire);

        current_has_space || cache_has_space
    }

    #[inline]
    fn empty(&self) -> bool {
        if !self.ensure_initialized() {
            return true;
        }

        let read_ptr = unsafe { *self.read_segment.get() };
        if read_ptr.is_null() {
            return true;
        }

        std::sync::atomic::fence(Ordering::Acquire);

        let write_ptr = unsafe { *self.write_segment.get() };

        unsafe { (*read_ptr).empty() && read_ptr == write_ptr }
    }
}

impl<T: Send + 'static> UnboundedQueue<T> {
    pub const fn shared_size() -> usize {
        mem::size_of::<Self>()
    }

    pub unsafe fn init_in_shared(mem_ptr: *mut u8) -> &'static mut Self {
        let self_ptr = mem_ptr as *mut Self;

        ptr::write(
            self_ptr,
            Self {
                write_segment: UnsafeCell::new(ptr::null_mut()),
                _padding1: [0; 64],
                read_segment: UnsafeCell::new(ptr::null_mut()),
                _padding2: [0; 64],
                segments_head: AtomicPtr::new(ptr::null_mut()),
                segments_tail: UnsafeCell::new(ptr::null_mut()),
                segment_mmap_size: AtomicUsize::new(0),
                ring_slot_cache: UnsafeCell::new(MaybeUninit::uninit().assume_init()),
                cache_head: AtomicUsize::new(0),
                cache_tail: AtomicUsize::new(0),
                transition_item: UnsafeCell::new(None),
                segment_count: AtomicUsize::new(0),
                initialized: AtomicBool::new(false),
            },
        );

        let me = &mut *self_ptr;

        let slot_array_ptr = me.ring_slot_cache.get();
        for i in 0..POOL_CAP {
            let ring_slot_ptr = (*slot_array_ptr).as_mut_ptr().add(i);
            ring_slot_ptr.write(MaybeUninit::new(RingSlot {
                segment_ptr: UnsafeCell::new(ptr::null_mut()),
                segment_len: AtomicUsize::new(0),
                flag: AtomicU32::new(0),
                initialized: AtomicBool::new(false),
                _padding: [0; 64],
            }));
        }

        let initial_segment = me
            ._allocate_segment()
            .expect("uSPSC: Failed to mmap initial segment in init");

        *me.write_segment.get() = initial_segment;
        *me.read_segment.get() = initial_segment;

        let pre_allocate = true;

        if pre_allocate {
            let pre_alloc_count = 8.min(POOL_CAP);

            for i in 0..pre_alloc_count {
                if let Some(segment) = me._allocate_segment() {
                    let slot_ref = unsafe {
                        let slot_ptr = (*slot_array_ptr).as_mut_ptr().add(i);
                        (*slot_ptr).assume_init_mut()
                    };

                    unsafe {
                        *slot_ref.segment_ptr.get() = segment;
                    }
                    slot_ref.segment_len.store(
                        me.segment_mmap_size.load(Ordering::Relaxed),
                        Ordering::Relaxed,
                    );
                    slot_ref.flag.store(BOTH_READY, Ordering::Relaxed);
                    slot_ref.initialized.store(true, Ordering::Release);
                }
            }

            me.cache_tail.store(pre_alloc_count, Ordering::Release);
        }

        me.initialized.store(true, Ordering::Release);
        me
    }
}

impl<T: Send + 'static> Drop for UnboundedQueue<T> {
    fn drop(&mut self) {
        unsafe {
            if let Some(item) = (*self.transition_item.get()).take() {
                drop(item);
            }
        }

        if !self.initialized.load(Ordering::Acquire) {
            return;
        }

        unsafe {
            if let Some(item) = (*self.transition_item.get()).take() {
                drop(item);
            }
        }

        let mut segments_to_dealloc: Vec<*mut LamportQueue<T>> = Vec::with_capacity(POOL_CAP + 2);

        let read_segment = *self.read_segment.get_mut();
        let write_segment = *self.write_segment.get_mut();

        *self.read_segment.get_mut() = ptr::null_mut();
        *self.write_segment.get_mut() = ptr::null_mut();

        if !read_segment.is_null() {
            segments_to_dealloc.push(read_segment);
        }

        if !write_segment.is_null() && write_segment != read_segment {
            segments_to_dealloc.push(write_segment);
        }

        let cache_h = self.cache_head.load(Ordering::Acquire);
        let cache_t = self.cache_tail.load(Ordering::Acquire);
        let slot_array_ptr = self.ring_slot_cache.get_mut();

        let mut h = cache_h;
        while h != cache_t && h.wrapping_sub(cache_h) < POOL_CAP {
            let slot_idx = h % POOL_CAP;

            let slot_meta = unsafe {
                (*slot_array_ptr)
                    .get_unchecked_mut(slot_idx)
                    .assume_init_mut()
            };

            if slot_meta.initialized.load(Ordering::Acquire) {
                let seg_ptr = *slot_meta.segment_ptr.get_mut();
                if !seg_ptr.is_null() && !segments_to_dealloc.contains(&seg_ptr) {
                    segments_to_dealloc.push(seg_ptr);
                }

                *slot_meta.segment_ptr.get_mut() = ptr::null_mut();
                slot_meta.initialized.store(false, Ordering::Release);
            }

            h = h.wrapping_add(1);
        }

        unsafe {
            let mut current = self.segments_head.load(Ordering::Acquire);

            while !current.is_null() {
                let next = (*current).next.load(Ordering::Acquire);

                let seg_ptr = (*current).segment;
                if !seg_ptr.is_null() && !segments_to_dealloc.contains(&seg_ptr) {
                    segments_to_dealloc.push(seg_ptr);
                }

                let _ = Box::from_raw(current);

                current = next;
            }
        }

        for seg_ptr in segments_to_dealloc {
            unsafe {
                self._deallocate_segment(seg_ptr);
            }
        }
        self.initialized.store(false, Ordering::Release);
    }
}
