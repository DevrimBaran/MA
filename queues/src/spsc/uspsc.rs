use crate::spsc::LamportQueue;
use crate::{DynListQueue, SpscQueue};
use nix::libc;
use std::{
    cell::UnsafeCell,
    mem::{self, ManuallyDrop},
    ptr,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

// Constants - match the paper
const BUF_CAP: usize = 32_768; // Size of each buffer (match RING_CAP in benchmark)
const POOL_CAP: usize = 32; // Number of buffers in pool

// Debug flag - set to true for debugging, false for performance
const DEBUG: bool = false;

#[inline]
fn debug_print(msg: &str) {
    if DEBUG {
        eprintln!("[uSPSC] {}", msg);
    }
}

// Wrapper for segment pointers to make them Send
#[repr(transparent)]
struct SegmentPtr<T: Send + 'static>(*mut LamportQueue<T>);

// Manually implement Copy and Clone
impl<T: Send + 'static> Copy for SegmentPtr<T> {}

impl<T: Send + 'static> Clone for SegmentPtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<T: Send + 'static> Send for SegmentPtr<T> {}
unsafe impl<T: Send + 'static> Sync for SegmentPtr<T> {}

impl<T: Send + 'static> SegmentPtr<T> {
    fn new(ptr: *mut LamportQueue<T>) -> Self {
        Self(ptr)
    }

    fn get(self) -> *mut LamportQueue<T> {
        self.0
    }

    fn is_null(self) -> bool {
        self.0.is_null()
    }
}

// Segment node used to link segments together
#[repr(C)]
struct SegmentNode<T: Send + 'static> {
    segment: *mut LamportQueue<T>,
    next: AtomicPtr<SegmentNode<T>>,
}

// Main queue structure - follow Torquati's design with additional safeguards
#[repr(C, align(128))]
pub struct UnboundedQueue<T: Send + 'static> {
    write_segment: UnsafeCell<*mut LamportQueue<T>>,
    _padding1: [u8; 64], // Padding between write and read pointers

    read_segment: UnsafeCell<*mut LamportQueue<T>>,
    _padding2: [u8; 64], // More padding

    // Add explicit linked list to track segments
    segments_head: AtomicPtr<SegmentNode<T>>,
    segments_tail: UnsafeCell<*mut SegmentNode<T>>,

    segment_mmap_size: AtomicUsize,

    // Paper's pool structure - using dSPSC for inuse list
    inuse: *mut DynListQueue<SegmentPtr<T>>,
    cache: *mut LamportQueue<SegmentPtr<T>>,

    transition_item: UnsafeCell<Option<T>>, // Store items during segment transitions
    initialized: AtomicBool,
}

unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

impl<T: Send + 'static> UnboundedQueue<T> {
    // Allocate a new segment - now truly unbounded
    unsafe fn _allocate_segment(&self) -> Option<*mut LamportQueue<T>> {
        if DEBUG {
            debug_print("Allocating new segment");
        }

        let size_to_mmap = LamportQueue::<T>::shared_size(BUF_CAP);
        if size_to_mmap == 0 {
            if DEBUG {
                debug_print("Error: mmap size is 0");
            }
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
            let err = std::io::Error::last_os_error();
            eprintln!("uSPSC: mmap failed in _allocate_segment: {}", err);
            return None;
        }

        self.segment_mmap_size
            .store(size_to_mmap, Ordering::Release);

        let queue_ptr = LamportQueue::init_in_shared(ptr as *mut u8, BUF_CAP);
        if DEBUG {
            debug_print(&format!("Allocated new segment at {:p}", queue_ptr));
        }

        // Create and add new segment node to our linked list
        let node_ptr = Box::into_raw(Box::new(SegmentNode {
            segment: queue_ptr,
            next: AtomicPtr::new(ptr::null_mut()),
        }));

        // Update the segment list - this ensures segments are never lost
        let prev_tail = *self.segments_tail.get();
        if !prev_tail.is_null() {
            (*prev_tail).next.store(node_ptr, Ordering::Release);
        } else {
            // First segment
            self.segments_head.store(node_ptr, Ordering::Release);
        }
        *self.segments_tail.get() = node_ptr;

        Some(queue_ptr)
    }

    // Deallocate a segment
    unsafe fn _deallocate_segment(&self, segment_ptr: *mut LamportQueue<T>) {
        if segment_ptr.is_null() {
            if DEBUG {
                debug_print("Warning: Attempting to deallocate null segment");
            }
            return;
        }

        if DEBUG {
            debug_print(&format!("Deallocating segment at {:p}", segment_ptr));
        }

        let size_to_munmap = self.segment_mmap_size.load(Ordering::Acquire);
        if size_to_munmap == 0 {
            eprintln!(
                "uSPSC: Warning - _deallocate_segment called with size 0 for segment {:p}",
                segment_ptr
            );
            return;
        }

        // Clean up items if type needs drop
        let segment = &mut *segment_ptr;
        if mem::needs_drop::<T>() {
            if DEBUG {
                debug_print("Type needs drop, cleaning up items");
            }

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

        // Clean up the buffer
        let md_box = ptr::read(&segment.buf);
        let _ = ManuallyDrop::into_inner(md_box);

        // Unmap the memory
        let result = libc::munmap(segment_ptr as *mut libc::c_void, size_to_munmap);
        if result != 0 {
            let err = std::io::Error::last_os_error();
            eprintln!("uSPSC: Error in munmap: {}", err);
        }
    }

    // Check if the queue is properly initialized
    #[inline]
    fn ensure_initialized(&self) -> bool {
        if !self.initialized.load(Ordering::Acquire) {
            if DEBUG {
                debug_print("Queue not initialized");
            }
            return false;
        }

        unsafe {
            let write_ptr = *self.write_segment.get();
            let read_ptr = *self.read_segment.get();

            if write_ptr.is_null() || read_ptr.is_null() {
                if DEBUG {
                    debug_print("Write or read segment is null");
                }
                return false;
            }
        }

        true
    }

    // Paper's pool.next_w() - get new segment for writer
    fn get_new_ring_from_pool_or_alloc(&self) -> Option<*mut LamportQueue<T>> {
        if DEBUG {
            debug_print("Attempting to get ring from pool");
        }

        unsafe {
            // Try cache first (paper line 28)
            if let Ok(segment_ptr) = (*self.cache).pop() {
                if !segment_ptr.is_null() {
                    let raw_ptr = segment_ptr.get();
                    // Reset segment's head and tail pointers
                    let segment = &mut *raw_ptr;
                    segment.head.store(0, Ordering::Release);
                    segment.tail.store(0, Ordering::Release);

                    // Paper line 30: inuse.push(buf)
                    if (*self.inuse).push(segment_ptr).is_ok() {
                        if DEBUG {
                            debug_print(&format!("Got segment from cache: {:p}", raw_ptr));
                        }
                        return Some(raw_ptr);
                    } else {
                        // If we can't track it in inuse, put it back
                        let _ = (*self.cache).push(segment_ptr);
                    }
                }
            }

            // If we couldn't get from cache, allocate new (paper line 29)
            if let Some(segment_ptr) = self._allocate_segment() {
                let wrapped = SegmentPtr::new(segment_ptr);
                // Paper line 30: inuse.push(buf)
                if (*self.inuse).push(wrapped).is_ok() {
                    if DEBUG {
                        debug_print(&format!("Allocated new segment: {:p}", segment_ptr));
                    }
                    return Some(segment_ptr);
                } else {
                    // If we can't track it, deallocate immediately
                    self._deallocate_segment(segment_ptr);
                }
            }
        }

        None
    }

    // Paper's pool.next_r() - get next segment for reader
    fn get_next_segment(&self) -> Result<*mut LamportQueue<T>, ()> {
        unsafe {
            // Paper line 35: return (inuse.pop(&buf)? buf : NULL)
            match (*self.inuse).pop() {
                Ok(segment_ptr) => {
                    if segment_ptr.is_null() {
                        Err(())
                    } else {
                        Ok(segment_ptr.get())
                    }
                }
                Err(_) => Err(()),
            }
        }
    }

    // Paper's pool.release() - recycle segment
    fn recycle_ring_to_pool_or_dealloc(&self, segment_to_recycle: *mut LamportQueue<T>) {
        if segment_to_recycle.is_null() {
            if DEBUG {
                debug_print("Cannot recycle null segment");
            }
            return;
        }

        if DEBUG {
            debug_print(&format!("Recycling segment {:p}", segment_to_recycle));
        }

        unsafe {
            // Paper line 38: buf->reset()
            let segment = &mut *segment_to_recycle;
            segment.head.store(0, Ordering::Release);
            segment.tail.store(0, Ordering::Release);

            // Paper line 39: if (!cache.push(buf)) deallocateSPSC(buf)
            if (*self.cache)
                .push(SegmentPtr::new(segment_to_recycle))
                .is_err()
            {
                if DEBUG {
                    debug_print("Cache full, deallocating segment");
                }
                self._deallocate_segment(segment_to_recycle);
            } else {
                if DEBUG {
                    debug_print("Recycled segment to cache");
                }
            }
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for UnboundedQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        if !self.ensure_initialized() {
            if DEBUG {
                debug_print("Queue not initialized in push");
            }
            return Err(());
        }

        // Get current producer segment
        let current_producer_segment = unsafe { *self.write_segment.get() };
        if current_producer_segment.is_null() {
            if DEBUG {
                debug_print("Producer segment is null");
            }
            return Err(());
        }

        unsafe {
            // First check if we have a pending item
            let transition_ref = &mut *self.transition_item.get();

            if let Some(pending) = transition_ref.take() {
                // Try pushing the pending item first
                let segment = &*current_producer_segment;

                // Check if queue is full (copying logic from LamportQueue::push)
                let tail = segment.tail.load(Ordering::Acquire);
                let next = tail + 1;
                let head = segment.head.load(Ordering::Acquire);

                if next == head + segment.mask + 1 {
                    // Queue is full, get a new segment
                    if DEBUG {
                        debug_print("Current segment full for pending item, getting new one");
                    }

                    // Put pending item back
                    *transition_ref = Some(pending);

                    // Get a new segment
                    let new_segment = match self.get_new_ring_from_pool_or_alloc() {
                        Some(segment) => segment,
                        None => {
                            // Save current item and return Ok - we'll try again next time
                            *transition_ref = Some(item);
                            return Ok(());
                        }
                    };

                    // Update write segment
                    *self.write_segment.get() = new_segment;
                    std::sync::atomic::fence(Ordering::Release);

                    // The following push will be on the new segment
                    let new_segment = &*new_segment;

                    // Attempt to push pending first, then current
                    if let Some(pending) = transition_ref.take() {
                        if new_segment.tail.load(Ordering::Acquire)
                            < new_segment.head.load(Ordering::Acquire) + new_segment.mask
                        {
                            // There's room for the pending item
                            let slot = new_segment.idx(new_segment.tail.load(Ordering::Relaxed));
                            *new_segment.buf[slot].get() = Some(pending);
                            new_segment.tail.store(
                                new_segment.tail.load(Ordering::Relaxed) + 1,
                                Ordering::Release,
                            );
                        } else {
                            // No room for pending item, which is highly unlikely
                            *transition_ref = Some(pending);
                        }
                    }

                    // Now try to push current item
                    if let Some(pending) = transition_ref.take() {
                        // Already have pending item, need to store current item too
                        *transition_ref = Some(item);
                        // Put pending back
                        *transition_ref = Some(pending);
                        return Ok(());
                    } else {
                        // Try to push current item
                        if new_segment.tail.load(Ordering::Acquire)
                            < new_segment.head.load(Ordering::Acquire) + new_segment.mask
                        {
                            // There's room for the current item
                            let slot = new_segment.idx(new_segment.tail.load(Ordering::Relaxed));
                            *new_segment.buf[slot].get() = Some(item);
                            new_segment.tail.store(
                                new_segment.tail.load(Ordering::Relaxed) + 1,
                                Ordering::Release,
                            );
                            return Ok(());
                        } else {
                            // No room for current item either, which is extremely unlikely
                            *transition_ref = Some(item);
                            return Ok(());
                        }
                    }
                } else {
                    // There's room for the pending item
                    let slot = segment.idx(tail);
                    *segment.buf[slot].get() = Some(pending);
                    segment.tail.store(next, Ordering::Release);
                }
            }

            // Now try to push the current item
            let segment = &*current_producer_segment;

            // Check if queue is full
            let tail = segment.tail.load(Ordering::Acquire);
            let next = tail + 1;
            let head = segment.head.load(Ordering::Acquire);

            if next == head + segment.mask + 1 {
                // Queue is full, get a new segment
                if DEBUG {
                    debug_print("Current segment full, getting new one");
                }

                // Get a new segment
                let new_segment = match self.get_new_ring_from_pool_or_alloc() {
                    Some(segment) => segment,
                    None => {
                        // Save current item and return Ok - we'll try again next time
                        *transition_ref = Some(item);
                        return Ok(());
                    }
                };

                // Update write segment
                *self.write_segment.get() = new_segment;
                std::sync::atomic::fence(Ordering::Release);

                // Push to new segment
                let new_segment = &*new_segment;

                // Try to push current item
                if new_segment.tail.load(Ordering::Acquire)
                    < new_segment.head.load(Ordering::Acquire) + new_segment.mask
                {
                    // There's room for the current item
                    let slot = new_segment.idx(new_segment.tail.load(Ordering::Relaxed));
                    *new_segment.buf[slot].get() = Some(item);
                    new_segment.tail.store(
                        new_segment.tail.load(Ordering::Relaxed) + 1,
                        Ordering::Release,
                    );
                    return Ok(());
                } else {
                    // No room for current item, which is unlikely
                    *transition_ref = Some(item);
                    return Ok(());
                }
            } else {
                // There's room for the current item
                let slot = segment.idx(tail);
                *segment.buf[slot].get() = Some(item);
                segment.tail.store(next, Ordering::Release);
                return Ok(());
            }
        }
    }

    fn pop(&self) -> Result<T, Self::PopError> {
        if !self.ensure_initialized() {
            if DEBUG {
                debug_print("Queue not initialized in pop");
            }
            return Err(());
        }

        // Get current consumer segment
        let current_consumer_segment = unsafe { *self.read_segment.get() };
        if current_consumer_segment.is_null() {
            if DEBUG {
                debug_print("Consumer segment is null");
            }
            return Err(());
        }

        // Try to pop from current segment
        match unsafe { (*current_consumer_segment).pop() } {
            Ok(item) => return Ok(item),
            Err(_) => {
                // Segment might be empty, but check if we're done
                if DEBUG {
                    debug_print(&format!(
                        "Pop failed from segment {:p}",
                        current_consumer_segment
                    ));
                }

                // Ensure we see latest producer segment
                std::sync::atomic::fence(Ordering::Acquire);

                // Get current producer segment
                let current_producer_segment = unsafe { *self.write_segment.get() };

                if DEBUG {
                    debug_print(&format!(
                        "Consumer segment: {:p}, Producer segment: {:p}",
                        current_consumer_segment, current_producer_segment
                    ));
                }

                // If producer and consumer on same segment, queue is empty
                if current_consumer_segment == current_producer_segment {
                    if DEBUG {
                        debug_print("Queue empty (same segment)");
                    }
                    return Err(());
                }

                // Check if current segment is empty
                let is_empty = unsafe { (*current_consumer_segment).empty() };
                if is_empty {
                    if DEBUG {
                        debug_print("Current segment empty, moving to next");
                    }

                    // Save old segment for recycling
                    let segment_to_recycle = current_consumer_segment;

                    // Get next segment using our robust method
                    match self.get_next_segment() {
                        Ok(next_segment) => {
                            if next_segment.is_null() {
                                if DEBUG {
                                    debug_print("Next segment is null");
                                }
                                return Err(());
                            }

                            // Update read segment
                            unsafe {
                                *self.read_segment.get() = next_segment;
                            }

                            // Ensure update is visible
                            std::sync::atomic::fence(Ordering::Release);

                            if DEBUG {
                                debug_print(&format!("Moved to next segment: {:p}", next_segment));
                            }

                            // Recycle old segment - this is now safer
                            self.recycle_ring_to_pool_or_dealloc(segment_to_recycle);

                            // Try to pop from the new segment
                            unsafe { (*next_segment).pop() }
                        }
                        Err(_) => {
                            if DEBUG {
                                debug_print("No next segment available");
                            }
                            Err(())
                        }
                    }
                } else {
                    if DEBUG {
                        debug_print("Current segment not empty, retrying pop");
                    }
                    // If segment not empty but pop failed first time, retry
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

        // Check if current segment has room
        unsafe { (*write_ptr).available() }
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

        // Ensure we see latest producer segment
        std::sync::atomic::fence(Ordering::Acquire);

        let write_ptr = unsafe { *self.write_segment.get() };

        // Queue is empty if current segment is empty and it's the same as producer's
        unsafe { (*read_ptr).empty() && read_ptr == write_ptr }
    }
}

impl<T: Send + 'static> UnboundedQueue<T> {
    pub fn shared_size(_segment_size: usize) -> usize {
        // Size includes: Self + DynListQueue + LamportQueue cache
        let self_size = mem::size_of::<Self>();
        let inuse_size = DynListQueue::<SegmentPtr<T>>::shared_size(_segment_size * 2);
        let cache_size = LamportQueue::<SegmentPtr<T>>::shared_size(_segment_size);

        // Align each component to 128 bytes
        let aligned_self = (self_size + 127) & !127;
        let aligned_inuse = (inuse_size + 127) & !127;
        let aligned_cache = (cache_size + 127) & !127;

        aligned_self + aligned_inuse + aligned_cache
    }

    pub unsafe fn init_in_shared(mem_ptr: *mut u8, _segment_size: usize) -> &'static mut Self {
        if DEBUG {
            debug_print("Initializing UnboundedQueue in shared memory");
        }

        // Calculate offsets
        let self_size = mem::size_of::<Self>();
        let aligned_self = (self_size + 127) & !127;

        let inuse_offset = aligned_self;
        let inuse_size = DynListQueue::<SegmentPtr<T>>::shared_size(_segment_size * 2);
        let aligned_inuse = (inuse_size + 127) & !127;

        let cache_offset = aligned_self + aligned_inuse;

        // Initialize DynListQueue for inuse tracking
        let inuse_ptr = mem_ptr.add(inuse_offset);
        let inuse = DynListQueue::init_in_shared(inuse_ptr, _segment_size * 2);

        // Initialize LamportQueue for cache
        let cache_ptr = mem_ptr.add(cache_offset);
        let cache = LamportQueue::init_in_shared(cache_ptr, _segment_size);

        // Create self
        let self_ptr = mem_ptr as *mut Self;

        // Initialize with default values
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
                inuse: inuse as *mut _,
                cache: cache as *mut _,
                transition_item: UnsafeCell::new(None),
                initialized: AtomicBool::new(false),
            },
        );

        let me = &mut *self_ptr;

        // Allocate and initialize first segment
        let initial_segment = me
            ._allocate_segment()
            .expect("uSPSC: Failed to mmap initial segment in init");

        *me.write_segment.get() = initial_segment;
        *me.read_segment.get() = initial_segment;

        // Add initial segment to inuse list
        (*me.inuse).push(SegmentPtr::new(initial_segment)).unwrap();

        // Pre-allocate some segments for the cache
        let pre_allocate = true;

        if pre_allocate {
            let pre_alloc_count = 8.min(POOL_CAP); // Pre-allocate more buffers

            for _ in 0..pre_alloc_count {
                if let Some(segment) = me._allocate_segment() {
                    if (*me.cache).push(SegmentPtr::new(segment)).is_err() {
                        // Cache full, deallocate
                        me._deallocate_segment(segment);
                        break;
                    }

                    if DEBUG {
                        debug_print(&format!("Pre-allocated segment to cache"));
                    }
                }
            }
        }

        // Mark as initialized
        me.initialized.store(true, Ordering::Release);

        if DEBUG {
            debug_print("UnboundedQueue initialization complete");
        }
        me
    }
}

impl<T: Send + 'static> Drop for UnboundedQueue<T> {
    fn drop(&mut self) {
        if DEBUG {
            debug_print("Dropping UnboundedQueue");
        }

        if !self.initialized.load(Ordering::Acquire) {
            if DEBUG {
                debug_print("Queue not initialized, skipping cleanup");
            }
            return;
        }

        // Drop the transition item if there is one
        unsafe {
            if let Some(item) = (*self.transition_item.get()).take() {
                drop(item);
            }
        }

        // Collect segments to deallocate
        let mut segments_to_dealloc: Vec<*mut LamportQueue<T>> = Vec::new();

        // Get read and write segments
        let read_segment = *self.read_segment.get_mut();
        let write_segment = *self.write_segment.get_mut();

        // Clear pointers to prevent use-after-free
        *self.read_segment.get_mut() = ptr::null_mut();
        *self.write_segment.get_mut() = ptr::null_mut();

        // Add to deallocation list if valid
        if !read_segment.is_null() {
            segments_to_dealloc.push(read_segment);
        }

        if !write_segment.is_null() && write_segment != read_segment {
            segments_to_dealloc.push(write_segment);
        }

        unsafe {
            // Process cache
            while let Ok(segment) = (*self.cache).pop() {
                if !segment.is_null() {
                    let raw_ptr = segment.get();
                    if !segments_to_dealloc.contains(&raw_ptr) {
                        segments_to_dealloc.push(raw_ptr);
                    }
                }
            }

            // Process inuse list
            while let Ok(segment) = (*self.inuse).pop() {
                if !segment.is_null() {
                    let raw_ptr = segment.get();
                    if !segments_to_dealloc.contains(&raw_ptr) {
                        segments_to_dealloc.push(raw_ptr);
                    }
                }
            }

            // Process segments from the linked list
            let mut current = self.segments_head.load(Ordering::Acquire);

            while !current.is_null() {
                let next = (*current).next.load(Ordering::Acquire);

                // Add segment to deallocation list if not already there
                let seg_ptr = (*current).segment;
                if !seg_ptr.is_null() && !segments_to_dealloc.contains(&seg_ptr) {
                    segments_to_dealloc.push(seg_ptr);
                }

                // Free the node
                let _ = Box::from_raw(current);

                current = next;
            }
        }

        // Deallocate all segments
        for seg_ptr in segments_to_dealloc {
            unsafe {
                self._deallocate_segment(seg_ptr);
            }
        }

        if DEBUG {
            debug_print("UnboundedQueue drop complete");
        }
        self.initialized.store(false, Ordering::Release);
    }
}
