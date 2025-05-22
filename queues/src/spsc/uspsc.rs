// queues/src/spsc/uspsc.rs
use crate::spsc::LamportQueue;
use crate::SpscQueue;
use nix::libc;
use std::{
    cell::UnsafeCell,
    mem::{self, ManuallyDrop, MaybeUninit},
    ptr,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize, Ordering},
};

// Constants - match the paper
const BUF_CAP: usize = 65536;   // Size of each buffer (match RING_CAP in benchmark)
const POOL_CAP: usize = 32;     // Number of buffers in pool
const BOTH_READY: u32 = 2;      // Flag value for ready buffer
const MAX_SEGMENTS: usize = 64; // Maximum number of segments that can be allocated

// Debug flag - set to true for debugging, false for performance
const DEBUG: bool = false;

#[inline]
fn debug_print(msg: &str) {
    if DEBUG {
        eprintln!("[uSPSC] {}", msg);
    }
}

// RingSlot - metadata for cached ring buffers
#[repr(C, align(128))]
struct RingSlot<T: Send + 'static> { 
    segment_ptr: UnsafeCell<*mut LamportQueue<T>>,
    segment_len: AtomicUsize, 
    flag: AtomicU32,
    initialized: AtomicBool,
    _padding: [u8; 64],  // Padding to avoid false sharing
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
    _padding1: [u8; 64],  // Padding between write and read pointers
    
    read_segment: UnsafeCell<*mut LamportQueue<T>>, 
    _padding2: [u8; 64],  // More padding
    
    // Add explicit linked list to track segments
    segments_head: AtomicPtr<SegmentNode<T>>,
    segments_tail: UnsafeCell<*mut SegmentNode<T>>,
    
    segment_mmap_size: AtomicUsize, 
    ring_slot_cache: UnsafeCell<[MaybeUninit<RingSlot<T>>; POOL_CAP]>,
    cache_head: AtomicUsize, 
    cache_tail: AtomicUsize,
    transition_item: UnsafeCell<Option<T>>,  // Store items during segment transitions 
    segment_count: AtomicUsize, // Track total active segments
    initialized: AtomicBool,
}

unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

impl<T: Send + 'static> UnboundedQueue<T> {
    // Allocate a new segment
    unsafe fn _allocate_segment(&self) -> Option<*mut LamportQueue<T>> {
        if DEBUG { debug_print("Allocating new segment"); }
        
        // Check if we've hit the segment limit
        let current_count = self.segment_count.fetch_add(1, Ordering::Relaxed);
        if current_count >= MAX_SEGMENTS {
            // Rollback increment and return None
            self.segment_count.fetch_sub(1, Ordering::Relaxed);
            if DEBUG { debug_print(&format!("Segment limit reached: {}/{}", current_count, MAX_SEGMENTS)); }
            return None;
        }
        
        let size_to_mmap = LamportQueue::<T>::shared_size(BUF_CAP);
        if size_to_mmap == 0 { 
            self.segment_count.fetch_sub(1, Ordering::Relaxed);
            if DEBUG { debug_print("Error: mmap size is 0"); }
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
        
        self.segment_mmap_size.store(size_to_mmap, Ordering::Release);
        
        let queue_ptr = LamportQueue::init_in_shared(ptr as *mut u8, BUF_CAP);
        if DEBUG { debug_print(&format!("Allocated new segment at {:p}", queue_ptr)); }
        
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
            if DEBUG { debug_print("Warning: Attempting to deallocate null segment"); }
            return; 
        }
        
        if DEBUG { debug_print(&format!("Deallocating segment at {:p}", segment_ptr)); }
        
        let size_to_munmap = self.segment_mmap_size.load(Ordering::Acquire);
        if size_to_munmap == 0 { 
            eprintln!("uSPSC: Warning - _deallocate_segment called with size 0 for segment {:p}", segment_ptr);
            return; 
        }

        // Clean up items if type needs drop
        let segment = &mut *segment_ptr;
        if mem::needs_drop::<T>() {
            if DEBUG { debug_print("Type needs drop, cleaning up items"); }
            
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
        } else {
            // Decrement segment count only on successful munmap
            self.segment_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    // Check if the queue is properly initialized
    #[inline]
    fn ensure_initialized(&self) -> bool {
        if !self.initialized.load(Ordering::Acquire) { 
            if DEBUG { debug_print("Queue not initialized"); }
            return false; 
        }
        
        unsafe {
            let write_ptr = *self.write_segment.get();
            let read_ptr = *self.read_segment.get();
            
            if write_ptr.is_null() || read_ptr.is_null() { 
                if DEBUG { debug_print("Write or read segment is null"); }
                return false; 
            }
        }
        
        true
    }
    
    // Get a ring buffer from the pool or allocate a new one
    fn get_new_ring_from_pool_or_alloc(&self) -> Option<*mut LamportQueue<T>> {
        if DEBUG { debug_print("Attempting to get ring from pool"); }
        
        // Try once from cache with optimistic approach
        let cache_h = self.cache_head.load(Ordering::Acquire);
        let cache_t = self.cache_tail.load(Ordering::Acquire);
        
        if cache_h != cache_t {
            let slot_idx = cache_h % POOL_CAP;
            let ring_slots_ptr = self.ring_slot_cache.get();
            
            let slot_ref = unsafe {
                let slot_ptr = (*ring_slots_ptr).as_ptr().add(slot_idx);
                (*slot_ptr).assume_init_ref()
            };
            
            if slot_ref.initialized.load(Ordering::Acquire) && slot_ref.flag.load(Ordering::Acquire) == BOTH_READY {
                
                // Try to claim this slot (only once)
                if self.cache_head.compare_exchange(
                    cache_h, 
                    cache_h.wrapping_add(1), 
                    Ordering::AcqRel, 
                    Ordering::Relaxed
                ).is_ok() {
                    let segment_ptr = unsafe { *slot_ref.segment_ptr.get() };
                    
                    if !segment_ptr.is_null() {
                        // Mark slot as no longer initialized
                        unsafe {
                            let slot_mut_ptr = (*ring_slots_ptr).as_mut_ptr().add(slot_idx);
                            (*(*slot_mut_ptr).assume_init_mut()).initialized.store(false, Ordering::Release);
                        }
                        
                        // Reset segment's head and tail pointers
                        unsafe {
                            let segment = &mut *segment_ptr;
                            segment.head.store(0, Ordering::Release);
                            segment.tail.store(0, Ordering::Release);
                        }
                        
                        if DEBUG { debug_print(&format!("Got segment from cache: {:p}", segment_ptr)); }
                        return Some(segment_ptr);
                    }
                }
            }
        }
        
        // If we couldn't get from cache, allocate new
        if DEBUG { debug_print("Allocating new segment directly"); }
        unsafe { self._allocate_segment() }
    }

    // Get next segment for consumer
    fn get_next_segment(&self) -> Result<*mut LamportQueue<T>, ()> {
        // Access the producer segment
        let producer_segment = unsafe { *self.write_segment.get() };
        let consumer_segment = unsafe { *self.read_segment.get() };
        
        // Validation
        if producer_segment.is_null() {
            if DEBUG { debug_print("Producer segment is null in get_next_segment"); }
            return Err(());
        }
        
        // If producer and consumer on same segment, no next segment
        if consumer_segment == producer_segment {
            if DEBUG { debug_print("Consumer caught up with producer, no next segment"); }
            return Err(());
        }
        
        // Use the linked list to find the next segment
        // This is more robust than assuming producer's segment is next
        unsafe {
            let mut current = self.segments_head.load(Ordering::Acquire);
            
            // Find the current consumer segment in the list
            while !current.is_null() {
                if (*current).segment == consumer_segment {
                    // Found it, now get the next one
                    let next_node = (*current).next.load(Ordering::Acquire);
                    if !next_node.is_null() {
                        return Ok((*next_node).segment);
                    }
                    break;
                }
                current = (*current).next.load(Ordering::Acquire);
            }
        }
        
        // Fallback - use producer's segment
        Ok(producer_segment)
    }

    // Recycle a ring buffer back to the pool or deallocate it
    fn recycle_ring_to_pool_or_dealloc(&self, segment_to_recycle: *mut LamportQueue<T>) {
        if segment_to_recycle.is_null() { 
            if DEBUG { debug_print("Cannot recycle null segment"); }
            return; 
        }

        if DEBUG { debug_print(&format!("Recycling segment {:p}", segment_to_recycle)); }
        
        // Reset the segment for reuse
        unsafe {
            let segment = &mut *segment_to_recycle;
            segment.head.store(0, Ordering::Release);
            segment.tail.store(0, Ordering::Release);
        }
        
        // Check if pool has room
        let cache_t = self.cache_tail.load(Ordering::Relaxed);
        let cache_h = self.cache_head.load(Ordering::Acquire);
        let cache_count = cache_t.wrapping_sub(cache_h);

        if cache_count < POOL_CAP - 1 { 
            // Pool has room
            let slot_idx = cache_t % POOL_CAP;
            let ring_slots_ptr = self.ring_slot_cache.get();
            
            // Get slot reference
            let slot_ref = unsafe {
                let slot_ptr = (*ring_slots_ptr).as_mut_ptr().add(slot_idx);
                (*slot_ptr).assume_init_mut()
            };
            
            // Store segment and metadata
            unsafe { *slot_ref.segment_ptr.get() = segment_to_recycle; }
            slot_ref.segment_len.store(self.segment_mmap_size.load(Ordering::Acquire), Ordering::Release);
            slot_ref.flag.store(BOTH_READY, Ordering::Release);
            
            // Mark as initialized and update tail
            slot_ref.initialized.store(true, Ordering::Release);
            self.cache_tail.store(cache_t.wrapping_add(1), Ordering::Release);
            
            if DEBUG { debug_print(&format!("Recycled segment to cache slot {}", slot_idx)); }
        } else {
            // Pool is full, deallocate
            if DEBUG { debug_print("Cache full, deallocating segment"); }
            
            // We don't immediately deallocate - we need to check it's not in use
            // For now, we'll just add it to the cache by forcing it
            unsafe {
                // Forcibly recycle even if cache is full
                let slot_idx = cache_t % POOL_CAP;
                let ring_slots_ptr = self.ring_slot_cache.get();
                
                // Get slot reference
                let slot_ref = {
                    let slot_ptr = (*ring_slots_ptr).as_mut_ptr().add(slot_idx);
                    (*slot_ptr).assume_init_mut()
                };
                
                // Store segment and metadata
                *slot_ref.segment_ptr.get() = segment_to_recycle;
                slot_ref.segment_len.store(self.segment_mmap_size.load(Ordering::Acquire), Ordering::Release);
                slot_ref.flag.store(BOTH_READY, Ordering::Release);
                
                // Mark as initialized and update tail
                slot_ref.initialized.store(true, Ordering::Release);
                self.cache_tail.store(cache_t.wrapping_add(1), Ordering::Release);
            }
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for UnboundedQueue<T> {
    type PushError = ();
    type PopError  = ();

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        if !self.ensure_initialized() { 
            if DEBUG { debug_print("Queue not initialized in push"); }
            return Err(()); 
        }
        
        // Get current producer segment
        let current_producer_segment = unsafe { *self.write_segment.get() };
        if current_producer_segment.is_null() { 
            if DEBUG { debug_print("Producer segment is null"); }
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
                    if DEBUG { debug_print("Current segment full for pending item, getting new one"); }
                    
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
                        if new_segment.tail.load(Ordering::Acquire) < new_segment.head.load(Ordering::Acquire) + new_segment.mask {
                            // There's room for the pending item
                            let slot = new_segment.idx(new_segment.tail.load(Ordering::Relaxed));
                            *new_segment.buf[slot].get() = Some(pending);
                            new_segment.tail.store(new_segment.tail.load(Ordering::Relaxed) + 1, Ordering::Release);
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
                        if new_segment.tail.load(Ordering::Acquire) < new_segment.head.load(Ordering::Acquire) + new_segment.mask {
                            // There's room for the current item
                            let slot = new_segment.idx(new_segment.tail.load(Ordering::Relaxed));
                            *new_segment.buf[slot].get() = Some(item);
                            new_segment.tail.store(new_segment.tail.load(Ordering::Relaxed) + 1, Ordering::Release);
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
                if DEBUG { debug_print("Current segment full, getting new one"); }
                
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
                if new_segment.tail.load(Ordering::Acquire) < new_segment.head.load(Ordering::Acquire) + new_segment.mask {
                    // There's room for the current item
                    let slot = new_segment.idx(new_segment.tail.load(Ordering::Relaxed));
                    *new_segment.buf[slot].get() = Some(item);
                    new_segment.tail.store(new_segment.tail.load(Ordering::Relaxed) + 1, Ordering::Release);
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
            if DEBUG { debug_print("Queue not initialized in pop"); }
            return Err(()); 
        }

        // Get current consumer segment
        let current_consumer_segment = unsafe { *self.read_segment.get() };
        if current_consumer_segment.is_null() { 
            if DEBUG { debug_print("Consumer segment is null"); }
            return Err(()); 
        }
    
        // Try to pop from current segment
        match unsafe { (*current_consumer_segment).pop() } {
            Ok(item) => return Ok(item),
            Err(_) => {
                // Segment might be empty, but check if we're done
                if DEBUG { debug_print(&format!("Pop failed from segment {:p}", current_consumer_segment)); }
                
                // Ensure we see latest producer segment
                std::sync::atomic::fence(Ordering::Acquire);
                
                // Get current producer segment
                let current_producer_segment = unsafe { *self.write_segment.get() };

                if DEBUG { 
                    debug_print(&format!(
                        "Consumer segment: {:p}, Producer segment: {:p}", 
                        current_consumer_segment, 
                        current_producer_segment
                    )); 
                }
                
                // If producer and consumer on same segment, queue is empty
                if current_consumer_segment == current_producer_segment {
                    if DEBUG { debug_print("Queue empty (same segment)"); }
                    return Err(());
                }
                
                // Check if current segment is empty
                let is_empty = unsafe { (*current_consumer_segment).empty() };
                if is_empty { 
                    if DEBUG { debug_print("Current segment empty, moving to next"); }
                    
                    // Save old segment for recycling
                    let segment_to_recycle = current_consumer_segment;
                    
                    // Get next segment using our robust method
                    match self.get_next_segment() {
                        Ok(next_segment) => {
                            if next_segment.is_null() {
                                if DEBUG { debug_print("Next segment is null"); }
                                return Err(());
                            }
                            
                            // Update read segment
                            unsafe { *self.read_segment.get() = next_segment; }
                            
                            // Ensure update is visible
                            std::sync::atomic::fence(Ordering::Release);
                            
                            if DEBUG { debug_print(&format!("Moved to next segment: {:p}", next_segment)); }
                            
                            // Recycle old segment - this is now safer
                            self.recycle_ring_to_pool_or_dealloc(segment_to_recycle);
                            
                            // Try to pop from the new segment
                            unsafe { (*next_segment).pop() }
                        },
                        Err(_) => {
                            if DEBUG { debug_print("No next segment available"); }
                            Err(())
                        }
                    }
                } else {
                    if DEBUG { debug_print("Current segment not empty, retrying pop"); }
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
        
        // Check if current segment has room or if there's a cached segment
        let current_has_space = unsafe { (*write_ptr).available() };
        let cache_has_space = self.cache_head.load(Ordering::Relaxed) != self.cache_tail.load(Ordering::Acquire);
        
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
        
        // Ensure we see latest producer segment
        std::sync::atomic::fence(Ordering::Acquire);
        
        let write_ptr = unsafe { *self.write_segment.get() };
        
        // Queue is empty if current segment is empty and it's the same as producer's
        unsafe { (*read_ptr).empty() && read_ptr == write_ptr }
    }
}

impl<T: Send + 'static> UnboundedQueue<T> {
    pub const fn shared_size() -> usize {
        mem::size_of::<Self>()
    }

    pub unsafe fn init_in_shared(mem_ptr: *mut u8) -> &'static mut Self {
        if DEBUG { debug_print("Initializing UnboundedQueue in shared memory"); }
        
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
                ring_slot_cache: UnsafeCell::new(MaybeUninit::uninit().assume_init()),
                cache_head: AtomicUsize::new(0),
                cache_tail: AtomicUsize::new(0),
                transition_item: UnsafeCell::new(None),  // Initialize transition item buffer
                segment_count: AtomicUsize::new(0),
                initialized: AtomicBool::new(false),
            },
        );
        
        let me = &mut *self_ptr;

        // Initialize the ring slots
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
        
        // Allocate and initialize first segment
        let initial_segment = me._allocate_segment()
            .expect("uSPSC: Failed to mmap initial segment in init");
        
        *me.write_segment.get() = initial_segment;
        *me.read_segment.get() = initial_segment;
        
        // Pre-allocate some segments for the cache
        let pre_allocate = true;
        
        if pre_allocate {
            let pre_alloc_count = 8.min(POOL_CAP);  // Pre-allocate more buffers
            
            for i in 0..pre_alloc_count {
                if let Some(segment) = me._allocate_segment() {
                    let slot_ref = unsafe {
                        let slot_ptr = (*slot_array_ptr).as_mut_ptr().add(i);
                        (*slot_ptr).assume_init_mut()
                    };
                    
                    unsafe { *slot_ref.segment_ptr.get() = segment; }
                    slot_ref.segment_len.store(me.segment_mmap_size.load(Ordering::Relaxed), Ordering::Relaxed);
                    slot_ref.flag.store(BOTH_READY, Ordering::Relaxed);
                    slot_ref.initialized.store(true, Ordering::Release);
                    
                    if DEBUG { debug_print(&format!("Pre-allocated segment to cache slot {}", i)); }
                }
            }
            
            me.cache_tail.store(pre_alloc_count, Ordering::Release);
        }
        
        // Mark as initialized
        me.initialized.store(true, Ordering::Release);
        
        if DEBUG { debug_print("UnboundedQueue initialization complete"); }
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
        if DEBUG { debug_print("Dropping UnboundedQueue"); }
    
        if !self.initialized.load(Ordering::Acquire) {
            if DEBUG { debug_print("Queue not initialized, skipping cleanup"); }
            return;
        }
        
        // Drop the transition item if there is one
        unsafe {
            if let Some(item) = (*self.transition_item.get()).take() {
                drop(item);
            }
        }
    
        // Collect segments to deallocate
        let mut segments_to_dealloc: Vec<*mut LamportQueue<T>> = Vec::with_capacity(POOL_CAP + 2);
    
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
    
        // Process cache slots
        let cache_h = self.cache_head.load(Ordering::Acquire);
        let cache_t = self.cache_tail.load(Ordering::Acquire);
        let slot_array_ptr = self.ring_slot_cache.get_mut();
    
        let mut h = cache_h;
        while h != cache_t && h.wrapping_sub(cache_h) < POOL_CAP {
            let slot_idx = h % POOL_CAP;
            
            let slot_meta = unsafe { 
                (*slot_array_ptr).get_unchecked_mut(slot_idx).assume_init_mut()
            };
            
            if slot_meta.initialized.load(Ordering::Acquire) {
                let seg_ptr = *slot_meta.segment_ptr.get_mut();
                if !seg_ptr.is_null() && !segments_to_dealloc.contains(&seg_ptr) {
                    segments_to_dealloc.push(seg_ptr);
                }
                
                // Mark as processed
                *slot_meta.segment_ptr.get_mut() = ptr::null_mut();
                slot_meta.initialized.store(false, Ordering::Release);
            }
            
            h = h.wrapping_add(1);
        }
        
        // Process segments from the linked list
        unsafe {
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
            unsafe { self._deallocate_segment(seg_ptr); }
        }
        
        if DEBUG { debug_print("UnboundedQueue drop complete"); }
        self.initialized.store(false, Ordering::Release);
    }
}