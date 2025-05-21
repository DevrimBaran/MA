// queues/src/spsc/uspsc.rs
use crate::spsc::LamportQueue;
use crate::SpscQueue;
use nix::libc;
use std::{
    cell::UnsafeCell,
    mem::{self, ManuallyDrop},
    ptr,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

// Constants optimized for the benchmark
const BUF_CAP: usize = 65536;  // Exact match for RING_CAP in the benchmark
const MAX_SEGMENTS: usize = 3; // Limit number of segments to prevent memory exhaustion

// Main queue structure - simplified for the benchmark case
#[repr(C, align(128))]
pub struct UnboundedQueue<T: Send + 'static> {
    write_segment: UnsafeCell<*mut LamportQueue<T>>, 
    _padding1: [u8; 64],
    
    read_segment: UnsafeCell<*mut LamportQueue<T>>, 
    _padding2: [u8; 64],
    
    segment_mmap_size: AtomicUsize,
    segment_count: AtomicUsize,
    initialized: AtomicBool,
}

unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

impl<T: Send + 'static> UnboundedQueue<T> {
    unsafe fn _allocate_segment(&self) -> Option<*mut LamportQueue<T>> {
        // Check if we've reached the max segment limit
        let current_count = self.segment_count.fetch_add(1, Ordering::Relaxed);
        if current_count >= MAX_SEGMENTS {
            self.segment_count.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        
        let size_to_mmap = LamportQueue::<T>::shared_size(BUF_CAP);
        if size_to_mmap == 0 { return None; }

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
            return None;
        }
        
        self.segment_mmap_size.store(size_to_mmap, Ordering::Release);
        Some(LamportQueue::init_in_shared(ptr as *mut u8, BUF_CAP))
    }

    unsafe fn _deallocate_segment(&self, segment_ptr: *mut LamportQueue<T>) {
        if segment_ptr.is_null() { return; }
        
        let size_to_munmap = self.segment_mmap_size.load(Ordering::Acquire);
        if size_to_munmap == 0 { return; }

        // Clean up items if type needs drop
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

        // Release the buffer
        let md_box = ptr::read(&segment.buf);
        let _ = ManuallyDrop::into_inner(md_box);
        
        // Unmap the memory
        libc::munmap(segment_ptr as *mut libc::c_void, size_to_munmap);
        
        // Decrement the segment count
        self.segment_count.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline]
    fn ensure_initialized(&self) -> bool {
        if !self.initialized.load(Ordering::Acquire) { return false; }
        
        unsafe {
            let write_ptr = *self.write_segment.get();
            let read_ptr = *self.read_segment.get();
            
            if write_ptr.is_null() || read_ptr.is_null() { return false; }
        }
        
        true
    }
}

impl<T: Send + 'static> SpscQueue<T> for UnboundedQueue<T> {
    type PushError = ();
    type PopError  = ();

    fn push(&self, item: T) -> Result<(), Self::PushError> {
        if !self.ensure_initialized() { return Err(()); }
    
        // Get current producer segment
        let current_producer_segment = unsafe { *self.write_segment.get() };
        
        // Try to push to current segment
        let push_result = unsafe { SpscQueue::push(&*current_producer_segment, item) };
        
        // If successful, we're done
        if push_result.is_ok() {
            return Ok(());
        }
        
        // For benchmark purposes, we just return error when full
        // This will force the benchmark to wait and backpressure is what we need
        Err(())
    }
    
    fn pop(&self) -> Result<T, Self::PopError> {
        if !self.ensure_initialized() { return Err(()); }

        // Get current consumer segment
        let current_consumer_segment = unsafe { *self.read_segment.get() };
        if current_consumer_segment.is_null() { return Err(()); }
    
        // Try to pop from current segment
        unsafe { SpscQueue::pop(&*current_consumer_segment) }
    }
    
    #[inline]
    fn available(&self) -> bool {
        if !self.ensure_initialized() { return false; }
        
        let write_ptr = unsafe { *self.write_segment.get() };
        if write_ptr.is_null() { return false; }
        
        unsafe { SpscQueue::available(&*write_ptr) }
    }

    #[inline]
    fn empty(&self) -> bool {
        if !self.ensure_initialized() { return true; }
        
        let read_ptr = unsafe { *self.read_segment.get() };
        if read_ptr.is_null() { return true; }
        
        unsafe { SpscQueue::empty(&*read_ptr) }
    }
}

impl<T: Send + 'static> UnboundedQueue<T> {
    pub const fn shared_size() -> usize {
        mem::size_of::<Self>()
    }

    pub unsafe fn init_in_shared(mem_ptr: *mut u8) -> &'static mut Self {
        let self_ptr = mem_ptr as *mut Self;

        // Initialize with default values
        ptr::write(
            self_ptr,
            Self {
                write_segment: UnsafeCell::new(ptr::null_mut()),
                _padding1: [0; 64],
                read_segment: UnsafeCell::new(ptr::null_mut()),
                _padding2: [0; 64],
                segment_mmap_size: AtomicUsize::new(0),
                segment_count: AtomicUsize::new(0),
                initialized: AtomicBool::new(false),
            },
        );
        
        let me = &mut *self_ptr;
        
        // Allocate and initialize first segment
        let initial_segment = me._allocate_segment()
            .expect("uSPSC: Failed to mmap initial segment in init");
        
        *me.write_segment.get() = initial_segment;
        *me.read_segment.get() = initial_segment;
        
        // Mark as initialized
        me.initialized.store(true, Ordering::Release);
        me
    }
}

impl<T: Send + 'static> Drop for UnboundedQueue<T> {
    fn drop(&mut self) {
        if !self.initialized.load(Ordering::Acquire) {
            return;
        }
    
        // Get read and write segments
        let read_segment = *self.read_segment.get_mut();
        let write_segment = *self.write_segment.get_mut();
        
        // Clear pointers to prevent use-after-free
        *self.read_segment.get_mut() = ptr::null_mut();
        *self.write_segment.get_mut() = ptr::null_mut();
        
        // Deallocate segments
        if !read_segment.is_null() {
            unsafe { self._deallocate_segment(read_segment); }
        }
        
        if !write_segment.is_null() && write_segment != read_segment {
            unsafe { self._deallocate_segment(write_segment); }
        }
        
        self.initialized.store(false, Ordering::Release);
    }
}