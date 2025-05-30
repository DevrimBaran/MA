use crate::spsc::LamportQueue;
use crate::{DynListQueue, SpscQueue};
use std::{
    ptr,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

const POOL_CACHE_SIZE: usize = 32;
const MAX_SEGMENTS: usize = 64;

// Wrapper for raw pointer to make it Send/Sync
#[derive(Copy, Clone)]
struct LamportPtr<T: Send + 'static>(*mut LamportQueue<T>);

unsafe impl<T: Send + 'static> Send for LamportPtr<T> {}
unsafe impl<T: Send + 'static> Sync for LamportPtr<T> {}

// Modified BufferPool for shared memory
#[repr(C)]
struct BufferPool<T: Send + 'static> {
    // Pointers to queues in shared memory
    inuse_ptr: *mut DynListQueue<LamportPtr<T>>,
    cache_ptr: *mut LamportQueue<LamportPtr<T>>,
    segment_size: usize,
    segment_count: AtomicUsize,
}

impl<T: Send + 'static> BufferPool<T> {
    unsafe fn inuse(&self) -> &DynListQueue<LamportPtr<T>> {
        &*self.inuse_ptr
    }

    unsafe fn cache(&self) -> &LamportQueue<LamportPtr<T>> {
        &*self.cache_ptr
    }

    // Paper: next_w() - Figure 3, lines 26-32
    fn next_w(&self) -> Option<*mut LamportQueue<T>> {
        unsafe {
            // Try to get from cache first
            if let Ok(buf_wrapper) = self.cache().pop() {
                let buf_ptr = buf_wrapper.0;
                // Add to inuse queue
                if self.inuse().push(buf_wrapper).is_err() {
                    // If inuse is full, put back in cache
                    let _ = self.cache().push(LamportPtr(buf_ptr));
                    return None;
                }
                return Some(buf_ptr);
            }
        }

        // In shared memory version, all segments are pre-allocated
        // We can't allocate new ones dynamically
        None
    }

    // Paper: next_r() - Figure 3, lines 33-36
    fn next_r(&self) -> Option<*mut LamportQueue<T>> {
        unsafe { self.inuse().pop().ok().map(|wrapper| wrapper.0) }
    }

    // Paper: release() - Figure 3, lines 37-40
    fn release(&self, buf: *mut LamportQueue<T>) {
        if buf.is_null() {
            return;
        }

        unsafe {
            // Reset the buffer (paper line 38)
            (*buf).head.store(0, Ordering::Relaxed);
            (*buf).tail.store(0, Ordering::Relaxed);

            // Try to cache it
            if self.cache().push(LamportPtr(buf)).is_err() {
                // Cache is full - in shared memory we can't deallocate
                // This is a limitation of the shared memory version
            }
        }
    }
}

// Paper's uSPSC structure - Figure 3, top
#[repr(C, align(128))]
pub struct UnboundedQueue<T: Send + 'static> {
    // Paper: buf_r - reader's buffer
    buf_r: AtomicPtr<LamportQueue<T>>,
    _padding1: [u8; 120],

    // Paper: buf_w - writer's buffer
    buf_w: AtomicPtr<LamportQueue<T>>,
    _padding2: [u8; 120],

    // Paper: Pool
    pool: BufferPool<T>,

    // Paper: size (segment size)
    size: usize,

    // Additional fields for compatibility
    initialized: AtomicBool,
    pub segment_count: AtomicUsize, // For testing
}

unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

impl<T: Send + 'static> UnboundedQueue<T> {
    pub fn with_capacity(segment_size: usize) -> Self {
        panic!("Use init_in_shared for shared memory version");
    }

    pub fn shared_size(segment_size: usize) -> usize {
        use std::alloc::Layout;

        // Calculate total size needed for shared memory version
        let self_layout = Layout::new::<Self>();
        let dspsc_size = DynListQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);
        let cache_size = LamportQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);

        // Pre-allocate space for multiple segments
        let segments_size = LamportQueue::<T>::shared_size(segment_size) * MAX_SEGMENTS;

        // Align each component properly
        let mut total = self_layout.size();
        total = (total + 127) & !127; // Align to 128 bytes
        total += dspsc_size;
        total = (total + 127) & !127;
        total += cache_size;
        total = (total + 127) & !127;
        total += segments_size;

        total
    }

    pub unsafe fn init_in_shared(mem_ptr: *mut u8, segment_size: usize) -> &'static mut Self {
        use std::alloc::Layout;

        assert!(segment_size > 1, "uSPSC requires size > 1 (Theorem 4.2)");
        assert!(
            segment_size.is_power_of_two(),
            "Segment size must be power of 2"
        );

        // Zero out the entire memory region first
        let total_size = Self::shared_size(segment_size);
        std::ptr::write_bytes(mem_ptr, 0, total_size);

        let self_ptr = mem_ptr as *mut Self;
        let mut offset = Layout::new::<Self>().size();
        offset = (offset + 127) & !127; // Align to 128

        // Initialize dSPSC queue in shared memory
        let dspsc_ptr = mem_ptr.add(offset);
        let inuse_queue = DynListQueue::init_in_shared(dspsc_ptr, POOL_CACHE_SIZE);
        let dspsc_size = DynListQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);
        offset += dspsc_size;
        offset = (offset + 127) & !127;

        // Initialize cache queue in shared memory
        let cache_ptr = mem_ptr.add(offset);
        let cache_queue = LamportQueue::init_in_shared(cache_ptr, POOL_CACHE_SIZE);
        let cache_size = LamportQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);
        offset += cache_size;
        offset = (offset + 127) & !127;

        // Pre-allocate segments in shared memory
        let segments_base = mem_ptr.add(offset);
        let mut segment_ptrs = Vec::with_capacity(MAX_SEGMENTS);

        for i in 0..MAX_SEGMENTS {
            let segment_offset = i * LamportQueue::<T>::shared_size(segment_size);
            let segment_ptr = segments_base.add(segment_offset);
            let segment = LamportQueue::init_in_shared(segment_ptr, segment_size);
            segment_ptrs.push(segment as *mut _);
        }

        // Use the first segment as the initial buffer
        let initial_segment = segment_ptrs[0];

        // Add remaining segments to the cache
        for i in 1..MAX_SEGMENTS.min(POOL_CACHE_SIZE) {
            cache_queue.push(LamportPtr(segment_ptrs[i])).ok();
        }

        // Create pool structure
        let pool = BufferPool {
            inuse_ptr: inuse_queue as *mut _,
            cache_ptr: cache_queue as *mut _,
            segment_size,
            segment_count: AtomicUsize::new(1),
        };

        // Add initial segment to inuse queue
        inuse_queue.push(LamportPtr(initial_segment)).unwrap();

        // Write self
        ptr::write(
            self_ptr,
            Self {
                buf_r: AtomicPtr::new(initial_segment),
                _padding1: [0; 120],
                buf_w: AtomicPtr::new(initial_segment),
                _padding2: [0; 120],
                pool,
                size: segment_size,
                initialized: AtomicBool::new(true),
                segment_count: AtomicUsize::new(1),
            },
        );

        &mut *self_ptr
    }
}

impl<T: Send + 'static> SpscQueue<T> for UnboundedQueue<T> {
    type PushError = ();
    type PopError = ();

    // Paper: push() method - Figure 3, lines 3-8
    fn push(&self, data: T) -> Result<(), Self::PushError> {
        let buf_w = self.buf_w.load(Ordering::Acquire);

        // Paper line 4: if (buf_w->full())
        if unsafe { !(*buf_w).available() } {
            // Paper line 5: buf_w = pool.next_w()
            if let Some(new_buf) = self.pool.next_w() {
                self.buf_w.store(new_buf, Ordering::Release);
                self.segment_count.fetch_add(1, Ordering::Relaxed);
                // Paper line 6: buf_w->push(data)
                unsafe { (*new_buf).push(data).map_err(|_| ()) }
            } else {
                Err(())
            }
        } else {
            // Paper line 6: buf_w->push(data)
            unsafe { (*buf_w).push(data).map_err(|_| ()) }
        }
    }

    // Paper: pop() method - Figure 3, lines 10-20
    fn pop(&self) -> Result<T, Self::PopError> {
        let buf_r = self.buf_r.load(Ordering::Acquire);

        // Paper line 11: if (buf_r->empty())
        if unsafe { (*buf_r).empty() } {
            // Paper line 12: if (buf_r == buf_w) return false
            if buf_r == self.buf_w.load(Ordering::Acquire) {
                return Err(());
            }

            // Paper line 13: if (buf_r->empty())
            std::sync::atomic::fence(Ordering::Acquire);
            if unsafe { (*buf_r).empty() } {
                // Paper lines 14-16
                if let Some(tmp) = self.pool.next_r() {
                    self.pool.release(buf_r);
                    self.buf_r.store(tmp, Ordering::Release);
                    self.segment_count.fetch_sub(1, Ordering::Relaxed);
                    // Try pop from new buffer
                    unsafe { (*tmp).pop() }
                } else {
                    Err(())
                }
            } else {
                // Race resolved, buffer not actually empty
                unsafe { (*buf_r).pop() }
            }
        } else {
            // Paper line 19: return buf_r->pop(data)
            unsafe { (*buf_r).pop() }
        }
    }

    fn available(&self) -> bool {
        unsafe { (*self.buf_w.load(Ordering::Acquire)).available() }
    }

    fn empty(&self) -> bool {
        let buf_r = self.buf_r.load(Ordering::Acquire);
        let buf_w = self.buf_w.load(Ordering::Acquire);
        unsafe { (*buf_r).empty() && buf_r == buf_w }
    }
}

impl<T: Send + 'static> Drop for UnboundedQueue<T> {
    fn drop(&mut self) {
        if !self.initialized.load(Ordering::Acquire) {
            return;
        }

        // For shared memory version, we don't deallocate anything
        // The memory will be unmapped by the benchmark
    }
}
