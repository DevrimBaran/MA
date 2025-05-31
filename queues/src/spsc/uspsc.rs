use crate::spsc::LamportQueue;
use crate::{DynListQueue, SpscQueue};
use std::{
    ptr,
    sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

const POOL_CACHE_SIZE: usize = 32;

// Wrapper for raw pointer to make it Send/Sync
#[derive(Copy, Clone)]
struct LamportPtr<T: Send + 'static>(*mut LamportQueue<T>);

unsafe impl<T: Send + 'static> Send for LamportPtr<T> {}
unsafe impl<T: Send + 'static> Sync for LamportPtr<T> {}

// Paper's BufferPool - Figure 3, lines 22-41
#[repr(C)]
struct BufferPool<T: Send + 'static> {
    // Paper: inuse queue and cache
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
            // Paper line 28: if (!cache.pop(&buf))
            if let Ok(buf_wrapper) = self.cache().pop() {
                let buf_ptr = buf_wrapper.0;
                // Paper line 30: inuse.push(buf)
                if self.inuse().push(buf_wrapper).is_err() {
                    // If inuse is full, put back in cache
                    let _ = self.cache().push(LamportPtr(buf_ptr));
                    return None;
                }
                return Some(buf_ptr);
            }

            // Paper line 29: buf = allocateSPSC(size)
            // For truly unbounded, we need dynamic allocation
            let new_queue = Box::new(LamportQueue::with_capacity(self.segment_size));
            let ptr = Box::into_raw(new_queue);

            // Paper line 30: inuse.push(buf)
            if self.inuse().push(LamportPtr(ptr)).is_err() {
                // Failed to add to inuse, cleanup
                let _ = Box::from_raw(ptr);
                return None;
            }

            self.segment_count.fetch_add(1, Ordering::Relaxed);
            Some(ptr)
        }
    }

    // Paper: next_r() - Figure 3, lines 33-36
    fn next_r(&self) -> Option<*mut LamportQueue<T>> {
        unsafe {
            // Paper line 35: return (inuse.pop(&buf)? buf : NULL)
            self.inuse().pop().ok().map(|wrapper| wrapper.0)
        }
    }

    // Paper: release() - Figure 3, lines 37-40
    fn release(&self, buf: *mut LamportQueue<T>) {
        if buf.is_null() {
            return;
        }

        unsafe {
            // Paper line 38: buf->reset() - reset pread and pwrite
            (*buf).head.store(0, Ordering::Relaxed);
            (*buf).tail.store(0, Ordering::Relaxed);

            // Paper line 39: if (!cache.push(buf)) deallocateSPSC(buf)
            if self.cache().push(LamportPtr(buf)).is_err() {
                // Cache is full, deallocate
                let _ = Box::from_raw(buf);
                self.segment_count.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

// Paper's uSPSC structure - Figure 3
#[repr(C, align(128))]
pub struct UnboundedQueue<T: Send + 'static> {
    // Paper: buf_r - reader's buffer pointer
    buf_r: AtomicPtr<LamportQueue<T>>,
    _padding1: [u8; 120],

    // Paper: buf_w - writer's buffer pointer
    buf_w: AtomicPtr<LamportQueue<T>>,
    _padding2: [u8; 120],

    // Paper: Pool
    pool: BufferPool<T>,

    // Paper: size (segment size)
    size: usize,

    // Additional fields
    initialized: AtomicBool,
    pub segment_count: AtomicUsize,
}

unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

impl<T: Send + 'static> UnboundedQueue<T> {
    pub fn with_capacity(segment_size: usize) -> Self {
        assert!(segment_size > 1, "uSPSC requires size > 1 (Theorem 4.2)");
        assert!(
            segment_size.is_power_of_two(),
            "Segment size must be power of 2"
        );

        // Create heap-based version
        let inuse = Box::new(DynListQueue::with_capacity(POOL_CACHE_SIZE));
        let cache = Box::new(LamportQueue::with_capacity(POOL_CACHE_SIZE));
        let initial_segment = Box::new(LamportQueue::with_capacity(segment_size));

        let inuse_ptr = Box::into_raw(inuse);
        let cache_ptr = Box::into_raw(cache);
        let initial_ptr = Box::into_raw(initial_segment);

        // Add initial segment to inuse queue
        unsafe {
            (*inuse_ptr).push(LamportPtr(initial_ptr)).unwrap();
        }

        let pool = BufferPool {
            inuse_ptr: inuse_ptr as *mut _,
            cache_ptr: cache_ptr as *mut _,
            segment_size,
            segment_count: AtomicUsize::new(1),
        };

        Self {
            buf_r: AtomicPtr::new(initial_ptr),
            _padding1: [0; 120],
            buf_w: AtomicPtr::new(initial_ptr),
            _padding2: [0; 120],
            pool,
            size: segment_size,
            initialized: AtomicBool::new(true),
            segment_count: AtomicUsize::new(1),
        }
    }

    pub fn shared_size(segment_size: usize) -> usize {
        // For shared memory benchmarks, we still need pre-allocated space
        // But the algorithm supports dynamic growth beyond this
        use std::alloc::Layout;

        let self_layout = Layout::new::<Self>();
        let dspsc_size = DynListQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);
        let cache_size = LamportQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);

        // Pre-allocate a reasonable number of segments
        let initial_segments = 8;
        let segments_size = LamportQueue::<T>::shared_size(segment_size) * initial_segments;

        let mut total = self_layout.size();
        total = (total + 127) & !127;
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

        // Zero initialize
        let total_size = Self::shared_size(segment_size);
        std::ptr::write_bytes(mem_ptr, 0, total_size);

        let self_ptr = mem_ptr as *mut Self;
        let mut offset = Layout::new::<Self>().size();
        offset = (offset + 127) & !127;

        // Initialize dSPSC queue
        let dspsc_ptr = mem_ptr.add(offset);
        let inuse_queue = DynListQueue::init_in_shared(dspsc_ptr, POOL_CACHE_SIZE);
        let dspsc_size = DynListQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);
        offset += dspsc_size;
        offset = (offset + 127) & !127;

        // Initialize cache queue
        let cache_ptr = mem_ptr.add(offset);
        let cache_queue = LamportQueue::init_in_shared(cache_ptr, POOL_CACHE_SIZE);
        let cache_size = LamportQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);
        offset += cache_size;
        offset = (offset + 127) & !127;

        // Pre-allocate initial segments
        let segments_base = mem_ptr.add(offset);
        let initial_segments = 8;
        let mut segment_ptrs = Vec::with_capacity(initial_segments);

        for i in 0..initial_segments {
            let segment_offset = i * LamportQueue::<T>::shared_size(segment_size);
            let segment_ptr = segments_base.add(segment_offset);
            let segment = LamportQueue::init_in_shared(segment_ptr, segment_size);
            segment_ptrs.push(segment as *mut _);
        }

        let initial_segment = segment_ptrs[0];

        // Add remaining segments to cache
        for i in 1..initial_segments.min(POOL_CACHE_SIZE) {
            cache_queue.push(LamportPtr(segment_ptrs[i])).ok();
        }

        // Create pool
        let pool = BufferPool {
            inuse_ptr: inuse_queue as *mut _,
            cache_ptr: cache_queue as *mut _,
            segment_size,
            segment_count: AtomicUsize::new(1),
        };

        // Add initial segment to inuse
        inuse_queue.push(LamportPtr(initial_segment)).unwrap();

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
                // The paper mentions WMB is enforced by dSPSC push in next_w
                fence(Ordering::Release);
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
            let buf_w = self.buf_w.load(Ordering::Acquire);
            if buf_r == buf_w {
                return Err(());
            }

            // Paper line 13: if (buf_r->empty())
            // This second check is CRITICAL for correctness under weak memory models
            fence(Ordering::Acquire);
            if unsafe { (*buf_r).empty() } {
                // Paper lines 14-16: switch to next buffer
                if let Some(tmp) = self.pool.next_r() {
                    self.pool.release(buf_r);
                    self.buf_r.store(tmp, Ordering::Release);
                    self.segment_count.fetch_sub(1, Ordering::Relaxed);
                    // Paper line 19: return buf_r->pop(data)
                    unsafe { (*tmp).pop() }
                } else {
                    Err(())
                }
            } else {
                // Buffer not actually empty - race resolved
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

        // Pop all remaining items
        while let Ok(item) = SpscQueue::pop(self) {
            drop(item);
        }

        // Clean up all segments
        unsafe {
            // Get all segments from inuse queue
            while let Ok(wrapper) = (*self.pool.inuse_ptr).pop() {
                let _ = Box::from_raw(wrapper.0);
            }

            // Get all segments from cache
            while let Ok(wrapper) = (*self.pool.cache_ptr).pop() {
                let _ = Box::from_raw(wrapper.0);
            }

            // Clean up the current buffers if they're not already handled
            let buf_r = self.buf_r.load(Ordering::Relaxed);
            let buf_w = self.buf_w.load(Ordering::Relaxed);

            // Only free if not already freed above
            if !buf_r.is_null() {
                // Check if this buffer was already freed by checking if it's still valid
                // In shared memory mode, we don't free these as they're in the pre-allocated space
            }

            // Free the pool queues themselves (only in heap mode)
            let inuse_ptr = self.pool.inuse_ptr;
            let cache_ptr = self.pool.cache_ptr;

            if !inuse_ptr.is_null() {
                let _ = Box::from_raw(inuse_ptr);
            }
            if !cache_ptr.is_null() {
                let _ = Box::from_raw(cache_ptr);
            }
        }
    }
}
