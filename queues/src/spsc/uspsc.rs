// paper in /paper/dspc-uspsc-mspsc-full.pdf and /paper/dspc-uspsc-mspsc.pdf
use crate::spsc::LamportQueue;
use crate::{DynListQueue, SpscQueue};
use std::{
    ptr,
    sync::atomic::{fence, AtomicPtr, Ordering},
};

// Wrapper for raw pointer
#[derive(Copy, Clone)]
struct LamportPtr<T: Send + 'static>(*mut LamportQueue<T>);

unsafe impl<T: Send + 'static> Send for LamportPtr<T> {}
unsafe impl<T: Send + 'static> Sync for LamportPtr<T> {}

// Paper's BufferPool
#[repr(C)]
struct BufferPool<T: Send + 'static> {
    inuse: DynListQueue<LamportPtr<T>>,
    cache: LamportQueue<LamportPtr<T>>,
    segment_size: usize,
}

impl<T: Send + 'static> BufferPool<T> {
    // Paper: next_w() - allocates new buffer for writer
    fn next_w(&self) -> Option<*mut LamportQueue<T>> {
        // Try to get from cache first
        if let Ok(buf_wrapper) = self.cache.pop() {
            let buf_ptr = buf_wrapper.0;
            // Add to inuse queue
            if self.inuse.push(buf_wrapper).is_err() {
                // Put back if inuse is full
                let _ = self.cache.push(LamportPtr(buf_ptr));
                return None;
            }
            // Reset the buffer before use
            unsafe {
                (*buf_ptr).head.store(0, Ordering::Relaxed);
                (*buf_ptr).tail.store(0, Ordering::Relaxed);
            }
            return Some(buf_ptr);
        }

        None
    }

    // Paper: next_r() - gets next buffer for reader
    fn next_r(&self) -> Option<*mut LamportQueue<T>> {
        self.inuse.pop().ok().map(|wrapper| wrapper.0)
    }

    // Paper: release() - returns buffer to pool
    fn release(&self, buf: *mut LamportQueue<T>) {
        if buf.is_null() {
            return;
        }

        unsafe {
            // Reset buffer
            (*buf).head.store(0, Ordering::Relaxed);
            (*buf).tail.store(0, Ordering::Relaxed);
        }

        // Try to add to cache
        let _ = self.cache.push(LamportPtr(buf));
    }
}

#[repr(C, align(128))]
pub struct UnboundedQueue<T: Send + 'static> {
    // Paper: buf_r and buf_w
    buf_r: AtomicPtr<LamportQueue<T>>,
    _padding1: [u8; 120],

    buf_w: AtomicPtr<LamportQueue<T>>,
    _padding2: [u8; 120],

    // Paper: Pool
    pool: BufferPool<T>,

    // Paper: size (segment size)
    size: usize,
}

unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

impl<T: Send + 'static> UnboundedQueue<T> {
    pub fn shared_size(segment_size: usize, num_segments: usize) -> usize {
        use std::alloc::Layout;

        // Ensure cache capacities are power of two
        let pool_capacity = num_segments.next_power_of_two();

        let layout_self = Layout::new::<Self>();
        let dspsc_size = DynListQueue::<LamportPtr<T>>::shared_size(pool_capacity, pool_capacity);
        let cache_size = LamportQueue::<LamportPtr<T>>::shared_size(pool_capacity);
        let segments_size = LamportQueue::<T>::shared_size(segment_size) * num_segments;

        let (layout1, _) = layout_self
            .extend(Layout::from_size_align(dspsc_size, 128).unwrap())
            .unwrap();
        let (layout2, _) = layout1
            .extend(Layout::from_size_align(cache_size, 128).unwrap())
            .unwrap();
        let (final_layout, _) = layout2
            .extend(Layout::from_size_align(segments_size, 128).unwrap())
            .unwrap();

        final_layout.size()
    }

    pub unsafe fn init_in_shared(
        mem_ptr: *mut u8,
        segment_size: usize,
        num_segments: usize,
    ) -> &'static mut Self {
        use std::alloc::Layout;

        assert!(
            segment_size > 1,
            "uSPSC requires size > 1 (paper's requirement)"
        );
        assert!(segment_size.is_power_of_two());

        let pool_capacity = num_segments.next_power_of_two();

        let self_ptr = mem_ptr as *mut Self;

        let layout_self = Layout::new::<Self>();
        let dspsc_size = DynListQueue::<LamportPtr<T>>::shared_size(pool_capacity, pool_capacity);
        let cache_size = LamportQueue::<LamportPtr<T>>::shared_size(pool_capacity);

        let (layout1, offset_dspsc) = layout_self
            .extend(Layout::from_size_align(dspsc_size, 128).unwrap())
            .unwrap();
        let (layout2, offset_cache) = layout1
            .extend(Layout::from_size_align(cache_size, 128).unwrap())
            .unwrap();
        let (_, offset_segments) = layout2
            .extend(
                Layout::from_size_align(
                    LamportQueue::<T>::shared_size(segment_size) * num_segments,
                    128,
                )
                .unwrap(),
            )
            .unwrap();

        // Initialize all segments
        let segments_base = mem_ptr.add(offset_segments);
        let mut segment_ptrs = Vec::with_capacity(num_segments);

        for i in 0..num_segments {
            let segment_offset = i * LamportQueue::<T>::shared_size(segment_size);
            let segment_ptr = segments_base.add(segment_offset);
            let segment = LamportQueue::init_in_shared(segment_ptr, segment_size);
            segment_ptrs.push(segment as *mut _);
        }

        // First segment is initial buffer
        let initial_segment = segment_ptrs[0];

        // Initialize pool components
        let dspsc_ptr = mem_ptr.add(offset_dspsc);
        let inuse_queue = DynListQueue::init_in_shared(dspsc_ptr, pool_capacity, pool_capacity);

        let cache_ptr = mem_ptr.add(offset_cache);
        let cache_queue = LamportQueue::init_in_shared(cache_ptr, pool_capacity);

        // Add remaining segments to cache
        for i in 1..num_segments {
            cache_queue.push(LamportPtr(segment_ptrs[i])).unwrap();
        }

        // Add initial segment to inuse
        inuse_queue.push(LamportPtr(initial_segment)).unwrap();

        // Create pool
        let pool = BufferPool {
            inuse: ptr::read(inuse_queue as *const _),
            cache: ptr::read(cache_queue as *const _),
            segment_size,
        };

        ptr::write(
            self_ptr,
            Self {
                buf_r: AtomicPtr::new(initial_segment),
                _padding1: [0; 120],
                buf_w: AtomicPtr::new(initial_segment),
                _padding2: [0; 120],
                pool,
                size: segment_size,
            },
        );

        fence(Ordering::SeqCst);
        &mut *self_ptr
    }
}

impl<T: Send + 'static> SpscQueue<T> for UnboundedQueue<T> {
    type PushError = ();
    type PopError = ();

    // Paper: push() - Figure 3, lines 3-8
    fn push(&self, data: T) -> Result<(), Self::PushError> {
        let buf_w = self.buf_w.load(Ordering::Acquire);

        // if (buf_w->full())
        if unsafe { !(*buf_w).available() } {
            // buf_w = pool.next_w()
            if let Some(new_buf) = self.pool.next_w() {
                // WMB enforced by dSPSC push in next_w
                fence(Ordering::Release);
                self.buf_w.store(new_buf, Ordering::Release);
                // buf_w->push(data)
                unsafe { (*new_buf).push(data).map_err(|_| ()) }
            } else {
                Err(()) // No free buffers
            }
        } else {
            // buf_w->push(data)
            unsafe { (*buf_w).push(data).map_err(|_| ()) }
        }
    }

    // Paper: pop() - Figure 3, lines 10-20
    fn pop(&self) -> Result<T, Self::PopError> {
        let buf_r = self.buf_r.load(Ordering::Acquire);

        // if (buf_r->empty())
        if unsafe { (*buf_r).empty() } {
            // if (buf_r == buf_w) return false
            let buf_w = self.buf_w.load(Ordering::Acquire);
            if buf_r == buf_w {
                return Err(()); // Queue is truly empty
            }

            // Second check after fence (paper's correctness requirement)
            fence(Ordering::Acquire);
            if unsafe { (*buf_r).empty() } {
                // Switch to next buffer
                if let Some(tmp) = self.pool.next_r() {
                    self.pool.release(buf_r);
                    self.buf_r.store(tmp, Ordering::Release);
                    // Try pop from new buffer
                    unsafe { (*tmp).pop() }
                } else {
                    Err(())
                }
            } else {
                // Buffer not actually empty
                unsafe { (*buf_r).pop() }
            }
        } else {
            // return buf_r->pop(data)
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
    fn drop(&mut self) {}
}
