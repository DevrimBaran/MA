// paper in /paper/dspc-uspsc-mspsc.pdf
use crate::spsc::{LamportQueue, Padded}; // Add Padded import
use crate::{DynListQueue, SpscQueue};
use std::{
    ptr,
    sync::atomic::{fence, AtomicPtr, Ordering},
};

// IPC: wrapper for LamportQueue pointers
#[derive(Copy, Clone)]
struct LamportPtr<T: Send + 'static>(*mut LamportQueue<T, Padded>); // Changed to Padded

unsafe impl<T: Send + 'static> Send for LamportPtr<T> {}
unsafe impl<T: Send + 'static> Sync for LamportPtr<T> {}

// BufferPool - Figure 3, lines 22-41 in paper
#[repr(C)]
struct BufferPool<T: Send + 'static> {
    inuse: DynListQueue<LamportPtr<T>>,         // Line 23
    cache: LamportQueue<LamportPtr<T>, Padded>, // Line 24 - Changed to Padded
    segment_size: usize,
}

impl<T: Send + 'static> BufferPool<T> {
    // next_w() - Lines 26-32 in Figure 3
    fn next_w(&self) -> Option<*mut LamportQueue<T, Padded>> {
        // Changed return type
        // Line 28 - try cache first
        if let Ok(buf_wrapper) = self.cache.pop() {
            let buf_ptr = buf_wrapper.0;
            // Line 30 - add to inuse queue
            if self.inuse.push(buf_wrapper).is_err() {
                let _ = self.cache.push(LamportPtr(buf_ptr)); // IPC: return to cache if inuse full
                return None;
            }
            // Reset buffer (paper implies this)
            unsafe {
                (*buf_ptr).head.store(0, Ordering::Relaxed);
                (*buf_ptr).tail.store(0, Ordering::Relaxed);
            }
            return Some(buf_ptr); // Line 31
        }

        None // IPC: no allocation, pre-allocated pool exhausted
    }

    // next_r() - Lines 33-36 in Figure 3
    fn next_r(&self) -> Option<*mut LamportQueue<T, Padded>> {
        // Changed return type
        self.inuse.pop().ok().map(|wrapper| wrapper.0) // Line 35
    }

    // release() - Lines 37-40 in Figure 3
    fn release(&self, buf: *mut LamportQueue<T, Padded>) {
        // Changed parameter type
        if buf.is_null() {
            return;
        }

        unsafe {
            // Line 38 - reset pread and pwrite
            (*buf).head.store(0, Ordering::Relaxed);
            (*buf).tail.store(0, Ordering::Relaxed);
        }

        // Line 39 - try to add to cache
        let _ = self.cache.push(LamportPtr(buf));
    }
}

// uSPSC queue - corresponds to paper's unbounded queue
#[repr(C, align(128))]
pub struct UnboundedQueue<T: Send + 'static> {
    buf_r: AtomicPtr<LamportQueue<T, Padded>>, // Reader's buffer pointer - Changed to Padded
    _padding1: [u8; 120],                      // IPC: cache line separation

    buf_w: AtomicPtr<LamportQueue<T, Padded>>, // Writer's buffer pointer - Changed to Padded
    _padding2: [u8; 120],

    pool: BufferPool<T>, // Pool of SPSC buffers

    size: usize, // Line 1 - segment size
}

unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

impl<T: Send + 'static> UnboundedQueue<T> {
    // IPC: calculate shared memory size
    pub fn shared_size(segment_size: usize, num_segments: usize) -> usize {
        use std::alloc::Layout;

        let pool_capacity = num_segments.next_power_of_two();

        let layout_self = Layout::new::<Self>();
        let dspsc_size = DynListQueue::<LamportPtr<T>>::shared_size(pool_capacity, pool_capacity);
        let cache_size = LamportQueue::<LamportPtr<T>, Padded>::shared_size(pool_capacity); // Changed
        let segments_size = LamportQueue::<T, Padded>::shared_size(segment_size) * num_segments; // Changed

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

    // IPC: initialize in shared memory
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
        let cache_size = LamportQueue::<LamportPtr<T>, Padded>::shared_size(pool_capacity); // Changed

        let (layout1, offset_dspsc) = layout_self
            .extend(Layout::from_size_align(dspsc_size, 128).unwrap())
            .unwrap();
        let (layout2, offset_cache) = layout1
            .extend(Layout::from_size_align(cache_size, 128).unwrap())
            .unwrap();
        let (_, offset_segments) = layout2
            .extend(
                Layout::from_size_align(
                    LamportQueue::<T, Padded>::shared_size(segment_size) * num_segments, // Changed
                    128,
                )
                .unwrap(),
            )
            .unwrap();

        // Initialize all segments
        let segments_base = mem_ptr.add(offset_segments);
        let mut segment_ptrs = Vec::with_capacity(num_segments);

        for i in 0..num_segments {
            let segment_offset = i * LamportQueue::<T, Padded>::shared_size(segment_size); // Changed
            let segment_ptr = segments_base.add(segment_offset);
            let segment = LamportQueue::<T, Padded>::init_in_shared(segment_ptr, segment_size); // Changed
            segment_ptrs.push(segment as *mut _);
        }

        // First segment is initial buffer
        let initial_segment = segment_ptrs[0];

        // Initialize pool components
        let dspsc_ptr = mem_ptr.add(offset_dspsc);
        let inuse_queue = DynListQueue::init_in_shared(dspsc_ptr, pool_capacity, pool_capacity);

        let cache_ptr = mem_ptr.add(offset_cache);
        let cache_queue =
            LamportQueue::<LamportPtr<T>, Padded>::init_in_shared(cache_ptr, pool_capacity); // Changed

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
                buf_r: AtomicPtr::new(initial_segment), // Initially same buffer
                _padding1: [0; 120],
                buf_w: AtomicPtr::new(initial_segment), // Initially same buffer
                _padding2: [0; 120],
                pool,
                size: segment_size,
            },
        );

        fence(Ordering::SeqCst);
        &mut *self_ptr
    }
}

// Rest of the implementation remains the same...
impl<T: Send + 'static> SpscQueue<T> for UnboundedQueue<T> {
    type PushError = ();
    type PopError = ();

    // push() - Lines 3-8 in Figure 3
    fn push(&self, data: T) -> Result<(), Self::PushError> {
        let buf_w = self.buf_w.load(Ordering::Acquire);

        // Line 4 - if (buf_w->full())
        if unsafe { !(*buf_w).available() } {
            // Line 5 - buf_w = pool.next_w()
            if let Some(new_buf) = self.pool.next_w() {
                fence(Ordering::Release); // WMB enforced by dSPSC push in next_w
                self.buf_w.store(new_buf, Ordering::Release);
                // Line 6 - buf_w->push(data)
                unsafe { (*new_buf).push(data).map_err(|_| ()) }
            } else {
                Err(()) // No free buffers
            }
        } else {
            // Line 6 - buf_w->push(data)
            unsafe { (*buf_w).push(data).map_err(|_| ()) }
        }
    }

    // pop() - Lines 10-20 in Figure 3
    fn pop(&self) -> Result<T, Self::PopError> {
        let buf_r = self.buf_r.load(Ordering::Acquire);

        // Line 11 - if (buf_r->empty())
        if unsafe { (*buf_r).empty() } {
            // Line 12 - if (buf_r == buf_w) return false
            let buf_w = self.buf_w.load(Ordering::Acquire);
            if buf_r == buf_w {
                return Err(()); // Queue is truly empty
            }

            // Line 13 - second check after fence (paper's correctness requirement)
            fence(Ordering::Acquire);
            if unsafe { (*buf_r).empty() } {
                // Lines 14-17 - switch to next buffer
                if let Some(tmp) = self.pool.next_r() {
                    self.pool.release(buf_r); // Line 15
                    self.buf_r.store(tmp, Ordering::Release); // Line 16
                                                              // Try pop from new buffer
                    unsafe { (*tmp).pop() }
                } else {
                    Err(())
                }
            } else {
                // Buffer not actually empty after fence
                unsafe { (*buf_r).pop() }
            }
        } else {
            // Line 19 - return buf_r->pop(data)
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
    fn drop(&mut self) {} // IPC: shared memory cleanup handled externally
}
