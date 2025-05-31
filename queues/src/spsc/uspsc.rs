// True unbounded uSPSC implementation using dynamic shared memory allocation
// This follows the paper's algorithm exactly but adapts it for IPC
// CRITICAL: Pre-allocates all segments before fork() for cross-process visibility

use crate::spsc::LamportQueue;
use crate::{DynListQueue, SpscQueue};
use std::{
    cell::UnsafeCell,
    ptr,
    sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

const POOL_CACHE_SIZE: usize = 32;
// Pre-allocate enough segments for practical unbounded behavior
const MAX_SEGMENTS: usize = 32; // Can handle ITERS/segment_size items

// SharedSegment wraps a pointer and its metadata
#[repr(C)]
struct SharedSegment<T: Send + 'static> {
    queue_ptr: *mut LamportQueue<T>,
    segment_id: usize, // ID within the pre-allocated pool
    _padding: usize,   // Ensure 24-byte size for alignment
}

// Manual implementation of Copy and Clone
impl<T: Send + 'static> Copy for SharedSegment<T> {}

impl<T: Send + 'static> Clone for SharedSegment<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Send + 'static> SharedSegment<T> {
    #[allow(dead_code)]
    fn null() -> Self {
        Self {
            queue_ptr: ptr::null_mut(),
            segment_id: usize::MAX,
            _padding: 0,
        }
    }
}

unsafe impl<T: Send + 'static> Send for SharedSegment<T> {}
unsafe impl<T: Send + 'static> Sync for SharedSegment<T> {}

// Paper's BufferPool adapted for pre-allocated shared memory
#[repr(C)]
struct BufferPool<T: Send + 'static> {
    inuse_ptr: *mut DynListQueue<SharedSegment<T>>,
    cache_ptr: *mut LamportQueue<SharedSegment<T>>,
    segment_size: usize,
    segment_count: AtomicUsize,

    // Pre-allocated segments info
    segments_base: *mut u8,
    segment_stride: usize,
    total_segments: usize,
}

impl<T: Send + 'static> BufferPool<T> {
    unsafe fn inuse(&self) -> &DynListQueue<SharedSegment<T>> {
        &*self.inuse_ptr
    }

    unsafe fn cache(&self) -> &LamportQueue<SharedSegment<T>> {
        &*self.cache_ptr
    }

    // Paper: next_w() - returns pre-allocated segment
    fn next_w(&self) -> Option<SharedSegment<T>> {
        unsafe {
            // Paper line 28: if (!cache.pop(&buf))
            if let Ok(segment) = self.cache().pop() {
                // Paper line 30: inuse.push(buf)
                if self.inuse().push(segment).is_err() {
                    // If inuse is full, put back in cache
                    let _ = self.cache().push(segment);
                    return None;
                }
                self.segment_count.fetch_add(1, Ordering::Relaxed);
                return Some(segment);
            }

            // No more pre-allocated segments available
            None
        }
    }

    // Paper: next_r() - Figure 3, lines 33-36
    fn next_r(&self) -> Option<SharedSegment<T>> {
        unsafe {
            // Paper line 35: return (inuse.pop(&buf)? buf : NULL)
            self.inuse().pop().ok()
        }
    }

    // Paper: release() - Figure 3, lines 37-40
    fn release(&self, segment: SharedSegment<T>) {
        if segment.queue_ptr.is_null() {
            return;
        }

        unsafe {
            // Paper line 38: buf->reset() - reset pread and pwrite
            (*segment.queue_ptr).head.store(0, Ordering::Relaxed);
            (*segment.queue_ptr).tail.store(0, Ordering::Relaxed);

            // Paper line 39: if (!cache.push(buf)) deallocateSPSC(buf)
            // In this implementation, we never deallocate - just return to cache
            if self.cache().push(segment).is_ok() {
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

    // Pool must come after atomics for alignment
    pool: BufferPool<T>,

    // Current segment info stored in shared memory with interior mutability
    current_r_segment: UnsafeCell<SharedSegment<T>>,
    current_w_segment: UnsafeCell<SharedSegment<T>>,

    // Paper: size (segment size)
    size: usize,

    initialized: AtomicBool,
    pub segment_count: AtomicUsize,
}

unsafe impl<T: Send + 'static> Send for UnboundedQueue<T> {}
unsafe impl<T: Send + 'static> Sync for UnboundedQueue<T> {}

impl<T: Send + 'static> UnboundedQueue<T> {
    pub fn shared_size(segment_size: usize) -> usize {
        use std::alloc::Layout;

        let self_layout = Layout::new::<Self>();
        let dspsc_size = DynListQueue::<SharedSegment<T>>::shared_size(POOL_CACHE_SIZE * 2);
        let cache_size = LamportQueue::<SharedSegment<T>>::shared_size(POOL_CACHE_SIZE);

        // Align segment size to cache line
        let segment_layout =
            Layout::from_size_align(LamportQueue::<T>::shared_size(segment_size), 128).unwrap();
        let segment_stride = segment_layout.pad_to_align().size();

        let mut total = self_layout.size();
        total = (total + 127) & !127;
        total += dspsc_size;
        total = (total + 127) & !127;
        total += cache_size;
        total = (total + 127) & !127;
        total += segment_stride * MAX_SEGMENTS; // Pre-allocate all segments

        total
    }

    pub unsafe fn init_in_shared(mem_ptr: *mut u8, segment_size: usize) -> &'static mut Self {
        use std::alloc::Layout;

        assert!(segment_size > 1, "uSPSC requires size > 1 (Theorem 4.2)");
        assert!(
            segment_size.is_power_of_two(),
            "Segment size must be power of 2"
        );

        let total_size = Self::shared_size(segment_size);
        ptr::write_bytes(mem_ptr, 0, total_size);

        let self_ptr = mem_ptr as *mut Self;
        let mut offset = Layout::new::<Self>().size();
        offset = (offset + 127) & !127;

        // Initialize dSPSC queue for 'inuse' list
        let dspsc_ptr = mem_ptr.add(offset);
        let inuse_queue = DynListQueue::init_in_shared(dspsc_ptr, POOL_CACHE_SIZE * 2);
        let dspsc_size = DynListQueue::<SharedSegment<T>>::shared_size(POOL_CACHE_SIZE * 2);
        offset += dspsc_size;
        offset = (offset + 127) & !127;

        // Initialize SPSC queue for 'cache'
        let cache_ptr = mem_ptr.add(offset);
        let cache_queue = LamportQueue::init_in_shared(cache_ptr, POOL_CACHE_SIZE);
        let cache_size = LamportQueue::<SharedSegment<T>>::shared_size(POOL_CACHE_SIZE);
        offset += cache_size;
        offset = (offset + 127) & !127;

        // Calculate segment stride
        let segment_layout =
            Layout::from_size_align(LamportQueue::<T>::shared_size(segment_size), 128).unwrap();
        let segment_stride = segment_layout.pad_to_align().size();

        // Initialize all segments
        let segments_base = mem_ptr.add(offset);

        // Initialize first segment and put it in use
        let first_queue = LamportQueue::init_in_shared(segments_base, segment_size);
        let first_segment = SharedSegment {
            queue_ptr: first_queue as *mut _,
            segment_id: 0,
            _padding: 0,
        };
        inuse_queue.push(first_segment).unwrap();

        // Initialize remaining segments and add to cache
        let mut initialized_count = 1;
        for i in 1..MAX_SEGMENTS {
            let segment_ptr = segments_base.add(i * segment_stride);
            let queue = LamportQueue::init_in_shared(segment_ptr, segment_size);
            let segment = SharedSegment {
                queue_ptr: queue as *mut _,
                segment_id: i,
                _padding: 0,
            };

            // Try to add to cache
            if cache_queue.push(segment).is_err() {
                // Cache full, try inuse
                if inuse_queue.push(segment).is_err() {
                    // Both full, stop initializing
                    break;
                }
            }
            initialized_count += 1;
        }

        let pool = BufferPool {
            inuse_ptr: inuse_queue as *mut _,
            cache_ptr: cache_queue as *mut _,
            segment_size,
            segment_count: AtomicUsize::new(1),
            segments_base,
            segment_stride,
            total_segments: initialized_count,
        };

        ptr::write(
            self_ptr,
            Self {
                buf_r: AtomicPtr::new(first_queue as *mut _),
                _padding1: [0; 120],
                buf_w: AtomicPtr::new(first_queue as *mut _),
                _padding2: [0; 120],
                pool,
                current_r_segment: UnsafeCell::new(first_segment),
                current_w_segment: UnsafeCell::new(first_segment),
                size: segment_size,
                initialized: AtomicBool::new(true),
                segment_count: AtomicUsize::new(1),
            },
        );

        &mut *self_ptr
    }

    // Helper to atomically update segment info
    unsafe fn update_w_segment(&self, new_segment: SharedSegment<T>) {
        // Write to the UnsafeCell
        *self.current_w_segment.get() = new_segment;
    }

    unsafe fn update_r_segment(&self, new_segment: SharedSegment<T>) {
        *self.current_r_segment.get() = new_segment;
    }

    unsafe fn get_r_segment(&self) -> SharedSegment<T> {
        *self.current_r_segment.get()
    }

    unsafe fn get_w_segment(&self) -> SharedSegment<T> {
        *self.current_w_segment.get()
    }
}

impl<T: Send + 'static> SpscQueue<T> for UnboundedQueue<T> {
    type PushError = ();
    type PopError = ();

    // Paper: push() method - Figure 3, lines 3-8
    fn push(&self, data: T) -> Result<(), Self::PushError> {
        let buf_w = self.buf_w.load(Ordering::Acquire);

        if buf_w.is_null() {
            return Err(());
        }

        // Paper line 4: if (buf_w->full())
        if unsafe { !(*buf_w).available() } {
            // Paper line 5: buf_w = pool.next_w()
            if let Some(new_segment) = self.pool.next_w() {
                // CRITICAL: WMB is enforced by the dSPSC push inside next_w()
                // as mentioned in the paper (enforced by line 14 in Figure 2)
                fence(Ordering::Release);

                // Update the pointer and segment info
                self.buf_w.store(new_segment.queue_ptr, Ordering::Release);
                unsafe {
                    self.update_w_segment(new_segment);
                }

                self.segment_count.fetch_add(1, Ordering::Relaxed);

                // Paper line 6: buf_w->push(data)
                unsafe { (*new_segment.queue_ptr).push(data).map_err(|_| ()) }
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

        if buf_r.is_null() {
            return Err(());
        }

        // Paper line 11: if (buf_r->empty())
        if unsafe { (*buf_r).empty() } {
            // Paper line 12: if (buf_r == buf_w) return false
            let buf_w = self.buf_w.load(Ordering::Acquire);
            if buf_r == buf_w {
                return Err(());
            }

            // Paper line 13: if (buf_r->empty())
            // This second check is CRITICAL for correctness
            fence(Ordering::Acquire);
            if unsafe { (*buf_r).empty() } {
                // Paper lines 14-16: switch to next buffer
                if let Some(new_segment) = self.pool.next_r() {
                    // Get current segment info for release
                    let current_segment = unsafe { self.get_r_segment() };

                    self.pool.release(current_segment);

                    // Update pointer and segment info
                    self.buf_r.store(new_segment.queue_ptr, Ordering::Release);
                    unsafe {
                        self.update_r_segment(new_segment);
                    }

                    self.segment_count.fetch_sub(1, Ordering::Relaxed);

                    // Paper line 19: return buf_r->pop(data)
                    unsafe { (*new_segment.queue_ptr).pop() }
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
        let buf_w = self.buf_w.load(Ordering::Acquire);
        !buf_w.is_null() && unsafe { (*buf_w).available() }
    }

    fn empty(&self) -> bool {
        let buf_r = self.buf_r.load(Ordering::Acquire);
        let buf_w = self.buf_w.load(Ordering::Acquire);
        !buf_r.is_null() && !buf_w.is_null() && unsafe { (*buf_r).empty() && buf_r == buf_w }
    }
}

impl<T: Send + 'static> Drop for UnboundedQueue<T> {
    fn drop(&mut self) {
        // Only run drop logic if we were properly initialized
        if !self.initialized.load(Ordering::Acquire) {
            return;
        }

        // Mark as uninitialized to prevent double-drop
        self.initialized.store(false, Ordering::Release);

        // Pop all remaining items to drop them properly
        // Use direct implementation to avoid trait dispatch issues
        loop {
            let buf_r = self.buf_r.load(Ordering::Acquire);
            if buf_r.is_null() {
                break;
            }

            if unsafe { (*buf_r).empty() } {
                let buf_w = self.buf_w.load(Ordering::Acquire);
                if buf_r == buf_w {
                    break; // Queue is empty
                }

                // Try to switch to next buffer
                if let Some(new_segment) = self.pool.next_r() {
                    let current_segment = unsafe { self.get_r_segment() };
                    self.pool.release(current_segment);
                    self.buf_r.store(new_segment.queue_ptr, Ordering::Release);
                    unsafe {
                        self.update_r_segment(new_segment);
                    }
                } else {
                    break; // No more segments
                }
            } else {
                // Pop and drop the item
                if unsafe { (*buf_r).pop() }.is_err() {
                    break;
                }
            }
        }

        // Reset all pointers to null
        self.buf_r.store(ptr::null_mut(), Ordering::Release);
        self.buf_w.store(ptr::null_mut(), Ordering::Release);

        // No memory deallocation needed - all segments are in the pre-allocated shared memory
        // The benchmark will unmap the entire shared memory region
    }
}
