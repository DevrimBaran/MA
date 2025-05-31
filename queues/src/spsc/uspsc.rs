use crate::spsc::LamportQueue;
use crate::{DynListQueue, SpscQueue};
use std::{
    ffi::CString,
    ptr,
    sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
};

const POOL_CACHE_SIZE: usize = 4096;

// Wrapper that includes segment ID for tracking
#[derive(Debug)]
struct LamportPtr<T: Send + 'static> {
    ptr: *mut LamportQueue<T>,
    segment_id: u64,
}

// Manual Copy/Clone implementation
impl<T: Send + 'static> Copy for LamportPtr<T> {}
impl<T: Send + 'static> Clone for LamportPtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

unsafe impl<T: Send + 'static> Send for LamportPtr<T> {}
unsafe impl<T: Send + 'static> Sync for LamportPtr<T> {}

// Paper's BufferPool with dynamic shared memory support
#[repr(C)]
struct BufferPool<T: Send + 'static> {
    inuse_ptr: *mut DynListQueue<LamportPtr<T>>,
    cache_ptr: *mut LamportQueue<LamportPtr<T>>,
    segment_size: usize,
    segment_count: AtomicUsize,

    // For dynamic shared memory allocation
    process_id: u32,
    queue_id: u64,
    next_segment_id: AtomicU64,
    is_shared: bool,

    // Track current reader segment to avoid remapping
    current_reader_segment: AtomicU64,
    current_reader_ptr: AtomicPtr<LamportQueue<T>>,
}

impl<T: Send + 'static> BufferPool<T> {
    unsafe fn inuse(&self) -> &DynListQueue<LamportPtr<T>> {
        &*self.inuse_ptr
    }

    unsafe fn cache(&self) -> &LamportQueue<LamportPtr<T>> {
        &*self.cache_ptr
    }

    #[cfg(unix)]
    unsafe fn create_segment(&self, segment_id: u64) -> Option<*mut LamportQueue<T>> {
        use nix::libc;

        let shm_name = format!("/uspsc_{}_{}", self.queue_id, segment_id);
        let c_name = CString::new(shm_name).unwrap();

        libc::shm_unlink(c_name.as_ptr());

        let fd = libc::shm_open(c_name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o666);

        if fd < 0 {
            return None;
        }

        let segment_size = LamportQueue::<T>::shared_size(self.segment_size);

        if libc::ftruncate(fd, segment_size as libc::off_t) < 0 {
            libc::close(fd);
            libc::shm_unlink(c_name.as_ptr());
            return None;
        }

        let ptr = libc::mmap(
            ptr::null_mut(),
            segment_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        );

        libc::close(fd);

        if ptr == libc::MAP_FAILED {
            libc::shm_unlink(c_name.as_ptr());
            return None;
        }

        let segment = LamportQueue::init_in_shared(ptr as *mut u8, self.segment_size);

        // CRITICAL: Ensure initialization is visible before returning
        fence(Ordering::SeqCst);

        Some(segment as *mut _)
    }

    #[cfg(unix)]
    unsafe fn open_segment(&self, segment_id: u64) -> Option<*mut LamportQueue<T>> {
        use nix::libc;

        // Check if we already have this segment mapped
        if self.current_reader_segment.load(Ordering::Acquire) == segment_id {
            let ptr = self.current_reader_ptr.load(Ordering::Acquire);
            if !ptr.is_null() {
                return Some(ptr);
            }
        }

        let shm_name = format!("/uspsc_{}_{}", self.queue_id, segment_id);
        let c_name = CString::new(shm_name.clone()).unwrap();

        let fd = libc::shm_open(c_name.as_ptr(), libc::O_RDWR, 0o666);

        if fd < 0 {
            // Don't leak fd - it's already -1
            return None;
        }

        // ALWAYS close fd, even on error
        let segment_size = LamportQueue::<T>::shared_size(self.segment_size);

        let ptr = libc::mmap(
            ptr::null_mut(),
            segment_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        );

        // Close fd immediately after mmap
        libc::close(fd);

        if ptr == libc::MAP_FAILED {
            return None;
        }

        let segment_ptr = ptr as *mut LamportQueue<T>;

        // Cache this mapping
        self.current_reader_segment
            .store(segment_id, Ordering::Release);
        self.current_reader_ptr
            .store(segment_ptr, Ordering::Release);

        Some(segment_ptr)
    }

    fn next_w(&self) -> Option<LamportPtr<T>> {
        unsafe {
            // Try cache first
            if let Ok(wrapper) = self.cache().pop() {
                eprintln!("Reusing segment {} from cache", wrapper.segment_id);
                if self.inuse().push(wrapper).is_err() {
                    let _ = self.cache().push(wrapper);
                    return None;
                }
                return Some(wrapper);
            }

            // Allocate new segment
            if self.is_shared {
                #[cfg(unix)]
                {
                    let segment_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
                    if let Some(segment) = self.create_segment(segment_id) {
                        let wrapper = LamportPtr {
                            ptr: segment,
                            segment_id,
                        };
                        if self.inuse().push(wrapper).is_err() {
                            eprintln!("Failed to push segment {} to inuse queue", segment_id);
                            return None;
                        } else {
                            eprintln!("Creating new segment {} and pushed", segment_id);
                        }
                        self.segment_count.fetch_add(1, Ordering::Relaxed);
                        return Some(wrapper);
                    } else {
                        eprintln!("Failed to create segment {}", segment_id);
                        return None;
                    }
                }
            } else {
                let new_queue = Box::new(LamportQueue::with_capacity(self.segment_size));
                let ptr = Box::into_raw(new_queue);
                let wrapper = LamportPtr { ptr, segment_id: 0 };

                if self.inuse().push(wrapper).is_err() {
                    let _ = Box::from_raw(ptr);
                    return None;
                }

                self.segment_count.fetch_add(1, Ordering::Relaxed);
                Some(wrapper)
            }
        }
    }

    fn next_r(&self) -> Option<LamportPtr<T>> {
        unsafe {
            if let Ok(mut wrapper) = self.inuse().pop() {
                if self.is_shared && wrapper.segment_id > 0 {
                    #[cfg(unix)]
                    {
                        // Retry opening the segment a few times
                        for attempt in 0..100 {
                            if let Some(mapped) = self.open_segment(wrapper.segment_id) {
                                wrapper.ptr = mapped;
                                return Some(wrapper);
                            }
                            // Small delay between retries
                            if attempt < 10 {
                                std::thread::yield_now();
                            } else {
                                std::thread::sleep(std::time::Duration::from_micros(10));
                            }
                        }

                        // If we still can't open it, put it back for later
                        eprintln!(
                            "Failed to open segment {} after retries",
                            wrapper.segment_id
                        );
                        let _ = self.inuse().push(wrapper);
                        return None;
                    }
                }
                Some(wrapper)
            } else {
                None
            }
        }
    }

    fn release(&self, wrapper: LamportPtr<T>) {
        eprintln!("Releasing segment {}", wrapper.segment_id);
        if wrapper.ptr.is_null() {
            return;
        }

        unsafe {
            (*wrapper.ptr).head.store(0, Ordering::Relaxed);
            (*wrapper.ptr).tail.store(0, Ordering::Relaxed);

            // Try to cache it
            if self.cache().push(wrapper).is_err() {
                // Cache full - unmap and unlink the segment
                if self.is_shared && wrapper.segment_id > 0 {
                    #[cfg(unix)]
                    {
                        use nix::libc;
                        let segment_size = LamportQueue::<T>::shared_size(self.segment_size);
                        libc::munmap(wrapper.ptr as *mut _, segment_size);

                        let shm_name = format!("/uspsc_{}_{}", self.queue_id, wrapper.segment_id);
                        let c_name = CString::new(shm_name).unwrap();
                        libc::shm_unlink(c_name.as_ptr());
                    }
                }
            }
        }
    }
}

#[repr(C, align(128))]
pub struct UnboundedQueue<T: Send + 'static> {
    buf_r: AtomicPtr<LamportQueue<T>>,
    buf_r_id: AtomicU64, // Track segment ID for reader
    _padding1: [u8; 112],

    buf_w: AtomicPtr<LamportQueue<T>>,
    buf_w_id: AtomicU64, // Track segment ID for writer
    _padding2: [u8; 112],

    pool: BufferPool<T>,
    size: usize,
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

        let inuse = Box::new(DynListQueue::with_capacity(POOL_CACHE_SIZE));
        let cache = Box::new(LamportQueue::with_capacity(POOL_CACHE_SIZE));
        let initial_segment = Box::new(LamportQueue::with_capacity(segment_size));

        let inuse_ptr = Box::into_raw(inuse);
        let cache_ptr = Box::into_raw(cache);
        let initial_ptr = Box::into_raw(initial_segment);

        unsafe {
            (*inuse_ptr)
                .push(LamportPtr {
                    ptr: initial_ptr,
                    segment_id: 0,
                })
                .unwrap();
        }

        let pool = BufferPool {
            inuse_ptr: inuse_ptr as *mut _,
            cache_ptr: cache_ptr as *mut _,
            segment_size,
            segment_count: AtomicUsize::new(1),
            process_id: std::process::id(),
            queue_id: 0,
            next_segment_id: AtomicU64::new(1),
            is_shared: false,
            current_reader_segment: AtomicU64::new(0),
            current_reader_ptr: AtomicPtr::new(ptr::null_mut()),
        };

        Self {
            buf_r: AtomicPtr::new(initial_ptr),
            buf_r_id: AtomicU64::new(0),
            _padding1: [0; 112],
            buf_w: AtomicPtr::new(initial_ptr),
            buf_w_id: AtomicU64::new(0),
            _padding2: [0; 112],
            pool,
            size: segment_size,
            initialized: AtomicBool::new(true),
            segment_count: AtomicUsize::new(1),
        }
    }

    pub fn shared_size(_segment_size: usize) -> usize {
        use std::alloc::Layout;

        let self_layout = Layout::new::<Self>();
        let dspsc_size = DynListQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);
        let cache_size = LamportQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);

        let mut total = self_layout.size();
        total = (total + 127) & !127;
        total += dspsc_size;
        total = (total + 127) & !127;
        total += cache_size;

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
        std::ptr::write_bytes(mem_ptr, 0, total_size);

        let self_ptr = mem_ptr as *mut Self;
        let mut offset = Layout::new::<Self>().size();
        offset = (offset + 127) & !127;

        let dspsc_ptr = mem_ptr.add(offset);
        let inuse_queue = DynListQueue::init_in_shared(dspsc_ptr, POOL_CACHE_SIZE);
        let dspsc_size = DynListQueue::<LamportPtr<T>>::shared_size(POOL_CACHE_SIZE);
        offset += dspsc_size;
        offset = (offset + 127) & !127;

        let cache_ptr = mem_ptr.add(offset);
        let cache_queue = LamportQueue::init_in_shared(cache_ptr, POOL_CACHE_SIZE);

        let process_id = std::process::id();
        // Use process ID + timestamp to ensure uniqueness across runs
        let queue_id = (std::process::id() as u64) << 32
            | (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as u64
                & 0xFFFFFFFF);

        #[cfg(unix)]
        let (initial_segment, initial_segment_id) = {
            use nix::libc;

            let segment_id = 0u64;
            let shm_name = format!("/uspsc_{}_{}_0", process_id, queue_id);
            let c_name = CString::new(shm_name).unwrap();

            let fd = libc::shm_open(c_name.as_ptr(), libc::O_CREAT | libc::O_RDWR, 0o666);

            if fd < 0 {
                panic!("Failed to create initial segment");
            }

            let seg_size = LamportQueue::<T>::shared_size(segment_size);

            if libc::ftruncate(fd, seg_size as libc::off_t) < 0 {
                libc::close(fd);
                panic!("Failed to set segment size");
            }

            let ptr = libc::mmap(
                ptr::null_mut(),
                seg_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );

            libc::close(fd);

            if ptr == libc::MAP_FAILED {
                panic!("Failed to map initial segment");
            }

            (
                LamportQueue::init_in_shared(ptr as *mut u8, segment_size),
                segment_id,
            )
        };

        #[cfg(not(unix))]
        let (initial_segment, initial_segment_id) = panic!("Shared memory requires Unix");

        let pool = BufferPool {
            inuse_ptr: inuse_queue as *mut _,
            cache_ptr: cache_queue as *mut _,
            segment_size,
            segment_count: AtomicUsize::new(1),
            process_id,
            queue_id,
            next_segment_id: AtomicU64::new(1),
            is_shared: true,
            current_reader_segment: AtomicU64::new(0),
            current_reader_ptr: AtomicPtr::new(ptr::null_mut()),
        };

        inuse_queue
            .push(LamportPtr {
                ptr: initial_segment as *mut _,
                segment_id: initial_segment_id,
            })
            .unwrap();

        ptr::write(
            self_ptr,
            Self {
                buf_r: AtomicPtr::new(initial_segment as *mut _),
                buf_r_id: AtomicU64::new(initial_segment_id),
                _padding1: [0; 112],
                buf_w: AtomicPtr::new(initial_segment as *mut _),
                buf_w_id: AtomicU64::new(initial_segment_id),
                _padding2: [0; 112],
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

    fn push(&self, data: T) -> Result<(), Self::PushError> {
        let buf_w = self.buf_w.load(Ordering::Acquire);

        if unsafe { !(*buf_w).available() } {
            if let Some(wrapper) = self.pool.next_w() {
                fence(Ordering::Release);
                self.buf_w.store(wrapper.ptr, Ordering::Release);
                self.buf_w_id.store(wrapper.segment_id, Ordering::Release);
                self.segment_count.fetch_add(1, Ordering::Relaxed);
                unsafe { (*wrapper.ptr).push(data).map_err(|_| ()) }
            } else {
                Err(())
            }
        } else {
            unsafe { (*buf_w).push(data).map_err(|_| ()) }
        }
    }

    fn pop(&self) -> Result<T, Self::PopError> {
        let mut buf_r = self.buf_r.load(Ordering::Acquire);
        let buf_r_id = self.buf_r_id.load(Ordering::Acquire);

        static mut LAST_BUFFER_ID: u64 = 0;
        unsafe {
            if LAST_BUFFER_ID != buf_r_id {
                eprintln!("Pop: switched to buffer {}", buf_r_id);
                LAST_BUFFER_ID = buf_r_id;
            }
        }

        // First time check - remap initial segment for consumer
        if self.pool.is_shared && buf_r as usize > 0x7000_0000_0000 {
            // This looks like a parent address, remap it
            let segment_id = self.buf_r_id.load(Ordering::Acquire);
            if let Some(mapped) = unsafe { self.pool.open_segment(segment_id) } {
                buf_r = mapped;
                self.buf_r.store(buf_r, Ordering::Release);
            }
        }

        if unsafe { (*buf_r).empty() } {
            let buf_w = self.buf_w.load(Ordering::Acquire);
            if buf_r == buf_w {
                eprintln!("Pop: queue empty (buf_r == buf_w)");
                return Err(());
            }

            fence(Ordering::Acquire);
            if unsafe { (*buf_r).empty() } {
                eprintln!("Pop: buffer {} empty, getting next", buf_r_id);
                if let Some(wrapper) = self.pool.next_r() {
                    eprintln!(
                        "Pop: got next buffer {} at {:p}",
                        wrapper.segment_id, wrapper.ptr
                    );
                    let old_wrapper = LamportPtr {
                        ptr: buf_r,
                        segment_id: self.buf_r_id.load(Ordering::Acquire),
                    };
                    self.pool.release(old_wrapper);

                    self.buf_r.store(wrapper.ptr, Ordering::Release);
                    self.buf_r_id.store(wrapper.segment_id, Ordering::Release);
                    self.segment_count.fetch_sub(1, Ordering::Relaxed);
                    unsafe { (*wrapper.ptr).pop() }
                } else {
                    Err(())
                }
            } else {
                unsafe { (*buf_r).pop() }
            }
        } else {
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

        while let Ok(item) = SpscQueue::pop(self) {
            drop(item);
        }

        #[cfg(unix)]
        if self.pool.is_shared {
            use nix::libc;
            use std::ffi::CString;

            unsafe {
                for i in 0..self.pool.next_segment_id.load(Ordering::Relaxed) {
                    let shm_name = format!(
                        "/uspsc_{}_{}_{}",
                        self.pool.process_id, self.pool.queue_id, i
                    );
                    let c_name = CString::new(shm_name).unwrap();
                    libc::shm_unlink(c_name.as_ptr());
                }
            }
        }

        unsafe {
            if !self.pool.is_shared {
                while let Ok(wrapper) = (*self.pool.inuse_ptr).pop() {
                    if wrapper.segment_id == 0 && !wrapper.ptr.is_null() {
                        let _ = Box::from_raw(wrapper.ptr);
                    }
                }

                while let Ok(wrapper) = (*self.pool.cache_ptr).pop() {
                    if wrapper.segment_id == 0 && !wrapper.ptr.is_null() {
                        let _ = Box::from_raw(wrapper.ptr);
                    }
                }

                let _ = Box::from_raw(self.pool.inuse_ptr);
                let _ = Box::from_raw(self.pool.cache_ptr);
            }
        }
    }
}
