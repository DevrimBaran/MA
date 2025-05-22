// queues/src/spsc/mspsc.rs
// multipush spsc by Torquati (TR-10-20)

use crate::spsc::LamportQueue;
use crate::SpscQueue;
use core::{cell::UnsafeCell, fmt, mem::MaybeUninit, ptr};
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::alloc::Layout; // Import Layout

// compile-time size of the producer’s scratch buffer (paper uses 16)
const LOCAL_BUF: usize = 16;

pub struct MultiPushQueue<T: Send + 'static> {
    inner: *mut LamportQueue<T>,
    local_buf: UnsafeCell<[MaybeUninit<T>; LOCAL_BUF]>,
    pub local_count: AtomicUsize,
    shared: AtomicBool,
}

unsafe impl<T: Send> Send for MultiPushQueue<T> {}
unsafe impl<T: Send> Sync for MultiPushQueue<T> {}

impl<T: Send + 'static> MultiPushQueue<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        let boxed_lamport = Box::new(LamportQueue::with_capacity(capacity));
        Self::from_raw(Box::into_raw(boxed_lamport), false)
    }

    pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
        let self_ptr = mem as *mut MaybeUninit<Self>;
        
        let self_layout = Layout::new::<Self>();
        let lamport_layout = Layout::from_size_align(
            LamportQueue::<T>::shared_size(capacity),
            core::mem::align_of::<LamportQueue<T>>()
        ).expect("Failed to create layout for LamportQueue in init_in_shared");

        let (_combined_layout, lamport_offset) = self_layout.extend(lamport_layout)
            .expect("Failed to extend layout for MultiPushQueue in init_in_shared");

        let lamport_q_ptr_raw = mem.add(lamport_offset);
        let lamport_q_instance = LamportQueue::init_in_shared(lamport_q_ptr_raw, capacity);
        
        let initial_value = Self::from_raw(lamport_q_instance as *mut _, true);
        ptr::write(self_ptr, MaybeUninit::new(initial_value));
        &mut *(*self_ptr).as_mut_ptr()
    }

    pub fn shared_size(capacity: usize) -> usize {
        let self_layout = Layout::new::<Self>();
        let lamport_layout = Layout::from_size_align(
            LamportQueue::<T>::shared_size(capacity),
            core::mem::align_of::<LamportQueue<T>>()
        ).expect("Failed to create layout for LamportQueue in shared_size");

        let (combined_layout, _offset_lamport) = self_layout.extend(lamport_layout)
            .expect("Failed to extend layout for MultiPushQueue in shared_size");
        
        combined_layout.pad_to_align().size()
    }

    #[inline(always)]
    fn from_raw(ring: *mut LamportQueue<T>, shared: bool) -> Self {
        Self {
            inner: ring,
            local_buf: UnsafeCell::new(unsafe { MaybeUninit::uninit().assume_init() }),
            local_count: AtomicUsize::new(0),
            shared: AtomicBool::new(shared),
        }
    }

    #[inline(always)]
    fn ring(&self) -> &LamportQueue<T> {
        unsafe { &*self.inner }
    }

    #[inline(always)]
    fn ring_mut(&self) -> &mut LamportQueue<T> {
        unsafe { &mut *self.inner }
    }

    #[inline(always)]
    fn contiguous_free_in_ring(&self) -> usize {
        let ring_ref = self.ring();
        let cap = ring_ref.capacity();
        let prod_idx = ring_ref.tail.load(Ordering::Relaxed); 
        let cons_idx = ring_ref.head.load(Ordering::Acquire);
        
        let used_slots = prod_idx.wrapping_sub(cons_idx) & (cap - 1);
        let free_total = cap.wrapping_sub(used_slots).wrapping_sub(1);
        let room_till_wrap = cap - (prod_idx & (cap - 1));
        free_total.min(room_till_wrap)
    }

    /// Flushes the producer's local buffer to the main ring buffer.
    /// Returns `true` if the flush was successful or if there was nothing to flush.
    /// Returns `false` if the flush was attempted but failed (e.g., ring buffer full).
    pub fn flush(&self) -> bool {
        let count_to_push = self.local_count.load(Ordering::Relaxed);
        if count_to_push == 0 {
            return true; // Nothing to flush
        }

        // Directly use self.inner assuming LamportQueue fields are pub(crate) or pub
        let ring_instance = unsafe { &*self.inner };

        if self.contiguous_free_in_ring() < count_to_push {
            return false; // Not enough contiguous space in the ring
        }

        let local_buf_array_ptr = self.local_buf.get();
        
        let ring_buffer_raw = ring_instance.buf.as_ptr() as *mut UnsafeCell<Option<T>>; // Access pub(crate) buf
        let ring_mask = ring_instance.mask; // Access pub(crate) mask
        let ring_tail_atomic_ptr = &ring_instance.tail; // Access pub(crate) tail

        let current_ring_tail_val = ring_tail_atomic_ptr.load(Ordering::Relaxed);

        unsafe {
            let local_buf_slice = &*local_buf_array_ptr;

            for i in (0..count_to_push).rev() {
                let item_from_local_buf = ptr::read(local_buf_slice[i].as_ptr());
                let target_slot_in_ring = (current_ring_tail_val.wrapping_add(i)) & ring_mask;
                
                let slot_cell_ptr = ring_buffer_raw.add(target_slot_in_ring);
                (*(*slot_cell_ptr).get()) = Some(item_from_local_buf);
            }
        }
        
        ring_tail_atomic_ptr.store(
            current_ring_tail_val.wrapping_add(count_to_push),
            Ordering::Release
        );

        self.local_count.store(0, Ordering::Relaxed);
        true
    }
}

impl<T: Send + 'static> Drop for MultiPushQueue<T> {
    fn drop(&mut self) {
        // Attempt to flush any remaining items.
        // This is best-effort as the ring might be full or other issues could prevent flushing.
        if self.local_count.load(Ordering::Relaxed) > 0 {
            self.flush(); 
        }

        // Drop any items that might still be in local_buf if flush failed or wasn't complete
        let final_local_count = self.local_count.load(Ordering::Relaxed);
        if final_local_count > 0 {
            let local_b_mut_ptr = self.local_buf.get();
            unsafe {
                let local_b_slice_mut = &mut *local_b_mut_ptr;
                for i in 0..final_local_count {
                    if std::mem::needs_drop::<T>() {
                        ptr::drop_in_place(local_b_slice_mut[i].as_mut_ptr());
                    }
                }
            }
        }

        if !self.shared.load(Ordering::Relaxed) {
            unsafe {
                drop(Box::from_raw(self.inner));
            }
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for MultiPushQueue<T> {
    type PushError = ();
    type PopError  = <LamportQueue<T> as SpscQueue<T>>::PopError;

    #[inline]
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        let current_local_idx = self.local_count.load(Ordering::Relaxed);

        if current_local_idx < LOCAL_BUF {
            unsafe {
                let slot_ptr = (*self.local_buf.get()).as_mut_ptr().add(current_local_idx);
                slot_ptr.write(MaybeUninit::new(item));
            }
            self.local_count.store(current_local_idx + 1, Ordering::Relaxed); 

            if current_local_idx + 1 == LOCAL_BUF {
                self.flush(); // Attempt to flush, ignore failure for now (item is in local_buf)
            }
            return Ok(());
        }

        // local_buf is full, try to flush
        if self.flush() {
            // Flush succeeded (or buffer was empty after all), local_buf is now empty.
            // Recursively call push; this is safe as local_count is now 0.
            return self.push(item);
        }

        // Fallback: local_buf full, AND flush failed (ring buffer also full for a batch).
        // Try a direct single push to the underlying ring.
        match self.ring_mut().push(item) {
            Ok(_) => Ok(()),
            Err(_) => Err(()),
        }
    }

    #[inline]
    fn pop(&self) -> Result<T, Self::PopError> {
        self.ring().pop()
    }

    #[inline]
    fn available(&self) -> bool {
        self.local_count.load(Ordering::Relaxed) < LOCAL_BUF || self.ring().available()
    }

    #[inline]
    fn empty(&self) -> bool {
        self.local_count.load(Ordering::Relaxed) == 0 && self.ring().empty()
    }
}

impl<T: Send> fmt::Debug for MultiPushQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiPushQueue")
            .field("local_count", &self.local_count.load(Ordering::Relaxed))
            .field("shared", &self.shared.load(Ordering::Relaxed))
            .finish()
    }
}