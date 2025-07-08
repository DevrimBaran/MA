// paper in /paper/dspc-uspsc-mspsc-full.pdf and /paper/dspc-uspsc-mspsc.pdf
use crate::spsc::LamportQueue;
use crate::SpscQueue;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::{cell::UnsafeCell, fmt, mem::MaybeUninit, ptr};
use std::alloc::Layout;

const LOCAL_BUF: usize = 32; // Paper's MULTIPUSH_BUFFER_SIZE

pub struct MultiPushQueue<T: Send + 'static> {
    inner: *mut LamportQueue<T>, // Underlying SPSC buffer
    local_buf: UnsafeCell<[MaybeUninit<T>; LOCAL_BUF]>, // Local buffer for batching
    pub local_count: AtomicUsize, // Paper's mcnt
    shared: AtomicBool,          // IPC: track if in shared memory
}

unsafe impl<T: Send> Send for MultiPushQueue<T> {}
unsafe impl<T: Send> Sync for MultiPushQueue<T> {}

impl<T: Send + 'static> MultiPushQueue<T> {
    // IPC: initialize in shared memory
    pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
        let self_ptr = mem as *mut MaybeUninit<Self>;

        let self_layout = Layout::new::<Self>();
        let lamport_layout = Layout::from_size_align(
            LamportQueue::<T>::shared_size(capacity),
            core::mem::align_of::<LamportQueue<T>>(),
        )
        .expect("Failed to create layout for LamportQueue in init_in_shared");

        let (_combined_layout, lamport_offset) = self_layout
            .extend(lamport_layout)
            .expect("Failed to extend layout for MultiPushQueue in init_in_shared");

        let lamport_q_ptr_raw = mem.add(lamport_offset);
        let lamport_q_instance = LamportQueue::init_in_shared(lamport_q_ptr_raw, capacity);

        let initial_value = Self::from_raw(lamport_q_instance as *mut _, true);
        ptr::write(self_ptr, MaybeUninit::new(initial_value));
        &mut *(*self_ptr).as_mut_ptr()
    }

    // IPC: calculate shared memory size
    pub fn shared_size(capacity: usize) -> usize {
        let self_layout = Layout::new::<Self>();
        let lamport_layout = Layout::from_size_align(
            LamportQueue::<T>::shared_size(capacity),
            core::mem::align_of::<LamportQueue<T>>(),
        )
        .expect("Failed to create layout for LamportQueue in shared_size");

        let (combined_layout, _offset_lamport) = self_layout
            .extend(lamport_layout)
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

    // Helper to calculate contiguous free space in ring
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

    // flush() - Lines 22-24 in Figure 4
    pub fn flush(&self) -> bool {
        let count_to_push = self.local_count.load(Ordering::Relaxed);
        if count_to_push == 0 {
            return true; // Line 23
        }

        let ring_instance = unsafe { &*self.inner };

        if self.contiguous_free_in_ring() < count_to_push {
            return false; // Not enough space
        }

        let local_buf_array_ptr = self.local_buf.get();

        let ring_buffer_raw = ring_instance.buf.as_ptr() as *mut UnsafeCell<Option<T>>;
        let ring_mask = ring_instance.mask;
        let ring_tail_atomic_ptr = &ring_instance.tail;

        let current_ring_tail_val = ring_tail_atomic_ptr.load(Ordering::Relaxed);

        unsafe {
            let local_buf_slice = &*local_buf_array_ptr;

            // Lines 6-12 in Figure 4 - write in reverse order (critical!)
            for i in (0..count_to_push).rev() {
                let item_from_local_buf = ptr::read(local_buf_slice[i].as_ptr());
                let target_slot_in_ring = (current_ring_tail_val.wrapping_add(i)) & ring_mask;

                let slot_cell_ptr = ring_buffer_raw.add(target_slot_in_ring);
                (*(*slot_cell_ptr).get()) = Some(item_from_local_buf);
            }
        }

        // Line 15 - update pwrite after all writes
        ring_tail_atomic_ptr.store(
            current_ring_tail_val.wrapping_add(count_to_push),
            Ordering::Release,
        );

        self.local_count.store(0, Ordering::Relaxed); // Line 16 - reset mcnt
        true
    }
}

impl<T: Send + 'static> Drop for MultiPushQueue<T> {
    fn drop(&mut self) {} // IPC: shared memory cleanup handled externally
}

impl<T: Send + 'static> SpscQueue<T> for MultiPushQueue<T> {
    type PushError = ();
    type PopError = <LamportQueue<T> as SpscQueue<T>>::PopError;

    // mpush() - Lines 26-36 in Figure 4
    #[inline]
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        let current_local_idx = self.local_count.load(Ordering::Relaxed);

        if current_local_idx < LOCAL_BUF {
            // Line 30 - add to local buffer
            unsafe {
                let slot_ptr = (*self.local_buf.get()).as_mut_ptr().add(current_local_idx);
                slot_ptr.write(MaybeUninit::new(item));
            }
            self.local_count
                .store(current_local_idx + 1, Ordering::Relaxed);

            // Lines 32-33 - flush if buffer full
            if current_local_idx + 1 == LOCAL_BUF {
                self.flush();
            }
            return Ok(());
        }

        // Buffer full, try to flush then retry
        if self.flush() {
            return self.push(item);
        }

        // Fallback to direct push
        match self.ring_mut().push(item) {
            Ok(_) => Ok(()),
            Err(_) => Err(()),
        }
    }

    #[inline]
    fn pop(&self) -> Result<T, Self::PopError> {
        self.ring().pop() // Regular pop from underlying SPSC
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
