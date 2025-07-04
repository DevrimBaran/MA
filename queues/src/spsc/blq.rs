//paper in /paper/spsc/Cache‐aware design of general‐purpose Single‐Producer Single‐Consumer queues.pdf
use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const K_CACHE_LINE_SLOTS: usize = 8;

#[repr(C)]
#[cfg_attr(any(target_arch = "x86_64", target_arch = "aarch64"), repr(align(64)))]
pub struct SharedIndices {
    pub write: AtomicUsize,
    pub read: AtomicUsize,
}

#[repr(C)]
#[cfg_attr(any(target_arch = "x86_64", target_arch = "aarch64"), repr(align(64)))]
struct ProducerPrivate {
    read_shadow: usize,

    write_priv: usize,
}

#[repr(C)]
#[cfg_attr(any(target_arch = "x86_64", target_arch = "aarch64"), repr(align(64)))]
struct ConsumerPrivate {
    write_shadow: usize,

    read_priv: usize,
}

#[repr(C)]
pub struct BlqQueue<T: Send + 'static> {
    shared_indices: SharedIndices,

    prod_private: UnsafeCell<ProducerPrivate>,

    cons_private: UnsafeCell<ConsumerPrivate>,
    capacity: usize,
    mask: usize,
    buffer: ManuallyDrop<Box<[UnsafeCell<MaybeUninit<T>>]>>,
    owns_buffer: bool,
}

unsafe impl<T: Send> Send for BlqQueue<T> {}
unsafe impl<T: Send> Sync for BlqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct BlqPushError<T>(pub T);

#[derive(Debug, PartialEq, Eq)]
pub struct BlqPopError;

impl<T: Send + 'static> BlqQueue<T> {
    pub fn shared_size(capacity: usize) -> usize {
        assert!(
            capacity.is_power_of_two(),
            "Capacity must be a power of two."
        );
        assert!(
            capacity > K_CACHE_LINE_SLOTS,
            "Capacity must be greater than K_CACHE_LINE_SLOTS"
        );

        let layout_header = std::alloc::Layout::new::<Self>();
        let layout_buffer_elements =
            std::alloc::Layout::array::<UnsafeCell<MaybeUninit<T>>>(capacity).unwrap();

        let (combined_layout, _offset_of_buffer) =
            layout_header.extend(layout_buffer_elements).unwrap();
        combined_layout.pad_to_align().size()
    }
    pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
        assert!(
            capacity.is_power_of_two(),
            "Capacity must be a power of two."
        );
        assert!(
            capacity > K_CACHE_LINE_SLOTS,
            "Capacity must be greater than K_CACHE_LINE_SLOTS"
        );

        let queue_struct_ptr = mem as *mut Self;

        let layout_header = std::alloc::Layout::new::<Self>();
        let layout_buffer_elements =
            std::alloc::Layout::array::<UnsafeCell<MaybeUninit<T>>>(capacity).unwrap();
        let (_combined_layout, offset_of_buffer) =
            layout_header.extend(layout_buffer_elements).unwrap();

        let buffer_data_start_ptr = mem.add(offset_of_buffer) as *mut UnsafeCell<MaybeUninit<T>>;

        let buffer_slice = std::slice::from_raw_parts_mut(buffer_data_start_ptr, capacity);
        let boxed_buffer = Box::from_raw(buffer_slice);

        ptr::write(
            queue_struct_ptr,
            Self {
                shared_indices: SharedIndices {
                    write: AtomicUsize::new(0),
                    read: AtomicUsize::new(0),
                },
                prod_private: UnsafeCell::new(ProducerPrivate {
                    read_shadow: 0,
                    write_priv: 0,
                }),
                cons_private: UnsafeCell::new(ConsumerPrivate {
                    write_shadow: 0,
                    read_priv: 0,
                }),
                capacity,
                mask: capacity - 1,
                buffer: ManuallyDrop::new(boxed_buffer),
                owns_buffer: false,
            },
        );

        &mut *queue_struct_ptr
    }

    #[inline]
    pub fn blq_enq_space(&self, needed: usize) -> usize {
        let prod_priv = unsafe { &mut *self.prod_private.get() };

        let mut free_slots = (self.capacity - K_CACHE_LINE_SLOTS)
            .wrapping_sub(prod_priv.write_priv.wrapping_sub(prod_priv.read_shadow));

        if free_slots < needed {
            prod_priv.read_shadow = self.shared_indices.read.load(Ordering::Acquire);
            free_slots = (self.capacity - K_CACHE_LINE_SLOTS)
                .wrapping_sub(prod_priv.write_priv.wrapping_sub(prod_priv.read_shadow));
        }
        free_slots
    }

    #[inline]
    pub fn blq_enq_local(&self, item: T) -> Result<(), BlqPushError<T>> {
        let prod_priv = unsafe { &mut *self.prod_private.get() };
        let current_write_priv = prod_priv.write_priv;

        let num_filled = current_write_priv.wrapping_sub(prod_priv.read_shadow);
        if num_filled >= self.capacity - K_CACHE_LINE_SLOTS {
            prod_priv.read_shadow = self.shared_indices.read.load(Ordering::Acquire);
            if current_write_priv.wrapping_sub(prod_priv.read_shadow)
                >= self.capacity - K_CACHE_LINE_SLOTS
            {
                return Err(BlqPushError(item));
            }
        }

        let slot_idx = current_write_priv & self.mask;
        unsafe {
            ptr::write(
                (*self.buffer.get_unchecked(slot_idx)).get(),
                MaybeUninit::new(item),
            );
        }
        prod_priv.write_priv = current_write_priv.wrapping_add(1);
        Ok(())
    }

    #[inline]
    pub fn blq_enq_publish(&self) {
        let prod_priv = unsafe { &*self.prod_private.get() };

        self.shared_indices
            .write
            .store(prod_priv.write_priv, Ordering::Release);
    }

    #[inline]
    pub fn blq_deq_space(&self, needed: usize) -> usize {
        let cons_priv = unsafe { &mut *self.cons_private.get() };

        let mut available_items = cons_priv.write_shadow.wrapping_sub(cons_priv.read_priv);

        if available_items < needed {
            cons_priv.write_shadow = self.shared_indices.write.load(Ordering::Acquire);
            available_items = cons_priv.write_shadow.wrapping_sub(cons_priv.read_priv);
        }
        available_items
    }

    #[inline]
    pub fn blq_deq_local(&self) -> Result<T, BlqPopError> {
        let cons_priv = unsafe { &mut *self.cons_private.get() };
        let current_read_priv = cons_priv.read_priv;

        if current_read_priv == cons_priv.write_shadow {
            cons_priv.write_shadow = self.shared_indices.write.load(Ordering::Acquire);
            if current_read_priv == cons_priv.write_shadow {
                return Err(BlqPopError);
            }
        }

        let slot_idx = current_read_priv & self.mask;
        let item = unsafe { ptr::read((*self.buffer.get_unchecked(slot_idx)).get()).assume_init() };
        cons_priv.read_priv = current_read_priv.wrapping_add(1);
        Ok(item)
    }

    #[inline]
    pub fn blq_deq_publish(&self) {
        let cons_priv = unsafe { &*self.cons_private.get() };

        self.shared_indices
            .read
            .store(cons_priv.read_priv, Ordering::Release);
    }
}

impl<T: Send + 'static> SpscQueue<T> for BlqQueue<T> {
    type PushError = BlqPushError<T>;
    type PopError = BlqPopError;

    #[inline]
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        if self.blq_enq_space(1) == 0 {
            return Err(BlqPushError(item));
        }
        self.blq_enq_local(item)?;
        self.blq_enq_publish();
        Ok(())
    }

    #[inline]
    fn pop(&self) -> Result<T, Self::PopError> {
        if self.blq_deq_space(1) == 0 {
            return Err(BlqPopError);
        }
        let item = self.blq_deq_local()?;
        self.blq_deq_publish();
        Ok(item)
    }

    #[inline]
    fn available(&self) -> bool {
        self.blq_enq_space(1) > 0
    }

    #[inline]
    fn empty(&self) -> bool {
        self.blq_deq_space(1) == 0
    }
}

impl<T: Send + 'static> Drop for BlqQueue<T> {
    fn drop(&mut self) {}
}

impl<T: Send + fmt::Debug + 'static> fmt::Debug for BlqQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prod_priv = unsafe { &*self.prod_private.get() };
        let cons_priv = unsafe { &*self.cons_private.get() };
        f.debug_struct("BlqQueue")
            .field("capacity", &self.capacity)
            .field("mask", &self.mask)
            .field(
                "shared_write",
                &self.shared_indices.write.load(Ordering::Relaxed),
            )
            .field(
                "shared_read",
                &self.shared_indices.read.load(Ordering::Relaxed),
            )
            .field("prod_write_priv", &prod_priv.write_priv)
            .field("prod_read_shadow", &prod_priv.read_shadow)
            .field("cons_read_priv", &cons_priv.read_priv)
            .field("cons_write_shadow", &cons_priv.write_shadow)
            .field("owns_buffer", &self.owns_buffer)
            .finish()
    }
}
