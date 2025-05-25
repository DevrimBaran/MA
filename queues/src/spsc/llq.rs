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
}

#[repr(C)]
#[cfg_attr(any(target_arch = "x86_64", target_arch = "aarch64"), repr(align(64)))]
struct ConsumerPrivate {
    write_shadow: usize,
}

#[repr(C)]
pub struct LlqQueue<T: Send + 'static> {
    pub shared_indices: SharedIndices,
    prod_private: UnsafeCell<ProducerPrivate>,
    cons_private: UnsafeCell<ConsumerPrivate>,
    capacity: usize,
    pub mask: usize,
    pub buffer: ManuallyDrop<Box<[UnsafeCell<MaybeUninit<T>>]>>,
}

unsafe impl<T: Send> Send for LlqQueue<T> {}
unsafe impl<T: Send> Sync for LlqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct LlqPushError<T>(pub T);

#[derive(Debug, PartialEq, Eq)]
pub struct LlqPopError;

impl<T: Send + 'static> LlqQueue<T> {
    pub fn llq_shared_size(capacity: usize) -> usize {
        assert!(
            capacity > K_CACHE_LINE_SLOTS,
            "Capacity must be greater than K_CACHE_LINE_SLOTS"
        );
        assert!(
            capacity.is_power_of_two(),
            "Capacity must be a power of two"
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
                prod_private: UnsafeCell::new(ProducerPrivate { read_shadow: 0 }),
                cons_private: UnsafeCell::new(ConsumerPrivate { write_shadow: 0 }),
                capacity,
                mask: capacity - 1,
                buffer: ManuallyDrop::new(boxed_buffer),
            },
        );

        &mut *queue_struct_ptr
    }

    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity.is_power_of_two(),
            "Capacity must be a power of two."
        );
        assert!(
            capacity > K_CACHE_LINE_SLOTS,
            "Capacity must be greater than K_CACHE_LINE_SLOTS"
        );

        let mut buffer_mem: Vec<UnsafeCell<MaybeUninit<T>>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer_mem.push(UnsafeCell::new(MaybeUninit::uninit()));
        }

        Self {
            shared_indices: SharedIndices {
                write: AtomicUsize::new(0),
                read: AtomicUsize::new(0),
            },
            prod_private: UnsafeCell::new(ProducerPrivate { read_shadow: 0 }),
            cons_private: UnsafeCell::new(ConsumerPrivate { write_shadow: 0 }),
            capacity,
            mask: capacity - 1,
            buffer: ManuallyDrop::new(buffer_mem.into_boxed_slice()),
        }
    }

    fn enqueue_internal(&self, item: T) -> Result<(), LlqPushError<T>> {
        let prod_priv = unsafe { &mut *self.prod_private.get() };
        let current_write = self.shared_indices.write.load(Ordering::Relaxed);

        if current_write.wrapping_sub(prod_priv.read_shadow) == self.capacity - K_CACHE_LINE_SLOTS {
            prod_priv.read_shadow = self.shared_indices.read.load(Ordering::Acquire);
            if current_write.wrapping_sub(prod_priv.read_shadow)
                == self.capacity - K_CACHE_LINE_SLOTS
            {
                return Err(LlqPushError(item));
            }
        }

        let slot_idx = current_write & self.mask;
        unsafe {
            ptr::write(
                (*self.buffer.get_unchecked(slot_idx)).get(),
                MaybeUninit::new(item),
            );
        }

        self.shared_indices
            .write
            .store(current_write.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    fn dequeue_internal(&self) -> Result<T, LlqPopError> {
        let cons_priv = unsafe { &mut *self.cons_private.get() };
        let current_read = self.shared_indices.read.load(Ordering::Relaxed);

        if current_read == cons_priv.write_shadow {
            cons_priv.write_shadow = self.shared_indices.write.load(Ordering::Acquire);
            if current_read == cons_priv.write_shadow {
                return Err(LlqPopError);
            }
        }

        let slot_idx = current_read & self.mask;
        let item = unsafe { ptr::read((*self.buffer.get_unchecked(slot_idx)).get()).assume_init() };

        self.shared_indices
            .read
            .store(current_read.wrapping_add(1), Ordering::Release);
        Ok(item)
    }
}

impl<T: Send + 'static> SpscQueue<T> for LlqQueue<T> {
    type PushError = LlqPushError<T>;
    type PopError = LlqPopError;

    #[inline]
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        self.enqueue_internal(item)
    }

    #[inline]
    fn pop(&self) -> Result<T, Self::PopError> {
        self.dequeue_internal()
    }

    #[inline]
    fn available(&self) -> bool {
        let current_write = self.shared_indices.write.load(Ordering::Relaxed);
        let current_read = self.shared_indices.read.load(Ordering::Acquire);
        current_write.wrapping_sub(current_read) < self.capacity - K_CACHE_LINE_SLOTS
    }

    #[inline]
    fn empty(&self) -> bool {
        let current_read = self.shared_indices.read.load(Ordering::Relaxed);
        let current_write = self.shared_indices.write.load(Ordering::Acquire);
        current_read == current_write
    }
}

impl<T: Send + 'static> Drop for LlqQueue<T> {
    fn drop(&mut self) {
        if std::mem::needs_drop::<T>() {
            let mut read_idx = *self.shared_indices.read.get_mut();
            let write_idx = *self.shared_indices.write.get_mut();
            while read_idx != write_idx {
                let slot_idx = read_idx & self.mask;
                unsafe {
                    (*self.buffer.get_unchecked_mut(slot_idx))
                        .get_mut()
                        .assume_init_drop();
                }
                read_idx = read_idx.wrapping_add(1);
            }
        }
        unsafe {
            ManuallyDrop::drop(&mut self.buffer);
        }
    }
}

impl<T: Send + fmt::Debug + 'static> fmt::Debug for LlqQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LlqQueue")
            .field("capacity", &self.capacity)
            .field("write", &self.shared_indices.write.load(Ordering::Relaxed))
            .field("read", &self.shared_indices.read.load(Ordering::Relaxed))
            .field("read_shadow (prod)", unsafe {
                &(*self.prod_private.get()).read_shadow
            })
            .field("write_shadow (cons)", unsafe {
                &(*self.cons_private.get()).write_shadow
            })
            .finish()
    }
}
