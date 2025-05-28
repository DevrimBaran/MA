use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};

const H_PARTITION_SIZE: usize = 32;
const LOCAL_BATCH_SIZE: usize = 32;

// Atomic slot that can store Copy types directly
#[repr(C, align(8))]
struct AtomicSlot<T: Copy> {
    occupied: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T: Copy> AtomicSlot<T> {
    const fn new(_value: T) -> Self {
        Self {
            occupied: AtomicBool::new(false),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    #[inline]
    fn is_occupied(&self) -> bool {
        self.occupied.load(Ordering::Acquire)
    }

    #[inline]
    fn store(&self, value: T) {
        unsafe {
            // Write the value first
            (*self.value.get()).write(value);
        }
        // Then set occupied flag with Release ordering to ensure the value write happens-before any subsequent read
        self.occupied.store(true, Ordering::Release);
    }

    #[inline]
    fn take(&self) -> Option<T> {
        // Use AcqRel for the swap to ensure:
        // - Acquire: we see the value write if occupied was true
        // - Release: our subsequent operations don't get reordered before this
        if self.occupied.swap(false, Ordering::AcqRel) {
            // Safety: if occupied was true, then a value was written
            Some(unsafe { (*self.value.get()).assume_init_read() })
        } else {
            None
        }
    }
}

#[repr(C, align(64))]
pub struct ProducerFieldsB<T: Copy + Send + Default + 'static> {
    write: AtomicUsize,
    limit: AtomicUsize,
    local_buffer: UnsafeCell<[MaybeUninit<T>; LOCAL_BATCH_SIZE]>,
    pub local_count: AtomicUsize,
}

#[repr(C, align(64))]
struct ConsumerFieldsB {
    read: AtomicUsize,
    clear: AtomicUsize,
}

#[repr(C, align(64))]
pub struct BiffqQueue<T: Copy + Send + Default + 'static> {
    pub prod: ProducerFieldsB<T>,
    cons: ConsumerFieldsB,
    capacity: usize,
    mask: usize,
    h_mask: usize,
    buffer: *mut AtomicSlot<T>,
    owns_buffer: bool,
}

unsafe impl<T: Copy + Send + Default> Send for BiffqQueue<T> {}
unsafe impl<T: Copy + Send + Default> Sync for BiffqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct BiffqPushError<T>(pub T);

#[derive(Debug, PartialEq, Eq)]
pub struct BiffqPopError;

impl<T: Copy + Send + Default + 'static> BiffqQueue<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity.is_power_of_two(),
            "Capacity must be a power of two."
        );
        assert_eq!(
            capacity % H_PARTITION_SIZE,
            0,
            "Capacity must be a multiple of H_PARTITION_SIZE."
        );
        assert!(
            capacity >= 2 * H_PARTITION_SIZE,
            "Capacity must be at least 2 * H_PARTITION_SIZE."
        );

        // Allocate buffer with proper alignment
        let layout = std::alloc::Layout::array::<AtomicSlot<T>>(capacity).unwrap();
        let buffer_ptr = unsafe {
            let ptr = std::alloc::alloc(layout) as *mut AtomicSlot<T>;
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            // Initialize all slots as unoccupied
            for i in 0..capacity {
                ptr::write(ptr.add(i), AtomicSlot::new(T::default()));
            }
            ptr
        };

        let local_buf_uninit: [MaybeUninit<T>; LOCAL_BATCH_SIZE] =
            unsafe { MaybeUninit::uninit().assume_init() };

        Self {
            prod: ProducerFieldsB {
                write: AtomicUsize::new(H_PARTITION_SIZE),
                limit: AtomicUsize::new(2 * H_PARTITION_SIZE),
                local_buffer: UnsafeCell::new(local_buf_uninit),
                local_count: AtomicUsize::new(0),
            },
            cons: ConsumerFieldsB {
                read: AtomicUsize::new(H_PARTITION_SIZE),
                clear: AtomicUsize::new(0),
            },
            capacity,
            mask: capacity - 1,
            h_mask: H_PARTITION_SIZE - 1,
            buffer: buffer_ptr,
            owns_buffer: true,
        }
    }

    pub fn shared_size(capacity: usize) -> usize {
        assert!(
            capacity > 0 && capacity.is_power_of_two(),
            "Capacity must be a power of two and > 0."
        );
        assert_eq!(
            capacity % H_PARTITION_SIZE,
            0,
            "Capacity must be a multiple of H_PARTITION_SIZE."
        );
        assert!(
            capacity >= 2 * H_PARTITION_SIZE,
            "Capacity must be at least 2 * H_PARTITION_SIZE."
        );

        let layout = std::alloc::Layout::new::<Self>();
        let buffer_layout = std::alloc::Layout::array::<AtomicSlot<T>>(capacity).unwrap();
        layout.extend(buffer_layout).unwrap().0.size()
    }

    pub unsafe fn init_in_shared(mem_ptr: *mut u8, capacity: usize) -> &'static mut Self {
        assert!(
            capacity.is_power_of_two(),
            "Capacity must be a power of two."
        );
        assert_eq!(
            capacity % H_PARTITION_SIZE,
            0,
            "Capacity must be a multiple of H_PARTITION_SIZE."
        );
        assert!(
            capacity >= 2 * H_PARTITION_SIZE,
            "Capacity must be at least 2 * H_PARTITION_SIZE."
        );

        let queue_ptr = mem_ptr as *mut Self;
        let buffer_data_ptr = mem_ptr.add(std::mem::size_of::<Self>()) as *mut AtomicSlot<T>;

        // Initialize all slots as unoccupied
        for i in 0..capacity {
            ptr::write(buffer_data_ptr.add(i), AtomicSlot::new(T::default()));
        }

        let local_buf_uninit: [MaybeUninit<T>; LOCAL_BATCH_SIZE] =
            MaybeUninit::uninit().assume_init();

        ptr::write(
            queue_ptr,
            Self {
                prod: ProducerFieldsB {
                    write: AtomicUsize::new(H_PARTITION_SIZE),
                    limit: AtomicUsize::new(2 * H_PARTITION_SIZE),
                    local_buffer: UnsafeCell::new(local_buf_uninit),
                    local_count: AtomicUsize::new(0),
                },
                cons: ConsumerFieldsB {
                    read: AtomicUsize::new(H_PARTITION_SIZE),
                    clear: AtomicUsize::new(0),
                },
                capacity,
                mask: capacity - 1,
                h_mask: H_PARTITION_SIZE - 1,
                buffer: buffer_data_ptr,
                owns_buffer: false,
            },
        );
        &mut *queue_ptr
    }

    #[inline]
    fn get_slot(&self, index: usize) -> &AtomicSlot<T> {
        unsafe { &*self.buffer.add(index & self.mask) }
    }

    fn publish_batch_internal(&self) -> Result<usize, ()> {
        let local_count = self.prod.local_count.load(Ordering::Relaxed);
        if local_count == 0 {
            return Ok(0);
        }

        let local_buf_ptr = self.prod.local_buffer.get();
        let mut current_write = self.prod.write.load(Ordering::Relaxed);
        let mut current_limit = self.prod.limit.load(Ordering::Acquire);
        let mut published_count = 0;

        for i in 0..local_count {
            if current_write == current_limit {
                let next_limit_potential = current_limit.wrapping_add(H_PARTITION_SIZE);
                let slot_to_check_idx = next_limit_potential & self.mask;
                let slot_occupied = self.get_slot(slot_to_check_idx).is_occupied();

                if slot_occupied {
                    // Slot is not empty, can't proceed
                    fence(Ordering::Release);
                    self.prod.write.store(current_write, Ordering::Release);

                    // Copy remaining items back to the beginning of local buffer
                    unsafe {
                        let remaining = local_count - i;
                        if remaining > 0 && i > 0 {
                            // Use memmove instead of copy to handle potential overlap
                            // First, read all remaining items into a temporary buffer
                            let mut temp: Vec<MaybeUninit<T>> = Vec::with_capacity(remaining);
                            for j in 0..remaining {
                                temp.push(ptr::read((*local_buf_ptr).as_ptr().add(i + j)));
                            }
                            // Then write them back to the beginning
                            for j in 0..remaining {
                                ptr::write((*local_buf_ptr).as_mut_ptr().add(j), temp[j]);
                            }
                        }
                    }
                    self.prod
                        .local_count
                        .store(local_count - i, Ordering::Release);
                    return if published_count > 0 {
                        Ok(published_count)
                    } else {
                        Err(())
                    };
                }
                self.prod
                    .limit
                    .store(next_limit_potential, Ordering::Release);
                current_limit = next_limit_potential;
            }

            let item_to_write = unsafe { ptr::read(&(*local_buf_ptr)[i]).assume_init() };
            let slot = self.get_slot(current_write);

            // Store value directly in the atomic slot
            slot.store(item_to_write);

            current_write = current_write.wrapping_add(1);
            published_count += 1;
        }

        fence(Ordering::Release);
        self.prod.write.store(current_write, Ordering::Release);
        self.prod.local_count.store(0, Ordering::Release);
        Ok(published_count)
    }

    fn dequeue_internal(&self) -> Result<T, BiffqPopError> {
        let current_read = self.cons.read.load(Ordering::Relaxed);
        let slot = self.get_slot(current_read);

        // Try to take the item atomically
        if let Some(item) = slot.take() {
            self.cons
                .read
                .store(current_read.wrapping_add(1), Ordering::Release);

            // Clear operation
            let current_clear = self.cons.clear.load(Ordering::Relaxed);
            let read_partition_start = current_read & !self.h_mask;
            let next_clear_target = read_partition_start.wrapping_sub(H_PARTITION_SIZE);

            let mut temp_clear = current_clear;
            let mut advanced_clear = false;
            while temp_clear != next_clear_target {
                if temp_clear == self.cons.read.load(Ordering::Acquire) {
                    break;
                }
                // Slots are already cleared by take() above, just advance pointer
                temp_clear = temp_clear.wrapping_add(1);
                advanced_clear = true;
            }
            if advanced_clear {
                self.cons.clear.store(temp_clear, Ordering::Release);
            }
            Ok(item)
        } else {
            Err(BiffqPopError)
        }
    }

    pub fn flush_producer_buffer(&self) -> Result<usize, ()> {
        self.publish_batch_internal()
    }
}

impl<T: Copy + Send + Default + 'static> SpscQueue<T> for BiffqQueue<T> {
    type PushError = BiffqPushError<T>;
    type PopError = BiffqPopError;

    #[inline]
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        let current_local_count = self.prod.local_count.load(Ordering::Relaxed);

        if current_local_count < LOCAL_BATCH_SIZE {
            unsafe {
                let local_buf_slot_ptr = (*self.prod.local_buffer.get())
                    .as_mut_ptr()
                    .add(current_local_count);
                ptr::write(local_buf_slot_ptr, MaybeUninit::new(item));
            }
            self.prod
                .local_count
                .store(current_local_count + 1, Ordering::Release);

            if current_local_count + 1 == LOCAL_BATCH_SIZE {
                let _ = self.publish_batch_internal();
            }
            Ok(())
        } else {
            match self.publish_batch_internal() {
                Ok(_published_count) => {
                    let new_local_count = self.prod.local_count.load(Ordering::Relaxed);
                    if new_local_count < LOCAL_BATCH_SIZE {
                        unsafe {
                            let local_buf_slot_ptr = (*self.prod.local_buffer.get())
                                .as_mut_ptr()
                                .add(new_local_count);
                            ptr::write(local_buf_slot_ptr, MaybeUninit::new(item));
                        }
                        self.prod
                            .local_count
                            .store(new_local_count + 1, Ordering::Release);
                        Ok(())
                    } else {
                        Err(BiffqPushError(item))
                    }
                }
                Err(_) => Err(BiffqPushError(item)),
            }
        }
    }

    #[inline]
    fn pop(&self) -> Result<T, Self::PopError> {
        self.dequeue_internal()
    }

    #[inline]
    fn available(&self) -> bool {
        if self.prod.local_count.load(Ordering::Relaxed) < LOCAL_BATCH_SIZE {
            return true;
        }
        let write = self.prod.write.load(Ordering::Relaxed);
        let limit = self.prod.limit.load(Ordering::Acquire);
        if write != limit {
            return true;
        }
        let next_limit_potential = limit.wrapping_add(H_PARTITION_SIZE);
        let slot_to_check_idx = next_limit_potential & self.mask;
        !self.get_slot(slot_to_check_idx).is_occupied()
    }

    #[inline]
    fn empty(&self) -> bool {
        let local_empty = self.prod.local_count.load(Ordering::Relaxed) == 0;
        if !local_empty {
            return false;
        }

        let current_read = self.cons.read.load(Ordering::Acquire);
        !self.get_slot(current_read).is_occupied()
    }
}

impl<T: Copy + Send + Default + 'static> Drop for BiffqQueue<T> {
    fn drop(&mut self) {
        if self.owns_buffer {
            // Flush any items in local buffer
            let local_count = *self.prod.local_count.get_mut();
            if local_count > 0 {
                let _ = self.publish_batch_internal();
            }

            // Drop items still in local buffer after flush attempt
            let remaining_local = *self.prod.local_count.get_mut();
            if remaining_local > 0 {
                unsafe {
                    let local_buf_ptr = (*self.prod.local_buffer.get_mut()).as_mut_ptr();
                    for i in 0..remaining_local {
                        ptr::drop_in_place(local_buf_ptr.add(i).cast::<T>());
                    }
                }
            }

            // No need to drop items in slots since they're Copy types
            // Just free the buffer
            unsafe {
                let layout = std::alloc::Layout::array::<AtomicSlot<T>>(self.capacity).unwrap();
                std::alloc::dealloc(self.buffer as *mut u8, layout);
            }
        }
    }
}

impl<T: Copy + Send + Default + fmt::Debug + 'static> fmt::Debug for BiffqQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BiffqQueue")
            .field("capacity", &self.capacity)
            .field(
                "local_count",
                &self.prod.local_count.load(Ordering::Relaxed),
            )
            .field("write", &self.prod.write.load(Ordering::Relaxed))
            .field("limit", &self.prod.limit.load(Ordering::Relaxed))
            .field("read", &self.cons.read.load(Ordering::Relaxed))
            .field("clear", &self.cons.clear.load(Ordering::Relaxed))
            .field("owns_buffer", &self.owns_buffer)
            .finish()
    }
}
