use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

const H_PARTITION_SIZE: usize = 32;

// Atomic wrapper for the slot to prevent data races
#[repr(C)]
struct AtomicSlot<T> {
    // Using AtomicUsize for the discriminant
    // 0 = None, 1 = Some
    state: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> AtomicSlot<T> {
    fn new_none() -> Self {
        Self {
            state: AtomicUsize::new(0),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    #[inline]
    fn is_some(&self) -> bool {
        self.state.load(Ordering::Acquire) == 1
    }

    #[inline]
    fn write(&self, value: T) {
        unsafe {
            (*self.value.get()).write(value);
        }
        // Release ordering ensures the value write happens-before state change
        self.state.store(1, Ordering::Release);
    }

    #[inline]
    fn read(&self) -> Option<T> {
        // Use swap to atomically take ownership
        if self.state.swap(0, Ordering::AcqRel) == 1 {
            // We successfully took ownership (changed from 1 to 0)
            Some(unsafe { (*self.value.get()).assume_init_read() })
        } else {
            None
        }
    }

    #[inline]
    fn clear(&self) {
        // This is now redundant since read() already clears
        self.state.store(0, Ordering::Release);
    }
}

#[repr(C, align(64))]
struct ProducerFields {
    write: AtomicUsize,
    limit: AtomicUsize,
}

#[repr(C, align(64))]
struct ConsumerFields {
    read: AtomicUsize,
    clear: AtomicUsize,
}

#[repr(C, align(64))]
pub struct IffqQueue<T: Send + 'static> {
    prod: ProducerFields,
    cons: ConsumerFields,
    capacity: usize,
    mask: usize,
    h_mask: usize,
    buffer: *mut AtomicSlot<T>,
    owns_buffer: bool,
}

unsafe impl<T: Send> Send for IffqQueue<T> {}
unsafe impl<T: Send> Sync for IffqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct IffqPushError<T>(pub T);

#[derive(Debug, PartialEq, Eq)]
pub struct IffqPopError;

impl<T: Send + 'static> IffqQueue<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity.is_power_of_two(),
            "Capacity must be a power of two."
        );
        assert_eq!(
            capacity % H_PARTITION_SIZE,
            0,
            "Capacity must be a multiple of H_PARTITION_SIZE ({}).",
            H_PARTITION_SIZE
        );
        assert!(
            capacity >= 2 * H_PARTITION_SIZE,
            "Capacity must be at least 2 * H_PARTITION_SIZE."
        );

        let layout = std::alloc::Layout::array::<AtomicSlot<T>>(capacity).unwrap();
        let buffer_ptr = unsafe {
            let ptr = std::alloc::alloc(layout) as *mut AtomicSlot<T>;
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            // Initialize all slots
            for i in 0..capacity {
                ptr::write(ptr.add(i), AtomicSlot::new_none());
            }
            ptr
        };

        Self {
            prod: ProducerFields {
                write: AtomicUsize::new(H_PARTITION_SIZE),
                limit: AtomicUsize::new(2 * H_PARTITION_SIZE),
            },
            cons: ConsumerFields {
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

        for i in 0..capacity {
            ptr::write(buffer_data_ptr.add(i), AtomicSlot::new_none());
        }

        ptr::write(
            queue_ptr,
            Self {
                prod: ProducerFields {
                    write: AtomicUsize::new(H_PARTITION_SIZE),
                    limit: AtomicUsize::new(2 * H_PARTITION_SIZE),
                },
                cons: ConsumerFields {
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

    fn enqueue_internal(&self, item: T) -> Result<(), IffqPushError<T>> {
        let current_write = self.prod.write.load(Ordering::Relaxed);
        let mut current_limit = self.prod.limit.load(Ordering::Acquire);

        if current_write == current_limit {
            let next_limit_potential = current_limit.wrapping_add(H_PARTITION_SIZE);
            let slot_to_check_idx = next_limit_potential & self.mask;

            let slot = self.get_slot(slot_to_check_idx);
            if slot.is_some() {
                return Err(IffqPushError(item));
            }

            self.prod
                .limit
                .store(next_limit_potential, Ordering::Release);
            current_limit = next_limit_potential;

            if current_write == current_limit {
                return Err(IffqPushError(item));
            }
        }

        let slot = self.get_slot(current_write);
        slot.write(item);

        self.prod
            .write
            .store(current_write.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    fn dequeue_internal(&self) -> Result<T, IffqPopError> {
        let current_read = self.cons.read.load(Ordering::Relaxed);
        let slot = self.get_slot(current_read);

        if let Some(item) = slot.read() {
            self.cons
                .read
                .store(current_read.wrapping_add(1), Ordering::Release);

            // Clear operation might need adjustment since read() already clears
            let current_clear = self.cons.clear.load(Ordering::Relaxed);
            let read_partition_start = current_read & !self.h_mask;
            let next_clear_target = read_partition_start.wrapping_sub(H_PARTITION_SIZE);

            let mut temp_clear = current_clear;
            let mut advanced_clear = false;
            while temp_clear != next_clear_target {
                if temp_clear == self.cons.read.load(Ordering::Acquire) {
                    break;
                }

                // Skip the clear since read() already cleared
                // let clear_slot = self.get_slot(temp_clear);
                // clear_slot.clear();

                temp_clear = temp_clear.wrapping_add(1);
                advanced_clear = true;
            }
            if advanced_clear {
                self.cons.clear.store(temp_clear, Ordering::Release);
            }

            Ok(item)
        } else {
            Err(IffqPopError)
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for IffqQueue<T> {
    type PushError = IffqPushError<T>;
    type PopError = IffqPopError;

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
        let write = self.prod.write.load(Ordering::Relaxed);
        let limit = self.prod.limit.load(Ordering::Acquire);
        if write != limit {
            return true;
        }
        let next_limit_potential = limit.wrapping_add(H_PARTITION_SIZE);
        let slot_to_check_idx = next_limit_potential & self.mask;
        !self.get_slot(slot_to_check_idx).is_some()
    }

    #[inline]
    fn empty(&self) -> bool {
        let current_read = self.cons.read.load(Ordering::Acquire);
        !self.get_slot(current_read).is_some()
    }
}

impl<T: Send + 'static> Drop for IffqQueue<T> {
    fn drop(&mut self) {
        if self.owns_buffer {
            unsafe {
                // Clear any remaining items
                if std::mem::needs_drop::<T>() {
                    for i in 0..self.capacity {
                        let slot = self.get_slot(i);
                        slot.clear();
                    }
                }

                let layout = std::alloc::Layout::array::<AtomicSlot<T>>(self.capacity).unwrap();
                std::alloc::dealloc(self.buffer as *mut u8, layout);
            }
        }
    }
}

impl<T: Send + fmt::Debug + 'static> fmt::Debug for IffqQueue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IffqQueue")
            .field("capacity", &self.capacity)
            .field("mask", &self.mask)
            .field("h_mask", &self.h_mask)
            .field("write", &self.prod.write.load(Ordering::Relaxed))
            .field("limit", &self.prod.limit.load(Ordering::Relaxed))
            .field("read", &self.cons.read.load(Ordering::Relaxed))
            .field("clear", &self.cons.clear.load(Ordering::Relaxed))
            .field("owns_buffer", &self.owns_buffer)
            .finish()
    }
}
