//paper in /paper/spsc/Cache‐aware design of general‐purpose Single‐Producer Single‐Consumer queues.pdf
use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const H_PARTITION_SIZE: usize = 32;

// Compact slot with atomic flag
#[repr(C)]
struct Slot<T> {
    // Atomic flag: false = empty, true = full
    flag: AtomicBool,
    // Small padding to align data nicely (7 bytes on 64-bit systems)
    _pad: [u8; 8 - std::mem::size_of::<AtomicBool>()],
    // The actual data
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            _pad: [0u8; 8 - std::mem::size_of::<AtomicBool>()],
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

#[repr(C, align(64))]
struct ProducerFields {
    write: AtomicUsize,
    limit: AtomicUsize,
}

#[repr(C, align(64))]
pub struct ConsumerFields {
    read: AtomicUsize,
    pub clear: AtomicUsize,
}

#[repr(C, align(64))]
pub struct IffqQueue<T: Send + 'static> {
    prod: ProducerFields,
    pub cons: ConsumerFields,
    capacity: usize,
    mask: usize,
    h_mask: usize,
    buffer: *mut Slot<T>,
    owns_buffer: bool,
}

unsafe impl<T: Send> Send for IffqQueue<T> {}
unsafe impl<T: Send> Sync for IffqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct IffqPushError<T>(pub T);

#[derive(Debug, PartialEq, Eq)]
pub struct IffqPopError;

impl<T: Send + 'static> IffqQueue<T> {
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
        let buffer_layout = std::alloc::Layout::array::<Slot<T>>(capacity).unwrap();
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
        let buffer_data_ptr = mem_ptr.add(std::mem::size_of::<Self>()) as *mut Slot<T>;

        for i in 0..capacity {
            ptr::write(buffer_data_ptr.add(i), Slot::new());
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
    fn get_slot(&self, index: usize) -> &Slot<T> {
        unsafe { &*self.buffer.add(index & self.mask) }
    }

    fn enqueue_internal(&self, item: T) -> Result<(), IffqPushError<T>> {
        let current_write = self.prod.write.load(Ordering::Relaxed);
        let mut current_limit = self.prod.limit.load(Ordering::Acquire);

        if current_write == current_limit {
            // Check H slots ahead to see if we can advance the limit
            let next_limit_potential = current_limit.wrapping_add(H_PARTITION_SIZE);
            let slot_to_check_idx = next_limit_potential & self.mask;

            let slot = self.get_slot(slot_to_check_idx);
            if slot.flag.load(Ordering::Acquire) {
                // Slot is occupied, can't advance
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

        // Write data first
        unsafe {
            (*slot.data.get()).write(item);
        }

        // Then set flag with Release ordering to ensure data write happens-before
        slot.flag.store(true, Ordering::Release);

        self.prod
            .write
            .store(current_write.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    fn dequeue_internal(&self) -> Result<T, IffqPopError> {
        let current_read = self.cons.read.load(Ordering::Relaxed);
        let slot = self.get_slot(current_read);

        // Check if slot is full
        if !slot.flag.load(Ordering::Acquire) {
            return Err(IffqPopError);
        }

        // Read data
        let item = unsafe { (*slot.data.get()).assume_init_read() };

        // Clear flag atomically
        slot.flag.store(false, Ordering::Release);

        self.cons
            .read
            .store(current_read.wrapping_add(1), Ordering::Release);

        // Lazy clear operation
        let current_clear = self.cons.clear.load(Ordering::Relaxed);
        let read_partition_start = current_read & !self.h_mask;
        let next_clear_target = read_partition_start.wrapping_sub(H_PARTITION_SIZE);

        let mut temp_clear = current_clear;
        let mut advanced_clear = false;
        while temp_clear != next_clear_target {
            if temp_clear == self.cons.read.load(Ordering::Acquire) {
                break;
            }

            // The slot is already cleared when we read it, so just advance
            temp_clear = temp_clear.wrapping_add(1);
            advanced_clear = true;
        }

        if advanced_clear {
            self.cons.clear.store(temp_clear, Ordering::Release);
        }

        Ok(item)
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
        !self
            .get_slot(slot_to_check_idx)
            .flag
            .load(Ordering::Acquire)
    }

    #[inline]
    fn empty(&self) -> bool {
        let current_read = self.cons.read.load(Ordering::Acquire);
        !self.get_slot(current_read).flag.load(Ordering::Acquire)
    }
}

impl<T: Send + 'static> Drop for IffqQueue<T> {
    fn drop(&mut self) {
        if self.owns_buffer {
            unsafe {
                // Drop any remaining items
                if std::mem::needs_drop::<T>() {
                    for i in 0..self.capacity {
                        let slot = self.get_slot(i);
                        if slot.flag.load(Ordering::Relaxed) {
                            ptr::drop_in_place((*slot.data.get()).as_mut_ptr());
                        }
                    }
                }

                let layout = std::alloc::Layout::array::<Slot<T>>(self.capacity)
                    .unwrap()
                    .align_to(64)
                    .unwrap();
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
