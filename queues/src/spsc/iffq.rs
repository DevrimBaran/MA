//paper in /paper/spsc/Cache‐aware design of general‐purpose Single‐Producer Single‐Consumer queues.pdf
use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const H_PARTITION_SIZE: usize = 32; // Section 4.2 - H value

// 8-byte slot with flag in pointer - Section 4.1
#[repr(transparent)]
struct Slot<T> {
    ptr: AtomicPtr<T>,
}

impl<T> Slot<T> {
    const FLAG_BIT: usize = 0x1; // Use lowest bit as flag

    fn new() -> Self {
        Self {
            ptr: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        let ptr = self.ptr.load(Ordering::Acquire);
        ptr.is_null() || (ptr as usize & Self::FLAG_BIT) == 0
    }

    #[inline]
    fn store_data(&self, data_ptr: *mut T) {
        // Set flag bit to indicate "full"
        let tagged_ptr = ((data_ptr as usize) | Self::FLAG_BIT) as *mut T;
        self.ptr.store(tagged_ptr, Ordering::Release);
    }

    #[inline]
    fn load_data(&self) -> *mut T {
        let tagged_ptr = self.ptr.load(Ordering::Acquire);
        // Clear flag bit to get real pointer
        ((tagged_ptr as usize) & !Self::FLAG_BIT) as *mut T
    }

    #[inline]
    fn clear(&self) {
        self.ptr.store(ptr::null_mut(), Ordering::Release);
    }
}

// Producer fields - group D
#[repr(C, align(64))]
struct ProducerFields {
    write: AtomicUsize, // Current write position - Section 4.2
    limit: AtomicUsize, // Write limit (H slots ahead check) - Section 4.2
}

// Consumer fields - group E
#[repr(C, align(64))]
pub struct ConsumerFields {
    read: AtomicUsize,      // Current read position - Section 4.2
    pub clear: AtomicUsize, // Lazy clear position - Section 4.2
}

#[repr(C, align(64))]
pub struct IffqQueue<T: Send + 'static> {
    prod: ProducerFields,
    pub cons: ConsumerFields,
    capacity: usize,
    mask: usize,   // For fast modulo
    h_mask: usize, // H_PARTITION_SIZE - 1
    buffer: *mut Slot<T>,
    data_buffer: *mut UnsafeCell<MaybeUninit<T>>, // IPC: separate data storage
    owns_buffer: bool,                            // IPC adaptation
}

unsafe impl<T: Send> Send for IffqQueue<T> {}
unsafe impl<T: Send> Sync for IffqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct IffqPushError<T>(pub T);

#[derive(Debug, PartialEq, Eq)]
pub struct IffqPopError;

impl<T: Send + 'static> IffqQueue<T> {
    // IPC adaptation - calculate shared memory size
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
        let slot_layout = std::alloc::Layout::array::<Slot<T>>(capacity).unwrap();
        let data_layout =
            std::alloc::Layout::array::<UnsafeCell<MaybeUninit<T>>>(capacity).unwrap();

        let (layout_with_slots, _) = layout.extend(slot_layout).unwrap();
        let (final_layout, _) = layout_with_slots.extend(data_layout).unwrap();
        final_layout.size()
    }

    // IPC adaptation - initialize in pre-allocated shared memory
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
        let slot_buffer_ptr = mem_ptr.add(std::mem::size_of::<Self>()) as *mut Slot<T>;
        let data_buffer_ptr = slot_buffer_ptr.add(capacity) as *mut UnsafeCell<MaybeUninit<T>>;

        // Initialize all slots as empty
        for i in 0..capacity {
            ptr::write(slot_buffer_ptr.add(i), Slot::new());
            ptr::write(
                data_buffer_ptr.add(i),
                UnsafeCell::new(MaybeUninit::uninit()),
            );
        }

        // Initialize with 2H slots unused - Section 4.2
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
                buffer: slot_buffer_ptr,
                data_buffer: data_buffer_ptr,
                owns_buffer: false, // IPC - buffer not heap allocated
            },
        );
        &mut *queue_ptr
    }

    #[inline]
    fn get_slot(&self, index: usize) -> &Slot<T> {
        unsafe { &*self.buffer.add(index & self.mask) }
    }

    #[inline]
    fn get_data_ptr(&self, index: usize) -> *mut UnsafeCell<MaybeUninit<T>> {
        unsafe { self.data_buffer.add(index & self.mask) }
    }

    // iffq_enqueue - Figure 11 in paper
    fn enqueue_internal(&self, item: T) -> Result<(), IffqPushError<T>> {
        let current_write = self.prod.write.load(Ordering::Relaxed);
        let mut current_limit = self.prod.limit.load(Ordering::Acquire);

        // Check if at limit
        if current_write == current_limit {
            // Check H slots ahead - Section 4.2
            let next_limit_potential = current_limit.wrapping_add(H_PARTITION_SIZE);
            let slot_to_check_idx = next_limit_potential & self.mask;

            let slot = self.get_slot(slot_to_check_idx);
            if !slot.is_empty() {
                // Slot occupied
                return Err(IffqPushError(item));
            }

            // Advance limit
            self.prod
                .limit
                .store(next_limit_potential, Ordering::Release);
            current_limit = next_limit_potential;

            if current_write == current_limit {
                return Err(IffqPushError(item));
            }
        }

        let slot = self.get_slot(current_write);
        let data_ptr = self.get_data_ptr(current_write);

        // Write data first
        unsafe {
            (*(*data_ptr).get()).write(item);
        }

        // Set flag with data pointer - linearization point
        slot.store_data(data_ptr as *mut T);

        self.prod
            .write
            .store(current_write.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    // iffq_trydeq_local - Figure 11 in paper
    fn dequeue_internal(&self) -> Result<T, IffqPopError> {
        let current_read = self.cons.read.load(Ordering::Relaxed);
        let slot = self.get_slot(current_read);

        // Check if slot is full
        if slot.is_empty() {
            return Err(IffqPopError);
        }

        // Get data pointer and read data
        let data_ptr = slot.load_data() as *mut UnsafeCell<MaybeUninit<T>>;
        let item = unsafe { (*(*data_ptr).get()).assume_init_read() };

        // Clear flag
        slot.clear();

        self.cons
            .read
            .store(current_read.wrapping_add(1), Ordering::Release);

        // Lazy clear operation - Section 4.2 (iifq_dec_publish function from paper integrated here)
        let current_clear = self.cons.clear.load(Ordering::Relaxed);
        let read_partition_start = current_read & !self.h_mask;
        let next_clear_target = read_partition_start.wrapping_sub(H_PARTITION_SIZE);

        let mut temp_clear = current_clear;
        let mut advanced_clear = false;
        while temp_clear != next_clear_target {
            if temp_clear == self.cons.read.load(Ordering::Acquire) {
                break;
            }

            // Advance clear pointer
            temp_clear = temp_clear.wrapping_add(1);
            advanced_clear = true;
        }

        if advanced_clear {
            self.cons.clear.store(temp_clear, Ordering::Release);
        }
        // iffq_dec_publish logic ends here

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
        // Check if can advance limit
        let next_limit_potential = limit.wrapping_add(H_PARTITION_SIZE);
        let slot_to_check_idx = next_limit_potential & self.mask;
        self.get_slot(slot_to_check_idx).is_empty()
    }

    #[inline]
    fn empty(&self) -> bool {
        let current_read = self.cons.read.load(Ordering::Acquire);
        self.get_slot(current_read).is_empty()
    }
}

impl<T: Send + 'static> Drop for IffqQueue<T> {
    fn drop(&mut self) {
        // IPC - cleanup handled externally
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
