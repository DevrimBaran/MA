//paper in /paper/spsc/Cache‐aware design of general‐purpose Single‐Producer Single‐Consumer queues.pdf
use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering};

const H_PARTITION_SIZE: usize = 32;
const LOCAL_BATCH_SIZE: usize = 32; // Section 4.3 - batch size

// 8-byte slot with flag in pointer - same as IFFQ
#[repr(transparent)]
struct Slot<T> {
    ptr: AtomicPtr<T>,
}

impl<T> Slot<T> {
    const FLAG_BIT: usize = 0x1;

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
        let tagged_ptr = ((data_ptr as usize) | Self::FLAG_BIT) as *mut T;
        self.ptr.store(tagged_ptr, Ordering::Release);
    }

    #[inline]
    fn load_data(&self) -> *mut T {
        let tagged_ptr = self.ptr.load(Ordering::Acquire);
        ((tagged_ptr as usize) & !Self::FLAG_BIT) as *mut T
    }

    #[inline]
    fn clear(&self) {
        self.ptr.store(ptr::null_mut(), Ordering::Release);
    }
}

// Producer fields with local buffer - Section 4.3
#[repr(C, align(64))]
pub struct ProducerFieldsB<T: Send + 'static> {
    write: AtomicUsize,
    pub limit: AtomicUsize,
    local_buffer: UnsafeCell<[MaybeUninit<T>; LOCAL_BATCH_SIZE]>, // Producer-private buffer
    pub local_count: AtomicUsize,                                 // Items in local buffer
}

// Consumer fields - same as IFFQ
#[repr(C, align(64))]
pub struct ConsumerFieldsB {
    read: AtomicUsize,
    pub clear: AtomicUsize,
}

#[repr(C, align(64))]
pub struct BiffqQueue<T: Send + 'static> {
    pub prod: ProducerFieldsB<T>,
    pub cons: ConsumerFieldsB,
    capacity: usize,
    mask: usize,
    h_mask: usize,
    buffer: *mut Slot<T>,
    data_buffer: *mut UnsafeCell<MaybeUninit<T>>, // IPC: separate data storage
    owns_buffer: bool,                            // IPC adaptation
}

unsafe impl<T: Send> Send for BiffqQueue<T> {}
unsafe impl<T: Send> Sync for BiffqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct BiffqPushError<T>(pub T);

#[derive(Debug, PartialEq, Eq)]
pub struct BiffqPopError;

impl<T: Send + 'static> BiffqQueue<T> {
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

    // Publish buffered items - Section 4.3 (biffq_enq_publish - figure 13)
    pub fn publish_batch_internal(&self) -> Result<usize, ()> {
        let local_count = self.prod.local_count.load(Ordering::Relaxed);
        if local_count == 0 {
            return Ok(0);
        }

        let local_buf_ptr = self.prod.local_buffer.get();
        let mut current_write = self.prod.write.load(Ordering::Relaxed);
        let mut current_limit = self.prod.limit.load(Ordering::Acquire);
        let mut published_count = 0;

        // Try to write burst - relies on race condition for FC optimization
        for i in 0..local_count {
            if current_write == current_limit {
                // Check H slots ahead
                let next_limit_potential = current_limit.wrapping_add(H_PARTITION_SIZE);
                let slot_to_check_idx = next_limit_potential & self.mask;

                if !self.get_slot(slot_to_check_idx).is_empty() {
                    // Can't proceed - copy remaining back
                    fence(Ordering::Release);
                    self.prod.write.store(current_write, Ordering::Release);

                    // Copy remaining items back to the beginning of local buffer
                    unsafe {
                        let remaining = local_count - i;
                        if remaining > 0 && i > 0 {
                            // Get a single mutable pointer to avoid aliasing issues
                            let buf_ptr = (*local_buf_ptr).as_mut_ptr();
                            // Use memmove to handle potential overlap
                            ptr::copy(buf_ptr.add(i), buf_ptr, remaining);
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
            let data_ptr = self.get_data_ptr(current_write);

            // Write data first
            unsafe {
                (*(*data_ptr).get()).write(item_to_write);
            }

            // Set flag - linearization point
            slot.store_data(data_ptr as *mut T);

            current_write = current_write.wrapping_add(1);
            published_count += 1;
        }

        fence(Ordering::Release);
        self.prod.write.store(current_write, Ordering::Release);
        self.prod.local_count.store(0, Ordering::Release);
        Ok(published_count)
    }

    // Same as IFFQ dequeue ("Consumer-side routines are not shown, as they are the same as illustrated in Figure 11 (iifq.rs)")
    fn dequeue_internal(&self) -> Result<T, BiffqPopError> {
        let current_read = self.cons.read.load(Ordering::Relaxed);
        let slot = self.get_slot(current_read);

        // Check if slot is full
        if slot.is_empty() {
            return Err(BiffqPopError);
        }

        // Get data pointer and read data
        let data_ptr = slot.load_data() as *mut UnsafeCell<MaybeUninit<T>>;
        let item = unsafe { (*(*data_ptr).get()).assume_init_read() };

        // Clear flag
        slot.clear();

        self.cons
            .read
            .store(current_read.wrapping_add(1), Ordering::Release);

        // Lazy clear - same as IFFQ
        let current_clear = self.cons.clear.load(Ordering::Relaxed);
        let read_partition_start = current_read & !self.h_mask;
        let next_clear_target = read_partition_start.wrapping_sub(H_PARTITION_SIZE);

        let mut temp_clear = current_clear;
        let mut advanced_clear = false;
        while temp_clear != next_clear_target {
            if temp_clear == self.cons.read.load(Ordering::Acquire) {
                break;
            }
            // Slots are already cleared when read, just advance pointer
            temp_clear = temp_clear.wrapping_add(1);
            advanced_clear = true;
        }
        if advanced_clear {
            self.cons.clear.store(temp_clear, Ordering::Release);
        }
        Ok(item)
    }

    pub fn flush_producer_buffer(&self) -> Result<usize, ()> {
        self.publish_batch_internal()
    }
}

impl<T: Send + 'static> SpscQueue<T> for BiffqQueue<T> {
    type PushError = BiffqPushError<T>;
    type PopError = BiffqPopError;

    // biffq_enq_local - Figure 13 in paper
    #[inline]
    fn push(&self, item: T) -> Result<(), Self::PushError> {
        let current_local_count = self.prod.local_count.load(Ordering::Relaxed);

        if current_local_count < LOCAL_BATCH_SIZE {
            // Store in local buffer
            unsafe {
                let local_buf_slot_ptr = (*self.prod.local_buffer.get())
                    .as_mut_ptr()
                    .add(current_local_count);
                ptr::write(local_buf_slot_ptr, MaybeUninit::new(item));
            }
            self.prod
                .local_count
                .store(current_local_count + 1, Ordering::Release);

            // Auto-publish when buffer full
            if current_local_count + 1 == LOCAL_BATCH_SIZE {
                let _ = self.publish_batch_internal();
            }
            Ok(())
        } else {
            // Buffer full, try to flush
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

    // integrates biffq_wspace - figure 13
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
        self.get_slot(slot_to_check_idx).is_empty()
    }

    #[inline]
    fn empty(&self) -> bool {
        let local_empty = self.prod.local_count.load(Ordering::Relaxed) == 0;
        if !local_empty {
            return false;
        }

        let current_read = self.cons.read.load(Ordering::Acquire);
        self.get_slot(current_read).is_empty()
    }
}

impl<T: Send + 'static> Drop for BiffqQueue<T> {
    fn drop(&mut self) {
        // IPC - cleanup handled externally
    }
}

impl<T: Send + fmt::Debug + 'static> fmt::Debug for BiffqQueue<T> {
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
