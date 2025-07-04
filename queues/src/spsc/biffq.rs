//paper in /paper/spsc/Cache‐aware design of general‐purpose Single‐Producer Single‐Consumer queues.pdf
use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{fence, AtomicBool, AtomicUsize, Ordering};

const H_PARTITION_SIZE: usize = 32;
const LOCAL_BATCH_SIZE: usize = 32;

// Compact slot with atomic flag - same as IFFQ
#[repr(C)]
struct Slot<T> {
    flag: AtomicBool,
    _pad: [u8; 8 - std::mem::size_of::<AtomicBool>()],
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
pub struct ProducerFieldsB<T: Send + 'static> {
    write: AtomicUsize,
    pub limit: AtomicUsize,
    local_buffer: UnsafeCell<[MaybeUninit<T>; LOCAL_BATCH_SIZE]>,
    pub local_count: AtomicUsize,
}

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
    owns_buffer: bool,
}

unsafe impl<T: Send> Send for BiffqQueue<T> {}
unsafe impl<T: Send> Sync for BiffqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct BiffqPushError<T>(pub T);

#[derive(Debug, PartialEq, Eq)]
pub struct BiffqPopError;

impl<T: Send + 'static> BiffqQueue<T> {
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

        // Initialize all slots as empty
        for i in 0..capacity {
            ptr::write(buffer_data_ptr.add(i), Slot::new());
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
    fn get_slot(&self, index: usize) -> &Slot<T> {
        unsafe { &*self.buffer.add(index & self.mask) }
    }

    pub fn publish_batch_internal(&self) -> Result<usize, ()> {
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

                if self
                    .get_slot(slot_to_check_idx)
                    .flag
                    .load(Ordering::Acquire)
                {
                    // Slot is occupied, can't proceed
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

            // Write data first
            unsafe {
                (*slot.data.get()).write(item_to_write);
            }

            // Then set flag - this is the linearization point
            slot.flag.store(true, Ordering::Release);

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

        // Check if slot is full
        if !slot.flag.load(Ordering::Acquire) {
            return Err(BiffqPopError);
        }

        // Read data
        let item = unsafe { (*slot.data.get()).assume_init_read() };

        // Clear flag
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
        !self
            .get_slot(slot_to_check_idx)
            .flag
            .load(Ordering::Acquire)
    }

    #[inline]
    fn empty(&self) -> bool {
        let local_empty = self.prod.local_count.load(Ordering::Relaxed) == 0;
        if !local_empty {
            return false;
        }

        let current_read = self.cons.read.load(Ordering::Acquire);
        !self.get_slot(current_read).flag.load(Ordering::Acquire)
    }
}

impl<T: Send + 'static> Drop for BiffqQueue<T> {
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
                    if std::mem::needs_drop::<T>() {
                        for i in 0..remaining_local {
                            ptr::drop_in_place((*local_buf_ptr.add(i)).as_mut_ptr());
                        }
                    }
                }
            }

            // Drop any remaining items in slots
            unsafe {
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
