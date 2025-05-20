// queues/src/spsc/blq.rs

use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

// K_CACHE_LINE_SLOTS: Number of items that fit in a cache line.
// The paper suggests leaving K entries unused to improve cache behavior
// when the queue is full (Section 3.2, applied to LLQ and by extension to BLQ).
// Assuming items are 8 bytes and cache lines are 64 bytes, K = 8.
pub const K_CACHE_LINE_SLOTS: usize = 8;

#[repr(C)]
#[cfg_attr(
   any(target_arch = "x86_64", target_arch = "aarch64"),
   repr(align(64)) // Align to cache line size
)]
pub struct SharedIndices {
   pub write: AtomicUsize, // Next slot for producer to write to
   pub read: AtomicUsize,  // Next slot for consumer to read from
}

#[repr(C)]
#[cfg_attr(
   any(target_arch = "x86_64", target_arch = "aarch64"),
   repr(align(64))
)]
struct ProducerPrivate {
   // Shadow copy of the consumer's 'read' index.
   // Used to check for available space without frequently reading the shared 'read' index.
   read_shadow: usize,
   // Producer's private write index. Items are written here before being published.
   write_priv: usize,
}

#[repr(C)]
#[cfg_attr(
   any(target_arch = "x86_64", target_arch = "aarch64"),
   repr(align(64))
)]
struct ConsumerPrivate {
   // Shadow copy of the producer's 'write' index.
   // Used to check for available items without frequently reading the shared 'write' index.
   write_shadow: usize,
   // Consumer's private read index. Items are read from here before their slots are published as free.
   read_priv: usize,
}

#[repr(C)]
pub struct BlqQueue<T: Send + 'static> {
   shared_indices: SharedIndices,
   // Producer-private fields, should not cause false sharing with consumer fields
   // or shared_indices if BlqQueue itself is aligned and fields are laid out properly.
   prod_private: UnsafeCell<ProducerPrivate>,
   // Consumer-private fields
   cons_private: UnsafeCell<ConsumerPrivate>,
   capacity: usize, // Total number of slots in the buffer
   mask: usize,     // Bitmask for ring buffer index calculation (capacity - 1)
   buffer: ManuallyDrop<Box<[UnsafeCell<MaybeUninit<T>>]>>, // The ring buffer
   owns_buffer: bool, // Flag to indicate if this instance owns the buffer (for Drop)
}

// Safety: BlqQueue is Send and Sync if T is Send.
// The UnsafeCell fields are accessed in a way that upholds SPSC invariants:
// - prod_private is only accessed by the producer.
// - cons_private is only accessed by the consumer.
// - Shared shared_indices are atomic.
// - Buffer access is coordinated by these indices.
unsafe impl<T: Send> Send for BlqQueue<T> {}
unsafe impl<T: Send> Sync for BlqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct BlqPushError<T>(pub T);

#[derive(Debug, PartialEq, Eq)]
pub struct BlqPopError;

impl<T: Send + 'static> BlqQueue<T> {
   /// Creates a new BlqQueue with the given capacity.
   /// Capacity must be a power of two and greater than K_CACHE_LINE_SLOTS.
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
         buffer: ManuallyDrop::new(buffer_mem.into_boxed_slice()),
         owns_buffer: true,
      }
   }

   /// Calculates the total shared memory size required for the queue.
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
      
      // The buffer elements follow the header in memory.
      let (combined_layout, _offset_of_buffer) =
         layout_header.extend(layout_buffer_elements).unwrap();
      combined_layout.pad_to_align().size()
   }

   /// Initializes the queue in a given shared memory region.
   /// # Safety
   /// The caller must ensure that `mem` points to a valid shared memory region
   /// of at least `shared_size(capacity)` bytes, and that it remains valid for 'static.
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

      // Calculate the offset to the buffer data, which directly follows the Self struct.
      let layout_header = std::alloc::Layout::new::<Self>();
      let layout_buffer_elements =
         std::alloc::Layout::array::<UnsafeCell<MaybeUninit<T>>>(capacity).unwrap();
      let (_combined_layout, offset_of_buffer) =
         layout_header.extend(layout_buffer_elements).unwrap();


      let buffer_data_start_ptr = mem.add(offset_of_buffer) 
         as *mut UnsafeCell<MaybeUninit<T>>;

      // Initialize buffer elements to uninitialized (they will hold Option<T>, initially None conceptually)
      // For MaybeUninit<T> in UnsafeCell, we just need to ensure the memory is there.
      // The Option<T> itself will be handled by push/pop logic.
      // Here, we are creating a slice from raw parts, so the memory must be valid.
      // The actual UnsafeCell<MaybeUninit<T>> will be constructed by ptr::write for Self.
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
               owns_buffer: false, // This instance does not own the buffer when init_in_shared
         },
      );

      &mut *queue_struct_ptr
   }

   /// Producer: Checks how many items can be enqueued.
   /// `needed`: The number of items the producer hopes to enqueue.
   /// Returns the actual number of free slots available.
   #[inline]
   pub fn blq_enq_space(&self, needed: usize) -> usize {
      let prod_priv = unsafe { &mut *self.prod_private.get() };
      // Available space calculation: (N - K) - (write_priv - read_shadow)
      // N is capacity. write_priv and read_shadow are absolute counts.
      let mut free_slots = (self.capacity - K_CACHE_LINE_SLOTS)
         .wrapping_sub(prod_priv.write_priv.wrapping_sub(prod_priv.read_shadow));

      if free_slots < needed {
         // Not enough space based on shadow, refresh read_shadow from shared read index.
         // This is a potentially costly read of a shared cache line.
         prod_priv.read_shadow = self.shared_indices.read.load(Ordering::Acquire);
         free_slots = (self.capacity - K_CACHE_LINE_SLOTS)
               .wrapping_sub(prod_priv.write_priv.wrapping_sub(prod_priv.read_shadow));
      }
      free_slots
   }

   /// Producer: Enqueues an item locally without publishing.
   /// Assumes `blq_enq_space` was called and confirmed space.
   #[inline]
   pub fn blq_enq_local(&self, item: T) -> Result<(), BlqPushError<T>> {
      let prod_priv = unsafe { &mut *self.prod_private.get() };
      let current_write_priv = prod_priv.write_priv;

      // This check should ideally be guaranteed by pre-calling blq_enq_space.
      // If we strictly follow paper's API, this check is redundant here,
      // but it's good for safety if used as a standalone SPSC push part.
      let num_filled = current_write_priv.wrapping_sub(prod_priv.read_shadow);
      if num_filled >= self.capacity - K_CACHE_LINE_SLOTS {
            // Refresh read_shadow as a last attempt before failing
         prod_priv.read_shadow = self.shared_indices.read.load(Ordering::Acquire);
         if current_write_priv.wrapping_sub(prod_priv.read_shadow) >= self.capacity - K_CACHE_LINE_SLOTS {
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

   /// Producer: Publishes all locally enqueued items.
   #[inline]
   pub fn blq_enq_publish(&self) {
      let prod_priv = unsafe { &*self.prod_private.get() };
      // Memory fence (Release) to ensure all previous writes to the buffer
      // are visible before the `write` index is updated.
      self.shared_indices
         .write
         .store(prod_priv.write_priv, Ordering::Release);
   }

   /// Consumer: Checks how many items are available to dequeue.
   /// `needed`: The number of items the consumer hopes to dequeue.
   /// Returns the actual number of items available.
   #[inline]
   pub fn blq_deq_space(&self, needed: usize) -> usize {
      let cons_priv = unsafe { &mut *self.cons_private.get() };
      // Available items: write_shadow - read_priv
      let mut available_items = cons_priv.write_shadow.wrapping_sub(cons_priv.read_priv);

      if available_items < needed {
         // Not enough items based on shadow, refresh write_shadow from shared write index.
         cons_priv.write_shadow = self.shared_indices.write.load(Ordering::Acquire);
         available_items = cons_priv.write_shadow.wrapping_sub(cons_priv.read_priv);
      }
      available_items
   }

   /// Consumer: Dequeues an item locally without publishing the free slot.
   /// Assumes `blq_deq_space` was called and confirmed items are available.
   #[inline]
   pub fn blq_deq_local(&self) -> Result<T, BlqPopError> {
      let cons_priv = unsafe { &mut *self.cons_private.get() };
      let current_read_priv = cons_priv.read_priv;

      // This check should ideally be guaranteed by pre-calling blq_deq_space.
      if current_read_priv == cons_priv.write_shadow {
         // Refresh write_shadow as a last attempt
         cons_priv.write_shadow = self.shared_indices.write.load(Ordering::Acquire);
         if current_read_priv == cons_priv.write_shadow {
               return Err(BlqPopError);
         }
      }

      let slot_idx = current_read_priv & self.mask;
      let item = unsafe {
         ptr::read((*self.buffer.get_unchecked(slot_idx)).get()).assume_init()
      };
      cons_priv.read_priv = current_read_priv.wrapping_add(1);
      Ok(item)
   }

   /// Consumer: Publishes all locally dequeued (now free) slots.
   #[inline]
   pub fn blq_deq_publish(&self) {
      let cons_priv = unsafe { &*self.cons_private.get() };
      // Memory fence (Release) to ensure that the consumer is done reading
      // the items before making the slots available to the producer.
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
      // blq_enq_local should not fail if space was confirmed.
      self.blq_enq_local(item)?; // Propagate error just in case, though unlikely.
      self.blq_enq_publish();
      Ok(())
   }

   #[inline]
   fn pop(&self) -> Result<T, Self::PopError> {
      if self.blq_deq_space(1) == 0 {
         return Err(BlqPopError);
      }
      // blq_deq_local should not fail if items were confirmed.
      let item = self.blq_deq_local()?; // Propagate error.
      self.blq_deq_publish();
      Ok(item)
   }

   #[inline]
   fn available(&self) -> bool {
      // Check if at least 1 slot is available.
      self.blq_enq_space(1) > 0
   }

   #[inline]
   fn empty(&self) -> bool {
      // Check if 0 items are available to dequeue.
      self.blq_deq_space(1) == 0
   }
}

impl<T: Send + 'static> Drop for BlqQueue<T> {
   fn drop(&mut self) {
      if self.owns_buffer {
         if std::mem::needs_drop::<T>() {
               // Get mutable references to private fields for drop
               let prod_priv = unsafe { &*self.prod_private.get() };
               let cons_priv = unsafe { &mut *self.cons_private.get() };

               // Items potentially in flight (written by producer but not yet published)
               // or items in buffer that were published but not yet consumed.
               // The shared indices reflect the state visible to the other party.
               // Private indices reflect local operations.
               
               // Drain based on private consumer index up to private write shadow
               let mut current_read = cons_priv.read_priv;
               let write_shadow = cons_priv.write_shadow; 

               while current_read != write_shadow {
                  let slot_idx = current_read & self.mask;
                  unsafe {
                     (*self.buffer.get_unchecked_mut(slot_idx))
                           .get_mut()
                           .assume_init_drop();
                  }
                  current_read = current_read.wrapping_add(1);
               }
         }
         // Deallocate the buffer
         unsafe {
               ManuallyDrop::drop(&mut self.buffer);
         }
      }
   }
}

impl<T: Send + fmt::Debug + 'static> fmt::Debug for BlqQueue<T> {
   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      let prod_priv = unsafe { &*self.prod_private.get() };
      let cons_priv = unsafe { &*self.cons_private.get() };
      f.debug_struct("BlqQueue")
         .field("capacity", &self.capacity)
         .field("mask", &self.mask)
         .field("shared_write", &self.shared_indices.write.load(Ordering::Relaxed))
         .field("shared_read", &self.shared_indices.read.load(Ordering::Relaxed))
         .field("prod_write_priv", &prod_priv.write_priv)
         .field("prod_read_shadow", &prod_priv.read_shadow)
         .field("cons_read_priv", &cons_priv.read_priv)
         .field("cons_write_shadow", &cons_priv.write_shadow)
         .field("owns_buffer", &self.owns_buffer)
         .finish()
   }
}