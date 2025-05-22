// FastForward from Moseley et al. 2008
use crate::SpscQueue;
use core::{cell::UnsafeCell, fmt, mem::MaybeUninit, ptr};
use std::sync::atomic::{AtomicBool, Ordering};

// An empty slot is represented by `None`; a full one by `Some(T)`.
type Slot<T> = Option<T>;

#[repr(C, align(64))]
pub struct FfqQueue<T: Send + 'static> {
   // Producer-local write cursor.
   head: UnsafeCell<usize>,
   
   // Padding to prevent false sharing
   _pad1: [u8; 64 - std::mem::size_of::<UnsafeCell<usize>>()],
   
   // Consumer-local read cursor.
   tail: UnsafeCell<usize>,
   
   // Padding to prevent false sharing
   _pad2: [u8; 64 - std::mem::size_of::<UnsafeCell<usize>>()],

   capacity: usize,
   mask: usize,
   buffer: *mut UnsafeCell<MaybeUninit<Slot<T>>>,
   owns_buffer: bool,
   initialized: AtomicBool,
}

unsafe impl<T: Send> Send for FfqQueue<T> {}
unsafe impl<T: Send> Sync for FfqQueue<T> {}

#[derive(Debug, PartialEq, Eq)]
pub struct FfqPushError<T>(pub T);
#[derive(Debug, PartialEq, Eq)]
pub struct FfqPopError;

impl<T: Send + 'static> FfqQueue<T> {
   // Build a new queue in process-local memory.
   // The capacity must be a power of two.
   pub fn with_capacity(capacity: usize) -> Self {
      assert!(capacity.is_power_of_two() && capacity > 0);

      // Allocate buffer aligned to cache line
      let layout = std::alloc::Layout::array::<UnsafeCell<MaybeUninit<Slot<T>>>>(capacity)
         .unwrap()
         .align_to(64)
         .unwrap();
      
      let ptr = unsafe { std::alloc::alloc(layout) as *mut UnsafeCell<MaybeUninit<Slot<T>>> };
      
      if ptr.is_null() {
         panic!("Failed to allocate buffer");
      }

      // Initialize all slots to None
      unsafe {
         for i in 0..capacity {
            ptr::write(ptr.add(i), UnsafeCell::new(MaybeUninit::new(None)));
         }
      }

      Self {
         head: UnsafeCell::new(0),
         _pad1: [0u8; 64 - std::mem::size_of::<UnsafeCell<usize>>()],
         tail: UnsafeCell::new(0),
         _pad2: [0u8; 64 - std::mem::size_of::<UnsafeCell<usize>>()],
         capacity,
         mask: capacity - 1,
         buffer: ptr,
         owns_buffer: true,
         initialized: AtomicBool::new(true),
      }
   }

   // Bytes required to place this queue in shared memory.
   pub fn shared_size(capacity: usize) -> usize {
      assert!(capacity.is_power_of_two() && capacity > 0);
      let self_layout = core::alloc::Layout::new::<Self>();
      let buf_layout =
         core::alloc::Layout::array::<UnsafeCell<MaybeUninit<Slot<T>>>>(capacity).unwrap();
      let (layout, _) = self_layout.extend(buf_layout).unwrap();
      layout.size()
   }

   // Construct in user-provided shared memory region (e.g. `mmap`).
   // The caller must guarantee the memory lives for `'static`.
   pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
      assert!(capacity.is_power_of_two() && capacity > 0);
      assert!(!mem.is_null());

      // Clear the memory first
      ptr::write_bytes(mem, 0, Self::shared_size(capacity));

      let queue_ptr = mem as *mut Self;
      let buf_ptr = mem.add(std::mem::size_of::<Self>())
         as *mut UnsafeCell<MaybeUninit<Slot<T>>>;

      // Initialize buffer slots
      for i in 0..capacity {
         ptr::write(buf_ptr.add(i), UnsafeCell::new(MaybeUninit::new(None)));
      }

      // Initialize the queue structure
      ptr::write(
         queue_ptr,
         Self {
            head: UnsafeCell::new(0),
            _pad1: [0u8; 64 - std::mem::size_of::<UnsafeCell<usize>>()],
            tail: UnsafeCell::new(0),
            _pad2: [0u8; 64 - std::mem::size_of::<UnsafeCell<usize>>()],
            capacity,
            mask: capacity - 1,
            buffer: buf_ptr,
            owns_buffer: false,
            initialized: AtomicBool::new(true),
         },
      );
      
      let queue_ref = &mut *queue_ptr;
      
      // Ensure initialization is visible
      queue_ref.initialized.store(true, Ordering::Release);
      
      queue_ref
   }

   #[inline]
   fn slot_ptr(&self, index: usize) -> *mut MaybeUninit<Slot<T>> {
      unsafe { (*self.buffer.add(index & self.mask)).get() }
   }
   
   // Helper to check if initialized
   #[inline]
   fn ensure_initialized(&self) {
      assert!(self.initialized.load(Ordering::Acquire), "Queue not initialized");
   }
}

impl<T: Send + 'static> SpscQueue<T> for FfqQueue<T> {
   type PushError = FfqPushError<T>;
   type PopError = FfqPopError;

   #[inline]
   fn push(&self, item: T) -> Result<(), Self::PushError> {
      self.ensure_initialized();
      
      let head = unsafe { *self.head.get() };
      let slot = self.slot_ptr(head);

      // Check if slot is empty (None)
      unsafe {
         let slot_ref = &*slot;
         if slot_ref.assume_init_ref().is_some() {
            return Err(FfqPushError(item)); // queue full
         }
         
         // Write the new value
         ptr::write(slot, MaybeUninit::new(Some(item)));
         
         // Update head
         *self.head.get() = head.wrapping_add(1);
      }
      
      Ok(())
   }

   #[inline]
   fn pop(&self) -> Result<T, Self::PopError> {
      self.ensure_initialized();
      
      let tail = unsafe { *self.tail.get() };
      let slot = self.slot_ptr(tail);

      unsafe {
         let slot_ref = &*slot;
         match slot_ref.assume_init_ref() {
            Some(_) => {
               // Read and take ownership of the value
               let val = ptr::read(slot).assume_init().unwrap();
               
               // Write None to mark slot as empty
               ptr::write(slot, MaybeUninit::new(None));
               
               // Update tail
               *self.tail.get() = tail.wrapping_add(1);
               
               Ok(val)
            }
            None => Err(FfqPopError),
         }
      }
   }

   #[inline]
   fn available(&self) -> bool {
      self.ensure_initialized();
      
      let head = unsafe { *self.head.get() };
      let slot = self.slot_ptr(head);
      unsafe {
         let slot_ref = &*slot;
         slot_ref.assume_init_ref().is_none()
      }
   }

   #[inline]
   fn empty(&self) -> bool {
      self.ensure_initialized();
      
      let tail = unsafe { *self.tail.get() };
      let slot = self.slot_ptr(tail);
      unsafe {
         let slot_ref = &*slot;
         slot_ref.assume_init_ref().is_none()
      }
   }
}

impl<T: Send + 'static> Drop for FfqQueue<T> {
   fn drop(&mut self) {
      if self.owns_buffer && !self.buffer.is_null() {
         unsafe {
            // Drop any remaining items
            if core::mem::needs_drop::<T>() {
               for i in 0..self.capacity {
                  let slot = self.slot_ptr(i);
                  let maybe = ptr::read(slot).assume_init();
                  drop(maybe); // Option's drop will handle Some(T)
               }
            }
            
            // Deallocate buffer
            let layout = std::alloc::Layout::array::<UnsafeCell<MaybeUninit<Slot<T>>>>(self.capacity)
               .unwrap()
               .align_to(64)
               .unwrap();
            std::alloc::dealloc(self.buffer as *mut u8, layout);
         }
      }
   }
}

impl<T: fmt::Debug + Send + 'static> fmt::Debug for FfqQueue<T> {
   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      f.debug_struct("FfqQueue")
         .field("capacity", &self.capacity)
         .field("head", unsafe { &*self.head.get() })
         .field("tail", unsafe { &*self.tail.get() })
         .field("owns_buffer", &self.owns_buffer)
         .field("initialized", &self.initialized.load(Ordering::Relaxed))
         .finish()
   }
}

// Temporal Slipping Support Methods
// These are provided for stages to manage slip as described in Section 3.4.1
// Will not be used in benchmark since this is an overhead for the benchmark and slipping is for when processes actually do other work too instead of just pushing and popping items. 
// And additionally this slipping technique is not wait-free but added for completeness eventhough not used. Was tested, works.
impl<T: Send + 'static> FfqQueue<T> {
   // Constants from paper Section 3.4.1
   pub const DANGER_THRESHOLD: usize = 16;  // 2 cachelines - when slip is likely to be lost
   pub const GOOD_THRESHOLD: usize = 48;    // 6 cachelines - appropriate amount of slip
   
   // Calculate distance between producer and consumer
   #[inline]
   pub fn distance(&self) -> usize {
      let head = unsafe { *self.head.get() };
      let tail = unsafe { *self.tail.get() };
      head.wrapping_sub(tail)
   }
   
   // Based on Figure 6 from the paper - to be called by consumer stage
   pub fn adjust_slip(&self, avg_stage_time_ns: u64) {
      let mut dist = self.distance();
      if dist < Self::DANGER_THRESHOLD {
         let mut dist_old;
         loop {
            dist_old = dist;
            
            // Calculate spin time based on distance from GOOD threshold
            let spin_time = avg_stage_time_ns * ((Self::GOOD_THRESHOLD + 1) - dist) as u64;
            
            // Spin wait as shown in paper
            let start = std::time::Instant::now();
            while start.elapsed().as_nanos() < spin_time as u128 {
               std::hint::spin_loop();
            }
            
            dist = self.distance();
            
            // Exit conditions from paper: reached GOOD or no progress
            if dist >= Self::GOOD_THRESHOLD || dist <= dist_old {
               break;
            }
         }
      }
   }
}