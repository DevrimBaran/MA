// Dehnavi 2021
// Wait-free FIFO implementation following the paper's algorithms exactly
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use crate::SpscQueue;

#[derive(Debug)]
pub struct DehnaviQueue<T: Send + 'static> { 
   pub(crate) buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
   pub capacity: usize, // k in the paper
   pub wc: AtomicUsize, // write counter
   pub rc: AtomicUsize, // read counter
   pub(crate) pclaim: AtomicBool, // producer claim
   pub(crate) cclaim: AtomicBool, // consumer claim
}

#[derive(Debug, PartialEq, Eq)]
pub struct PushError<T>(pub T); 

#[derive(Debug, PartialEq, Eq)]
pub struct PopError; 

impl<T: Send + 'static> DehnaviQueue<T> { 
   pub fn new(capacity: usize) -> Self {
      assert!(capacity > 0, "Capacity (k) must be greater than 0");
      
      let buffer_size = capacity;
      let mut buffer_vec = Vec::with_capacity(buffer_size);
      for _ in 0..buffer_size {
         buffer_vec.push(UnsafeCell::new(MaybeUninit::uninit()));
      }
      Self {
         buffer: buffer_vec.into_boxed_slice(),
         capacity: buffer_size, 
         wc: AtomicUsize::new(0),
         rc: AtomicUsize::new(0),
         pclaim: AtomicBool::new(false),
         cclaim: AtomicBool::new(false),
      }
   }
   
   pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
      assert!(capacity > 0, "Capacity (k) must be greater than 0");
      let buffer_size = capacity;

      let header_ptr = mem as *mut Self;
      let buffer_data_ptr = mem.add(std::mem::size_of::<Self>()) as *mut UnsafeCell<MaybeUninit<T>>; 

      for i in 0..buffer_size {
         ptr::write(buffer_data_ptr.add(i), UnsafeCell::new(MaybeUninit::uninit()));
      }
      
      let buffer_slice = std::slice::from_raw_parts_mut(buffer_data_ptr, buffer_size);
      let boxed_buffer = Box::from_raw(buffer_slice as *mut [_]);

      ptr::write(header_ptr, Self {
         buffer: boxed_buffer,
         capacity: buffer_size,
         wc: AtomicUsize::new(0),
         rc: AtomicUsize::new(0),
         pclaim: AtomicBool::new(false),
         cclaim: AtomicBool::new(false),
      });

      &mut *header_ptr
   }

   pub const fn shared_size(capacity: usize) -> usize {
      std::mem::size_of::<Self>() + capacity * std::mem::size_of::<UnsafeCell<MaybeUninit<T>>>()
   }
}

impl<T: Send + 'static> SpscQueue<T> for DehnaviQueue<T> {
   type PushError = PushError<T>; 
   type PopError = PopError;

   // Algorithm 1: Write to the wait-free channel
   fn push(&self, item: T) -> Result<(), Self::PushError> {
      // Line 1: while ((wc+1) % k) == rc /*FIFO full*/ do
      loop {
         let wc = self.wc.load(Ordering::Acquire);
         let rc = self.rc.load(Ordering::Acquire);
         
         if (wc + 1) % self.capacity != rc {
            // FIFO not full, exit loop
            break;
         }
         
         // Line 2: if cclaim==0 then
         if !self.cclaim.load(Ordering::Acquire) {
            // Line 3: pclaim=1
            self.pclaim.store(true, Ordering::Release);
            
            // Line 4: if cclaim==0 then
            if !self.cclaim.load(Ordering::Acquire) {
               // Line 5: rc=(rc+1) % k
               let current_rc = self.rc.load(Ordering::Acquire);
               self.rc.store((current_rc + 1) % self.capacity, Ordering::Release);
            }
            // Line 6: pclaim=0
            self.pclaim.store(false, Ordering::Release);
         }
         
         // Continue loop to check if still full
         std::hint::spin_loop();
      }
      
      // Line 7: Write token
      let wc = self.wc.load(Ordering::Acquire);
      unsafe {
         ptr::write((*self.buffer.get_unchecked(wc)).get(), MaybeUninit::new(item));
      }
      
      // Line 8: wc = (wc + 1) % k
      self.wc.store((wc + 1) % self.capacity, Ordering::Release);
      Ok(())
   }

   // Algorithm 2: Read from the wait-free channel
   fn pop(&self) -> Result<T, Self::PopError> {
      // Line 0: if wc==rc /*FIFO empty*/ then return Null;
      let wc = self.wc.load(Ordering::Acquire);
      let rc = self.rc.load(Ordering::Acquire);
      if wc == rc {
         return Err(PopError);
      }

      // Line 1: cclaim=1
      self.cclaim.store(true, Ordering::Release);
      
      // Line 2: while (pclaim==1);
      while self.pclaim.load(Ordering::Acquire) {
         std::hint::spin_loop();
      }
      
      // Line 3: Read token
      let rc = self.rc.load(Ordering::Acquire);
      let item = unsafe {
         ptr::read((*self.buffer.get_unchecked(rc)).get())
      };
      
      // Line 4: rc = (rc+1) % k
      self.rc.store((rc + 1) % self.capacity, Ordering::Release);
      
      // Line 5: cclaim=0
      self.cclaim.store(false, Ordering::Release);
      
      unsafe { Ok(item.assume_init()) }
   }

   fn available(&self) -> bool {
      let wc = self.wc.load(Ordering::Relaxed);
      let rc = self.rc.load(Ordering::Relaxed);
      (wc + 1) % self.capacity != rc
   }

   fn empty(&self) -> bool {
      let wc = self.wc.load(Ordering::Relaxed);
      let rc = self.rc.load(Ordering::Relaxed);
      wc == rc
   }
}

impl<T: Send + 'static> Drop for DehnaviQueue<T> { 
   fn drop(&mut self) {
      if !std::mem::needs_drop::<T>() || self.buffer.is_empty() {
         return;
      }
      
      let mut current_rc = *self.rc.get_mut();
      let current_wc = *self.wc.get_mut();

      while current_rc != current_wc {
         unsafe {
            let item_ptr = (*self.buffer.get_unchecked_mut(current_rc)).get();
            MaybeUninit::assume_init_drop(&mut *item_ptr);
         }
         current_rc = (current_rc + 1) % self.capacity;
      }
   }
}

unsafe impl<T: Send + 'static> Send for DehnaviQueue<T> {} 
unsafe impl<T: Send + 'static> Sync for DehnaviQueue<T> {}