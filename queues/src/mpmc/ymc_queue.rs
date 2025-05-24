use std::sync::atomic::{AtomicUsize, AtomicPtr, AtomicU64, AtomicBool, Ordering, fence};
use std::ptr::{self, null_mut};
use std::mem::{self, MaybeUninit};
use std::alloc::{self, Layout};
use std::cell::UnsafeCell;

use crate::MpscQueue;

// Constants
const SEGMENT_SIZE: usize = 1024; // N in the paper
const PATIENCE: usize = 10;
const MAX_GARBAGE: usize = 64;
const CACHE_LINE_SIZE: usize = 64;

// Special values
const BOTTOM: usize = usize::MAX;
const TOP: usize = usize::MAX - 1;

// Enqueue request state
#[repr(C)]
#[derive(Clone, Copy)]
struct EnqReq {
   val: usize,
   state: AtomicU64, // (pending: 1 bit, id: 63 bits)
}

impl EnqReq {
   fn new() -> Self {
      Self {
         val: BOTTOM,
         state: AtomicU64::new(0),
      }
   }
   
   fn get_state(&self) -> (bool, u64) {
      let s = self.state.load(Ordering::Acquire);
      let pending = (s >> 63) != 0;
      let id = s & 0x7FFFFFFFFFFFFFFF;
      (pending, id)
   }
   
   fn set_state(&self, pending: bool, id: u64) {
      let s = ((pending as u64) << 63) | (id & 0x7FFFFFFFFFFFFFFF);
      self.state.store(s, Ordering::Release);
   }
   
   fn try_claim(&self, old_id: u64, new_id: u64) -> bool {
      let old = (1u64 << 63) | old_id;
      let new = new_id & 0x7FFFFFFFFFFFFFFF;
      self.state.compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire).is_ok()
   }
}

// Dequeue request state
#[repr(C)]
#[derive(Clone, Copy)]
struct DeqReq {
   id: u64,
   state: AtomicU64, // (pending: 1 bit, idx: 63 bits)
}

impl DeqReq {
   fn new() -> Self {
      Self {
         id: 0,
         state: AtomicU64::new(0),
      }
   }
   
   fn get_state(&self) -> (bool, u64) {
      let s = self.state.load(Ordering::Acquire);
      let pending = (s >> 63) != 0;
      let idx = s & 0x7FFFFFFFFFFFFFFF;
      (pending, idx)
   }
   
   fn set_state(&self, pending: bool, idx: u64) {
      let s = ((pending as u64) << 63) | (idx & 0x7FFFFFFFFFFFFFFF);
      self.state.store(s, Ordering::Release);
   }
}

// Cell structure
#[repr(C, align(64))]
struct Cell {
   val: AtomicUsize,
   enq: AtomicPtr<EnqReq>,
   deq: AtomicPtr<DeqReq>,
   _padding: [u8; CACHE_LINE_SIZE - 24],
}

impl Cell {
   fn new() -> Self {
      Self {
         val: AtomicUsize::new(BOTTOM),
         enq: AtomicPtr::new(null_mut()),
         deq: AtomicPtr::new(null_mut()),
         _padding: [0; CACHE_LINE_SIZE - 24],
      }
   }
}

// Segment structure
#[repr(C)]
struct Segment {
   id: usize,
   next: AtomicPtr<Segment>,
   cells: [Cell; SEGMENT_SIZE],
}

impl Segment {
   unsafe fn new(id: usize) -> *mut Self {
      let layout = Layout::new::<Self>();
      let ptr = alloc::alloc(layout) as *mut Self;
      if ptr.is_null() {
         alloc::handle_alloc_error(layout);
      }
      
      (*ptr).id = id;
      (*ptr).next = AtomicPtr::new(null_mut());
      
      // Initialize cells
      for i in 0..SEGMENT_SIZE {
         ptr::write(&mut (*ptr).cells[i], Cell::new());
      }
      
      ptr
   }
}

// Thread handle
#[repr(C, align(128))]
pub struct Handle {
   tail: *mut Segment,
   head: *mut Segment,
   next: *mut Handle,
   enq_req: EnqReq,
   enq_peer: *mut Handle,
   enq_id: u64,
   deq_req: DeqReq,
   deq_peer: *mut Handle,
   hazard: AtomicPtr<Segment>,
   _padding: [u8; 128],
}

impl Handle {
   pub fn new() -> Box<Self> {
      Box::new(Self {
         tail: null_mut(),
         head: null_mut(),
         next: null_mut(),
         enq_req: EnqReq::new(),
         enq_peer: null_mut(),
         enq_id: 0,
         deq_req: DeqReq::new(),
         deq_peer: null_mut(),
         hazard: AtomicPtr::new(null_mut()),
         _padding: [0; 128],
      })
   }
}

// Main queue structure
#[repr(C)]
pub struct YangCrummeyQueue<T: Send + Clone + 'static> {
   q: AtomicPtr<Segment>,
   t: AtomicU64,
   h: AtomicU64,
   i: AtomicUsize,
   handles: UnsafeCell<Vec<*mut Handle>>,
   _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for YangCrummeyQueue<T> {}
unsafe impl<T: Send + Clone> Sync for YangCrummeyQueue<T> {}

impl<T: Send + Clone + 'static> YangCrummeyQueue<T> {
   pub fn new(num_threads: usize) -> Self {
      unsafe {
         // Create initial segment
         let seg0 = Segment::new(0);
         
         // Create and link handles
         let mut handles = Vec::with_capacity(num_threads);
         for _ in 0..num_threads {
               let handle = Box::into_raw(Handle::new());
               (*handle).head = seg0;
               (*handle).tail = seg0;
               handles.push(handle);
         }
         
         // Link handles in a ring
         for i in 0..num_threads {
               let curr = handles[i];
               let next = handles[(i + 1) % num_threads];
               (*curr).next = next;
               (*curr).enq_peer = next;
               (*curr).deq_peer = next;
         }
         
         Self {
               q: AtomicPtr::new(seg0),
               t: AtomicU64::new(0),
               h: AtomicU64::new(0),
               i: AtomicUsize::new(0),
               handles: UnsafeCell::new(handles),
               _phantom: std::marker::PhantomData,
         }
      }
   }

   pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
      let queue_ptr = mem as *mut Self;
      
      // Calculate offsets
      let queue_size = mem::size_of::<Self>();
      let handles_offset = (queue_size + 127) & !127; // Align to 128
      let segment_offset = handles_offset + num_threads * mem::size_of::<Handle>();
      
      // Create initial segment
      let seg0_ptr = mem.add(segment_offset) as *mut Segment;
      ptr::write(seg0_ptr, Segment {
         id: 0,
         next: AtomicPtr::new(null_mut()),
         cells: unsafe { MaybeUninit::uninit().assume_init() },
      });
      
      // Initialize cells
      for i in 0..SEGMENT_SIZE {
         ptr::write(&mut (*seg0_ptr).cells[i], Cell::new());
      }
      
      // Create handles
      let mut handles = Vec::with_capacity(num_threads);
      let handles_base = mem.add(handles_offset) as *mut Handle;
      
      for i in 0..num_threads {
         let handle_ptr = handles_base.add(i);
         ptr::write(handle_ptr, Handle {
               tail: seg0_ptr,
               head: seg0_ptr,
               next: null_mut(),
               enq_req: EnqReq::new(),
               enq_peer: null_mut(),
               enq_id: 0,
               deq_req: DeqReq::new(),
               deq_peer: null_mut(),
               hazard: AtomicPtr::new(null_mut()),
               _padding: [0; 128],
         });
         handles.push(handle_ptr);
      }
      
      // Link handles
      for i in 0..num_threads {
         let curr = handles[i];
         let next = handles[(i + 1) % num_threads];
         (*curr).next = next;
         (*curr).enq_peer = next;
         (*curr).deq_peer = next;
      }
      
      // Initialize queue
      ptr::write(queue_ptr, Self {
         q: AtomicPtr::new(seg0_ptr),
         t: AtomicU64::new(0),
         h: AtomicU64::new(0),
         i: AtomicUsize::new(0),
         handles: UnsafeCell::new(handles),
         _phantom: std::marker::PhantomData,
      });
      
      &mut *queue_ptr
   }
   
   pub fn shared_size(num_threads: usize) -> usize {
      let queue_size = mem::size_of::<Self>();
      let handles_size = num_threads * mem::size_of::<Handle>();
      let initial_segment_size = mem::size_of::<Segment>();
      
      let total = queue_size + handles_size + initial_segment_size + 256; // Extra padding
      (total + 4095) & !4095 // Page align
   }

   // Find cell helper
   unsafe fn find_cell(&self, sp: &mut *mut Segment, cell_id: u64) -> *mut Cell {
      let seg_id = (cell_id / SEGMENT_SIZE as u64) as usize;
      let cell_idx = (cell_id % SEGMENT_SIZE as u64) as usize;
      
      let mut s = *sp;
      
      // Traverse to target segment
      while (*s).id < seg_id {
         let next = (*s).next.load(Ordering::Acquire);
         if next.is_null() {
               // Need to allocate new segment
               let new_seg = Segment::new((*s).id + 1);
               match (*s).next.compare_exchange(null_mut(), new_seg, Ordering::AcqRel, Ordering::Acquire) {
                  Ok(_) => {
                     s = new_seg;
                  }
                  Err(_) => {
                     // Someone else added it
                     alloc::dealloc(new_seg as *mut u8, Layout::new::<Segment>());
                     s = (*s).next.load(Ordering::Acquire);
                  }
               }
         } else {
               s = next;
         }
      }
      
      *sp = s;
      &mut (*s).cells[cell_idx] as *mut Cell
   }

   unsafe fn advance_end(&self, e: &AtomicU64, cid: u64) {
      loop {
         let curr = e.load(Ordering::Acquire);
         if curr >= cid {
               break;
         }
         if e.compare_exchange(curr, cid, Ordering::AcqRel, Ordering::Acquire).is_ok() {
               break;
         }
      }
   }

   // Fast path enqueue
   unsafe fn enq_fast(&self, handle: *mut Handle, v: usize, cid: &mut u64) -> bool {
      let i = self.t.fetch_add(1, Ordering::AcqRel);
      let c = self.find_cell(&mut (*handle).tail, i);
      
      match (*c).val.compare_exchange(BOTTOM, v, Ordering::AcqRel, Ordering::Acquire) {
         Ok(_) => true,
         Err(_) => {
               *cid = i;
               false
         }
      }
   }

   // Slow path enqueue
   unsafe fn enq_slow(&self, handle: *mut Handle, v: usize, mut cell_id: u64) {
      // Publish request
      let r = &(*handle).enq_req;
      r.val = v;
      r.set_state(true, cell_id);
      
      let mut tmp_tail = (*handle).tail;
      
      loop {
         // Get new cell
         let i = self.t.fetch_add(1, Ordering::AcqRel);
         let c = self.find_cell(&mut tmp_tail, i);
         
         // Try to reserve cell
         let enq_ptr = (*c).enq.compare_exchange(
               null_mut(),
               r as *const EnqReq as *mut EnqReq,
               Ordering::AcqRel,
               Ordering::Acquire
         );
         
         if enq_ptr.is_ok() && (*c).val.load(Ordering::Acquire) == BOTTOM {
               r.try_claim(cell_id, i);
               break;
         }
         
         let (pending, _) = r.get_state();
         if !pending {
               break;
         }
      }
      
      // Commit the value
      let (_, id) = r.get_state();
      let c = self.find_cell(&mut (*handle).tail, id);
      self.enq_commit(c, v, id);
   }

   unsafe fn enq_commit(&self, c: *mut Cell, v: usize, cid: u64) {
      self.advance_end(&self.t, cid + 1);
      (*c).val.store(v, Ordering::Release);
   }

   // Help enqueue
   unsafe fn help_enq(&self, handle: *mut Handle, c: *mut Cell, i: u64) -> Result<usize, ()> {
      // Try to set TOP if cell is BOTTOM
      match (*c).val.compare_exchange(BOTTOM, TOP, Ordering::AcqRel, Ordering::Acquire) {
         Ok(_) => {
               // Cell was empty, help enqueuers
               if (*c).enq.load(Ordering::Acquire).is_null() {
                  // Find a peer to help
                  let mut iterations = 0;
                  loop {
                     iterations += 1;
                     if iterations > 2 {
                           break;
                     }
                     
                     let p = (*handle).enq_peer;
                     if p.is_null() {
                           break;
                     }
                     
                     let peer_req = &(*p).enq_req;
                     let (pending, id) = peer_req.get_state();
                     
                     if (*handle).enq_id == 0 || (*handle).enq_id == id {
                           if pending && id <= i {
                              // Try to help
                              match (*c).enq.compare_exchange(
                                 null_mut(),
                                 peer_req as *const EnqReq as *mut EnqReq,
                                 Ordering::AcqRel,
                                 Ordering::Acquire
                              ) {
                                 Ok(_) => {
                                       (*handle).enq_peer = (*p).next;
                                       break;
                                 }
                                 Err(_) => {
                                       (*handle).enq_id = id;
                                 }
                              }
                           } else {
                              (*handle).enq_peer = (*p).next;
                              break;
                           }
                     } else {
                           (*handle).enq_id = 0;
                           (*handle).enq_peer = (*p).next;
                     }
                  }
                  
                  // If no request to help, mark as unusable
                  if (*c).enq.load(Ordering::Acquire).is_null() {
                     (*c).enq.store(1 as *mut EnqReq, Ordering::Release); // Use 1 as marker
                  }
               }
               
               // Check if we can return EMPTY
               let enq_ptr = (*c).enq.load(Ordering::Acquire);
               if enq_ptr == 1 as *mut EnqReq {
                  // No enqueue will use this cell
                  if self.t.load(Ordering::Acquire) <= i {
                     return Err(()); // EMPTY
                  }
               }
               
               // Help complete the enqueue if there's a request
               if !enq_ptr.is_null() && enq_ptr != 1 as *mut EnqReq {
                  let req = &*enq_ptr;
                  let (pending, req_id) = req.get_state();
                  
                  if req_id > i {
                     // Request not suitable for this cell
                     if (*c).val.load(Ordering::Acquire) == TOP && self.t.load(Ordering::Acquire) <= i {
                           return Err(()); // EMPTY
                     }
                  } else if req.try_claim(req_id, i) || (!pending && req_id == i && (*c).val.load(Ordering::Acquire) == TOP) {
                     self.enq_commit(c, req.val, i);
                  }
               }
               
               // Return the value in the cell
               match (*c).val.load(Ordering::Acquire) {
                  TOP => Ok(TOP),
                  v => Ok(v),
               }
         }
         Err(v) if v != TOP => Ok(v),
         Err(_) => Ok(TOP),
      }
   }

   // Dequeue operations
   unsafe fn deq_fast(&self, handle: *mut Handle, id: &mut u64) -> Result<usize, ()> {
      let i = self.h.fetch_add(1, Ordering::AcqRel);
      let c = self.find_cell(&mut (*handle).head, i);
      
      match self.help_enq(handle, c, i)? {
         TOP => {
               *id = i;
               Ok(TOP)
         }
         v => {
               // Try to claim the value
               let deq_ptr = (*c).deq.compare_exchange(
                  null_mut(),
                  1 as *mut DeqReq, // Mark as taken by fast path
                  Ordering::AcqRel,
                  Ordering::Acquire
               );
               
               if deq_ptr.is_ok() {
                  Ok(v)
               } else {
                  *id = i;
                  Ok(TOP)
               }
         }
      }
   }

   unsafe fn deq_slow(&self, handle: *mut Handle, cid: u64) -> Result<usize, ()> {
      // Publish dequeue request
      let r = &(*handle).deq_req;
      r.id = cid;
      r.set_state(true, cid);
      
      // Help complete the request
      self.help_deq(handle, handle);
      
      // Get result
      let (_, idx) = r.get_state();
      let c = self.find_cell(&mut (*handle).head, idx);
      let v = (*c).val.load(Ordering::Acquire);
      
      self.advance_end(&self.h, idx + 1);
      
      if v == TOP {
         Err(())
      } else {
         Ok(v)
      }
   }

   unsafe fn help_deq(&self, handle: *mut Handle, helpee: *mut Handle) {
      let r = &(*helpee).deq_req;
      let (pending, idx) = r.get_state();
      let id = r.id;
      
      if !pending || idx < id {
         return;
      }
      
      let mut ha = (*helpee).head;
      let (pending, mut prior) = r.get_state();
      let mut i = id;
      let mut cand = 0;
      
      // Set hazard pointer
      (*handle).hazard.store(ha, Ordering::Release);
      fence(Ordering::SeqCst);
      
      loop {
         // Find candidate
         let mut hc = ha;
         while cand == 0 && prior == idx {
               i += 1;
               let c = self.find_cell(&mut hc, i);
               
               match self.help_enq(handle, c, i) {
                  Err(_) => cand = i, // EMPTY
                  Ok(v) if v != TOP && (*c).deq.load(Ordering::Acquire).is_null() => {
                     cand = i;
                  }
                  _ => {
                     let (p, new_idx) = r.get_state();
                     prior = new_idx;
                     if !p {
                           (*handle).hazard.store(null_mut(), Ordering::Release);
                           return;
                     }
                  }
               }
         }
         
         // Try to announce candidate
         if cand != 0 {
               let old_state = (1u64 << 63) | prior;
               let new_state = (1u64 << 63) | cand;
               r.state.compare_exchange(old_state, new_state, Ordering::AcqRel, Ordering::Acquire).ok();
               let (_, new_idx) = r.get_state();
               prior = new_idx;
         }
         
         // Check if request complete
         let (pending, _) = r.get_state();
         if !pending || r.id != id {
               (*handle).hazard.store(null_mut(), Ordering::Release);
               return;
         }
         
         // Try to claim announced cell
         let c = self.find_cell(&mut ha, prior);
         let val = (*c).val.load(Ordering::Acquire);
         
         if val == TOP || 
            (*c).deq.compare_exchange(null_mut(), r as *const DeqReq as *mut DeqReq, Ordering::AcqRel, Ordering::Acquire).is_ok() ||
            (*c).deq.load(Ordering::Acquire) == r as *const DeqReq as *mut DeqReq {
               // Complete the request
               let old_state = (1u64 << 63) | prior;
               let new_state = prior;
               r.state.compare_exchange(old_state, new_state, Ordering::AcqRel, Ordering::Acquire).ok();
               (*handle).hazard.store(null_mut(), Ordering::Release);
               return;
         }
         
         // Prepare for next iteration
         if prior >= i {
               cand = 0;
               i = prior;
         }
      }
   }

   // Memory reclamation
   unsafe fn cleanup(&self, handle: *mut Handle) {
      let i = self.i.load(Ordering::Acquire);
      if i == usize::MAX {
         return; // Cleanup in progress
      }
      
      let e = (*handle).head;
      if e.is_null() || (*e).id.saturating_sub(i) < MAX_GARBAGE {
         return;
      }
      
      // Try to start cleanup
      match self.i.compare_exchange(i, usize::MAX, Ordering::AcqRel, Ordering::Acquire) {
         Ok(_) => {
               // Do cleanup
               let s = self.q.load(Ordering::Acquire);
               let mut current = s;
               let mut to_free = Vec::new();
               
               // Find segments to free
               while !current.is_null() && (*current).id < (*e).id {
                  let next = (*current).next.load(Ordering::Acquire);
                  
                  // Check if any thread is using this segment
                  let handles = &*self.handles.get();
                  let mut in_use = false;
                  
                  for &h in handles.iter() {
                     if h.is_null() {
                           continue;
                     }
                     
                     let hazard = (*h).hazard.load(Ordering::Acquire);
                     if !hazard.is_null() && (*hazard).id == (*current).id {
                           in_use = true;
                           break;
                     }
                     
                     if (*(*h).head).id == (*current).id || (*(*h).tail).id == (*current).id {
                           in_use = true;
                           break;
                     }
                  }
                  
                  if !in_use && (*current).id < (*e).id - 1 {
                     to_free.push(current);
                  } else {
                     break;
                  }
                  
                  current = next;
               }
               
               // Update queue head if we freed segments
               if !to_free.is_empty() {
                  let new_head = (*to_free.last().unwrap()).next.load(Ordering::Acquire);
                  self.q.store(new_head, Ordering::Release);
                  self.i.store((*new_head).id, Ordering::Release);
                  
                  // Free segments
                  for seg in to_free {
                     alloc::dealloc(seg as *mut u8, Layout::new::<Segment>());
                  }
               } else {
                  self.i.store(i, Ordering::Release);
               }
         }
         Err(_) => {}
      }
   }

   pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), ()> {
      unsafe {
         let handles = &*self.handles.get();
         if thread_id >= handles.len() {
               return Err(());
         }
         
         let handle = handles[thread_id];
         (*handle).hazard.store((*handle).tail, Ordering::Release);
         
         // Convert item to usize (for simplicity in this implementation)
         let v = Box::into_raw(Box::new(item)) as usize;
         
         // Try fast path
         let mut cell_id = 0;
         for _ in 0..PATIENCE {
               if self.enq_fast(handle, v, &mut cell_id) {
                  (*handle).hazard.store(null_mut(), Ordering::Release);
                  return Ok(());
               }
         }
         
         // Use slow path
         self.enq_slow(handle, v, cell_id);
         (*handle).hazard.store(null_mut(), Ordering::Release);
         Ok(())
      }
   }

   pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
      unsafe {
         let handles = &*self.handles.get();
         if thread_id >= handles.len() {
               return Err(());
         }
         
         let handle = handles[thread_id];
         (*handle).hazard.store((*handle).head, Ordering::Release);
         
         // Try fast path
         let mut v = TOP;
         let mut cell_id = 0;
         
         for _ in 0..PATIENCE {
               match self.deq_fast(handle, &mut cell_id) {
                  Ok(val) if val != TOP => {
                     v = val;
                     break;
                  }
                  Err(_) => {
                     (*handle).hazard.store(null_mut(), Ordering::Release);
                     self.cleanup(handle);
                     return Err(()); // EMPTY
                  }
                  _ => {}
               }
         }
         
         // Use slow path if needed
         if v == TOP {
               match self.deq_slow(handle, cell_id) {
                  Ok(val) => v = val,
                  Err(_) => {
                     (*handle).hazard.store(null_mut(), Ordering::Release);
                     self.cleanup(handle);
                     return Err(());
                  }
               }
         }
         
         // Help peer before returning
         self.help_deq(handle, (*handle).deq_peer);
         (*handle).deq_peer = (*(*handle).deq_peer).next;
         
         (*handle).hazard.store(null_mut(), Ordering::Release);
         self.cleanup(handle);
         
         // Convert back from usize to T
         let boxed = Box::from_raw(v as *mut T);
         Ok(*boxed)
      }
   }
}

// Implement MpscQueue trait
impl<T: Send + Clone + 'static> MpscQueue<T> for YangCrummeyQueue<T> {
   type PushError = ();
   type PopError = ();
   
   fn push(&self, item: T) -> Result<(), Self::PushError> {
      // For the trait, we'll use thread ID 0
      // In real usage, each thread should use its own ID
      self.enqueue(0, item)
   }
   
   fn pop(&self) -> Result<T, Self::PopError> {
      // For the trait, we'll use thread ID 0
      self.dequeue(0)
   }
   
   fn is_empty(&self) -> bool {
      self.h.load(Ordering::Acquire) >= self.t.load(Ordering::Acquire)
   }
   
   fn is_full(&self) -> bool {
      false // Unbounded queue
   }
}

impl<T: Send + Clone> Drop for YangCrummeyQueue<T> {
   fn drop(&mut self) {
      unsafe {
         // Clean up all segments
         let mut current = self.q.load(Ordering::Relaxed);
         while !current.is_null() {
               let next = (*current).next.load(Ordering::Relaxed);
               
               // Drop any remaining values
               for i in 0..SEGMENT_SIZE {
                  let val = (*current).cells[i].val.load(Ordering::Relaxed);
                  if val != BOTTOM && val != TOP && val != 0 {
                     drop(Box::from_raw(val as *mut T));
                  }
               }
               
               alloc::dealloc(current as *mut u8, Layout::new::<Segment>());
               current = next;
         }
         
         // Clean up handles
         let handles = &*self.handles.get();
         for &handle in handles.iter() {
               if !handle.is_null() {
                  drop(Box::from_raw(handle));
               }
         }
      }
   }
}