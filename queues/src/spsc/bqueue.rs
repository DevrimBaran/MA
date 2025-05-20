// bqueue from wang et al. 2013
#![allow(clippy::cast_possible_truncation)]

use crate::SpscQueue;
use std::cell::Cell;
use std::mem;
use std::mem::MaybeUninit;
use std::ptr;

#[repr(C)]
pub struct BQueue<T: Send + 'static> {
    buf: *mut MaybeUninit<Option<T>>,
    cap: usize,
    mask: usize,
    head: Cell<usize>, 
    batch_head: Cell<usize>, 
    tail: Cell<usize>,
    batch_tail: Cell<usize>, 
    history: Cell<usize>,
}

const BATCH_SIZE: usize = 256;
const BQUEUE_BATCH_MAX: usize = BATCH_SIZE;
const BQUEUE_INCREMENT: usize = 1;

unsafe impl<T: Send + 'static> Sync for BQueue<T> {}
unsafe impl<T: Send + 'static> Send for BQueue<T> {}

impl<T: Send + 'static> BQueue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        let mut v: Vec<MaybeUninit<Option<T>>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            v.push(MaybeUninit::new(None));
        }
        let buf_ptr = Box::into_raw(v.into_boxed_slice()) as *mut MaybeUninit<Option<T>>;
        
        // Initialize batch_head and batch_tail to be 'edge' (cap - 1).
        // This forces an immediate probe on first push/pop to establish a valid batch.
        let edge = capacity - 1; 
        let initial_history = BATCH_SIZE.min(capacity); // Initial history value

        BQueue {
            buf: buf_ptr,
            cap: capacity,
            mask: capacity - 1, // Mask is cap - 1 for power-of-two capacities
            head: Cell::new(0),
            batch_head: Cell::new(edge), // Initial batch_head (forces probe)
            tail: Cell::new(0),
            batch_tail: Cell::new(edge), // Initial batch_tail (forces backtrack)
            history: Cell::new(initial_history),
        }
    }

    pub const fn shared_size(capacity: usize) -> usize {
        mem::size_of::<Self>() + capacity * mem::size_of::<MaybeUninit<Option<T>>>()
    }

    pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        let header = mem as *mut Self;
        let buf_ptr = mem.add(mem::size_of::<Self>()) as *mut MaybeUninit<Option<T>>;
        
        // Initialize the BQueue struct fields first
        ptr::write(header, BQueue::new_inplace(buf_ptr, capacity));
        
        // Then initialize the buffer content
        for i in 0..capacity {
            ptr::write(buf_ptr.add(i), MaybeUninit::new(None));
        }
        &mut *header
    }

    fn new_inplace(buf_ptr: *mut MaybeUninit<Option<T>>, capacity: usize) -> Self {
        let edge = capacity - 1;
        let initial_history = BATCH_SIZE.min(capacity);
        BQueue {
            buf: buf_ptr,
            cap: capacity,
            mask: capacity - 1,
            head: Cell::new(0),
            batch_head: Cell::new(edge),
            tail: Cell::new(0),
            batch_tail: Cell::new(edge),
            history: Cell::new(initial_history),
        }
    }

    #[inline]
    fn next(&self, idx: usize) -> usize {
        (idx + 1) & self.mask
    }

    pub fn push(&self, item: T) -> Result<(), T> {
        let h = self.head.get();
        if h == self.batch_head.get() { // Need to probe for a new batch
            let new_bh = (h + BATCH_SIZE -1) & self.mask;
            
            unsafe {
                if (*self.buf.add(new_bh)).assume_init_ref().is_some() {
                    return Err(item);
                }
            }
            self.batch_head.set(new_bh);
        }
        unsafe {
            let slot = self.buf.add(h) as *mut Option<T>;
            ptr::write(slot, Some(item));
        }
        self.head.set(self.next(h));
        Ok(())
    }

    pub fn pop(&self) -> Result<T, ()> {
        let t = self.tail.get(); // Current tail: Fig 7, local var 'tail'

        if t != self.batch_tail.get() {
            // Fast path: items are believed to be in the current batch
            unsafe {
                let slot_ptr = self.buf.add(t) as *mut Option<T>;
                // Directly attempt to read.
                if let Some(v) = ptr::read(slot_ptr) { // Fig 7, Q19: *value = buffer[tail]
                    ptr::write(slot_ptr, None);        // Fig 7, Q20: buffer[tail] = NULL
                    self.tail.set(self.next(t));       // Fig 7, Q21: tail = NEXT(tail)
                    return Ok(v);                      // Fig 7, Q22: return SUCCESS
                }
            }
        }

        // 1. Initialize batch_size for probing based on history (Fig 10, N03-N06)
        let mut current_adaptive_hist_val = self.history.get().min(self.cap); // Start with history.
        if current_adaptive_hist_val < BQUEUE_BATCH_MAX { // Fig 10, N03
            current_adaptive_hist_val = BQUEUE_BATCH_MAX.min(current_adaptive_hist_val + BQUEUE_INCREMENT); // Fig 10, N04
        }
        // 'current_adaptive_hist_val' is now the 'batch_history' to be used for this attempt's initial 'batch_size'.
        
        let mut current_probe_batch_size = current_adaptive_hist_val; // Fig 10, N06: batch_size = batch_history;
        let mut new_found_batch_tail_candidate; // Local 'batch_tail' for probing in Fig 10.

        // 2. Backtracking search loop (Fig 10, N08-N16)
        loop {
            if current_probe_batch_size == 0 {
                // This should ideally not be hit if history starts >= 1 and decrements correctly.
                // If it does, it means an exhaustive search failed.
                self.history.set(1); // Reset history to minimal probe size.
                return Err(()); // Corresponds to Fig 10, N15 (FAILURE)
            }
            // Fig 10, N07 & N12: batch_tail = MOD(tail + batch_size - 1)
            new_found_batch_tail_candidate = (t + current_probe_batch_size - 1) & self.mask;

            unsafe {
                // Fig 10, N08: while (!buffer[batch_tail]) -- loop if slot is NULL
                if (*self.buf.add(new_found_batch_tail_candidate)).assume_init_ref().is_some() {
                    // Found a non-empty slot. Backtracking SUCCESS.
                    self.batch_tail.set(new_found_batch_tail_candidate); // Update the BQueue's batch_tail
                    self.history.set(current_probe_batch_size);        // Fig 10, N17: batch_history = batch_size
                    break; // Exit backtracking loop
                }
            }

            // Slot was empty.
            // Fig 10, N09: spin_wait(TICKS); // Omitted for "producer is done" scenario.
            if current_probe_batch_size > 1 { // Fig 10, N10
                current_probe_batch_size >>= 1; // Fig 10, N11: Halve probe_batch_size
            } else {
                // current_probe_batch_size is 1, and buffer[t] (as new_found_batch_tail_candidate would be 't') was also empty.
                // Backtracking FAILURE.
                self.history.set(1);
                return Err(());
            }
        }

        // 3. Dequeue item after successful backtracking (Fig 7, Q19-Q22)
        unsafe {
            let slot_ptr = self.buf.add(t) as *mut Option<T>;
            if let Some(v) = ptr::read(slot_ptr) {
                ptr::write(slot_ptr, None);
                self.tail.set(self.next(t));
                let items_in_new_batch = (self.batch_tail.get().wrapping_sub(t) & self.mask).wrapping_add(1);
                self.history.set(items_in_new_batch.min(self.cap));

                return Ok(v);
            } else {
                return Err(());
            }
        }
    }

    pub fn available(&self) -> bool {
        let h = self.head.get();
        let bh = self.batch_head.get();
        if h != bh { return true; }
        let probe_idx = (h + BATCH_SIZE -1) & self.mask; // Sticking to existing logic's probe point for consistency.
        unsafe { (*self.buf.add(probe_idx)).assume_init_ref().is_none() }
    }

    pub fn empty(&self) -> bool {
        let t = self.tail.get();
        let bt = self.batch_tail.get();
        if t != bt { return false; } // Items definitely in current batch
        unsafe { (*self.buf.add(bt)).assume_init_ref().is_none() }
    }
}

impl<T: Send + 'static> SpscQueue<T> for BQueue<T> {
    type PushError = ();
    type PopError = (); 

    fn push(&self, item: T) -> Result<(), Self::PushError> { 
        self.push(item).map_err(|_| ()) 
    }
    fn pop(&self) -> Result<T, Self::PopError> { 
        self.pop() 
    }
    fn available(&self) -> bool { 
        self.available() 
    }
    fn empty(&self) -> bool { 
        self.empty()
    }
}

impl<T: Send + 'static> Drop for BQueue<T> {
    fn drop(&mut self) {
        // Drain any remaining items.
        // This pop respects the queue logic, so it's safe.
        if std::mem::needs_drop::<T>() {
            while let Ok(_item) = self.pop() {
                // Item is dropped as it goes out of scope
            }
        }
        if !self.buf.is_null() {
            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.buf, self.cap));
            }
        }
    }
}