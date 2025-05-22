// B-Queue from Wang et al. 2013
// Efficient and Practical Queuing for Fast Core-to-Core Communication

use crate::SpscQueue;
use std::cell::Cell;
use std::mem::{self, MaybeUninit};
use std::ptr;

#[repr(C)]
pub struct BQueue<T: Send + 'static> {
    // The buffer stores both data and a validity flag
    // Paper uses NULL detection - we use a separate valid array
    buf: *mut MaybeUninit<T>,
    valid: *mut bool,  // Tracks if slot contains valid data (non-NULL in paper)
    cap: usize,
    mask: usize,
    
    // Producer local variables
    head: Cell<usize>,
    batch_head: Cell<usize>,
    
    // Consumer local variables
    tail: Cell<usize>,
    batch_tail: Cell<usize>,
}

const BATCH_SIZE: usize = 256;

unsafe impl<T: Send + 'static> Sync for BQueue<T> {}
unsafe impl<T: Send + 'static> Send for BQueue<T> {}

impl<T: Send + 'static> BQueue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        
        // Allocate buffer for data
        let mut buf_vec: Vec<MaybeUninit<T>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buf_vec.push(MaybeUninit::uninit());
        }
        let buf = Box::into_raw(buf_vec.into_boxed_slice()) as *mut MaybeUninit<T>;
        
        // Allocate validity tracking array (represents NULL/non-NULL in paper)
        let valid = Box::into_raw(
            vec![false; capacity].into_boxed_slice()
        ) as *mut bool;
        
        BQueue {
            buf,
            valid,
            cap: capacity,
            mask: capacity - 1,
            head: Cell::new(0),
            batch_head: Cell::new(0),
            tail: Cell::new(0),
            batch_tail: Cell::new(0),
        }
    }

    pub const fn shared_size(capacity: usize) -> usize {
        mem::size_of::<Self>() + 
        capacity * mem::size_of::<MaybeUninit<T>>() +
        capacity * mem::size_of::<bool>()
    }

    pub unsafe fn init_in_shared(mem: *mut u8, capacity: usize) -> &'static mut Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        
        let header_ptr = mem as *mut Self;
        let buf_ptr = mem.add(mem::size_of::<Self>()) as *mut MaybeUninit<T>;
        let valid_ptr = mem.add(mem::size_of::<Self>() + capacity * mem::size_of::<MaybeUninit<T>>()) as *mut bool;
        
        // Initialize buffer
        for i in 0..capacity {
            ptr::write(buf_ptr.add(i), MaybeUninit::uninit());
            ptr::write(valid_ptr.add(i), false);
        }
        
        ptr::write(header_ptr, BQueue {
            buf: buf_ptr,
            valid: valid_ptr,
            cap: capacity,
            mask: capacity - 1,
            head: Cell::new(0),
            batch_head: Cell::new(0),
            tail: Cell::new(0),
            batch_tail: Cell::new(0),
        });
        
        &mut *header_ptr
    }

    #[inline]
    fn next(&self, idx: usize) -> usize {
        (idx + 1) & self.mask
    }
    
    #[inline]
    fn mod_(&self, idx: usize) -> usize {
        idx & self.mask
    }

    // Algorithm 1: Enqueue operation (Figure 7 in paper)
    pub fn push(&self, item: T) -> Result<(), T> {
        let head = self.head.get();
        
        // Line Q03: if (head == batch_head)
        if head == self.batch_head.get() {
            // Need to find a new batch of empty slots
            
            // Line Q04: batch_head = MOD(head + BATCH_SIZE)
            let probe_idx = self.mod_(head + BATCH_SIZE);
            
            // Line Q05: if (buffer[batch_head] != NULL) return FULL
            unsafe {
                if *self.valid.add(probe_idx) {
                    return Err(item); // Queue is full
                }
            }
            
            // Line Q06: // Update batch_head
            self.batch_head.set(probe_idx);
        }
        
        // Line Q08: buffer[head] = element
        unsafe {
            ptr::write(self.buf.add(head), MaybeUninit::new(item));
            *self.valid.add(head) = true; // Mark as non-NULL
        }
        
        // Line Q09: head = NEXT(head)
        self.head.set(self.next(head));
        
        // Line Q10: return SUCCESS
        Ok(())
    }

    // Algorithm 2: Dequeue operation (Figure 7 in paper)
    pub fn pop(&self) -> Result<T, ()> {
        let tail = self.tail.get();
        
        // First check if current slot has data
        unsafe {
            if !*self.valid.add(tail) {
                // Current slot is empty, need to search for data
                match self.backtrack_deq() {
                    Some(new_batch_tail) => {
                        self.batch_tail.set(new_batch_tail);
                    }
                    None => {
                        return Err(()); // Queue is empty
                    }
                }
            }
        }
        
        // Line Q18: value = buffer[tail]
        let value = unsafe {
            let item = ptr::read(self.buf.add(tail));
            item.assume_init()
        };
        
        // Line Q19: buffer[tail] = NULL
        unsafe {
            *self.valid.add(tail) = false; // Mark as NULL
        }
        
        // Line Q20: tail = NEXT(tail)
        self.tail.set(self.next(tail));
        
        // Line Q21: return SUCCESS
        Ok(value)
    }

    // Basic backtracking algorithm (Figure 9 in paper)
    fn backtrack_deq(&self) -> Option<usize> {
        // Line B01: tail = current tail position
        let tail = self.tail.get();
        
        // Line B03: batch_size = BATCH_SIZE
        let mut batch_size = BATCH_SIZE.min(self.cap);
        
        // Line B04: batch_tail = MOD(tail + batch_size - 1)
        let mut batch_tail;
        
        // Line B05: while (!buffer[batch_tail])
        loop {
            if batch_size == 0 {
                return None; // No data found
            }
            
            // Line B07: batch_tail = MOD(tail + batch_size - 1)
            batch_tail = self.mod_(tail + batch_size - 1);
            
            // Line B08: Check if buffer[batch_tail] != NULL
            unsafe {
                if *self.valid.add(batch_tail) {
                    // Found a filled slot
                    // Line B13: return batch_tail
                    return Some(batch_tail);
                }
            }
            
            // Line B09: spin_wait(TICKS) - omitted as per paper's note
            
            // Line B10: if (batch_size > 1)
            if batch_size > 1 {
                // Line B11: batch_size = batch_size / 2
                batch_size >>= 1;
            } else {
                // Check the current position as last resort
                unsafe {
                    if *self.valid.add(tail) {
                        return Some(tail);
                    }
                }
                // Line B06: return FAILURE
                return None;
            }
            // Line B12: Continue loop
        }
    }

    pub fn available(&self) -> bool {
        let head = self.head.get();
        let batch_head = self.batch_head.get();
        
        // Fast path: we're within current batch
        if head != batch_head {
            return true;
        }
        
        // Slow path: check if we can allocate a new batch
        let probe_idx = self.mod_(head + BATCH_SIZE);
        unsafe { !*self.valid.add(probe_idx) }
    }

    pub fn empty(&self) -> bool {
        // Check if any slot contains valid data
        let tail = self.tail.get();
        unsafe {
            // Quick check: current position
            if *self.valid.add(tail) {
                return false;
            }
        }
        
        // Full scan to be sure
        self.backtrack_deq().is_none()
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
        // Clean up remaining items
        if std::mem::needs_drop::<T>() {
            let mut tail = *self.tail.get_mut();
            let head = *self.head.get_mut();
            
            while tail != head {
                unsafe {
                    if *self.valid.add(tail) {
                        let item = ptr::read(self.buf.add(tail));
                        drop(item.assume_init());
                    }
                }
                tail = self.next(tail);
            }
        }
        
        // Free allocated memory
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.buf, self.cap));
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.valid, self.cap));
        }
    }
}