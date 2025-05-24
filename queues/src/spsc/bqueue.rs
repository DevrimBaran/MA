

use crate::SpscQueue;
use std::cell::Cell;
use std::mem::{self, MaybeUninit};
use std::ptr;

#[repr(C)]
pub struct BQueue<T: Send + 'static> {
    buf: *mut MaybeUninit<T>,
    valid: *mut bool,  
    cap: usize,
    mask: usize,
    head: Cell<usize>,
    batch_head: Cell<usize>,    
    tail: Cell<usize>,
    batch_tail: Cell<usize>,
}

const BATCH_SIZE: usize = 256;

unsafe impl<T: Send + 'static> Sync for BQueue<T> {}
unsafe impl<T: Send + 'static> Send for BQueue<T> {}

impl<T: Send + 'static> BQueue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        
        
        let mut buf_vec: Vec<MaybeUninit<T>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buf_vec.push(MaybeUninit::uninit());
        }
        let buf = Box::into_raw(buf_vec.into_boxed_slice()) as *mut MaybeUninit<T>;
        
        
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

    
    pub fn push(&self, item: T) -> Result<(), T> {
        let head = self.head.get();
        
        
        if head == self.batch_head.get() {
            
            
            
            let probe_idx = self.mod_(head + BATCH_SIZE);
            
            
            unsafe {
                if *self.valid.add(probe_idx) {
                    return Err(item); 
                }
            }
            
            
            self.batch_head.set(probe_idx);
        }
        
        
        unsafe {
            ptr::write(self.buf.add(head), MaybeUninit::new(item));
            *self.valid.add(head) = true; 
        }
        
        
        self.head.set(self.next(head));
        
        
        Ok(())
    }

    
    pub fn pop(&self) -> Result<T, ()> {
        let tail = self.tail.get();
        
        
        unsafe {
            if !*self.valid.add(tail) {
                
                match self.backtrack_deq() {
                    Some(new_batch_tail) => {
                        self.batch_tail.set(new_batch_tail);
                    }
                    None => {
                        return Err(()); 
                    }
                }
            }
        }
        
        
        let value = unsafe {
            let item = ptr::read(self.buf.add(tail));
            item.assume_init()
        };
        
        
        unsafe {
            *self.valid.add(tail) = false; 
        }
        
        
        self.tail.set(self.next(tail));
        
        
        Ok(value)
    }

    
    fn backtrack_deq(&self) -> Option<usize> {
        
        let tail = self.tail.get();
        
        
        let mut batch_size = BATCH_SIZE.min(self.cap);
        
        
        let mut batch_tail;
        
        
        loop {
            if batch_size == 0 {
                return None; 
            }
            
            
            batch_tail = self.mod_(tail + batch_size - 1);
            
            
            unsafe {
                if *self.valid.add(batch_tail) {
                    
                    
                    return Some(batch_tail);
                }
            }
            
            
            
            
            if batch_size > 1 {
                
                batch_size >>= 1;
            } else {
                
                unsafe {
                    if *self.valid.add(tail) {
                        return Some(tail);
                    }
                }
                
                return None;
            }
            
        }
    }

    pub fn available(&self) -> bool {
        let head = self.head.get();
        let batch_head = self.batch_head.get();
        
        
        if head != batch_head {
            return true;
        }
        
        
        let probe_idx = self.mod_(head + BATCH_SIZE);
        unsafe { !*self.valid.add(probe_idx) }
    }

    pub fn empty(&self) -> bool {
        
        let tail = self.tail.get();
        unsafe {
            
            if *self.valid.add(tail) {
                return false;
            }
        }
        
        
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
        
        
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.buf, self.cap));
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(self.valid, self.cap));
        }
    }
}