// queues/src/mpmc/kw_queue.rs
// Full implementation of Khanchandani-Wattenhofer MPMC Queue
// Based on "On the Importance of Synchronization Primitives with Low Consensus Numbers"

use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::MpmcQueue;

// Constants
const MAX_OPS: usize = 2_000_000;

// Debug flag
const DEBUG: bool = false;

macro_rules! debug_log {
    ($($arg:tt)*) => {
        if DEBUG {
            eprintln!("[KW {}] {}", std::process::id(), format!($($arg)*));
        }
    };
}

// Algorithm 8: TH Register with half-increment and half-max operations
#[repr(C, align(64))]
struct THRegister {
    value: AtomicU64, // Lower 32: tail, Upper 32: head
}

impl THRegister {
    fn new() -> Self {
        Self {
            value: AtomicU64::new(1), // t=1, h=0
        }
    }

    // Algorithm 8: half-increment operation
    fn half_increment(&self) -> isize {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let current = self.value.load(Ordering::Acquire);
            let t = (current & 0xFFFFFFFF) as u32;
            let h = (current >> 32) as u32;

            if t > h {
                return -1;
            }

            let new_value = ((h as u64) << 32) | ((t + 1) as u64);
            match self.value.compare_exchange_weak(
                current,
                new_value,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return t as isize,
                Err(_) => {
                    if attempts > 1000 {
                        return -1;
                    }
                    continue;
                }
            }
        }
    }

    // Algorithm 8: half-max operation
    fn half_max(&self, i: u32) {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let current = self.value.load(Ordering::Acquire);
            let t = (current & 0xFFFFFFFF) as u32;
            let h = (current >> 32) as u32;

            if h >= i {
                return;
            }

            let new_value = ((i as u64) << 32) | (t as u64);
            match self.value.compare_exchange_weak(
                current,
                new_value,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(_) => {
                    if attempts > 1000 {
                        return;
                    }
                    continue;
                }
            }
        }
    }
}

// P register for base case (stores element pointer and counter)
#[repr(C)]
struct PRegister {
    value: AtomicU64, // Upper 32: element index, Lower 32: counter
}

impl PRegister {
    fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    fn read(&self) -> (u32, u32) {
        let val = self.value.load(Ordering::Acquire);
        let elem_idx = (val >> 32) as u32;
        let counter = (val & 0xFFFFFFFF) as u32;
        (elem_idx, counter)
    }

    fn write(&self, elem_idx: u32, counter: u32) {
        let val = ((elem_idx as u64) << 32) | (counter as u64);
        self.value.store(val, Ordering::Release);
    }
}

// C register storing l1|r1|l2|r2
#[repr(C)]
struct CRegister {
    value: AtomicU64,
}

impl CRegister {
    fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    fn read(&self) -> (u16, u16, u16, u16) {
        let val = self.value.load(Ordering::Acquire);
        (
            ((val >> 48) & 0xFFFF) as u16,
            ((val >> 32) & 0xFFFF) as u16,
            ((val >> 16) & 0xFFFF) as u16,
            (val & 0xFFFF) as u16,
        )
    }

    fn pack(l1: u16, r1: u16, l2: u16, r2: u16) -> u64 {
        ((l1 as u64) << 48) | ((r1 as u64) << 32) | ((l2 as u64) << 16) | (r2 as u64)
    }

    fn compare_and_swap(&self, expected: (u16, u16, u16, u16), new: (u16, u16, u16, u16)) -> bool {
        let expected_val = Self::pack(expected.0, expected.1, expected.2, expected.3);
        let new_val = Self::pack(new.0, new.1, new.2, new.3);
        self.value
            .compare_exchange(expected_val, new_val, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

// Counting set structure (shared memory compatible)
#[repr(C)]
struct CountingSet {
    // For k > 1
    c: CRegister,
    k: u32,
    sqrt_k: u32,
    left_k: u32,
    right_k: u32,

    // For k = 1
    p: PRegister,

    // Offsets to sub-structures and arrays
    cl_offset: u32,      // Offset to left counting set
    cr_offset: u32,      // Offset to right counting set
    t_array_offset: u32, // Offset to T array
    l_array_offset: u32, // Offset to L array
    r_array_offset: u32, // Offset to R array
}

impl CountingSet {
    unsafe fn get_cl(&self) -> &CountingSet {
        if self.cl_offset == 0 {
            panic!("No left counting set");
        }
        let ptr = (self as *const Self as *const u8).add(self.cl_offset as usize);
        &*(ptr as *const CountingSet)
    }

    unsafe fn get_cr(&self) -> &CountingSet {
        if self.cr_offset == 0 {
            panic!("No right counting set");
        }
        let ptr = (self as *const Self as *const u8).add(self.cr_offset as usize);
        &*(ptr as *const CountingSet)
    }

    unsafe fn get_t_array(&self) -> &[AtomicU64] {
        if self.t_array_offset == 0 {
            return &[];
        }
        let ptr = (self as *const Self as *const u8).add(self.t_array_offset as usize);
        std::slice::from_raw_parts(ptr as *const AtomicU64, MAX_OPS)
    }

    unsafe fn get_l_array(&self) -> &[AtomicU64] {
        if self.l_array_offset == 0 {
            return &[];
        }
        let ptr = (self as *const Self as *const u8).add(self.l_array_offset as usize);
        std::slice::from_raw_parts(ptr as *const AtomicU64, MAX_OPS)
    }

    unsafe fn get_r_array(&self) -> &[AtomicU64] {
        if self.r_array_offset == 0 {
            return &[];
        }
        let ptr = (self as *const Self as *const u8).add(self.r_array_offset as usize);
        std::slice::from_raw_parts(ptr as *const AtomicU64, MAX_OPS)
    }

    // Algorithm 3: insert operation
    unsafe fn insert(&self, thread_id: usize, elem_idx: u32) -> usize {
        if self.k == 1 {
            // Base case - elem_idx is just passed through
            let (_, t) = self.p.read();
            self.p.write(elem_idx, t + 1);
            return (t + 1) as usize;
        }

        // Recursive case
        let is_left = thread_id < (self.left_k as usize);
        let subset_id = if is_left {
            thread_id
        } else {
            thread_id - (self.left_k as usize)
        };

        // Call insert on appropriate subset
        let r = if is_left {
            self.get_cl().insert(subset_id, elem_idx)
        } else {
            self.get_cr().insert(subset_id, elem_idx)
        };

        // Update C register
        loop {
            let cb = self.c.read();
            let tl = self.get_cl().total() as u16;
            let tr = self.get_cr().total() as u16;
            self.log(cb);
            let cb_new = (cb.2, cb.3, tl, tr); // new_value: l1←l2, r1←r2, l2←tl, r2←tr

            if self.c.compare_and_swap(cb, cb_new) {
                break;
            }
        }

        self.log(self.c.read());
        self.lookup(r, is_left)
    }

    // Algorithm 7: remove operation
    unsafe fn remove(&self, i: usize) -> Option<u32> {
        if self.k == 1 {
            let (e, t) = self.p.read();
            if i > t as usize {
                return None;
            }
            self.p.write(0, t);
            if e == 0 {
                None
            } else {
                Some(e)
            }
        } else {
            self.log(self.c.read());

            // Find in T array
            let mut h = i;
            let t_array = self.get_t_array();
            while h < MAX_OPS && t_array[h].load(Ordering::Acquire) == 0 {
                h += 1;
                if h > i + self.sqrt_k as usize {
                    return None;
                }
            }

            if h >= MAX_OPS {
                return None;
            }

            let packed = t_array[h].load(Ordering::Acquire);
            let (l1, r1, l2, _) = Self::unpack_c_value(packed);

            let l1_r1 = (l1 + r1) as usize;
            let l2_l1 = (l2 - l1) as usize;

            if l1_r1 < i && i <= l1_r1 + l2_l1 {
                let i_prime = i - l1_r1 + l1 as usize;
                self.get_cl().remove(i_prime)
            } else {
                let i_prime = i - l1_r1 - l2_l1 + r1 as usize;
                self.get_cr().remove(i_prime)
            }
        }
    }

    // Algorithm 4: total operation
    unsafe fn total(&self) -> usize {
        if self.k == 1 {
            let (_, t) = self.p.read();
            t as usize
        } else {
            let (_, _, l2, r2) = self.c.read();
            (l2 + r2) as usize
        }
    }

    // Algorithm 6: optimized log function
    unsafe fn log(&self, cb: (u16, u16, u16, u16)) {
        if self.k == 1 {
            return;
        }

        let packed = CRegister::pack(cb.0, cb.1, cb.2, cb.3);

        // Update arrays with sqrt(k) spacing
        let total = (cb.2 + cb.3) as usize;
        let t_array = self.get_t_array();
        if total < MAX_OPS {
            t_array[total].store(packed, Ordering::Release);
        }

        let mut i = (cb.0 + cb.1) as usize + self.sqrt_k as usize;
        while i < total && i < MAX_OPS {
            t_array[i].store(packed, Ordering::Release);
            i += self.sqrt_k as usize;
        }

        if cb.2 != cb.0 {
            let l_array = self.get_l_array();
            let l2 = cb.2 as usize;
            if l2 < MAX_OPS {
                l_array[l2].store(packed, Ordering::Release);
            }

            let mut i = cb.0 as usize + self.sqrt_k as usize;
            while i < l2 && i < MAX_OPS {
                l_array[i].store(packed, Ordering::Release);
                i += self.sqrt_k as usize;
            }
        }

        if cb.3 != cb.1 {
            let r_array = self.get_r_array();
            let r2 = cb.3 as usize;
            if r2 < MAX_OPS {
                r_array[r2].store(packed, Ordering::Release);
            }

            let mut i = cb.1 as usize + self.sqrt_k as usize;
            while i < r2 && i < MAX_OPS {
                r_array[i].store(packed, Ordering::Release);
                i += self.sqrt_k as usize;
            }
        }
    }

    // Algorithm 5: lookup function
    unsafe fn lookup(&self, r: usize, in_left: bool) -> usize {
        if r == 0 || r >= MAX_OPS {
            return 0;
        }

        let array = if in_left {
            self.get_l_array()
        } else {
            self.get_r_array()
        };

        let mut s = r;
        while s < MAX_OPS && s < r + self.sqrt_k as usize + 1 {
            if array[s].load(Ordering::Acquire) != 0 {
                break;
            }
            s += 1;
        }

        if s >= MAX_OPS {
            return 0;
        }

        let packed = array[s].load(Ordering::Acquire);
        let (l1, r1, l2, _) = Self::unpack_c_value(packed);

        if in_left {
            (l1 + r1) as usize + (r - l1 as usize)
        } else {
            (l1 + r1) as usize + (l2 - l1) as usize + (r - r1 as usize)
        }
    }

    fn unpack_c_value(val: u64) -> (u16, u16, u16, u16) {
        (
            ((val >> 48) & 0xFFFF) as u16,
            ((val >> 32) & 0xFFFF) as u16,
            ((val >> 16) & 0xFFFF) as u16,
            (val & 0xFFFF) as u16,
        )
    }
}

// Main queue structure
#[repr(C)]
pub struct KWQueue<T: Send + Clone + 'static> {
    th: THRegister,
    num_threads: usize,

    // Offsets in shared memory
    counting_set_offset: u32,
    array_offset: u32,
    elements_offset: u32,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for KWQueue<T> {}
unsafe impl<T: Send + Clone> Sync for KWQueue<T> {}

impl<T: Send + Clone + 'static> KWQueue<T> {
    pub fn new(num_threads: usize) -> Box<Self> {
        // Not used in shared memory mode
        Box::new(Self {
            th: THRegister::new(),
            num_threads,
            counting_set_offset: 0,
            array_offset: 0,
            elements_offset: 0,
            _phantom: std::marker::PhantomData,
        })
    }

    unsafe fn get_counting_set(&self) -> &CountingSet {
        let ptr = (self as *const Self as *const u8).add(self.counting_set_offset as usize);
        &*(ptr as *const CountingSet)
    }

    unsafe fn get_array(&self) -> &[UnsafeCell<Option<T>>] {
        let ptr = (self as *const Self as *const u8).add(self.array_offset as usize);
        std::slice::from_raw_parts(ptr as *const UnsafeCell<Option<T>>, MAX_OPS)
    }

    // Algorithm 9: enqueue operation
    pub fn enqueue(&self, thread_id: usize, x: T) -> Result<(), ()> {
        unsafe {
            // Get slot from counting set (using thread_id as placeholder for elem)
            let i = self.get_counting_set().insert(thread_id, thread_id as u32);

            if i == 0 || i >= MAX_OPS {
                return Err(());
            }

            // Write directly to array
            (*self.get_array()[i].get()) = Some(x);

            // Remove from counting set to mark completion
            self.get_counting_set().remove(i);

            // Update head
            self.th.half_max(i as u32);

            Ok(())
        }
    }

    // Algorithm 9: dequeue operation
    pub fn dequeue(&self, _thread_id: usize) -> Result<T, ()> {
        unsafe {
            let i = self.th.half_increment();
            if i == -1 {
                return Err(());
            }

            let i = i as usize;
            if i >= MAX_OPS {
                return Err(());
            }

            // Try remove from counting set first (in case enqueue is still in progress)
            if let Some(_elem_idx) = self.get_counting_set().remove(i) {
                // Wait for the array to be written
                let mut spins = 0;
                loop {
                    if let Some(value) = (*self.get_array()[i].get()).take() {
                        return Ok(value);
                    }
                    spins += 1;
                    if spins > 1000 {
                        return Err(());
                    }
                    std::hint::spin_loop();
                }
            }

            // Get from array
            if let Some(value) = (*self.get_array()[i].get()).take() {
                Ok(value)
            } else {
                Err(())
            }
        }
    }

    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        debug_log!("KWQueue::init_in_shared(n={}) at {:p}", num_threads, mem);

        let queue_ptr = mem as *mut Self;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + 63) & !63;

        // Initialize queue header
        ptr::write(&mut (*queue_ptr).th, THRegister::new());
        (*queue_ptr).num_threads = num_threads;

        // Initialize counting set recursively
        let counting_set_offset = queue_aligned;
        (*queue_ptr).counting_set_offset = counting_set_offset as u32;

        let (_cs_size, array_offset) = Self::init_counting_set(
            mem.add(counting_set_offset),
            num_threads,
            counting_set_offset,
        );

        (*queue_ptr).array_offset = array_offset as u32;

        // Initialize main array
        let array_ptr = mem.add(array_offset) as *mut [UnsafeCell<Option<T>>; MAX_OPS];
        for i in 0..MAX_OPS {
            let cell_ptr = &mut (*array_ptr)[i] as *mut UnsafeCell<Option<T>>;
            ptr::write(cell_ptr, UnsafeCell::new(None::<T>));
        }

        debug_log!("KWQueue::init_in_shared completed");
        &mut *queue_ptr
    }

    unsafe fn init_counting_set(mem: *mut u8, k: usize, base_offset: usize) -> (usize, usize) {
        let cs_ptr = mem as *mut CountingSet;

        (*cs_ptr).k = k as u32;
        (*cs_ptr).sqrt_k = (k as f64).sqrt().floor() as u32;

        if k == 1 {
            // Base case
            (*cs_ptr).p = PRegister::new();
            (*cs_ptr).cl_offset = 0;
            (*cs_ptr).cr_offset = 0;
            (*cs_ptr).t_array_offset = 0;
            (*cs_ptr).l_array_offset = 0;
            (*cs_ptr).r_array_offset = 0;
            (*cs_ptr).left_k = 0;
            (*cs_ptr).right_k = 0;

            let size = mem::size_of::<CountingSet>();
            let aligned = (size + 63) & !63;
            return (aligned, base_offset + aligned);
        }

        // Recursive case
        (*cs_ptr).c = CRegister::new();

        let k_left = k / 2;
        let k_right = k - k_left;
        (*cs_ptr).left_k = k_left as u32;
        (*cs_ptr).right_k = k_right as u32;

        let cs_size = mem::size_of::<CountingSet>();
        let cs_aligned = (cs_size + 63) & !63;

        // Initialize left subset
        let cl_offset = cs_aligned;
        (*cs_ptr).cl_offset = cl_offset as u32;
        let (cl_size, _after_cl) =
            Self::init_counting_set(mem.add(cl_offset), k_left, base_offset + cl_offset);

        // Initialize right subset
        let cr_offset = cl_offset + cl_size;
        (*cs_ptr).cr_offset = cr_offset as u32;
        let (cr_size, _after_cr) =
            Self::init_counting_set(mem.add(cr_offset), k_right, base_offset + cr_offset);

        // Initialize arrays
        let arrays_offset = cr_offset + cr_size;
        let array_size = MAX_OPS * mem::size_of::<AtomicU64>();
        let array_aligned = (array_size + 63) & !63;

        (*cs_ptr).t_array_offset = arrays_offset as u32;
        (*cs_ptr).l_array_offset = (arrays_offset + array_aligned) as u32;
        (*cs_ptr).r_array_offset = (arrays_offset + 2 * array_aligned) as u32;

        // Initialize arrays to zero
        let t_ptr = mem.add(arrays_offset) as *mut AtomicU64;
        let l_ptr = mem.add(arrays_offset + array_aligned) as *mut AtomicU64;
        let r_ptr = mem.add(arrays_offset + 2 * array_aligned) as *mut AtomicU64;

        for i in 0..MAX_OPS {
            ptr::write(t_ptr.add(i), AtomicU64::new(0));
            ptr::write(l_ptr.add(i), AtomicU64::new(0));
            ptr::write(r_ptr.add(i), AtomicU64::new(0));
        }

        let total_size = arrays_offset + 3 * array_aligned;
        (total_size, base_offset + total_size)
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + 63) & !63;

        // Calculate counting set size recursively
        let cs_size = Self::counting_set_size(num_threads);

        // Main array size
        let array_size = MAX_OPS * mem::size_of::<UnsafeCell<Option<usize>>>();
        let array_aligned = (array_size + 63) & !63;

        let total = queue_aligned + cs_size + array_aligned;
        (total + 4095) & !4095
    }

    fn counting_set_size(k: usize) -> usize {
        if k == 1 {
            let size = mem::size_of::<CountingSet>();
            (size + 63) & !63
        } else {
            let cs_size = mem::size_of::<CountingSet>();
            let cs_aligned = (cs_size + 63) & !63;

            let k_left = k / 2;
            let k_right = k - k_left;

            let left_size = Self::counting_set_size(k_left);
            let right_size = Self::counting_set_size(k_right);

            let array_size = MAX_OPS * mem::size_of::<AtomicU64>();
            let array_aligned = (array_size + 63) & !63;

            cs_aligned + left_size + right_size + 3 * array_aligned
        }
    }

    pub fn is_empty(&self) -> bool {
        let val = self.th.value.load(Ordering::Acquire);
        let t = (val & 0xFFFFFFFF) as u32;
        let h = (val >> 32) as u32;
        t > h
    }

    pub fn is_full(&self) -> bool {
        false
    }
}

impl<T: Send + Clone + 'static> MpmcQueue<T> for KWQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, item: T, thread_id: usize) -> Result<(), Self::PushError> {
        self.enqueue(thread_id, item)
    }

    fn pop(&self, thread_id: usize) -> Result<T, Self::PopError> {
        self.dequeue(thread_id)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + Clone> Drop for KWQueue<T> {
    fn drop(&mut self) {
        // Nothing to clean up in shared memory version
    }
}
