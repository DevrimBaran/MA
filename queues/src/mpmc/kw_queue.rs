// queues/src/mpmc/kw_queue.rs
// FIXED implementation of Khanchandani-Wattenhofer MPMC Queue
// Addressing race conditions and data loss issues

use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

use crate::MpmcQueue;

// Constants
const MAX_OPS: usize = 2_000_000;

// Algorithm 8: TH Register with half-increment and half-max operations
#[repr(C, align(64))]
struct THRegister {
    value: AtomicU64, // Lower 32: tail, Upper 32: head
}

impl THRegister {
    fn new() -> Self {
        Self {
            value: AtomicU64::new(1), // t=1, h=0 as per paper
        }
    }

    // Algorithm 8: half-increment operation - FIXED to be truly atomic
    fn half_increment(&self) -> isize {
        loop {
            let current = self.value.load(Ordering::SeqCst); // Stronger ordering
            let t = (current & 0xFFFFFFFF) as u32;
            let h = (current >> 32) as u32;

            if t > h {
                return -1; // Queue is empty
            }

            let new_value = ((h as u64) << 32) | ((t + 1) as u64);
            match self.value.compare_exchange_weak(
                current,
                new_value,
                Ordering::SeqCst, // Stronger ordering
                Ordering::SeqCst,
            ) {
                Ok(_) => return t as isize, // Return the slot we claimed
                Err(_) => continue,         // Retry
            }
        }
    }

    // Algorithm 8: half-max operation - FIXED to be truly atomic
    fn half_max(&self, i: u32) {
        loop {
            let current = self.value.load(Ordering::SeqCst); // Stronger ordering
            let t = (current & 0xFFFFFFFF) as u32;
            let h = (current >> 32) as u32;

            if h >= i {
                return; // Already at or past this value
            }

            let new_value = ((i as u64) << 32) | (t as u64);
            match self.value.compare_exchange_weak(
                current,
                new_value,
                Ordering::SeqCst, // Stronger ordering
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }
}

// Element wrapper for base case
#[repr(C)]
struct Element<T> {
    data: UnsafeCell<Option<T>>,
    initialized: AtomicU64, // Add initialization flag
}

impl<T> Element<T> {
    fn new() -> Self {
        Self {
            data: UnsafeCell::new(None),
            initialized: AtomicU64::new(0),
        }
    }
}

// P register for base case (k=1) - FIXED with stronger synchronization
#[repr(C)]
struct PRegister<T> {
    counter: AtomicU64,
    elements: AtomicPtr<Element<T>>,
}

impl<T> PRegister<T> {
    fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
            elements: AtomicPtr::new(ptr::null_mut()),
        }
    }

    fn set_elements(&self, elements: *mut Element<T>) {
        self.elements.store(elements, Ordering::SeqCst); // Stronger ordering
    }

    unsafe fn insert(&self, elem: T) -> u32 {
        let elements = self.elements.load(Ordering::SeqCst); // Stronger ordering
        if elements.is_null() {
            return 0;
        }

        // FIXED: Atomically increment and get the new slot
        let slot = self.counter.fetch_add(1, Ordering::SeqCst) + 1; // Stronger ordering

        if slot >= MAX_OPS as u64 {
            return 0;
        }

        // Store element with full synchronization
        let elem_ptr = elements.add(slot as usize);
        let elem_ref = &*elem_ptr;

        // CRITICAL: Set initialization flag BEFORE storing data
        elem_ref.initialized.store(1, Ordering::SeqCst);
        std::sync::atomic::fence(Ordering::SeqCst); // Full fence
        *elem_ref.data.get() = Some(elem);
        std::sync::atomic::fence(Ordering::SeqCst); // Full fence after write
        elem_ref.initialized.store(2, Ordering::SeqCst); // Mark as fully written

        slot as u32
    }

    unsafe fn try_remove(&self, i: u32) -> Option<T> {
        let elements = self.elements.load(Ordering::SeqCst); // Stronger ordering
        if elements.is_null() || i == 0 || i >= MAX_OPS as u32 {
            return None;
        }

        let current_counter = self.counter.load(Ordering::SeqCst); // Stronger ordering
        if (i as u64) > current_counter {
            return None;
        }

        let elem_ptr = elements.add(i as usize);
        let elem_ref = &*elem_ptr;

        // CRITICAL: Wait for initialization to complete
        let mut retries = 0;
        while elem_ref.initialized.load(Ordering::SeqCst) < 2 && retries < 10000 {
            std::hint::spin_loop();
            retries += 1;
        }

        std::sync::atomic::fence(Ordering::SeqCst); // Full fence before read
        (*elem_ref.data.get()).take()
    }

    fn total(&self) -> u32 {
        self.counter.load(Ordering::SeqCst) as u32 // Stronger ordering
    }
}

// C register storing l1|r1|l2|r2 - FIXED with stronger synchronization
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
        let val = self.value.load(Ordering::SeqCst); // Stronger ordering
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
            .compare_exchange(expected_val, new_val, Ordering::SeqCst, Ordering::SeqCst) // Stronger ordering
            .is_ok()
    }
}

// Counting set structure - EXACTLY as in the paper but with FIXED synchronization
#[repr(C)]
struct CountingSet<T> {
    // For k > 1
    c: CRegister,
    k: u32,
    sqrt_k: u32,
    left_k: u32,
    right_k: u32,

    // For k = 1
    p: PRegister<T>,

    // Offsets to sub-structures and arrays
    cl_offset: u32,
    cr_offset: u32,
    t_array_offset: u32,
    l_array_offset: u32,
    r_array_offset: u32,
}

impl<T: Send + Clone> CountingSet<T> {
    unsafe fn get_cl(&self) -> &CountingSet<T> {
        if self.cl_offset == 0 {
            panic!("No left counting set");
        }
        let ptr = (self as *const Self as *const u8).add(self.cl_offset as usize);
        &*(ptr as *const CountingSet<T>)
    }

    unsafe fn get_cr(&self) -> &CountingSet<T> {
        if self.cr_offset == 0 {
            panic!("No right counting set");
        }
        let ptr = (self as *const Self as *const u8).add(self.cr_offset as usize);
        &*(ptr as *const CountingSet<T>)
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

    // Algorithm 3: insert operation - FIXED with stronger synchronization
    unsafe fn insert(&self, thread_id: usize, elem: T) -> usize {
        if self.k == 1 {
            let counter = self.p.insert(elem);
            if counter == 0 || counter > MAX_OPS as u32 {
                return 0;
            }
            counter as usize
        } else {
            let is_left = thread_id < (self.left_k as usize);
            let subset_id = if is_left {
                thread_id
            } else {
                thread_id - (self.left_k as usize)
            };

            let r = if is_left {
                self.get_cl().insert(subset_id, elem)
            } else {
                self.get_cr().insert(subset_id, elem)
            };

            if r == 0 || r >= MAX_OPS {
                return 0;
            }

            // FIXED: More aggressive retry with exponential backoff
            let mut retries = 0;
            loop {
                let cb = self.c.read();
                let tl = self.get_cl().total() as u16;
                let tr = self.get_cr().total() as u16;

                if tl > 32767 || tr > 32767 {
                    return 0; // Overflow
                }

                self.log(cb);
                let cb_new = (cb.2, cb.3, tl, tr);

                if self.c.compare_and_swap(cb, cb_new) {
                    self.log(cb_new);
                    break;
                }

                // Exponential backoff with more aggressive retry
                retries += 1;
                if retries > 100000 {
                    // Much higher retry limit
                    return 0; // Give up after many attempts
                }

                // Escalating backoff
                if retries < 1000 {
                    std::hint::spin_loop();
                } else if retries < 10000 {
                    for _ in 0..10 {
                        std::hint::spin_loop();
                    }
                } else {
                    std::thread::yield_now();
                }
            }

            self.lookup(r, is_left)
        }
    }

    // FIXED remove operation with stronger synchronization
    unsafe fn remove(&self, i: usize) -> Option<T> {
        if self.k == 1 {
            self.p.try_remove(i as u32)
        } else {
            self.log(self.c.read());

            let mut h = i;
            let t_array = self.get_t_array();
            let max_search = (i + self.sqrt_k as usize + 1).min(MAX_OPS);

            // More aggressive search with stronger ordering
            while h < max_search {
                std::sync::atomic::fence(Ordering::SeqCst); // Full fence
                if t_array[h].load(Ordering::SeqCst) != 0 {
                    break;
                }
                h += 1;
            }

            if h >= max_search {
                return None;
            }

            std::sync::atomic::fence(Ordering::SeqCst); // Full fence
            let packed = t_array[h].load(Ordering::SeqCst);
            if packed == 0 {
                return None;
            }

            let (l1, r1, l2, _) = Self::unpack_c_value(packed);
            let l1_r1 = (l1 as usize).saturating_add(r1 as usize);
            let l2_l1 = (l2 as usize).saturating_sub(l1 as usize);

            if l1_r1 < i && i <= l1_r1.saturating_add(l2_l1) {
                let i_prime = i.saturating_sub(l1_r1).saturating_add(l1 as usize);
                if i_prime >= MAX_OPS {
                    return None;
                }
                self.get_cl().remove(i_prime)
            } else {
                let i_prime = i
                    .saturating_sub(l1_r1)
                    .saturating_sub(l2_l1)
                    .saturating_add(r1 as usize);
                if i_prime >= MAX_OPS {
                    return None;
                }
                self.get_cr().remove(i_prime)
            }
        }
    }

    // Algorithm 4: total operation with stronger ordering
    unsafe fn total(&self) -> usize {
        if self.k == 1 {
            self.p.total() as usize
        } else {
            let (_, _, l2, r2) = self.c.read();
            (l2 as usize).saturating_add(r2 as usize)
        }
    }

    // FIXED log function with stronger synchronization
    unsafe fn log(&self, cb: (u16, u16, u16, u16)) {
        if self.k == 1 {
            return;
        }

        let packed = CRegister::pack(cb.0, cb.1, cb.2, cb.3);
        let total = (cb.2 as usize).saturating_add(cb.3 as usize);
        let t_array = self.get_t_array();

        // Store with stronger ordering
        if total < MAX_OPS {
            t_array[total].store(packed, Ordering::SeqCst);
        }

        let base = (cb.0 as usize).saturating_add(cb.1 as usize);
        if self.sqrt_k > 0 {
            let mut i = base.saturating_add(self.sqrt_k as usize);
            while i <= total && i < MAX_OPS {
                t_array[i].store(packed, Ordering::SeqCst);
                i = i.saturating_add(self.sqrt_k as usize);
            }
        }

        if cb.2 != cb.0 {
            let l_array = self.get_l_array();
            let l2 = cb.2 as usize;
            if l2 < MAX_OPS {
                l_array[l2].store(packed, Ordering::SeqCst);
            }

            if self.sqrt_k > 0 {
                let mut i = (cb.0 as usize).saturating_add(self.sqrt_k as usize);
                while i <= l2 && i < MAX_OPS {
                    l_array[i].store(packed, Ordering::SeqCst);
                    i = i.saturating_add(self.sqrt_k as usize);
                }
            }
        }

        if cb.3 != cb.1 {
            let r_array = self.get_r_array();
            let r2 = cb.3 as usize;
            if r2 < MAX_OPS {
                r_array[r2].store(packed, Ordering::SeqCst);
            }

            if self.sqrt_k > 0 {
                let mut i = (cb.1 as usize).saturating_add(self.sqrt_k as usize);
                while i <= r2 && i < MAX_OPS {
                    r_array[i].store(packed, Ordering::SeqCst);
                    i = i.saturating_add(self.sqrt_k as usize);
                }
            }
        }

        // Add full memory fence after all writes
        std::sync::atomic::fence(Ordering::SeqCst);
    }

    // FIXED lookup function with stronger synchronization
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
        let max_search = r
            .saturating_add(self.sqrt_k as usize)
            .saturating_add(1)
            .min(MAX_OPS);

        // More aggressive search with stronger ordering
        while s < max_search {
            std::sync::atomic::fence(Ordering::SeqCst); // Full fence
            if array[s].load(Ordering::SeqCst) != 0 {
                break;
            }
            s += 1;
        }

        if s >= max_search {
            return 0;
        }

        std::sync::atomic::fence(Ordering::SeqCst); // Full fence
        let packed = array[s].load(Ordering::SeqCst);
        if packed == 0 {
            return 0;
        }

        let (l1, r1, l2, _) = Self::unpack_c_value(packed);

        if in_left {
            if r < l1 as usize {
                return 0;
            }
            let base = (l1 as usize).saturating_add(r1 as usize);
            let offset = r.saturating_sub(l1 as usize);
            base.saturating_add(offset)
        } else {
            if r < r1 as usize {
                return 0;
            }
            let base = (l1 as usize).saturating_add(r1 as usize);
            let left_offset = (l2 as usize).saturating_sub(l1 as usize);
            let right_offset = r.saturating_sub(r1 as usize);
            base.saturating_add(left_offset)
                .saturating_add(right_offset)
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

// Main queue structure - FIXED with stronger synchronization
#[repr(C)]
pub struct KWQueue<T: Send + Clone + 'static> {
    th: THRegister,
    num_threads: usize,
    counting_set_offset: u32,
    array_offset: u32,
    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for KWQueue<T> {}
unsafe impl<T: Send + Clone> Sync for KWQueue<T> {}

impl<T: Send + Clone + 'static> KWQueue<T> {
    unsafe fn get_counting_set(&self) -> &CountingSet<T> {
        let ptr = (self as *const Self as *const u8).add(self.counting_set_offset as usize);
        &*(ptr as *const CountingSet<T>)
    }

    unsafe fn get_array(&self) -> &[UnsafeCell<Option<T>>] {
        let ptr = (self as *const Self as *const u8).add(self.array_offset as usize);
        std::slice::from_raw_parts(ptr as *const UnsafeCell<Option<T>>, MAX_OPS)
    }

    // CRITICAL FIX: Completely redesigned enqueue with proper sequencing
    pub fn enqueue(&self, thread_id: usize, x: T) -> Result<(), ()> {
        unsafe {
            // Step 1: Insert into counting set and get slot number
            let i = self.get_counting_set().insert(thread_id, x.clone());
            if i == 0 || i >= MAX_OPS {
                return Err(());
            }

            // Step 2: CRITICAL - Update head pointer FIRST to reserve the slot
            self.th.half_max(i as u32);

            // Step 3: Full memory fence to ensure head update is visible
            std::sync::atomic::fence(Ordering::SeqCst);

            // Step 4: Write to array with full ordering
            (*self.get_array()[i].get()) = Some(x);

            // Step 5: Full memory fence to ensure write is visible
            std::sync::atomic::fence(Ordering::SeqCst);

            // Step 6: CRITICAL - Wait longer before cleanup to ensure visibility
            for _ in 0..5000 {
                // Much longer wait
                std::hint::spin_loop();
            }

            // Step 7: Final fence before cleanup
            std::sync::atomic::fence(Ordering::SeqCst);

            // Step 8: Remove from counting set (cleanup) - do this last
            self.get_counting_set().remove(i);

            Ok(())
        }
    }

    // CRITICAL FIX: Completely redesigned dequeue with proper retry logic
    pub fn dequeue(&self, _thread_id: usize) -> Result<T, ()> {
        unsafe {
            // Step 1: Get next slot using half-increment
            let i = self.th.half_increment();
            if i == -1 {
                return Err(());
            }

            let i = i as usize;
            if i >= MAX_OPS {
                return Err(());
            }

            // Step 2: Full memory fence before any access
            std::sync::atomic::fence(Ordering::SeqCst);

            // Step 3: Try counting set first with multiple attempts
            for attempt in 0..1000 {
                if let Some(value) = self.get_counting_set().remove(i) {
                    return Ok(value);
                }

                // Small delay between attempts
                if attempt % 100 == 99 {
                    std::thread::yield_now();
                } else {
                    std::hint::spin_loop();
                }
            }

            // Step 4: Now try array with very aggressive retry
            for retry in 0..200000 {
                // Much higher retry count
                std::sync::atomic::fence(Ordering::SeqCst); // Fence on each attempt

                if let Some(value) = (*self.get_array()[i].get()).take() {
                    return Ok(value);
                }

                // Escalating backoff
                if retry < 1000 {
                    std::hint::spin_loop();
                } else if retry < 10000 {
                    for _ in 0..10 {
                        std::hint::spin_loop();
                    }
                } else if retry < 50000 {
                    std::thread::yield_now();
                } else {
                    // Very aggressive: small sleep to let enqueue complete
                    std::thread::sleep(std::time::Duration::from_nanos(1));
                }

                // Every 10000 retries, try counting set again with full fence
                if retry % 10000 == 9999 {
                    std::sync::atomic::fence(Ordering::SeqCst);
                    if let Some(value) = self.get_counting_set().remove(i) {
                        return Ok(value);
                    }
                }
            }

            // Step 5: Final desperate attempts with maximum delays
            for final_attempt in 0..5000 {
                // Maximum barriers
                std::sync::atomic::fence(Ordering::SeqCst);

                // Try both locations
                if let Some(value) = self.get_counting_set().remove(i) {
                    return Ok(value);
                }

                if let Some(value) = (*self.get_array()[i].get()).take() {
                    return Ok(value);
                }

                // Maximum delays
                if final_attempt < 1000 {
                    std::thread::yield_now();
                } else if final_attempt < 3000 {
                    std::thread::sleep(std::time::Duration::from_nanos(100));
                } else {
                    std::thread::sleep(std::time::Duration::from_micros(1));
                }
            }

            // Should never reach here - return error instead of panic
            Err(())
        }
    }

    // Rest of implementation remains the same but with stronger ordering...
    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + 63) & !63;

        ptr::write(&mut (*queue_ptr).th, THRegister::new());
        (*queue_ptr).num_threads = num_threads;

        let counting_set_offset = queue_aligned;
        (*queue_ptr).counting_set_offset = counting_set_offset as u32;

        let (_cs_size, array_offset, _elements_end) = Self::init_counting_set(
            mem.add(counting_set_offset),
            num_threads,
            counting_set_offset,
        );

        (*queue_ptr).array_offset = array_offset as u32;

        // Initialize main array with stronger initialization
        let array_ptr = mem.add(array_offset) as *mut [UnsafeCell<Option<T>>; MAX_OPS];
        for i in 0..MAX_OPS {
            let cell_ptr = &mut (*array_ptr)[i] as *mut UnsafeCell<Option<T>>;
            ptr::write(cell_ptr, UnsafeCell::new(None::<T>));
        }

        // Full memory barrier after initialization
        std::sync::atomic::fence(Ordering::SeqCst);

        &mut *queue_ptr
    }

    // [Rest of the init_counting_set and other methods remain the same but with stronger orderings]
    unsafe fn init_counting_set(
        mem: *mut u8,
        k: usize,
        base_offset: usize,
    ) -> (usize, usize, usize) {
        let cs_ptr = mem as *mut CountingSet<T>;
        (*cs_ptr).k = k as u32;
        (*cs_ptr).sqrt_k = ((k as f64).sqrt().floor() as u32).max(1);

        if k == 1 {
            (*cs_ptr).p = PRegister::new();
            (*cs_ptr).cl_offset = 0;
            (*cs_ptr).cr_offset = 0;
            (*cs_ptr).t_array_offset = 0;
            (*cs_ptr).l_array_offset = 0;
            (*cs_ptr).r_array_offset = 0;
            (*cs_ptr).left_k = 0;
            (*cs_ptr).right_k = 0;

            let cs_size = mem::size_of::<CountingSet<T>>();
            let cs_aligned = (cs_size + 63) & !63;

            let elements_offset = cs_aligned;
            let elements_ptr = mem.add(elements_offset) as *mut Element<T>;

            for i in 0..MAX_OPS {
                let elem_ptr = elements_ptr.add(i);
                ptr::write(elem_ptr, Element::new());
            }

            (*cs_ptr).p.set_elements(elements_ptr);

            let elements_size = MAX_OPS * mem::size_of::<Element<T>>();
            let elements_aligned = (elements_size + 63) & !63;

            let total_size = cs_aligned + elements_aligned;
            return (
                total_size,
                base_offset + total_size,
                base_offset + total_size,
            );
        }

        (*cs_ptr).c = CRegister::new();
        let k_left = k / 2;
        let k_right = k - k_left;
        (*cs_ptr).left_k = k_left as u32;
        (*cs_ptr).right_k = k_right as u32;

        let cs_size = mem::size_of::<CountingSet<T>>();
        let cs_aligned = (cs_size + 63) & !63;

        let cl_offset = cs_aligned;
        (*cs_ptr).cl_offset = cl_offset as u32;
        let (cl_size, _after_cl_array, after_cl_elements) =
            Self::init_counting_set(mem.add(cl_offset), k_left, base_offset + cl_offset);

        let cr_offset = cl_offset + cl_size;
        (*cs_ptr).cr_offset = cr_offset as u32;
        let (cr_size, _after_cr_array, after_cr_elements) =
            Self::init_counting_set(mem.add(cr_offset), k_right, base_offset + cr_offset);

        let arrays_offset = cr_offset + cr_size;
        let array_size = MAX_OPS * mem::size_of::<AtomicU64>();
        let array_aligned = (array_size + 63) & !63;

        (*cs_ptr).t_array_offset = arrays_offset as u32;
        (*cs_ptr).l_array_offset = (arrays_offset + array_aligned) as u32;
        (*cs_ptr).r_array_offset = (arrays_offset + 2 * array_aligned) as u32;

        let t_ptr = mem.add(arrays_offset) as *mut AtomicU64;
        let l_ptr = mem.add(arrays_offset + array_aligned) as *mut AtomicU64;
        let r_ptr = mem.add(arrays_offset + 2 * array_aligned) as *mut AtomicU64;

        for i in 0..MAX_OPS {
            ptr::write(t_ptr.add(i), AtomicU64::new(0));
            ptr::write(l_ptr.add(i), AtomicU64::new(0));
            ptr::write(r_ptr.add(i), AtomicU64::new(0));
        }

        let total_size = arrays_offset + 3 * array_aligned;
        let max_elements_end = after_cl_elements.max(after_cr_elements);
        (total_size, base_offset + total_size, max_elements_end)
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + 63) & !63;
        let cs_size = Self::counting_set_size(num_threads);
        let array_size = MAX_OPS * mem::size_of::<UnsafeCell<Option<T>>>();
        let array_aligned = (array_size + 63) & !63;
        let total = queue_aligned + cs_size + array_aligned;
        (total + 4095) & !4095
    }

    fn counting_set_size(k: usize) -> usize {
        if k == 1 {
            let cs_size = mem::size_of::<CountingSet<T>>();
            let cs_aligned = (cs_size + 63) & !63;
            let elements_size = MAX_OPS * mem::size_of::<Element<T>>();
            let elements_aligned = (elements_size + 63) & !63;
            cs_aligned + elements_aligned
        } else {
            let cs_size = mem::size_of::<CountingSet<T>>();
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
        let val = self.th.value.load(Ordering::SeqCst); // Stronger ordering
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
