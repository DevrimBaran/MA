// queues/src/mpsc/dqueue.rs
use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::MpscQueue;

const DQUEUE_LOG_ENABLED: bool = false;

macro_rules! dqueue_log {
    ($($arg:tt)*) => {
        if DQUEUE_LOG_ENABLED {
            println!("[DQueue] {}", format!($($arg)*));
        }
    };
}

// Needs to be sized correctly since segment recycling is not shown in paper
pub const L_LOCAL_BUFFER_CAPACITY: usize = 65536;  // Local buffer size for each producer
pub const N_SEGMENT_CAPACITY: usize = 262144;     // Number of cells per segment (should be power of 2)

#[repr(C, align(64))]  // Ensure cache line alignment
struct Segment<T> {
    id: u64,                                  // Segment ID
    cells: *mut UnsafeCell<MaybeUninit<Option<T>>>, // Array of cells
    next: AtomicPtr<Segment<T>>,             // Link to next segment
}

#[repr(C)]
struct Request<T> {
    val: MaybeUninit<T>,
    cid: u64,
}

// Producer's local state - one per producer thread
#[repr(C, align(64))] // Cache line alignment
pub struct Producer<T> {
    local_buffer: UnsafeCell<[MaybeUninit<Request<T>>; L_LOCAL_BUFFER_CAPACITY]>,
    local_head: AtomicUsize,
    local_tail: AtomicUsize,
    pseg: AtomicPtr<Segment<T>>, // Producer's cached segment pointer
}

impl<T> Producer<T> {
    unsafe fn init_in_place(producer_ptr: *mut Self, initial_segment: *mut Segment<T>) {
        // Initialize local buffer with uninit requests
        let buffer_ptr = (*producer_ptr).local_buffer.get() as *mut MaybeUninit<Request<T>>;
        for i in 0..L_LOCAL_BUFFER_CAPACITY {
            ptr::write(buffer_ptr.add(i), MaybeUninit::uninit());
        }
        
        // Initialize atomic fields
        ptr::addr_of_mut!((*producer_ptr).local_head).write(AtomicUsize::new(0));
        ptr::addr_of_mut!((*producer_ptr).local_tail).write(AtomicUsize::new(0));
        ptr::addr_of_mut!((*producer_ptr).pseg).write(AtomicPtr::new(initial_segment));
    }

    // Helper functions for the ring buffer operations
    #[inline(always)]
    fn local_wrap(i: usize) -> usize { i % L_LOCAL_BUFFER_CAPACITY }
    
    #[inline(always)]
    fn local_next(i: usize) -> usize { Self::local_wrap(i + 1) }
}

// Main queue structure
#[repr(C, align(64))]
pub struct DQueue<T: Send + Clone + 'static> {
    q_head: AtomicU64,                      // Consumer's head index
    q_tail: AtomicU64,                      // Global tail index
    qseg: AtomicPtr<Segment<T>>,            // Queue head segment
    
    producers_array: *mut Producer<T>,      // Array of producer structures
    num_producers: usize,                   // Number of producers
    
    segment_pool: *mut Segment<T>,          // Pool of pre-allocated segments
    segment_cells_pool: *mut UnsafeCell<MaybeUninit<Option<T>>>, // Pool of cells
    segment_pool_capacity: usize,           // Number of segments in the pool
    next_free_segment_idx: AtomicUsize,     // Next available segment in pool
    
    cseg: AtomicPtr<Segment<T>>,            // Consumer's cached segment pointer
}

unsafe impl<T: Send + Clone + 'static> Send for DQueue<T> {}
unsafe impl<T: Send + Clone + 'static> Sync for DQueue<T> {}

impl<T: Send + Clone + 'static> DQueue<T> {
    // Calculate shared memory size required for the queue
    pub fn shared_size(num_producers: usize, segment_pool_capacity: usize) -> usize {
        let self_align = mem::align_of::<Self>();
        let producers_align = mem::align_of::<Producer<T>>();
        let segment_meta_align = mem::align_of::<Segment<T>>();
        let cell_align = mem::align_of::<UnsafeCell<MaybeUninit<Option<T>>>>();
        
        let mut total_size = 0;
        
        // Align and add size for main structure
        total_size = (total_size + self_align - 1) & !(self_align - 1);
        total_size += mem::size_of::<Self>();
        
        // Align and add size for producer array
        total_size = (total_size + producers_align - 1) & !(producers_align - 1);
        if num_producers > 0 {
            total_size += num_producers * mem::size_of::<Producer<T>>();
        }
        
        // Align and add size for segment pool
        total_size = (total_size + segment_meta_align - 1) & !(segment_meta_align - 1);
        total_size += segment_pool_capacity * mem::size_of::<Segment<T>>();
        
        // Align and add size for segment cells pool
        total_size = (total_size + cell_align - 1) & !(cell_align - 1);
        total_size += segment_pool_capacity * N_SEGMENT_CAPACITY * mem::size_of::<UnsafeCell<MaybeUninit<Option<T>>>>();
        
        // Final alignment to cache line
        (total_size + 63) & !63
    }

    // Initialize the queue in shared memory
    pub unsafe fn init_in_shared(
        mem_ptr: *mut u8, 
        num_producers: usize, 
        segment_pool_capacity: usize
    ) -> &'static mut Self {
        dqueue_log!("init_in_shared: ENTER num_prods={}, seg_pool_cap={}", num_producers, segment_pool_capacity);
        
        let mut current_offset = 0usize;
        
        // Helper function to align memory and advance pointer
        let align_and_advance = |co: &mut usize, size: usize, align: usize| -> *mut u8 {
            *co = (*co + align - 1) & !(align - 1);
            let ptr = mem_ptr.add(*co);
            *co += size;
            ptr
        };
        
        // Allocate main structure
        let q_ptr = align_and_advance(&mut current_offset, mem::size_of::<Self>(), mem::align_of::<Self>()) as *mut Self;
        
        // Allocate producer array
        let p_arr_ptr = if num_producers > 0 {
            align_and_advance(&mut current_offset, num_producers * mem::size_of::<Producer<T>>(), mem::align_of::<Producer<T>>()) as *mut Producer<T>
        } else {
            ptr::null_mut()
        };
        
        // Allocate segment pool
        let seg_pool_meta_ptr = align_and_advance(&mut current_offset, segment_pool_capacity * mem::size_of::<Segment<T>>(), mem::align_of::<Segment<T>>()) as *mut Segment<T>;
        
        // Allocate segment cells pool
        let seg_cells_pool_ptr = align_and_advance(&mut current_offset, segment_pool_capacity * N_SEGMENT_CAPACITY * mem::size_of::<UnsafeCell<MaybeUninit<Option<T>>>>(), mem::align_of::<UnsafeCell<MaybeUninit<Option<T>>>>()) as *mut UnsafeCell<MaybeUninit<Option<T>>>;
        
        // Initialize first segment
        let init_seg_idx_atomic = AtomicUsize::new(0);
        let init_seg_ptr = Self::alloc_segment_from_pool_raw(seg_pool_meta_ptr, seg_cells_pool_ptr, segment_pool_capacity, &init_seg_idx_atomic, 0);
        
        if init_seg_ptr.is_null() {
            panic!("DQueue: Failed to allocate initial segment.");
        }
        
        (*init_seg_ptr).next.store(ptr::null_mut(), Ordering::Relaxed);

        // Initialize producers
        if num_producers > 0 && !p_arr_ptr.is_null() {
            for i in 0..num_producers {
                Producer::init_in_place(p_arr_ptr.add(i), init_seg_ptr);
            }
        }
        
        // Initialize the queue structure
        ptr::write(q_ptr, Self {
            q_head: AtomicU64::new(0),
            q_tail: AtomicU64::new(0),
            qseg: AtomicPtr::new(init_seg_ptr),
            producers_array: p_arr_ptr,
            num_producers,
            segment_pool: seg_pool_meta_ptr,
            segment_cells_pool: seg_cells_pool_ptr,
            segment_pool_capacity,
            next_free_segment_idx: AtomicUsize::new(init_seg_idx_atomic.load(Ordering::Relaxed)),
            cseg: AtomicPtr::new(init_seg_ptr),
        });
        
        dqueue_log!("init_in_shared: EXIT. Next_free_seg_idx: {}", (*q_ptr).next_free_segment_idx.load(Ordering::Relaxed));
        
        &mut *q_ptr
    }
    
    // Allocate a new segment from the pool
    unsafe fn alloc_segment_from_pool_raw(
        seg_pool_meta_start: *mut Segment<T>,
        seg_cells_pool_start: *mut UnsafeCell<MaybeUninit<Option<T>>>,
        seg_pool_cap: usize,
        next_free_idx_atomic: &AtomicUsize,
        seg_id: u64,
    ) -> *mut Segment<T> {
        let idx = next_free_idx_atomic.fetch_add(1, Ordering::Relaxed);
        
        if idx >= seg_pool_cap {
            // Pool exhausted, roll back
            next_free_idx_atomic.fetch_sub(1, Ordering::Relaxed);
            dqueue_log!("alloc_segment_from_pool_raw: POOL EXHAUSTED. seg_id={}, idx={}, cap={}", seg_id, idx, seg_pool_cap);
            return ptr::null_mut();
        }
        
        // Get memory for the new segment
        let seg_meta_ptr = seg_pool_meta_start.add(idx);
        let cells_start_ptr = seg_cells_pool_start.add(idx * N_SEGMENT_CAPACITY);
        
        // Initialize segment fields
        ptr::addr_of_mut!((*seg_meta_ptr).id).write(seg_id);
        ptr::addr_of_mut!((*seg_meta_ptr).cells).write(cells_start_ptr);
        ptr::addr_of_mut!((*seg_meta_ptr).next).write(AtomicPtr::new(ptr::null_mut()));
        
        // Initialize cells to None
        for i in 0..N_SEGMENT_CAPACITY {
            ptr::write((*(cells_start_ptr.add(i))).get(), MaybeUninit::new(None));
        }
        
        dqueue_log!("alloc_segment_from_pool_raw: Allocated seg_id={} at idx={}, ptr={:p}", seg_id, idx, seg_meta_ptr);
        
        seg_meta_ptr
    }

    // Allocate a new segment
    unsafe fn new_segment(&self, id: u64) -> *mut Segment<T> {
        dqueue_log!("new_segment: ENTER for id={}", id);
        
        // Allocate from pool
        let seg_ptr = Self::alloc_segment_from_pool_raw(
            self.segment_pool,
            self.segment_cells_pool,
            self.segment_pool_capacity,
            &self.next_free_segment_idx,
            id,
        );
        
        if !seg_ptr.is_null() {
            // Initialize next pointer
            (*seg_ptr).next.store(ptr::null_mut(), Ordering::Relaxed);
            dqueue_log!("new_segment: EXIT success for id={}, ptr={:p}", id, seg_ptr);
        } else {
            dqueue_log!("new_segment: EXIT FAILED for id={}", id);
        }
        
        seg_ptr
    }

    // Find segment containing cell with given ID - optimized version
    unsafe fn find_segment(&self, sp_cache: *mut Segment<T>, target_cid: u64) -> *mut Segment<T> {
        let target_segment_id = target_cid / N_SEGMENT_CAPACITY as u64;
        
        dqueue_log!("find_segment: target_cid={}, target_seg_id={}, sp_cache={:p}", 
                   target_cid, target_segment_id, sp_cache);
        
        // Fast path: check if the cached segment is the target
        if !sp_cache.is_null() {
            let cached_id = (*sp_cache).id;
            
            if cached_id == target_segment_id {
                return sp_cache;
            }
            
            // If cached segment is past our target, restart from qseg
            if cached_id > target_segment_id {
                dqueue_log!("find_segment: cached segment id {} > target_seg_id {}, using qseg", 
                           cached_id, target_segment_id);
                let qseg = self.qseg.load(Ordering::Acquire);
                return self.find_segment(qseg, target_cid);
            }
        }
        
        // Start with the cache or qseg
        let starting_seg_ptr = if sp_cache.is_null() {
            dqueue_log!("find_segment: sp_cache is null, loading from qseg");
            self.qseg.load(Ordering::Acquire)
        } else {
            sp_cache
        };
        
        if starting_seg_ptr.is_null() {
            dqueue_log!("find_segment: current_seg_ptr is NULL. Target_cid={}. Returning NULL.", target_cid);
            return ptr::null_mut();
        }
        
        let mut current_seg_ptr = starting_seg_ptr;
        
        // Traverse segments until we find the target or go past it
        let mut loop_count = 0;
        let max_loops = self.segment_pool_capacity + 10; // Limit to prevent infinite loops
        
        while (*current_seg_ptr).id < target_segment_id {
            loop_count += 1;
            if loop_count > max_loops {
                dqueue_log!("find_segment: Possible infinite loop after {} iterations", loop_count);
                return ptr::null_mut();
            }
            
            // Check next segment
            let mut next_ptr = (*current_seg_ptr).next.load(Ordering::Acquire);
            
            // Create new segment if needed
            if next_ptr.is_null() {
                let next_expected_id = (*current_seg_ptr).id + 1;
                dqueue_log!("find_segment: seg_id {} next is null. Attempting to alloc new segment for id {}", 
                           (*current_seg_ptr).id, next_expected_id);
                
                let new_seg_ptr = self.new_segment(next_expected_id);
                
                if new_seg_ptr.is_null() {
                    dqueue_log!("find_segment: new_segment alloc FAILED for id {}. Returning null.", next_expected_id);
                    return ptr::null_mut();
                }
                
                // Link the new segment
                match (*current_seg_ptr).next.compare_exchange(
                    ptr::null_mut(),
                    new_seg_ptr,
                    Ordering::AcqRel,
                    Ordering::Acquire
                ) {
                    Ok(_) => {
                        // Successfully linked our new segment
                        next_ptr = new_seg_ptr;
                        dqueue_log!("find_segment: CAS success - linked new segment id={}, ptr={:p}", (*next_ptr).id, next_ptr);
                    }
                    Err(existing_next) => {
                        // Another thread linked a segment, use that one
                        next_ptr = existing_next;
                        dqueue_log!("find_segment: CAS failed - using existing next segment ptr={:p}", next_ptr);
                    }
                }
            }
            
            current_seg_ptr = next_ptr;
            
            if current_seg_ptr.is_null() {
                dqueue_log!("find_segment: next_ptr became null unexpectedly");
                return ptr::null_mut();
            }
        }
        
        // Return the found segment
        dqueue_log!("find_segment: EXIT found segment id={}, target_id={}", (*current_seg_ptr).id, target_segment_id);
        current_seg_ptr
    }

    unsafe fn dump_local_buffer(&self, producer_idx: usize) {
        dqueue_log!("dump_local_buffer: ENTER producer_idx={}", producer_idx);
        
        if self.num_producers == 0 || producer_idx >= self.num_producers {
            return;
        }
        
        let p_struct_ptr = self.producers_array.add(producer_idx);
        let local_head = &(*p_struct_ptr).local_head;
        let local_tail = &(*p_struct_ptr).local_tail;
        let local_buf_ptr = (*p_struct_ptr).local_buffer.get();
        let pseg_atomic = &(*p_struct_ptr).pseg;
        
        let head_idx = local_head.load(Ordering::Relaxed);
        let tail_idx = local_tail.load(Ordering::Relaxed);
        
        if head_idx == tail_idx {
            dqueue_log!("dump_local_buffer: Producer {} buffer empty", producer_idx);
            return;
        }
        
        // Optimization: Group requests by target segment to reduce lookups
        let mut current_seg: *mut Segment<T> = ptr::null_mut();
        let mut current_seg_id: u64 = u64::MAX;
        
        // Load producer's cached segment pointer
        let producer_pseg = pseg_atomic.load(Ordering::Acquire);
        
        let mut current_idx = head_idx;
        let mut dumped_count = 0;
        let max_iters = L_LOCAL_BUFFER_CAPACITY + 5; // Prevent infinite loops
        let mut iter_count = 0;
        
        while current_idx != tail_idx {
            iter_count += 1;
            if iter_count > max_iters {
                dqueue_log!("dump_local_buffer: Possible stuck loop, producer_idx={}, iter={}", producer_idx, iter_count);
                break;
            }
            
            // Get the request
            let req_mu_ptr = (local_buf_ptr as *const MaybeUninit<Request<T>>).add(current_idx);
            let req_ptr = (*req_mu_ptr).as_ptr();
            
            let cid = (*req_ptr).cid;
            let val_clone = (*(*req_ptr).val.as_ptr()).clone();
            
            // Calculate target segment ID
            let target_seg_id = cid / N_SEGMENT_CAPACITY as u64;
            
            // Optimize segment lookup by reusing current segment when possible
            if current_seg.is_null() || current_seg_id != target_seg_id {
                // Need to lookup the segment
                current_seg = self.find_segment(producer_pseg, cid);
                
                if current_seg.is_null() {
                    dqueue_log!("dump_local_buffer: find_segment for cid {} returned null. ABORTING DUMP.", cid);
                    break;
                }
                
                current_seg_id = (*current_seg).id;
                
                // Update producer's segment cache
                if producer_pseg != current_seg {
                    pseg_atomic.store(current_seg, Ordering::Release);
                }
            }
            
            // Write value to cell
            let cell_idx = (cid % N_SEGMENT_CAPACITY as u64) as usize;
            let cell_ptr = (*current_seg).cells.add(cell_idx);
            ptr::write((*cell_ptr).get(), MaybeUninit::new(Some(val_clone)));
            
            // Increment local head
            current_idx = Producer::<T>::local_next(current_idx);
            local_head.store(current_idx, Ordering::Relaxed);
            dumped_count += 1;
        }
        
        dqueue_log!("dump_local_buffer: EXIT producer_idx={} dumped {} items", producer_idx, dumped_count);
    }

    unsafe fn help_enqueue(&self) {
        dqueue_log!("help_enqueue: Consumer ENTER");
    
        if self.num_producers == 0 { return; }
    
        for i in 0..self.num_producers {
            let p_struct_ptr  = self.producers_array.add(i);
            let p_local_head  = &(*p_struct_ptr).local_head;          // <-- new
            let p_local_tail  = &(*p_struct_ptr).local_tail;
            let p_local_buf   = (*p_struct_ptr).local_buffer.get();
            let producer_pseg = (*p_struct_ptr).pseg.load(Ordering::Acquire);
    
            let mut h = p_local_head.load(Ordering::Acquire);
            let t      = p_local_tail.load(Ordering::Acquire);
            if h == t { continue; }
    
            let mut cur_seg : *mut Segment<T> = ptr::null_mut();
            let mut cur_seg_id : u64 = u64::MAX;
    
            let mut iter = 0;
            while h != t {
                if iter > L_LOCAL_BUFFER_CAPACITY + 5 {
                    dqueue_log!("help_enqueue: producer {} unusual long buffer", i);
                    break;
                }
                iter += 1;
    
                let req_ptr = (*(p_local_buf as *const MaybeUninit<Request<T>>).add(h)).as_ptr();
                let cid      = (*req_ptr).cid;
                let val_clone= (*(*req_ptr).val.as_ptr()).clone();
    
                let target_seg_id = cid / N_SEGMENT_CAPACITY as u64;
                if cur_seg.is_null() || cur_seg_id != target_seg_id {
                    cur_seg = self.find_segment(producer_pseg, cid);
                    if cur_seg.is_null() { break; }
                    cur_seg_id = (*cur_seg).id;
                }
    
                let cell_idx = (cid % N_SEGMENT_CAPACITY as u64) as usize;
                let cell_ptr = (*cur_seg).cells.add(cell_idx);
                let opt_ptr  = (*(*cell_ptr).get()).as_mut_ptr();
                if (*opt_ptr).is_none() {
                    ptr::write(opt_ptr, Some(val_clone));
                }
    
                h = Producer::<T>::local_next(h);
                p_local_head.store(h, Ordering::Relaxed);            // <-- **advance**
            }
        }
    
        dqueue_log!("help_enqueue: Consumer EXIT");
    }
    
    // Enqueue an item into the queue
    pub fn enqueue(&self, producer_idx: usize, item: T) -> Result<(), ()> {
        if self.num_producers == 0 || producer_idx >= self.num_producers {
            return Err(());
        }
        
        unsafe {
            let p_struct = self.producers_array.add(producer_idx);
            let local_head = &(*p_struct).local_head;
            let local_tail = &(*p_struct).local_tail;
            let local_buf_ptr = (*p_struct).local_buffer.get();
            
            // Check if buffer is full
            let current_tail = local_tail.load(Ordering::Relaxed);
            if Producer::<T>::local_next(current_tail) == local_head.load(Ordering::Acquire) {
                dqueue_log!("enqueue: producer {} local buffer full. Dumping...", producer_idx);
                self.dump_local_buffer(producer_idx);
                
                // If buffer is still full after dumping, we can't enqueue
                if Producer::<T>::local_next(local_tail.load(Ordering::Relaxed)) == local_head.load(Ordering::Acquire) {
                    return Err(());
                }
            }
            
            // Get a new cell ID from the global tail
            let cid = self.q_tail.fetch_add(1, Ordering::AcqRel);
            
            // Store in local buffer
            let tail_idx = local_tail.load(Ordering::Relaxed);
            let req_slot_ptr = (local_buf_ptr as *mut MaybeUninit<Request<T>>).add(tail_idx);
            
            req_slot_ptr.write(MaybeUninit::new(Request {
                val: MaybeUninit::new(item),
                cid,
            }));
            
            // Update the tail pointer
            local_tail.store(Producer::<T>::local_next(tail_idx), Ordering::Release);
            
            dqueue_log!("enqueue: producer={} stored cid={} in local_idx={}", producer_idx, cid, tail_idx);
        }
        
        Ok(())
    }

    // Dequeue an item from the queue
    pub fn dequeue(&self) -> Option<T> {
        dqueue_log!(
            "dequeue: Consumer ENTER. q_head={}, q_tail={}",
            self.q_head.load(Ordering::Relaxed),
            self.q_tail.load(Ordering::Relaxed)
        );

        unsafe {
            let head_val = self.q_head.load(Ordering::Acquire);
            let seg_hint = self.cseg.load(Ordering::Acquire);
            let seg = self.find_segment(seg_hint, head_val);
            if seg.is_null() {
                dqueue_log!("dequeue: find_segment({}) returned null", head_val);
                return None;
            }
            if seg != seg_hint {
                self.cseg.store(seg, Ordering::Release);
            }

            let cell_idx = (head_val % N_SEGMENT_CAPACITY as u64) as usize;
            let cell_ptr = (*seg).cells.add(cell_idx);

            // ---------- FIX 1 --------------------------------------------------------
            let item_opt: Option<T> = {
                let opt_ref = (&*(*cell_ptr).get()).assume_init_ref();
                opt_ref.clone()
            };
            // -------------------------------------------------------------------------

            match item_opt {
                Some(item) => {
                    // Fast path
                    ptr::write((*(*cell_ptr).get()).as_mut_ptr(), None);
                    self.q_head.store(head_val + 1, Ordering::Release);
                    dqueue_log!("dequeue: SUCCESS head={}", head_val);
                    return Some(item);
                }
                None => {
                    let tail_now = self.q_tail.load(Ordering::Acquire);
                    if head_val == tail_now {
                        dqueue_log!("dequeue: queue empty (head==tail)");
                        return None;
                    }
                    dqueue_log!("dequeue: empty cell, helping producers");
                    self.help_enqueue();

                    // ---------- FIX 2 -------------------------------------------------
                    let item_opt_after: Option<T> = {
                        let opt_ref = (&*(*cell_ptr).get()).assume_init_ref();
                        opt_ref.clone()
                    };
                    // ------------------------------------------------------------------

                    if let Some(item) = item_opt_after {
                        ptr::write((*(*cell_ptr).get()).as_mut_ptr(), None);
                        self.q_head.store(head_val + 1, Ordering::Release);
                        dqueue_log!("dequeue: SUCCESS after help head={}", head_val);
                        return Some(item);
                    }
                    dqueue_log!("dequeue: still empty after help");
                    None
                }
            }
        }
    }
}

// Implement the MpscQueue trait for DQueue
impl<T: Send + Clone + 'static> MpscQueue<T> for DQueue<T> {
    type PushError = ();
    type PopError = ();
    
    fn push(&self, _item: T) -> Result<(), Self::PushError> {
        panic!("DQueue::push on MpscQueue trait. Use DQueue::enqueue(producer_id, item) or BenchMpscQueue::bench_push.");
    }
    
    fn pop(&self) -> Result<T, Self::PopError> {
        self.dequeue().ok_or(())
    }
    
    fn is_empty(&self) -> bool {
        let head = self.q_head.load(Ordering::Acquire);
        let tail = self.q_tail.load(Ordering::Acquire);

        if head < tail {
            unsafe {
                let seg_hint = self.cseg.load(Ordering::Acquire);
                let seg = self.find_segment(seg_hint, head);
                if seg.is_null() {
                    return true;
                }
                let idx = (head % N_SEGMENT_CAPACITY as u64) as usize;
                let cell_ptr = (*seg).cells.add(idx);

                // ---------- FIX 3 -------------------------------------------------
                let item_opt: Option<T> = {
                    let opt_ref = (&*(*cell_ptr).get()).assume_init_ref();
                    opt_ref.clone()
                };
                // ------------------------------------------------------------------

                if item_opt.is_some() {
                    return false;
                }

                // maybe producers still have it buffered
                for i in 0..self.num_producers {
                    let p = self.producers_array.add(i);
                    if (*p).local_head.load(Ordering::Acquire)
                        != (*p).local_tail.load(Ordering::Acquire)
                    {
                        return false;
                    }
                }
                true
            }
        } else {
            true
        }
    }
    
    fn is_full(&self) -> bool {
        false // The queue is never considered full
    }
}