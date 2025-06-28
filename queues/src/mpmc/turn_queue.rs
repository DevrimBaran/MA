// paper in /paper/mpmc/crturnqueue-2016.pdf
use crate::MpmcQueue;
use std::mem;
use std::ptr;
use std::sync::atomic::{fence, AtomicI32, AtomicPtr, AtomicUsize, Ordering};

const NOIDX: i32 = -1;
const CACHE_LINE_SIZE: usize = 64;

// Node structure as described in Algorithm 1
#[repr(C, align(64))]
struct Node<T> {
    item: *mut T,
    enq_tid: usize,
    deq_tid: AtomicI32,
    next: AtomicPtr<Node<T>>,
    _padding: [u8; CACHE_LINE_SIZE - 32],
}

impl<T> Node<T> {
    fn new_sentinel() -> Self {
        Self {
            item: ptr::null_mut(),
            enq_tid: 0,
            deq_tid: AtomicI32::new(NOIDX),
            next: AtomicPtr::new(ptr::null_mut()),
            _padding: [0; CACHE_LINE_SIZE - 32],
        }
    }

    fn new(item: *mut T, tid: usize) -> Self {
        Self {
            item,
            enq_tid: tid,
            deq_tid: AtomicI32::new(NOIDX),
            next: AtomicPtr::new(ptr::null_mut()),
            _padding: [0; CACHE_LINE_SIZE - 32],
        }
    }
}

// Hazard pointer implementation for wait-free memory reclamation
#[repr(C)]
struct HazardPointers<T> {
    hp_list: *mut AtomicPtr<Node<T>>,
    num_threads: usize,
    max_hps_per_thread: usize,
}

impl<T> HazardPointers<T> {
    const HP_HEAD: usize = 0;
    const HP_TAIL: usize = 1;
    const HP_NEXT: usize = 2;
    const HP_DEQ: usize = 3;
    const MAX_HPS: usize = 4;

    unsafe fn new(base_ptr: *mut u8, offset: usize, num_threads: usize) -> Self {
        let hp_ptr = base_ptr.add(offset) as *mut AtomicPtr<Node<T>>;
        let total_hps = num_threads * Self::MAX_HPS;

        // Initialize all HPs to null
        for i in 0..total_hps {
            ptr::write(hp_ptr.add(i), AtomicPtr::new(ptr::null_mut()));
        }

        Self {
            hp_list: hp_ptr,
            num_threads,
            max_hps_per_thread: Self::MAX_HPS,
        }
    }

    unsafe fn protect_ptr(
        &self,
        thread_id: usize,
        hp_index: usize,
        ptr: *mut Node<T>,
    ) -> *mut Node<T> {
        let idx = thread_id * self.max_hps_per_thread + hp_index;
        (*self.hp_list.add(idx)).store(ptr, Ordering::Release);
        fence(Ordering::SeqCst);
        ptr
    }

    unsafe fn clear(&self, thread_id: usize) {
        for i in 0..self.max_hps_per_thread {
            let idx = thread_id * self.max_hps_per_thread + i;
            (*self.hp_list.add(idx)).store(ptr::null_mut(), Ordering::Release);
        }
    }

    fn size() -> usize {
        mem::size_of::<AtomicPtr<Node<T>>>() * Self::MAX_HPS
    }
}

// Main Turn Queue structure
#[repr(C)]
pub struct TurnQueue<T: Send + Clone + 'static> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    enqueuers: *mut AtomicPtr<Node<T>>,
    deqself: *mut AtomicPtr<Node<T>>,
    deqhelp: *mut AtomicPtr<Node<T>>,
    hazard_pointers: HazardPointers<T>,
    num_threads: usize,

    // Memory management
    node_pool: *mut Node<T>,
    item_pool: *mut T,
    pool_size: usize,
    next_node: AtomicUsize,
    next_item: AtomicUsize,

    base_ptr: *mut u8,
    total_size: usize,
    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send + Clone> Send for TurnQueue<T> {}
unsafe impl<T: Send + Clone> Sync for TurnQueue<T> {}

impl<T: Send + Clone + 'static> TurnQueue<T> {
    // Algorithm 2: Turn enqueue()
    pub fn enqueue(&self, thread_id: usize, item: T) -> Result<(), ()> {
        unsafe {
            if thread_id >= self.num_threads {
                return Err(());
            }

            // Allocate space for item and node
            let item_idx = self.next_item.fetch_add(1, Ordering::AcqRel);
            if item_idx >= self.pool_size {
                return Err(()); // Pool exhausted
            }

            let item_ptr = self.item_pool.add(item_idx);
            ptr::write(item_ptr, item);

            let node_idx = self.next_node.fetch_add(1, Ordering::AcqRel);
            if node_idx >= self.pool_size {
                return Err(()); // Pool exhausted
            }

            let my_node = self.node_pool.add(node_idx);
            ptr::write(my_node, Node::new(item_ptr, thread_id));

            // Line 4: Store node in enqueuers array
            (*self.enqueuers.add(thread_id)).store(my_node, Ordering::Release);

            // Lines 5-25: Main enqueue loop
            for _ in 0..self.num_threads {
                // Line 6-8: Check if our request was completed
                if (*self.enqueuers.add(thread_id))
                    .load(Ordering::Acquire)
                    .is_null()
                {
                    self.hazard_pointers.clear(thread_id);
                    return Ok(());
                }

                // Line 10-11: Protect and validate tail
                let ltail = self.hazard_pointers.protect_ptr(
                    thread_id,
                    HazardPointers::<T>::HP_TAIL,
                    self.tail.load(Ordering::Acquire),
                );

                if ltail != self.tail.load(Ordering::Acquire) {
                    continue;
                }

                // Lines 12-15: Clear completed request from enqueuers
                if (*self.enqueuers.add((*ltail).enq_tid)).load(Ordering::Acquire) == ltail {
                    (*self.enqueuers.add((*ltail).enq_tid))
                        .compare_exchange(
                            ltail,
                            ptr::null_mut(),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .ok();
                }

                // Lines 16-22: Find next node to help
                for j in 1..=self.num_threads {
                    let idx = (j + (*ltail).enq_tid) % self.num_threads;
                    let node_help = (*self.enqueuers.add(idx)).load(Ordering::Acquire);

                    if !node_help.is_null() {
                        // Line 20: Try to append node
                        let null_node = ptr::null_mut();
                        if (*ltail)
                            .next
                            .compare_exchange(
                                null_node,
                                node_help,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            break;
                        } else {
                            break;
                        }
                    }
                }

                // Lines 23-24: Advance tail if needed
                let lnext = (*ltail).next.load(Ordering::Acquire);
                if !lnext.is_null() {
                    self.tail
                        .compare_exchange(ltail, lnext, Ordering::AcqRel, Ordering::Acquire)
                        .ok();
                }
            }

            // Line 26: Clear our request
            (*self.enqueuers.add(thread_id)).store(ptr::null_mut(), Ordering::Release);
            self.hazard_pointers.clear(thread_id);
            Ok(())
        }
    }

    // Algorithm 3: Turn dequeue()
    pub fn dequeue(&self, thread_id: usize) -> Result<T, ()> {
        unsafe {
            if thread_id >= self.num_threads {
                return Err(());
            }

            // Lines 3-5: Setup request
            let pr_req = (*self.deqself.add(thread_id)).load(Ordering::Acquire);
            let my_req = (*self.deqhelp.add(thread_id)).load(Ordering::Acquire);
            (*self.deqself.add(thread_id)).store(my_req, Ordering::Release);

            // Lines 6-23: Main dequeue loop
            for _ in 0..self.num_threads {
                // Line 7: Check if request completed
                if (*self.deqhelp.add(thread_id)).load(Ordering::Acquire) != my_req {
                    break;
                }

                // Lines 8-9: Protect and validate head
                let lhead = self.hazard_pointers.protect_ptr(
                    thread_id,
                    HazardPointers::<T>::HP_HEAD,
                    self.head.load(Ordering::Acquire),
                );

                if lhead != self.head.load(Ordering::Acquire) {
                    continue;
                }

                // Lines 10-19: Check if queue is empty
                if lhead == self.tail.load(Ordering::Acquire) {
                    (*self.deqself.add(thread_id)).store(pr_req, Ordering::Release);
                    self.give_up(thread_id, my_req);

                    if (*self.deqhelp.add(thread_id)).load(Ordering::Acquire) != my_req {
                        (*self.deqself.add(thread_id)).store(my_req, Ordering::Relaxed);
                        break;
                    }

                    self.hazard_pointers.clear(thread_id);
                    return Err(());
                }

                // Lines 20-22: Process non-empty queue
                let lnext = self.hazard_pointers.protect_ptr(
                    thread_id,
                    HazardPointers::<T>::HP_NEXT,
                    (*lhead).next.load(Ordering::Acquire),
                );

                if lhead != self.head.load(Ordering::Acquire) {
                    continue;
                }

                if !lnext.is_null() {
                    let deq_tid = self.search_next(lhead, lnext);
                    if deq_tid != NOIDX {
                        self.cas_deq_and_head(lhead, lnext, thread_id);
                    }
                }
            }

            // Lines 24-31: Return result - FIXED LOGIC
            let my_node = (*self.deqhelp.add(thread_id)).load(Ordering::Acquire);

            // Check if we got a valid node assigned to us
            if my_node.is_null() || my_node == my_req {
                // No node was assigned or still pointing to original request
                self.hazard_pointers.clear(thread_id);
                return Err(());
            }

            // Help advance head if needed (lines 25-28 of paper)
            let lhead = self.hazard_pointers.protect_ptr(
                thread_id,
                HazardPointers::<T>::HP_HEAD,
                self.head.load(Ordering::Acquire),
            );

            if lhead == self.head.load(Ordering::Acquire)
                && my_node == (*lhead).next.load(Ordering::Acquire)
            {
                self.head
                    .compare_exchange(lhead, my_node, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
            }

            self.hazard_pointers.clear(thread_id);

            // Now return the item from the node that was assigned to us
            if (*my_node).item.is_null() {
                // This shouldn't happen unless it's the sentinel
                Err(())
            } else {
                // Read and return the item
                let item = ptr::read((*my_node).item);

                // In the paper, this is where hp.retire(prReq) would happen,
                // but we don't need it in shared memory implementation

                Ok(item)
            }
        }
    }

    // Algorithm 4, lines 34-45: searchNext
    unsafe fn search_next(&self, lhead: *mut Node<T>, lnext: *mut Node<T>) -> i32 {
        let turn = (*lhead).deq_tid.load(Ordering::Acquire);

        for idx in (turn + 1)..(turn + self.num_threads as i32 + 1) {
            let id_deq = (idx % self.num_threads as i32) as usize;

            if (*self.deqself.add(id_deq)).load(Ordering::Acquire)
                != (*self.deqhelp.add(id_deq)).load(Ordering::Acquire)
            {
                continue;
            }

            if (*lnext).deq_tid.load(Ordering::Acquire) == NOIDX {
                (*lnext)
                    .deq_tid
                    .compare_exchange(NOIDX, id_deq as i32, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
            }
            break;
        }

        (*lnext).deq_tid.load(Ordering::Acquire)
    }

    // Algorithm 4, lines 47-58: casDeqAndHead
    unsafe fn cas_deq_and_head(&self, lhead: *mut Node<T>, lnext: *mut Node<T>, thread_id: usize) {
        let ldeq_tid = (*lnext).deq_tid.load(Ordering::Acquire) as usize;

        if ldeq_tid == thread_id {
            (*self.deqhelp.add(ldeq_tid)).store(lnext, Ordering::Release);
        } else if ldeq_tid < self.num_threads {
            let ldeqhelp = self.hazard_pointers.protect_ptr(
                thread_id,
                HazardPointers::<T>::HP_DEQ,
                (*self.deqhelp.add(ldeq_tid)).load(Ordering::Acquire),
            );

            if ldeqhelp != lnext && lhead == self.head.load(Ordering::Acquire) {
                (*self.deqhelp.add(ldeq_tid))
                    .compare_exchange(ldeqhelp, lnext, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
            }
        }

        self.head
            .compare_exchange(lhead, lnext, Ordering::AcqRel, Ordering::Acquire)
            .ok();
    }

    // Algorithm 4, lines 60-72: giveUp
    unsafe fn give_up(&self, thread_id: usize, my_req: *mut Node<T>) {
        let lhead = self.head.load(Ordering::Acquire);

        if (*self.deqhelp.add(thread_id)).load(Ordering::Acquire) != my_req {
            return;
        }

        if lhead == self.tail.load(Ordering::Acquire) {
            return;
        }

        let protected_head =
            self.hazard_pointers
                .protect_ptr(thread_id, HazardPointers::<T>::HP_HEAD, lhead);

        if protected_head != self.head.load(Ordering::Acquire) {
            return;
        }

        let lnext = self.hazard_pointers.protect_ptr(
            thread_id,
            HazardPointers::<T>::HP_NEXT,
            (*protected_head).next.load(Ordering::Acquire),
        );

        if protected_head != self.head.load(Ordering::Acquire) {
            return;
        }

        if self.search_next(protected_head, lnext) == NOIDX {
            (*lnext)
                .deq_tid
                .compare_exchange(NOIDX, thread_id as i32, Ordering::AcqRel, Ordering::Acquire)
                .ok();
        }

        self.cas_deq_and_head(protected_head, lnext, thread_id);
    }

    // Initialize in shared memory
    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Arrays
        let enqueuers_offset = queue_aligned;
        let enqueuers_size = num_threads * mem::size_of::<AtomicPtr<Node<T>>>();
        let enqueuers_aligned = (enqueuers_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let deqself_offset = enqueuers_offset + enqueuers_aligned;
        let deqself_size = num_threads * mem::size_of::<AtomicPtr<Node<T>>>();
        let deqself_aligned = (deqself_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let deqhelp_offset = deqself_offset + deqself_aligned;
        let deqhelp_size = num_threads * mem::size_of::<AtomicPtr<Node<T>>>();
        let deqhelp_aligned = (deqhelp_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Hazard pointers
        let hp_offset = deqhelp_offset + deqhelp_aligned;
        let hp_size = num_threads * HazardPointers::<T>::size();
        let hp_aligned = (hp_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Node and item pools
        let items_per_thread = 250_000;
        let pool_size = num_threads * items_per_thread * 2;

        let node_pool_offset = hp_offset + hp_aligned;
        let node_pool_size = pool_size * mem::size_of::<Node<T>>();
        let node_pool_aligned = (node_pool_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let item_pool_offset = node_pool_offset + node_pool_aligned;
        let item_pool_size = pool_size * mem::size_of::<T>();

        let total_size = item_pool_offset + item_pool_size;

        // Initialize arrays
        let enqueuers_ptr = mem.add(enqueuers_offset) as *mut AtomicPtr<Node<T>>;
        let deqself_ptr = mem.add(deqself_offset) as *mut AtomicPtr<Node<T>>;
        let deqhelp_ptr = mem.add(deqhelp_offset) as *mut AtomicPtr<Node<T>>;

        for i in 0..num_threads {
            ptr::write(enqueuers_ptr.add(i), AtomicPtr::new(ptr::null_mut()));
            ptr::write(deqself_ptr.add(i), AtomicPtr::new(ptr::null_mut()));
            ptr::write(deqhelp_ptr.add(i), AtomicPtr::new(ptr::null_mut()));
        }

        // Initialize node pool
        let node_pool_ptr = mem.add(node_pool_offset) as *mut Node<T>;
        let item_pool_ptr = mem.add(item_pool_offset) as *mut T;

        // Create sentinel node - it has no item
        let sentinel = node_pool_ptr;
        ptr::write(sentinel, Node::new_sentinel());

        // Initialize deqhelp with sentinel but deqself with null to ensure they're different initially
        for i in 0..num_threads {
            (*deqhelp_ptr.add(i)).store(sentinel, Ordering::Release);
            // deqself remains null from the earlier initialization
        }

        // Initialize queue
        ptr::write(
            queue_ptr,
            Self {
                head: AtomicPtr::new(sentinel),
                tail: AtomicPtr::new(sentinel),
                enqueuers: enqueuers_ptr,
                deqself: deqself_ptr,
                deqhelp: deqhelp_ptr,
                hazard_pointers: HazardPointers::new(mem, hp_offset, num_threads),
                num_threads,
                node_pool: node_pool_ptr,
                item_pool: item_pool_ptr,
                pool_size,
                next_node: AtomicUsize::new(1), // Skip sentinel
                next_item: AtomicUsize::new(0),
                base_ptr: mem,
                total_size,
                _phantom: std::marker::PhantomData,
            },
        );

        &mut *queue_ptr
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let array_size = num_threads * mem::size_of::<AtomicPtr<Node<T>>>();
        let array_aligned = (array_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let hp_size = num_threads * HazardPointers::<T>::size();
        let hp_aligned = (hp_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let items_per_thread = 250_000;
        let pool_size = num_threads * items_per_thread * 2;

        let node_pool_size = pool_size * mem::size_of::<Node<T>>();
        let node_pool_aligned = (node_pool_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        let item_pool_size = pool_size * mem::size_of::<T>();

        let total =
            queue_aligned + 3 * array_aligned + hp_aligned + node_pool_aligned + item_pool_size;
        (total + 4095) & !4095 // Page align
    }

    pub fn is_empty(&self) -> bool {
        unsafe {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);
            head == tail || (*head).next.load(Ordering::Acquire).is_null()
        }
    }

    pub fn is_full(&self) -> bool {
        self.next_node.load(Ordering::Acquire) >= self.pool_size
            || self.next_item.load(Ordering::Acquire) >= self.pool_size
    }
}

impl<T: Send + Clone + 'static> MpmcQueue<T> for TurnQueue<T> {
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

impl<T: Send + Clone> Drop for TurnQueue<T> {
    fn drop(&mut self) {
        // Shared memory cleanup handled externally
    }
}
