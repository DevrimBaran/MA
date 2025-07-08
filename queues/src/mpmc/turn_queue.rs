// paper in /paper/mpmc/crturnqueue-2016.pdf
use crate::MpmcQueue;
use std::mem;
use std::ptr;
use std::sync::atomic::{fence, AtomicI32, AtomicPtr, AtomicUsize, Ordering};

const NOIDX: i32 = -1;
const CACHE_LINE_SIZE: usize = 64;

// Node structure - Algorithm 1
#[repr(C, align(64))]
struct Node<T> {
    item: *mut T,
    enq_tid: usize,     // Used only by enqueue()
    deq_tid: AtomicI32, // Used by dequeue()
    next: AtomicPtr<Node<T>>,
    _padding: [u8; CACHE_LINE_SIZE - 32],
}

impl<T> Node<T> {
    fn new_sentinel() -> Self {
        Self {
            item: ptr::null_mut(),
            enq_tid: 0, // Paper sets to 0 for sentinel
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

// Hazard pointer implementation for memory reclamation (Section 3.1)
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
    const HP_DEQ: usize = 3; // For protecting deqhelp[ldeqTid] in line 52
    const MAX_HPS: usize = 4;

    // IPC adaptation - initialize in shared memory
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

    // protectPtr from paper - Algorithm 5 line 84
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

    // clear() method mentioned in paper
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
    enqueuers: *mut AtomicPtr<Node<T>>, // Array for enqueue requests
    deqself: *mut AtomicPtr<Node<T>>,   // First array for dequeue requests
    deqhelp: *mut AtomicPtr<Node<T>>,   // Second array for dequeue results
    hazard_pointers: HazardPointers<T>,
    num_threads: usize,

    // IPC memory management - replaces heap allocation
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

            // IPC adaptation - allocate from pools instead of heap
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

            // Line 3: new Node(item,myidx)
            let my_node = self.node_pool.add(node_idx);
            ptr::write(my_node, Node::new(item_ptr, thread_id));

            // Line 4: enqueuers[myidx].store(myNode)
            (*self.enqueuers.add(thread_id)).store(my_node, Ordering::Release);

            // Line 5: for (int i = 0; i < maxThreads; i++)
            for _ in 0..self.num_threads {
                // Lines 6-8: Check if our request was completed
                if (*self.enqueuers.add(thread_id))
                    .load(Ordering::Acquire)
                    .is_null()
                {
                    self.hazard_pointers.clear(thread_id);
                    return Ok(());
                }

                // Line 10: hp.protectPtr(kHpTail, tail.load())
                let ltail = self.hazard_pointers.protect_ptr(
                    thread_id,
                    HazardPointers::<T>::HP_TAIL,
                    self.tail.load(Ordering::Acquire),
                );

                // Line 11: if (ltail != tail.load()) continue
                if ltail != self.tail.load(Ordering::Acquire) {
                    continue;
                }

                // Lines 12-15: Clear completed request
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

                    // Line 18: if (nodeHelp == nullptr) continue
                    if !node_help.is_null() {
                        // Line 20: ltail->next.compare_exchange_strong(nullnode, nodeHelp)
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

            // Line 26: enqueuers[myidx].store(nullptr)
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

            // Line 6: for (int i=0; i < maxThreads; i++)
            for _ in 0..self.num_threads {
                // Line 7: if (deqhelp[myidx].load() != myReq) break
                if (*self.deqhelp.add(thread_id)).load(Ordering::Acquire) != my_req {
                    break;
                }

                // Line 8: hp.protectPtr(kHpHead, head.load())
                let lhead = self.hazard_pointers.protect_ptr(
                    thread_id,
                    HazardPointers::<T>::HP_HEAD,
                    self.head.load(Ordering::Acquire),
                );

                // Line 9: if (lhead != head.load()) continue
                if lhead != self.head.load(Ordering::Acquire) {
                    continue;
                }

                // Lines 10-19: Check if queue is empty
                if lhead == self.tail.load(Ordering::Acquire) {
                    // Line 11: deqself[myidx].store(prReq)
                    (*self.deqself.add(thread_id)).store(pr_req, Ordering::Release);
                    // Line 12: giveUp(myReq, myidx)
                    self.give_up(thread_id, my_req);

                    // Lines 13-16: Check if giveUp succeeded
                    if (*self.deqhelp.add(thread_id)).load(Ordering::Acquire) != my_req {
                        (*self.deqself.add(thread_id)).store(my_req, Ordering::Relaxed);
                        break;
                    }

                    // Lines 17-18: Return nullptr
                    self.hazard_pointers.clear(thread_id);
                    return Err(());
                }

                // Line 20: hp.protectPtr(kHpNext, lhead->next.load())
                let lnext = self.hazard_pointers.protect_ptr(
                    thread_id,
                    HazardPointers::<T>::HP_NEXT,
                    (*lhead).next.load(Ordering::Acquire),
                );

                // Line 21: if (lhead != head.load()) continue
                if lhead != self.head.load(Ordering::Acquire) {
                    continue;
                }

                // Line 22: searchNext and casDeqAndHead
                if !lnext.is_null() {
                    let deq_tid = self.search_next(lhead, lnext);
                    if deq_tid != NOIDX {
                        self.cas_deq_and_head(lhead, lnext, thread_id);
                    }
                }
            }

            // Lines 24-31: Return result
            let my_node = (*self.deqhelp.add(thread_id)).load(Ordering::Acquire);

            // IPC adaptation - validate we got a valid node
            if my_node.is_null() || my_node == my_req {
                self.hazard_pointers.clear(thread_id);
                return Err(());
            }

            // Lines 25-28: Help advance head if needed
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

            // Line 29: hp.clear()
            self.hazard_pointers.clear(thread_id);

            // Line 31: return myNode->item
            if (*my_node).item.is_null() {
                Err(()) // Sentinel node
            } else {
                let item = ptr::read((*my_node).item);
                Ok(item)
            }
        }
    }

    // Algorithm 4, lines 34-45: searchNext
    unsafe fn search_next(&self, lhead: *mut Node<T>, lnext: *mut Node<T>) -> i32 {
        // Line 35: const int turn = lhead->deqTid.load()
        let turn = (*lhead).deq_tid.load(Ordering::Acquire);

        // Line 36: for (int idx=turn+1; idx < turn+maxThreads+1; idx++)
        for idx in (turn + 1)..(turn + self.num_threads as i32 + 1) {
            let id_deq = (idx % self.num_threads as i32) as usize;

            // Line 38: if (deqself[idDeq] != deqhelp[idDeq]) continue
            if (*self.deqself.add(id_deq)).load(Ordering::Acquire)
                != (*self.deqhelp.add(id_deq)).load(Ordering::Acquire)
            {
                continue;
            }

            // Lines 39-41: Try to assign node
            if (*lnext).deq_tid.load(Ordering::Acquire) == NOIDX {
                (*lnext)
                    .deq_tid
                    .compare_exchange(NOIDX, id_deq as i32, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
            }
            break;
        }

        // Line 44: return lnext->deqTid.load()
        (*lnext).deq_tid.load(Ordering::Acquire)
    }

    // Algorithm 4, lines 47-58: casDeqAndHead
    unsafe fn cas_deq_and_head(&self, lhead: *mut Node<T>, lnext: *mut Node<T>, thread_id: usize) {
        // Line 48: const int ldeqTid = lnext->deqTid.load()
        let ldeq_tid = (*lnext).deq_tid.load(Ordering::Acquire) as usize;

        // Lines 49-50: Direct assignment if it's our thread
        if ldeq_tid == thread_id {
            (*self.deqhelp.add(ldeq_tid)).store(lnext, Ordering::Release);
        } else if ldeq_tid < self.num_threads {
            // Line 52: hp.protectPtr(kHpDeq, deqhelp[ldeqTid].load())
            let ldeqhelp = self.hazard_pointers.protect_ptr(
                thread_id,
                HazardPointers::<T>::HP_DEQ,
                (*self.deqhelp.add(ldeq_tid)).load(Ordering::Acquire),
            );

            // Lines 53-54: CAS to update deqhelp
            if ldeqhelp != lnext && lhead == self.head.load(Ordering::Acquire) {
                (*self.deqhelp.add(ldeq_tid))
                    .compare_exchange(ldeqhelp, lnext, Ordering::AcqRel, Ordering::Acquire)
                    .ok();
            }
        }

        // Line 57: head.compare_exchange_strong(lhead, lnext)
        self.head
            .compare_exchange(lhead, lnext, Ordering::AcqRel, Ordering::Acquire)
            .ok();
    }

    // Algorithm 4, lines 60-72: giveUp
    unsafe fn give_up(&self, thread_id: usize, my_req: *mut Node<T>) {
        // Line 61: lhead = head.load()
        let lhead = self.head.load(Ordering::Acquire);

        // Line 62: if (deqhelp[myidx] != myReq) return
        if (*self.deqhelp.add(thread_id)).load(Ordering::Acquire) != my_req {
            return;
        }

        // Line 63: if (lhead == tail.load()) return
        if lhead == self.tail.load(Ordering::Acquire) {
            return;
        }

        // Line 64: hp.protectPtr(kHpHead, lhead)
        let protected_head =
            self.hazard_pointers
                .protect_ptr(thread_id, HazardPointers::<T>::HP_HEAD, lhead);

        // Line 65: if (lhead != head.load()) return
        if protected_head != self.head.load(Ordering::Acquire) {
            return;
        }

        // Line 66: hp.protectPtr(kHpNext, lhead->next.load())
        let lnext = self.hazard_pointers.protect_ptr(
            thread_id,
            HazardPointers::<T>::HP_NEXT,
            (*protected_head).next.load(Ordering::Acquire),
        );

        // Line 67: if (lhead != head.load()) return
        if protected_head != self.head.load(Ordering::Acquire) {
            return;
        }

        // Lines 68-70: Try to assign to self if no one else claimed it
        if self.search_next(protected_head, lnext) == NOIDX {
            (*lnext)
                .deq_tid
                .compare_exchange(NOIDX, thread_id as i32, Ordering::AcqRel, Ordering::Acquire)
                .ok();
        }

        // Line 71: casDeqAndHead(lhead, lnext, myidx)
        self.cas_deq_and_head(protected_head, lnext, thread_id);
    }

    // IPC adaptation - initialize in shared memory
    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);

        // Arrays for enqueuers, deqself, deqhelp
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

        // IPC pools instead of heap allocation
        let items_per_thread = 150_000;
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

        // Create sentinel node
        let sentinel = node_pool_ptr;
        ptr::write(sentinel, Node::new_sentinel());

        // Paper requires deqhelp != deqself initially
        for i in 0..num_threads {
            (*deqhelp_ptr.add(i)).store(sentinel, Ordering::Release);
            // deqself remains null
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

        let items_per_thread = 150_000;
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
