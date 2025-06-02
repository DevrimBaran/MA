// queues/src/mpmc/burden_wf.rs
// Wait-Free MPMC Queue from "Wait-free Algorithms: the Burden of the Past"
// Based on Algorithm 4 (Section 6) which uses both CAS and memory-to-memory swap

use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::MpmcQueue;

// Enqueue node structure (ENode from the paper)
#[repr(C)]
struct ENode<T> {
    prev: AtomicPtr<ENode<T>>,
    value: T, // Direct storage as in paper
    next: AtomicPtr<ENode<T>>,
}

// Dequeue node structure (DNode from the paper)
#[repr(C)]
struct DNode<T> {
    prev: AtomicPtr<DNode<T>>,
    value: AtomicPtr<T>, // Will point to value in ENode
    match_node: AtomicPtr<ENode<T>>,
}

// Main queue structure
#[repr(C)]
pub struct BurdenWFQueue<T: Send + 'static> {
    // Head pointers for both lists
    enqueues: AtomicPtr<ENode<T>>,
    dequeues: AtomicPtr<DNode<T>>,

    // Memory management
    base_ptr: *mut u8,
    total_size: usize,
    enodes_offset: usize,
    dnodes_offset: usize,
    next_enode: AtomicUsize,
    next_dnode: AtomicUsize,
    pool_size: usize,

    _phantom: std::marker::PhantomData<T>,
}

unsafe impl<T: Send> Send for BurdenWFQueue<T> {}
unsafe impl<T: Send> Sync for BurdenWFQueue<T> {}

// This is a simulation of memory-to-memory swap
// Real implementation would require hardware support or more complex synchronization
// For benchmarking purposes, we use a simplified version that may not be truly atomic
unsafe fn atomic_swap<T>(a: &AtomicPtr<T>, b: &AtomicPtr<T>) {
    // WARNING: This is NOT a true atomic memory-to-memory swap
    // A real implementation would need hardware support
    let val_b = b.load(Ordering::Acquire);
    let val_a = a.swap(val_b, Ordering::AcqRel);
    b.store(val_a, Ordering::Release);
}

impl<T: Send + 'static> BurdenWFQueue<T> {
    unsafe fn get_enode(&self, index: usize) -> *mut ENode<T> {
        let nodes_ptr = self.base_ptr.add(self.enodes_offset) as *mut ENode<T>;
        nodes_ptr.add(index)
    }

    unsafe fn get_dnode(&self, index: usize) -> *mut DNode<T> {
        let nodes_ptr = self.base_ptr.add(self.dnodes_offset) as *mut DNode<T>;
        nodes_ptr.add(index)
    }

    unsafe fn allocate_enode(&self, value: T) -> *mut ENode<T> {
        let index = self.next_enode.fetch_add(1, Ordering::AcqRel);
        if index >= self.pool_size {
            panic!("ENode pool exhausted");
        }

        let node = self.get_enode(index);
        // Write fields individually to avoid moving value
        (*node).prev = AtomicPtr::new(ptr::null_mut());
        (*node).next = AtomicPtr::new(ptr::null_mut());
        ptr::write(&mut (*node).value as *mut T, value);
        node
    }

    unsafe fn allocate_dnode(&self) -> *mut DNode<T> {
        let index = self.next_dnode.fetch_add(1, Ordering::AcqRel);
        if index >= self.pool_size {
            panic!("DNode pool exhausted");
        }

        let node = self.get_dnode(index);
        (*node).prev = AtomicPtr::new(ptr::null_mut());
        (*node).value = AtomicPtr::new(ptr::null_mut());
        (*node).match_node = AtomicPtr::new(ptr::null_mut());
        node
    }

    // Algorithm 4: enqueue operation
    pub fn enqueue(&self, _thread_id: usize, v: T) -> Result<(), ()> {
        unsafe {
            // Line 5: Create new enqueue node with value v
            let ei = self.allocate_enode(v);

            // Line 6: ei.prev.write(ei)
            (*ei).prev.store(ei, Ordering::Release);

            // Line 7: swap(enqueues, ei.prev)
            atomic_swap(&self.enqueues, &(*ei).prev);

            // Line 8: helpEnqueue(ei)
            self.help_enqueue(ei);

            Ok(())
        }
    }

    // helpEnqueue function from Algorithm 4
    unsafe fn help_enqueue(&self, ei: *mut ENode<T>) {
        self.help_enqueue_recursive(ei, 0);
    }

    unsafe fn help_enqueue_recursive(&self, ei: *mut ENode<T>, depth: usize) {
        // Guard against stack overflow - fall back to iterative if too deep
        const MAX_RECURSION_DEPTH: usize = 10000;

        if depth > MAX_RECURSION_DEPTH {
            // Fall back to iterative approach to prevent stack overflow
            self.help_enqueue_iterative(ei);
            return;
        }

        // Line 9: e'i ← ei.prev.read()
        let e_prime_i = (*ei).prev.load(Ordering::Acquire);

        // Line 10: if e'i ≠ ⊥
        if !e_prime_i.is_null() {
            // Line 11: helpEnqueue(e'i)
            self.help_enqueue_recursive(e_prime_i, depth + 1);

            // Line 12: e'i.next.write(ei)
            (*e_prime_i).next.store(ei, Ordering::Release);
        }

        // Line 13: ei.prev.write(⊥)
        (*ei).prev.store(ptr::null_mut(), Ordering::Release);
    }

    // Fallback iterative version for deep chains
    unsafe fn help_enqueue_iterative(&self, mut ei: *mut ENode<T>) {
        let mut nodes = Vec::with_capacity(100);
        let mut current = ei;

        while !current.is_null() {
            nodes.push(current);
            current = (*current).prev.load(Ordering::Acquire);
        }

        for i in (1..nodes.len()).rev() {
            (*nodes[i]).next.store(nodes[i - 1], Ordering::Release);
        }

        for &node in &nodes {
            (*node).prev.store(ptr::null_mut(), Ordering::Release);
        }
    }

    // Algorithm 4: dequeue operation
    pub fn dequeue(&self, _thread_id: usize) -> Result<T, ()> {
        unsafe {
            // Line 14: Create new dequeue node
            let di = self.allocate_dnode();

            // Line 15: di.prev.write(di)
            (*di).prev.store(di, Ordering::Release);

            // Line 16: swap(dequeues, di.prev)
            atomic_swap(&self.dequeues, &(*di).prev);

            // Line 17: helpDequeue(di)
            self.help_dequeue(di);

            // Line 18: return di.value.read()
            let value_ptr = (*di).value.load(Ordering::Acquire);
            if value_ptr.is_null() {
                Err(())
            } else {
                Ok(ptr::read(value_ptr))
            }
        }
    }

    // helpDequeue function from Algorithm 4
    unsafe fn help_dequeue(&self, di: *mut DNode<T>) {
        self.help_dequeue_recursive(di, 0);
    }

    unsafe fn help_dequeue_recursive(&self, di: *mut DNode<T>, depth: usize) {
        // Guard against stack overflow
        const MAX_RECURSION_DEPTH: usize = 1000;

        if depth > MAX_RECURSION_DEPTH {
            // Fall back to iterative approach
            self.help_dequeue_iterative(di);
            return;
        }

        // Line 19: d'i ← di.prev.read()
        let d_prime_i = (*di).prev.load(Ordering::Acquire);

        // Line 20: if d'i ≠ ⊥
        if !d_prime_i.is_null() {
            // Line 21: helpDequeue(d'i)
            self.help_dequeue_recursive(d_prime_i, depth + 1);

            // Line 22: e'i ← d'i.match.read()
            let e_prime_i = (*d_prime_i).match_node.load(Ordering::Acquire);

            if !e_prime_i.is_null() {
                // Line 23: ei ← e'i.next.read()
                let mut ei = (*e_prime_i).next.load(Ordering::Acquire);

                // Line 24: if ei = ⊥ then ei ← e'i
                if ei.is_null() {
                    ei = e_prime_i;
                }

                // Line 25: di.match.CAS(⊥, ei)
                (*di)
                    .match_node
                    .compare_exchange(ptr::null_mut(), ei, Ordering::AcqRel, Ordering::Acquire)
                    .ok();

                // Line 26: ei ← di.match.read()
                let ei_final = (*di).match_node.load(Ordering::Acquire);

                // Line 27: if ei ≠ e'i then di.value.write(ei.value)
                if ei_final != e_prime_i && !ei_final.is_null() {
                    // Point to the value in the enqueue node
                    let value_ptr = &(*ei_final).value as *const T as *mut T;
                    (*di).value.store(value_ptr, Ordering::Release);
                }
            }
        }

        // Line 28: di.prev.write(⊥)
        (*di).prev.store(ptr::null_mut(), Ordering::Release);
    }

    // Fallback iterative version
    unsafe fn help_dequeue_iterative(&self, di: *mut DNode<T>) {
        let mut to_process = Vec::with_capacity(100);
        let mut current = di;

        while !current.is_null() {
            let prev = (*current).prev.load(Ordering::Acquire);
            if prev.is_null() {
                break;
            }
            to_process.push((current, prev));
            current = prev;
        }

        for &(d, d_prime) in to_process.iter().rev() {
            let e_prime = (*d_prime).match_node.load(Ordering::Acquire);

            if !e_prime.is_null() {
                let mut e = (*e_prime).next.load(Ordering::Acquire);
                if e.is_null() {
                    e = e_prime;
                }

                (*d).match_node
                    .compare_exchange(ptr::null_mut(), e, Ordering::AcqRel, Ordering::Acquire)
                    .ok();

                let e_final = (*d).match_node.load(Ordering::Acquire);
                if e_final != e_prime && !e_final.is_null() {
                    let value_ptr = &(*e_final).value as *const T as *mut T;
                    (*d).value.store(value_ptr, Ordering::Release);
                }
            }

            (*d).prev.store(ptr::null_mut(), Ordering::Release);
        }

        // Handle di itself
        let d_prime_i = (*di).prev.load(Ordering::Acquire);
        if !d_prime_i.is_null() {
            let e_prime_i = (*d_prime_i).match_node.load(Ordering::Acquire);

            if !e_prime_i.is_null() {
                let mut ei = (*e_prime_i).next.load(Ordering::Acquire);
                if ei.is_null() {
                    ei = e_prime_i;
                }

                (*di)
                    .match_node
                    .compare_exchange(ptr::null_mut(), ei, Ordering::AcqRel, Ordering::Acquire)
                    .ok();

                let ei_final = (*di).match_node.load(Ordering::Acquire);
                if ei_final != e_prime_i && !ei_final.is_null() {
                    let value_ptr = &(*ei_final).value as *const T as *mut T;
                    (*di).value.store(value_ptr, Ordering::Release);
                }
            }
        }

        (*di).prev.store(ptr::null_mut(), Ordering::Release);
    }

    // Initialize queue in shared memory
    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        // Calculate memory layout
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + 63) & !63;

        // Pool sizes - much larger allocation for benchmarking
        // Each operation creates a new node, so we need enough for all operations
        let items_per_thread = 500_000; // Very generous for 200k operations
        let pool_size = num_threads * items_per_thread * 4; // 4x safety margin

        // Memory sections
        let enodes_offset = queue_aligned;
        let enodes_size = pool_size * mem::size_of::<ENode<T>>();
        let enodes_aligned = (enodes_size + 63) & !63;

        let dnodes_offset = enodes_offset + enodes_aligned;
        let dnodes_size = pool_size * mem::size_of::<DNode<T>>();

        let total_size = dnodes_offset + dnodes_size;

        // Initialize queue header
        ptr::write(
            queue_ptr,
            Self {
                enqueues: AtomicPtr::new(ptr::null_mut()),
                dequeues: AtomicPtr::new(ptr::null_mut()),
                base_ptr: mem,
                total_size,
                enodes_offset,
                dnodes_offset,
                next_enode: AtomicUsize::new(1), // Start at 1, 0 is reserved for sentinel
                next_dnode: AtomicUsize::new(1),
                pool_size,
                _phantom: std::marker::PhantomData,
            },
        );

        let queue = &mut *queue_ptr;

        // Initialize as described in paper: dummy nodes e0 and d0
        // "Both enqueues and dequeues are initialized with dummy nodes e0 and d0, such that d0.match = e0"
        let e0 = queue.get_enode(0);
        (*e0).prev = AtomicPtr::new(ptr::null_mut());
        (*e0).next = AtomicPtr::new(ptr::null_mut());
        // Note: value is uninitialized for dummy node, which is fine as it's never accessed

        let d0 = queue.get_dnode(0);
        (*d0).prev = AtomicPtr::new(ptr::null_mut());
        (*d0).value = AtomicPtr::new(ptr::null_mut());
        (*d0).match_node = AtomicPtr::new(e0);

        // Set initial head pointers
        queue.enqueues.store(e0, Ordering::Release);
        queue.dequeues.store(d0, Ordering::Release);

        queue
    }

    // Calculate required shared memory size
    pub fn shared_size(num_threads: usize) -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + 63) & !63;

        let items_per_thread = 500_000;
        let pool_size = num_threads * items_per_thread * 4;

        let enodes_size = pool_size * mem::size_of::<ENode<T>>();
        let enodes_aligned = (enodes_size + 63) & !63;

        let dnodes_size = pool_size * mem::size_of::<DNode<T>>();

        let total = queue_aligned + enodes_aligned + dnodes_size;
        (total + 4095) & !4095 // Page align
    }

    pub fn is_empty(&self) -> bool {
        unsafe {
            let d = self.dequeues.load(Ordering::Acquire);
            if d.is_null() {
                return true;
            }

            let match_node = (*d).match_node.load(Ordering::Acquire);
            if match_node.is_null() {
                return true;
            }

            let next = (*match_node).next.load(Ordering::Acquire);
            next.is_null()
        }
    }

    pub fn is_full(&self) -> bool {
        false // Unbounded queue in the paper
    }
}

impl<T: Send + 'static> MpmcQueue<T> for BurdenWFQueue<T> {
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

impl<T: Send> Drop for BurdenWFQueue<T> {
    fn drop(&mut self) {
        // Nothing to clean up in shared memory version
    }
}
