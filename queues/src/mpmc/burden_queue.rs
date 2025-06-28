// paper in /paper/mpmc/Wait-free_Algorithms_the_Burden_of_the_Past.pdf
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::MpmcQueue;

#[repr(C)]
struct ENode<T> {
    prev: AtomicPtr<ENode<T>>,
    value: T,
    next: AtomicPtr<ENode<T>>,
}

#[repr(C)]
struct DNode<T> {
    prev: AtomicPtr<DNode<T>>,
    value: AtomicPtr<T>,
    match_node: AtomicPtr<ENode<T>>,
}

#[repr(C)]
pub struct BurdenWFQueue<T: Send + 'static> {
    enqueues: AtomicPtr<ENode<T>>,
    dequeues: AtomicPtr<DNode<T>>,

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

unsafe fn atomic_swap<T>(a: &AtomicPtr<T>, b: &AtomicPtr<T>) {
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

    pub fn enqueue(&self, _thread_id: usize, v: T) -> Result<(), ()> {
        unsafe {
            let ei = self.allocate_enode(v);

            (*ei).prev.store(ei, Ordering::Release);

            atomic_swap(&self.enqueues, &(*ei).prev);

            self.help_enqueue(ei);

            Ok(())
        }
    }

    unsafe fn help_enqueue(&self, ei: *mut ENode<T>) {
        self.help_enqueue_recursive(ei, 0);
    }

    unsafe fn help_enqueue_recursive(&self, ei: *mut ENode<T>, depth: usize) {
        const MAX_RECURSION_DEPTH: usize = 10000;

        if depth > MAX_RECURSION_DEPTH {
            self.help_enqueue_iterative(ei);
            return;
        }

        let e_prime_i = (*ei).prev.load(Ordering::Acquire);

        if !e_prime_i.is_null() {
            self.help_enqueue_recursive(e_prime_i, depth + 1);

            (*e_prime_i).next.store(ei, Ordering::Release);
        }

        (*ei).prev.store(ptr::null_mut(), Ordering::Release);
    }

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

    pub fn dequeue(&self, _thread_id: usize) -> Result<T, ()> {
        unsafe {
            let di = self.allocate_dnode();

            (*di).prev.store(di, Ordering::Release);

            atomic_swap(&self.dequeues, &(*di).prev);

            self.help_dequeue(di);

            let value_ptr = (*di).value.load(Ordering::Acquire);
            if value_ptr.is_null() {
                Err(())
            } else {
                Ok(ptr::read(value_ptr))
            }
        }
    }

    unsafe fn help_dequeue(&self, di: *mut DNode<T>) {
        self.help_dequeue_recursive(di, 0);
    }

    unsafe fn help_dequeue_recursive(&self, di: *mut DNode<T>, depth: usize) {
        const MAX_RECURSION_DEPTH: usize = 1000;

        if depth > MAX_RECURSION_DEPTH {
            self.help_dequeue_iterative(di);
            return;
        }

        let d_prime_i = (*di).prev.load(Ordering::Acquire);

        if !d_prime_i.is_null() {
            self.help_dequeue_recursive(d_prime_i, depth + 1);

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

    pub unsafe fn init_in_shared(mem: *mut u8, num_threads: usize) -> &'static mut Self {
        let queue_ptr = mem as *mut Self;

        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + 63) & !63;

        let items_per_thread = 500_000;
        let pool_size = num_threads * items_per_thread * 4;

        let enodes_offset = queue_aligned;
        let enodes_size = pool_size * mem::size_of::<ENode<T>>();
        let enodes_aligned = (enodes_size + 63) & !63;

        let dnodes_offset = enodes_offset + enodes_aligned;
        let dnodes_size = pool_size * mem::size_of::<DNode<T>>();

        let total_size = dnodes_offset + dnodes_size;

        ptr::write(
            queue_ptr,
            Self {
                enqueues: AtomicPtr::new(ptr::null_mut()),
                dequeues: AtomicPtr::new(ptr::null_mut()),
                base_ptr: mem,
                total_size,
                enodes_offset,
                dnodes_offset,
                next_enode: AtomicUsize::new(1),
                next_dnode: AtomicUsize::new(1),
                pool_size,
                _phantom: std::marker::PhantomData,
            },
        );

        let queue = &mut *queue_ptr;

        let e0 = queue.get_enode(0);
        (*e0).prev = AtomicPtr::new(ptr::null_mut());
        (*e0).next = AtomicPtr::new(ptr::null_mut());

        let d0 = queue.get_dnode(0);
        (*d0).prev = AtomicPtr::new(ptr::null_mut());
        (*d0).value = AtomicPtr::new(ptr::null_mut());
        (*d0).match_node = AtomicPtr::new(e0);

        queue.enqueues.store(e0, Ordering::Release);
        queue.dequeues.store(d0, Ordering::Release);

        queue
    }

    pub fn shared_size(num_threads: usize) -> usize {
        let queue_size = mem::size_of::<Self>();
        let queue_aligned = (queue_size + 63) & !63;

        let items_per_thread = 500_000;
        let pool_size = num_threads * items_per_thread * 4;

        let enodes_size = pool_size * mem::size_of::<ENode<T>>();
        let enodes_aligned = (enodes_size + 63) & !63;

        let dnodes_size = pool_size * mem::size_of::<DNode<T>>();

        let total = queue_aligned + enodes_aligned + dnodes_size;
        (total + 4095) & !4095
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
        false
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
    fn drop(&mut self) {}
}
