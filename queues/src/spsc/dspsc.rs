// paper in /paper/dspc-uspsc-mspsc.pdf and /paper/dspc-uspsc.pdf (oriented towards latter one)
use crate::spsc::lamport::LamportQueue;
use crate::SpscQueue;
use std::{
    ptr::{self, null_mut},
    sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering},
};

const CACHE_LINE_SIZE: usize = 64;

// Node structure - Figure 2 in paper
#[repr(C, align(128))]
struct Node<T: Send + 'static> {
    val: Option<T>,
    next: AtomicPtr<Node<T>>,
    _padding: [u8; CACHE_LINE_SIZE - 24], // IPC: cache line alignment
}

// IPC: wrapper for node pointers in shared memory
#[repr(transparent)]
#[derive(Copy, Clone)]
struct NodePtr<U: Send + 'static>(*mut Node<U>);

unsafe impl<U: Send + 'static> Send for NodePtr<U> {}
unsafe impl<U: Send + 'static> Sync for NodePtr<U> {}

// dSPSC queue - Figure 2 in paper
#[repr(C, align(128))]
pub struct DynListQueue<T: Send + 'static> {
    head: AtomicPtr<Node<T>>,  // Line 5 in Figure 2
    tail: AtomicPtr<Node<T>>,  // Line 5 in Figure 2
    _padding1: [u8; 128 - 16], // IPC: separate cache lines

    // IPC: pre-allocated node pool instead of malloc
    nodes_pool: *mut Node<T>,
    next_free_node: AtomicUsize,
    _padding2: [u8; 128 - 16],

    node_cache: LamportQueue<NodePtr<T>>, // Line 6 - SPSC cache
    pool_size: usize,
}

unsafe impl<T: Send> Send for DynListQueue<T> {}
unsafe impl<T: Send> Sync for DynListQueue<T> {}

impl<T: Send + 'static> DynListQueue<T> {
    // IPC: calculate shared memory size
    pub fn shared_size(cache_capacity: usize, nodes_count: usize) -> usize {
        use std::alloc::Layout;

        let layout_self = Layout::new::<Self>();
        let layout_nodes = Layout::array::<Node<T>>(nodes_count).unwrap();
        let lamport_size = LamportQueue::<NodePtr<T>>::shared_size(cache_capacity);

        let (layout1, _) = layout_self.extend(layout_nodes).unwrap();
        let (final_layout, _) = layout1
            .extend(Layout::from_size_align(lamport_size, 128).unwrap())
            .unwrap();

        final_layout.size()
    }

    // IPC: initialize in shared memory
    pub unsafe fn init_in_shared(
        mem_ptr: *mut u8,
        cache_capacity: usize,
        nodes_count: usize,
    ) -> &'static mut Self {
        use std::alloc::Layout;

        let cache_size = cache_capacity;
        let self_ptr = mem_ptr as *mut Self;

        let layout_self = Layout::new::<Self>();
        let layout_nodes = Layout::array::<Node<T>>(nodes_count).unwrap();
        let lamport_size = LamportQueue::<NodePtr<T>>::shared_size(cache_size);

        let (layout1, offset_nodes) = layout_self.extend(layout_nodes).unwrap();
        let (_, offset_cache) = layout1
            .extend(Layout::from_size_align(lamport_size, 128).unwrap())
            .unwrap();

        // Initialize nodes
        let nodes_ptr = mem_ptr.add(offset_nodes) as *mut Node<T>;
        for i in 0..nodes_count {
            ptr::write(
                nodes_ptr.add(i),
                Node {
                    val: None,
                    next: AtomicPtr::new(null_mut()),
                    _padding: [0; CACHE_LINE_SIZE - 24],
                },
            );
        }

        // First node is dummy (paper requirement)
        let dummy_ptr = nodes_ptr;

        // Initialize cache for node recycling
        let cache_ptr = mem_ptr.add(offset_cache);
        let cache = LamportQueue::<NodePtr<T>>::init_in_shared(cache_ptr, cache_size);

        // Add all free nodes (except dummy) to cache
        for i in 1..nodes_count {
            let node_ptr = nodes_ptr.add(i);
            let _ = cache.push(NodePtr(node_ptr));
        }

        ptr::write(
            self_ptr,
            DynListQueue {
                head: AtomicPtr::new(dummy_ptr), // Initially points to dummy
                tail: AtomicPtr::new(dummy_ptr), // Initially points to dummy
                _padding1: [0; 128 - 16],
                nodes_pool: nodes_ptr,
                next_free_node: AtomicUsize::new(nodes_count),
                _padding2: [0; 128 - 16],
                node_cache: ptr::read(cache as *const _),
                pool_size: nodes_count,
            },
        );

        fence(Ordering::SeqCst);
        &mut *self_ptr
    }

    // Lines 10-11 in Figure 2 - try cache (we allocate from pre allocated pool)
    fn alloc_node(&self, v: T) -> Option<*mut Node<T>> {
        if let Ok(node_ptr) = self.node_cache.pop() {
            let node = node_ptr.0;
            unsafe {
                ptr::write(&mut (*node).val, Some(v));
                (*node).next.store(null_mut(), Ordering::Relaxed);
            }
            Some(node)
        } else {
            None // IPC: no malloc fallback, pool exhausted
        }
    }

    // Line 23 in Figure 2 - try to recycle to cache
    fn recycle_node(&self, node: *mut Node<T>) {
        if node.is_null() {
            return;
        }

        unsafe {
            // Rust: explicitly drop value
            if let Some(val) = ptr::replace(&mut (*node).val, None) {
                drop(val);
            }
            (*node).next.store(null_mut(), Ordering::Relaxed);
        }

        let _ = self.node_cache.push(NodePtr(node));
    }
}

impl<T: Send + 'static> SpscQueue<T> for DynListQueue<T> {
    type PushError = ();
    type PopError = ();

    // push() - Lines 8-16 in Figure 2
    fn push(&self, item: T) -> Result<(), ()> {
        let new_node = self.alloc_node(item).ok_or(())?; // Lines 10-11

        fence(Ordering::Release); // Line 13 - WMB from paper

        let current_tail = self.tail.load(Ordering::Acquire);
        unsafe {
            (*current_tail).next.store(new_node, Ordering::Release); // Line 14
        }

        self.tail.store(new_node, Ordering::Release); // Line 14
        Ok(())
    }

    // pop() - Lines 18-25 in Figure 2
    fn pop(&self) -> Result<T, ()> {
        let current_dummy = self.head.load(Ordering::Acquire);
        let item_node = unsafe { (*current_dummy).next.load(Ordering::Acquire) }; // Line 19

        if item_node.is_null() {
            return Err(()); // Line 19 - empty queue
        }

        let value = unsafe { ptr::replace(&mut (*item_node).val, None).ok_or(())? }; // Line 21

        self.head.store(item_node, Ordering::Release); // Line 22
        self.recycle_node(current_dummy); // Line 23

        Ok(value)
    }

    fn available(&self) -> bool {
        !self.node_cache.empty() // Check if cache has free nodes
    }

    fn empty(&self) -> bool {
        let h = self.head.load(Ordering::Acquire);
        unsafe { (*h).next.load(Ordering::Acquire).is_null() }
    }
}

impl<T: Send + 'static> Drop for DynListQueue<T> {
    fn drop(&mut self) {} // IPC: shared memory cleanup handled externally
}
