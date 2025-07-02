// paper in /paper/dspc-uspsc-mspsc-full.pdf and /paper/dspc-uspsc-mspsc.pdf
use crate::spsc::lamport::LamportQueue;
use crate::SpscQueue;
use std::{
    ptr::{self, null_mut},
    sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering},
};

const CACHE_LINE_SIZE: usize = 64;

#[repr(C, align(128))]
struct Node<T: Send + 'static> {
    val: Option<T>,
    next: AtomicPtr<Node<T>>,
    _padding: [u8; CACHE_LINE_SIZE - 24],
}

#[repr(transparent)]
#[derive(Copy, Clone)]
struct NodePtr<U: Send + 'static>(*mut Node<U>);

unsafe impl<U: Send + 'static> Send for NodePtr<U> {}
unsafe impl<U: Send + 'static> Sync for NodePtr<U> {}

#[repr(C, align(128))]
pub struct DynListQueue<T: Send + 'static> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    _padding1: [u8; 128 - 16],

    nodes_pool: *mut Node<T>,
    next_free_node: AtomicUsize,
    _padding2: [u8; 128 - 16],

    node_cache: LamportQueue<NodePtr<T>>,
    pool_size: usize,
}

unsafe impl<T: Send> Send for DynListQueue<T> {}
unsafe impl<T: Send> Sync for DynListQueue<T> {}

impl<T: Send + 'static> DynListQueue<T> {
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

        // First node is dummy
        let dummy_ptr = nodes_ptr;

        // Initialize cache
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
                head: AtomicPtr::new(dummy_ptr),
                tail: AtomicPtr::new(dummy_ptr),
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

    fn alloc_node(&self, v: T) -> Option<*mut Node<T>> {
        // Only use cache, no heap allocation
        if let Ok(node_ptr) = self.node_cache.pop() {
            let node = node_ptr.0;
            unsafe {
                ptr::write(&mut (*node).val, Some(v));
                (*node).next.store(null_mut(), Ordering::Relaxed);
            }
            Some(node)
        } else {
            None
        }
    }

    fn recycle_node(&self, node: *mut Node<T>) {
        if node.is_null() {
            return;
        }

        unsafe {
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

    fn push(&self, item: T) -> Result<(), ()> {
        let new_node = self.alloc_node(item).ok_or(())?;

        fence(Ordering::Release); // WMB from paper

        let current_tail = self.tail.load(Ordering::Acquire);
        unsafe {
            (*current_tail).next.store(new_node, Ordering::Release);
        }

        self.tail.store(new_node, Ordering::Release);
        Ok(())
    }

    fn pop(&self) -> Result<T, ()> {
        let current_dummy = self.head.load(Ordering::Acquire);
        let item_node = unsafe { (*current_dummy).next.load(Ordering::Acquire) };

        if item_node.is_null() {
            return Err(());
        }

        let value = unsafe { ptr::replace(&mut (*item_node).val, None).ok_or(())? };

        self.head.store(item_node, Ordering::Release);
        self.recycle_node(current_dummy);

        Ok(value)
    }

    fn available(&self) -> bool {
        // Check if cache has free nodes
        !self.node_cache.empty()
    }

    fn empty(&self) -> bool {
        let h = self.head.load(Ordering::Acquire);
        unsafe { (*h).next.load(Ordering::Acquire).is_null() }
    }
}

impl<T: Send + 'static> Drop for DynListQueue<T> {
    fn drop(&mut self) {
        // For shared memory, just drain items
        while let Ok(item) = SpscQueue::pop(self) {
            drop(item);
        }
        // Nodes are in shared memory, don't free
    }
}
