// queues/src/spsc/dspsc.rs
use crate::spsc::lamport::LamportQueue;
use crate::SpscQueue;
use std::{
    alloc::Layout,
    ptr::{self, null_mut},
    sync::atomic::{compiler_fence, fence, AtomicPtr, AtomicUsize, Ordering},
};

#[inline(always)]
const fn null_node<T: Send>() -> *mut Node<T> {
    null_mut()
}

const CACHE_LINE_SIZE: usize = 256;

#[repr(C, align(128))]
struct Node<T: Send + 'static> {
    val: UnsafeCell<Option<T>>,
    next: AtomicPtr<Node<T>>,
    _padding: [u8; CACHE_LINE_SIZE - 24],
}

use std::cell::UnsafeCell;

#[repr(transparent)]
#[derive(Copy, Clone, Debug)]
struct NodePtr<U: Send + 'static>(*mut Node<U>);

unsafe impl<U: Send + 'static> Send for NodePtr<U> {}
unsafe impl<U: Send + 'static> Sync for NodePtr<U> {}

#[repr(C, align(128))]
pub struct DynListQueue<T: Send + 'static> {
    head: AtomicPtr<Node<T>>,
    _padding1: [u8; 128 - 8],

    tail: AtomicPtr<Node<T>>,
    _padding2: [u8; 128 - 8],

    nodes_pool_ptr: *mut Node<T>,
    next_free_node: AtomicUsize,

    node_cache: LamportQueue<NodePtr<T>>,

    base_ptr: *mut Node<T>,
    pool_capacity: usize,
    cache_capacity: usize,
    owns_all: bool,

    pub heap_allocs: AtomicUsize,
    pub heap_frees: AtomicUsize,
}

unsafe impl<T: Send> Send for DynListQueue<T> {}
unsafe impl<T: Send> Sync for DynListQueue<T> {}

impl<T: Send + 'static> DynListQueue<T> {
    pub fn shared_size(capacity: usize) -> usize {
        let preallocated_nodes = capacity / 2;
        let node_cache_capacity = capacity;

        let layout_self = Layout::new::<Self>();
        let lamport_cache_size = LamportQueue::<NodePtr<T>>::shared_size(node_cache_capacity);
        let layout_dummy_node = Layout::new::<Node<T>>();
        let layout_pool_array = Layout::array::<Node<T>>(preallocated_nodes).unwrap();

        let (layout1, _) = layout_self.extend(layout_dummy_node).unwrap();
        let (layout2, _) = layout1.extend(layout_pool_array).unwrap();

        let lamport_align = std::cmp::max(std::mem::align_of::<LamportQueue<NodePtr<T>>>(), 128);
        let (final_layout, _) = layout2
            .align_to(lamport_align)
            .unwrap()
            .extend(Layout::from_size_align(lamport_cache_size, lamport_align).unwrap())
            .unwrap();

        final_layout.size()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let preallocated_nodes = capacity / 2;
        let node_cache_capacity = capacity;

        let dummy = Box::into_raw(Box::new(Node {
            val: UnsafeCell::new(None),
            next: AtomicPtr::new(null_node()),
            _padding: [0; CACHE_LINE_SIZE - 24],
        }));

        let mut pool_nodes_vec: Vec<Node<T>> = Vec::with_capacity(preallocated_nodes);
        for _ in 0..preallocated_nodes {
            pool_nodes_vec.push(Node {
                val: UnsafeCell::new(None),
                next: AtomicPtr::new(null_node()),
                _padding: [0; CACHE_LINE_SIZE - 24],
            });
        }
        let pool_ptr = Box::into_raw(pool_nodes_vec.into_boxed_slice()) as *mut Node<T>;

        let node_cache = LamportQueue::<NodePtr<T>>::with_capacity(node_cache_capacity);

        Self {
            head: AtomicPtr::new(dummy),
            tail: AtomicPtr::new(dummy),
            _padding1: [0; 128 - 8],
            _padding2: [0; 128 - 8],
            base_ptr: dummy,
            nodes_pool_ptr: pool_ptr,
            next_free_node: AtomicUsize::new(0),
            node_cache,
            pool_capacity: preallocated_nodes,
            cache_capacity: node_cache_capacity,
            owns_all: true,
            heap_allocs: AtomicUsize::new(0),
            heap_frees: AtomicUsize::new(0),
        }
    }

    pub unsafe fn init_in_shared(mem_ptr: *mut u8, capacity: usize) -> &'static mut Self {
        let preallocated_nodes = capacity / 2;
        let node_cache_capacity = capacity;

        let self_ptr = mem_ptr as *mut Self;

        let layout_self = Layout::new::<Self>();
        let layout_dummy_node = Layout::new::<Node<T>>();
        let layout_pool_array = Layout::array::<Node<T>>(preallocated_nodes).unwrap();

        let lamport_cache_size = LamportQueue::<NodePtr<T>>::shared_size(node_cache_capacity);
        let lamport_align = std::cmp::max(std::mem::align_of::<LamportQueue<NodePtr<T>>>(), 128);

        let (layout1, offset_dummy) = layout_self.extend(layout_dummy_node).unwrap();
        let (layout2, offset_pool_array) = layout1.extend(layout_pool_array).unwrap();
        let (_, offset_node_cache) = layout2
            .align_to(lamport_align)
            .unwrap()
            .extend(Layout::from_size_align(lamport_cache_size, lamport_align).unwrap())
            .unwrap();

        let dummy_ptr_val = mem_ptr.add(offset_dummy) as *mut Node<T>;

        ptr::write(
            dummy_ptr_val,
            Node {
                val: UnsafeCell::new(None),
                next: AtomicPtr::new(null_node()),
                _padding: [0; CACHE_LINE_SIZE - 24],
            },
        );

        let pool_nodes_ptr_val = mem_ptr.add(offset_pool_array) as *mut Node<T>;

        for i in 0..preallocated_nodes {
            ptr::write(
                pool_nodes_ptr_val.add(i),
                Node {
                    val: UnsafeCell::new(None),
                    next: AtomicPtr::new(null_node()),
                    _padding: [0; CACHE_LINE_SIZE - 24],
                },
            );
        }

        let node_cache_mem_start = mem_ptr.add(offset_node_cache);

        let initialized_node_cache_ref =
            LamportQueue::<NodePtr<T>>::init_in_shared(node_cache_mem_start, node_cache_capacity);

        ptr::write(
            self_ptr,
            DynListQueue {
                head: AtomicPtr::new(dummy_ptr_val),
                tail: AtomicPtr::new(dummy_ptr_val),
                _padding1: [0; 128 - 8],
                _padding2: [0; 128 - 8],
                base_ptr: dummy_ptr_val,
                nodes_pool_ptr: pool_nodes_ptr_val,
                next_free_node: AtomicUsize::new(0),
                node_cache: ptr::read(initialized_node_cache_ref as *const _),
                pool_capacity: preallocated_nodes,
                cache_capacity: node_cache_capacity,
                owns_all: false,
                heap_allocs: AtomicUsize::new(0),
                heap_frees: AtomicUsize::new(0),
            },
        );

        &mut *self_ptr
    }

    fn alloc_node(&self, v: T) -> *mut Node<T> {
        // Try cache first (paper line 10)
        if let Ok(node_ptr_wrapper) = self.node_cache.pop() {
            let node_ptr = node_ptr_wrapper.0;
            if !node_ptr.is_null() {
                unsafe {
                    // Reset the node
                    (*node_ptr).next.store(null_node(), Ordering::Relaxed);
                    *(*node_ptr).val.get() = Some(v);
                }
                return node_ptr;
            }
        }

        // Try pool
        let idx = self.next_free_node.fetch_add(1, Ordering::Relaxed);
        if idx < self.pool_capacity {
            let node = unsafe { self.nodes_pool_ptr.add(idx) };
            unsafe {
                (*node).next.store(null_node(), Ordering::Relaxed);
                *(*node).val.get() = Some(v);
            }
            return node;
        }

        // Allocate from heap (paper line 11)
        let layout = Layout::from_size_align(std::mem::size_of::<Node<T>>(), 128).unwrap();
        let ptr = unsafe { std::alloc::alloc(layout) as *mut Node<T> };

        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        self.heap_allocs.fetch_add(1, Ordering::Relaxed);

        unsafe {
            ptr::write(
                ptr,
                Node {
                    val: UnsafeCell::new(Some(v)),
                    next: AtomicPtr::new(null_node()),
                    _padding: [0; CACHE_LINE_SIZE - 24],
                },
            );
        }

        ptr
    }

    #[inline]
    fn is_pool_node(&self, p: *mut Node<T>) -> bool {
        if p == self.base_ptr {
            return true;
        }

        if self.nodes_pool_ptr.is_null() {
            return false;
        }

        let start = self.nodes_pool_ptr as usize;
        let end = unsafe { self.nodes_pool_ptr.add(self.pool_capacity) } as usize;
        let addr = p as usize;

        addr >= start && addr < end
    }

    fn recycle_node(&self, node_to_recycle: *mut Node<T>) {
        if node_to_recycle.is_null() {
            return;
        }

        unsafe {
            // Clear the value
            *(*node_to_recycle).val.get() = None;
            (*node_to_recycle)
                .next
                .store(null_node(), Ordering::Relaxed);
        }

        // Try to cache it (paper line 23)
        if self.node_cache.push(NodePtr(node_to_recycle)).is_err() {
            // Cache full, free if heap allocated
            if !self.is_pool_node(node_to_recycle) {
                unsafe {
                    let layout =
                        Layout::from_size_align(std::mem::size_of::<Node<T>>(), 128).unwrap();
                    std::alloc::dealloc(node_to_recycle as *mut u8, layout);
                    self.heap_frees.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for DynListQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, item: T) -> Result<(), ()> {
        // Paper Figure 2, lines 8-16
        let new_node = self.alloc_node(item);

        // Get current tail
        let current_tail = self.tail.load(Ordering::Acquire);

        // Paper line 13: WMB() - ensure the node is fully initialized before linking
        fence(Ordering::Release);

        // Paper line 14: tail->next = n
        unsafe {
            (*current_tail).next.store(new_node, Ordering::Release);
        }

        // Paper line 14: tail = n
        self.tail.store(new_node, Ordering::Release);

        Ok(())
    }

    fn pop(&self) -> Result<T, ()> {
        // Paper Figure 2, lines 18-25

        let current_head = self.head.load(Ordering::Acquire);

        // Load next with acquire to synchronize with the release store in push
        let next_node = unsafe { (*current_head).next.load(Ordering::Acquire) };

        if next_node.is_null() {
            return Err(());
        }

        // Read the value - the acquire load above ensures this is safe
        let value = unsafe {
            match (*(*next_node).val.get()).take() {
                Some(v) => v,
                None => return Err(()), // Should not happen
            }
        };

        // Paper line 22: head = head->next
        self.head.store(next_node, Ordering::Release);

        // Paper line 23: if (!cache.push(n)) free(n)
        self.recycle_node(current_head);

        Ok(value)
    }

    #[inline]
    fn available(&self) -> bool {
        true
    }

    #[inline]
    fn empty(&self) -> bool {
        let h = self.head.load(Ordering::Acquire);
        unsafe { (*h).next.load(Ordering::Acquire).is_null() }
    }
}

impl<T: Send + 'static> Drop for DynListQueue<T> {
    fn drop(&mut self) {
        // Pop all remaining items
        while let Ok(item) = SpscQueue::pop(self) {
            drop(item);
        }

        unsafe {
            if self.owns_all {
                // Calculate pool bounds before we free anything
                let pool_start = self.nodes_pool_ptr as usize;
                let pool_end = if self.nodes_pool_ptr.is_null() {
                    0
                } else {
                    self.nodes_pool_ptr.add(self.pool_capacity) as usize
                };

                // Helper closure to check if a node is in the pool
                let is_in_pool = |node: *mut Node<T>| -> bool {
                    if node.is_null() || self.nodes_pool_ptr.is_null() {
                        return false;
                    }
                    let addr = node as usize;
                    addr >= pool_start && addr < pool_end
                };

                // Collect all nodes that need to be freed
                let mut heap_allocated_nodes = Vec::new();

                // Drain the node cache
                while let Ok(node_ptr) = self.node_cache.pop() {
                    if !node_ptr.0.is_null()
                        && !is_in_pool(node_ptr.0)
                        && node_ptr.0 != self.base_ptr
                    {
                        heap_allocated_nodes.push(node_ptr.0);
                    }
                }

                // Walk the linked list
                let mut current = self.head.load(Ordering::Relaxed);
                while !current.is_null() {
                    let next = (*current).next.load(Ordering::Relaxed);

                    if current != self.base_ptr && !is_in_pool(current) {
                        heap_allocated_nodes.push(current);
                    }

                    current = next;
                }

                // Free heap-allocated nodes
                for node in heap_allocated_nodes {
                    let layout =
                        Layout::from_size_align(std::mem::size_of::<Node<T>>(), 128).unwrap();
                    std::alloc::dealloc(node as *mut u8, layout);
                }

                // Free the pool
                if !self.nodes_pool_ptr.is_null() {
                    let _ = Box::from_raw(std::slice::from_raw_parts_mut(
                        self.nodes_pool_ptr,
                        self.pool_capacity,
                    ));
                }

                // Free the base dummy node
                if !self.base_ptr.is_null() {
                    let _ = Box::from_raw(self.base_ptr);
                }
            } else {
                // For shared memory, need to free heap-allocated nodes
                // but not pool nodes or base dummy

                // Calculate pool bounds
                let pool_start = self.nodes_pool_ptr as usize;
                let pool_end = if self.nodes_pool_ptr.is_null() {
                    0
                } else {
                    self.nodes_pool_ptr.add(self.pool_capacity) as usize
                };

                let is_in_pool = |node: *mut Node<T>| -> bool {
                    if node.is_null() || self.nodes_pool_ptr.is_null() {
                        return false;
                    }
                    let addr = node as usize;
                    addr >= pool_start && addr < pool_end
                };

                // Free nodes from cache that were heap-allocated
                while let Ok(node_ptr) = self.node_cache.pop() {
                    if !node_ptr.0.is_null()
                        && !is_in_pool(node_ptr.0)
                        && node_ptr.0 != self.base_ptr
                    {
                        let layout =
                            Layout::from_size_align(std::mem::size_of::<Node<T>>(), 128).unwrap();
                        std::alloc::dealloc(node_ptr.0 as *mut u8, layout);
                    }
                }

                // Walk the linked list and free heap-allocated nodes
                let mut current = self.head.load(Ordering::Relaxed);
                while !current.is_null() {
                    let next = (*current).next.load(Ordering::Relaxed);

                    // In shared memory, only free nodes that were heap-allocated
                    if !is_in_pool(current) && current != self.base_ptr {
                        let layout =
                            Layout::from_size_align(std::mem::size_of::<Node<T>>(), 128).unwrap();
                        std::alloc::dealloc(current as *mut u8, layout);
                    }

                    current = next;
                }
            }
        }
    }
}
