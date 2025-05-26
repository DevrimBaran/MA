use crate::spsc::lamport::LamportQueue;
use crate::SpscQueue;
use std::{
    alloc::Layout,
    ptr::{self, null_mut},
    sync::atomic::{fence, AtomicPtr, AtomicUsize, Ordering},
};

#[inline(always)]
const fn null_node<T: Send>() -> *mut Node<T> {
    null_mut()
}

// Fixed cache line size - actual size on x86_64
const CACHE_LINE_SIZE: usize = 64;

#[repr(C, align(128))]
struct Node<T: Send + 'static> {
    val: Option<T>,
    next: AtomicPtr<Node<T>>,
    _padding: [u8; CACHE_LINE_SIZE - 24], // 24 = size of val + next on 64-bit
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug)]
struct NodePtr<U: Send + 'static>(*mut Node<U>);

unsafe impl<U: Send + 'static> Send for NodePtr<U> {}
unsafe impl<U: Send + 'static> Sync for NodePtr<U> {}

#[repr(C, align(128))]
pub struct DynListQueue<T: Send + 'static> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,

    padding1: [u8; 128 - 16], // 16 = 2 pointers

    nodes_pool_ptr: *mut Node<T>,
    next_free_node: AtomicUsize,

    padding2: [u8; 128 - 16], // 16 = 1 pointer + 1 atomic

    node_cache: LamportQueue<NodePtr<T>>,

    base_ptr: *mut Node<T>,
    pool_capacity: usize,
    cache_capacity: usize,
    owns_all: bool,

    heap_allocs: AtomicUsize,
    heap_frees: AtomicUsize,
}

unsafe impl<T: Send> Send for DynListQueue<T> {}
unsafe impl<T: Send> Sync for DynListQueue<T> {}

impl<T: Send + 'static> DynListQueue<T> {
    pub fn shared_size(capacity: usize) -> usize {
        // Calculate sizes based on capacity
        let preallocated_nodes = capacity / 2; // Half of ring cap for pre-allocated
        let node_cache_capacity = capacity; // Full ring cap for cache

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
            val: None,
            next: AtomicPtr::new(null_node()),
            _padding: [0; CACHE_LINE_SIZE - 24],
        }));

        let mut pool_nodes_vec: Vec<Node<T>> = Vec::with_capacity(preallocated_nodes);
        for _ in 0..preallocated_nodes {
            pool_nodes_vec.push(Node {
                val: None,
                next: AtomicPtr::new(null_node()),
                _padding: [0; CACHE_LINE_SIZE - 24],
            });
        }
        let pool_ptr = Box::into_raw(pool_nodes_vec.into_boxed_slice()) as *mut Node<T>;

        let node_cache = LamportQueue::<NodePtr<T>>::with_capacity(node_cache_capacity);

        Self {
            head: AtomicPtr::new(dummy),
            tail: AtomicPtr::new(dummy),
            padding1: [0; 128 - 16],
            base_ptr: dummy,
            nodes_pool_ptr: pool_ptr,
            next_free_node: AtomicUsize::new(0),
            padding2: [0; 128 - 16],
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
                val: None,
                next: AtomicPtr::new(null_node()),
                _padding: [0; CACHE_LINE_SIZE - 24],
            },
        );

        let pool_nodes_ptr_val = mem_ptr.add(offset_pool_array) as *mut Node<T>;

        for i in 0..preallocated_nodes {
            ptr::write(
                pool_nodes_ptr_val.add(i),
                Node {
                    val: None,
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
                padding1: [0; 128 - 16],
                base_ptr: dummy_ptr_val,
                nodes_pool_ptr: pool_nodes_ptr_val,
                next_free_node: AtomicUsize::new(0),
                padding2: [0; 128 - 16],
                node_cache: ptr::read(initialized_node_cache_ref as *const _),
                pool_capacity: preallocated_nodes,
                cache_capacity: node_cache_capacity,
                owns_all: false,
                heap_allocs: AtomicUsize::new(0),
                heap_frees: AtomicUsize::new(0),
            },
        );

        fence(Ordering::SeqCst);

        &mut *self_ptr
    }

    fn alloc_node(&self, v: T) -> *mut Node<T> {
        for _ in 0..3 {
            if let Ok(node_ptr_wrapper) = self.node_cache.pop() {
                let node_ptr = node_ptr_wrapper.0;
                if !node_ptr.is_null() {
                    unsafe {
                        ptr::write(&mut (*node_ptr).val, Some(v));
                        (*node_ptr).next.store(null_node(), Ordering::SeqCst);
                    }
                    return node_ptr;
                }
            }

            std::hint::spin_loop();
        }

        let idx = self.next_free_node.fetch_add(1, Ordering::SeqCst);
        if idx < self.pool_capacity {
            let node = unsafe { self.nodes_pool_ptr.add(idx) };

            unsafe {
                ptr::write(&mut (*node).val, Some(v));
                (*node).next.store(null_node(), Ordering::SeqCst);
            }
            return node;
        }

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
                    val: Some(v),
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
            if let Some(val) = ptr::replace(&mut (*node_to_recycle).val, None) {
                drop(val);
            }
            (*node_to_recycle).next.store(null_node(), Ordering::SeqCst);
        }
        if self.is_pool_node(node_to_recycle) {
            let _ = self.node_cache.push(NodePtr(node_to_recycle));
        } else {
            unsafe {
                let layout = Layout::from_size_align(std::mem::size_of::<Node<T>>(), 128).unwrap();
                std::alloc::dealloc(node_to_recycle as *mut u8, layout);
                self.heap_frees.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl<T: Send + 'static> SpscQueue<T> for DynListQueue<T> {
    type PushError = ();
    type PopError = ();

    fn push(&self, item: T) -> Result<(), ()> {
        let new_node = self.alloc_node(item);

        fence(Ordering::SeqCst);

        let current_tail_ptr = self.tail.load(Ordering::SeqCst);

        if current_tail_ptr.is_null() {
            return Err(());
        }

        unsafe {
            (*current_tail_ptr).next.store(new_node, Ordering::SeqCst);
        }

        fence(Ordering::SeqCst);

        self.tail.store(new_node, Ordering::SeqCst);

        Ok(())
    }

    fn pop(&self) -> Result<T, ()> {
        let current_dummy_ptr = self.head.load(Ordering::SeqCst);

        if current_dummy_ptr.is_null() {
            return Err(());
        }

        fence(Ordering::SeqCst);

        let item_node_ptr = unsafe { (*current_dummy_ptr).next.load(Ordering::SeqCst) };

        if item_node_ptr.is_null() {
            return Err(());
        }

        let value = unsafe {
            if item_node_ptr.is_null() {
                return Err(());
            }

            if let Some(value) = ptr::replace(&mut (*item_node_ptr).val, None) {
                value
            } else {
                return Err(());
            }
        };

        fence(Ordering::SeqCst);

        self.head.store(item_node_ptr, Ordering::SeqCst);

        fence(Ordering::SeqCst);

        self.recycle_node(current_dummy_ptr);

        Ok(value)
    }

    #[inline]
    fn available(&self) -> bool {
        true
    }

    #[inline]
    fn empty(&self) -> bool {
        let h = self.head.load(Ordering::SeqCst);

        if h.is_null() {
            return true;
        }

        unsafe { (*h).next.load(Ordering::SeqCst).is_null() }
    }
}

impl<T: Send + 'static> Drop for DynListQueue<T> {
    fn drop(&mut self) {
        if self.owns_all {
            while let Ok(item) = SpscQueue::pop(self) {
                drop(item);
            }

            unsafe {
                while let Ok(node_ptr) = self.node_cache.pop() {
                    if !node_ptr.0.is_null() && !self.is_pool_node(node_ptr.0) {
                        ptr::drop_in_place(&mut (*node_ptr.0).val);
                        let layout =
                            Layout::from_size_align(std::mem::size_of::<Node<T>>(), 128).unwrap();
                        std::alloc::dealloc(node_ptr.0 as *mut u8, layout);
                    }
                }

                ptr::drop_in_place(&mut self.node_cache.buf);
            }

            unsafe {
                if !self.nodes_pool_ptr.is_null() {
                    for i in 0..self.pool_capacity {
                        let node = self.nodes_pool_ptr.add(i);
                        ptr::drop_in_place(&mut (*node).val);
                    }

                    let _ = Box::from_raw(std::slice::from_raw_parts_mut(
                        self.nodes_pool_ptr,
                        self.pool_capacity,
                    ));
                }

                if !self.base_ptr.is_null() {
                    if self.head.load(Ordering::Relaxed) == self.base_ptr {
                        ptr::drop_in_place(&mut (*self.base_ptr).val);
                        let _ = Box::from_raw(self.base_ptr);
                    }
                }
            }
        }
    }
}
