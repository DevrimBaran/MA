// File: queues/src/mpsc/sesd_jp_queue.rs
#![allow(dead_code)] // Remove this once integrated

use std::ptr;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicPtr, Ordering, fence};

// Represents a node in the SESD queue's linked list.
#[repr(C)]
pub struct Node<T: Send + Clone> {
    pub(super) item: MaybeUninit<T>, // Item is stored here; T is (ActualItem, Timestamp)
    pub(super) next: AtomicPtr<Node<T>>,
}

impl<T: Send + Clone> Node<T> {
    // Helper to initialize a node (typically a dummy node)
    pub unsafe fn init_dummy(node_ptr: *mut Self) {
        ptr::addr_of_mut!((*node_ptr).item).write(MaybeUninit::uninit());
        (*ptr::addr_of_mut!((*node_ptr).next)).store(ptr::null_mut(), Ordering::Relaxed);
    }
}

/// A wait-free Single-Enqueuer Single-Dequeuer queue.
/// Based on Figure 2 from "Logarithmic-Time Single Deleter, Multiple Inserter Wait-Free Queues and Stacks"
/// by Jayanti and Petrovic.
///
/// `T` is expected to be `(ActualItem, Timestamp)` tuple.
#[repr(C)]
pub struct SesdJpQueue<T: Send + Clone> {
    // Pointers to the first and last nodes in the linked list.
    // `first` is accessed by the dequeuer.
    // `last` is accessed by the enqueuer.
    first: AtomicPtr<Node<T>>,
    last: AtomicPtr<Node<T>>,

    // Coordination variables between enqueuer (E) and dequeuer (D)
    // for `readFronte` and `dequeue2`.
    // `announce_enq`: E writes `first` here before reading `first.item`. D reads it.
    announce_enq: AtomicPtr<Node<T>>,
    // `help_deq_slot`: D writes `first.item` here if `announce_enq == first` during dequeue. E reads it.
    // This points to a dedicated shared memory slot for the item.
    help_deq_slot: *mut MaybeUninit<T>,
    // `free_later_deq`: D stores a node here if it cannot be freed immediately
    // because `announce_enq` pointed to it. Freed on the *next* dequeue.
    free_later_deq: AtomicPtr<Node<T>>,
}

impl<T: Send + Clone> SesdJpQueue<T> {
    /// Initializes a new `SesdJpQueue` in the provided shared memory locations.
    ///
    /// # Safety
    /// All pointers must be valid, correctly aligned, and point to distinct,
    /// writable memory regions within the shared memory segment.
    /// `shm_ptr_self`: Pointer to where the SesdJpQueue struct itself will be stored.
    /// `shm_ptr_initial_dummy_node`: Pointer to memory for the initial dummy node.
    /// `shm_ptr_help_deq_slot`: Pointer to memory for the `help_deq_slot` parameter.
    /// `shm_ptr_free_later_node_dummy`: Pointer to memory for a dummy node for `free_later_deq`.
    pub unsafe fn new_in_shm(
        shm_ptr_self: *mut Self,
        shm_ptr_initial_dummy_node: *mut Node<T>,
        shm_ptr_help_deq_slot: *mut MaybeUninit<T>, // Parameter name
        shm_ptr_free_later_node_dummy: *mut Node<T>,
    ) -> &'static mut Self {
        // Initialize the initial dummy node for the queue
        Node::init_dummy(shm_ptr_initial_dummy_node);

        // Initialize the dummy node for free_later_deq (it will hold no meaningful item)
        Node::init_dummy(shm_ptr_free_later_node_dummy);

        // Initialize the memory pointed to by shm_ptr_help_deq_slot as uninitialized
        shm_ptr_help_deq_slot.write(MaybeUninit::uninit());

        // Write the SesdJpQueue struct itself
        ptr::write(shm_ptr_self, SesdJpQueue {
            first: AtomicPtr::new(shm_ptr_initial_dummy_node),
            last: AtomicPtr::new(shm_ptr_initial_dummy_node),
            announce_enq: AtomicPtr::new(ptr::null_mut()), // Initially null
            help_deq_slot: shm_ptr_help_deq_slot, // CORRECTED: Use the parameter directly
            free_later_deq: AtomicPtr::new(shm_ptr_free_later_node_dummy), // Initialize with a dummy node
        });

        &mut *shm_ptr_self
    }

    /// Enqueues an item. Called only by the designated enqueuer process.
    /// `new_node_ptr` must be a pointer to a fresh node allocated from a shared pool,
    /// which will become the new dummy node.
    pub fn enqueue2(&self, item_val: T, new_node_ptr: *mut Node<T>) {
        unsafe {
            // Initialize the new node (it will be the new dummy)
            Node::init_dummy(new_node_ptr);

            // Get the current last node (which is the current dummy)
            let old_last_ptr = self.last.load(Ordering::Relaxed); // E owns Last

            // Store the item in the current dummy node's item field
            ptr::addr_of_mut!((*old_last_ptr).item).write(MaybeUninit::new(item_val));

            // Link the new dummy node after the current dummy node
            (*old_last_ptr).next.store(new_node_ptr, Ordering::Release); // Make item visible before next

            // Swing Last to point to the new dummy node
            self.last.store(new_node_ptr, Ordering::Release);
        }
    }

    /// Reads the front element of the queue without dequeuing.
    /// Called only by the designated enqueuer process.
    pub fn read_fronte(&self) -> Option<T> {
        unsafe {
            let first_ptr = self.first.load(Ordering::Acquire); // D might change First
            let last_ptr = self.last.load(Ordering::Relaxed); // E owns Last

            if first_ptr == last_ptr { // Queue is empty (only dummy node)
                return None;
            }

            // Announce interest in the current first node
            self.announce_enq.store(first_ptr, Ordering::Release);

            // Ensure the announcement is visible before re-reading `first`.
            fence(Ordering::SeqCst);

            let current_first_ptr_after_announce = self.first.load(Ordering::Acquire);

            if current_first_ptr_after_announce == first_ptr {
                // Dequeuer hasn't dequeued `first_ptr` yet.
                // Safe to read `first_ptr.item`. The dequeuer will see `announce_enq`
                // and use `self.help_deq_slot` if it dequeues this node.
                let item_ref = (*first_ptr).item.assume_init_ref();
                Some(item_ref.clone())
            } else {
                // Dequeuer has acted. Read from `self.help_deq_slot`.
                let help_item_ref = (*self.help_deq_slot).assume_init_ref();
                Some(help_item_ref.clone())
            }
        }
    }

    /// Dequeues an item from the front of the queue.
    /// Called only by the designated dequeuer process.
    /// `node_to_free_pool`: A mutable reference to a pointer where the dequeued node
    /// (that can be freed) will be placed. The caller is responsible for freeing it.
    pub fn dequeue2(&self, node_to_free_pool: &mut *mut Node<T>) -> Option<T> {
        unsafe {
            let first_ptr = self.first.load(Ordering::Relaxed); // D owns First
            let last_ptr = self.last.load(Ordering::Acquire); // E might change Last

            if first_ptr == last_ptr { // Queue is empty
                *node_to_free_pool = ptr::null_mut();
                return None;
            }

            // Get the next node in the list
            let next_ptr = (*first_ptr).next.load(Ordering::Acquire); // E links this

            // Read the item from the current first node.
            let item_val = (*(*first_ptr).item.as_ptr()).clone(); // Must be initialized if not dummy

            // Check if the enqueuer is currently interested in `first_ptr` via `readFronte`.
            let announced_node_ptr = self.announce_enq.load(Ordering::Acquire);

            if announced_node_ptr == first_ptr {
                // Enqueuer is interested. Copy item to `help_deq_slot`.
                self.help_deq_slot.write(MaybeUninit::new(item_val.clone()));
                fence(Ordering::Release); // Ensure help_deq write is visible

                // Defer freeing `first_ptr`. Store it in `free_later_deq`.
                // The node previously in `free_later_deq` can now be freed.
                let previously_in_free_later = self.free_later_deq.swap(first_ptr, Ordering::AcqRel);
                *node_to_free_pool = previously_in_free_later;
            } else {
                // Enqueuer is not interested. `first_ptr` can be freed.
                *node_to_free_pool = first_ptr;
            }

            // Advance `first` pointer to the next node.
            self.first.store(next_ptr, Ordering::Release);

            Some(item_val)
        }
    }

    /// Reads the front element of the queue without dequeuing.
    /// Called only by the designated dequeuer process.
    pub fn read_frontd(&self) -> Option<T> {
        unsafe {
            let first_ptr = self.first.load(Ordering::Relaxed); // D owns First
            if first_ptr == self.last.load(Ordering::Acquire) { // E might change Last
                None // Empty
            } else {
                // Safe to read item because this is the dequeuer's own view.
                let item_ref = (*first_ptr).item.assume_init_ref();
                Some(item_ref.clone())
            }
        }
    }
}
