// part of jayanti petrovic - Section 2 implementation
use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

// Node structure - Figure 2 in paper
#[repr(C)]
pub struct Node<T: Send + Clone> {
    pub item: MaybeUninit<T>,
    pub next: AtomicPtr<Node<T>>,
}

impl<T: Send + Clone> Node<T> {
    pub unsafe fn init_dummy(node_ptr: *mut Self) {
        ptr::addr_of_mut!((*node_ptr).item).write(MaybeUninit::uninit());
        (*ptr::addr_of_mut!((*node_ptr).next)).store(ptr::null_mut(), Ordering::Relaxed);
    }
}

// Single Enqueuer Single Dequeuer Queue - Figure 2 in paper
#[repr(C)]
pub struct SesdJpQueue<T: Send + Clone> {
    first: AtomicPtr<Node<T>>,      // First in Figure 2
    last: AtomicPtr<Node<T>>,       // Last in Figure 2
    announce: AtomicPtr<Node<T>>,   // Announce in Figure 2
    free_later: AtomicPtr<Node<T>>, // FreeLater in Figure 2
    help: *mut MaybeUninit<T>,      // Help in Figure 2
}

impl<T: Send + Clone> SesdJpQueue<T> {
    // IPC adaptation - initialize in shared memory
    pub unsafe fn new_in_shm(
        shm_ptr_self: *mut Self,
        shm_ptr_initial_dummy_node: *mut Node<T>,
        shm_ptr_help_slot: *mut MaybeUninit<T>,
        shm_ptr_free_later_dummy: *mut Node<T>,
    ) -> &'static mut Self {
        // Initialization section in Figure 2
        Node::init_dummy(shm_ptr_initial_dummy_node);
        Node::init_dummy(shm_ptr_free_later_dummy);
        shm_ptr_help_slot.write(MaybeUninit::uninit());

        ptr::write(
            shm_ptr_self,
            SesdJpQueue {
                first: AtomicPtr::new(shm_ptr_initial_dummy_node), // First = Last = new Node()
                last: AtomicPtr::new(shm_ptr_initial_dummy_node),
                announce: AtomicPtr::new(ptr::null_mut()),
                free_later: AtomicPtr::new(shm_ptr_free_later_dummy), // FreeLater = new Node()
                help: shm_ptr_help_slot,
            },
        );
        &mut *shm_ptr_self
    }

    // enqueue(v) - Lines 1-5 in Figure 2
    pub fn enqueue2(&self, item_val: T, new_node_ptr: *mut Node<T>) {
        unsafe {
            Node::init_dummy(new_node_ptr); // Line 1: newNode = new Node()
            let tmp = self.last.load(Ordering::Relaxed); // Line 2: tmp = Last
            ptr::addr_of_mut!((*tmp).item).write(MaybeUninit::new(item_val)); // Line 3: tmp.val = v
            (*tmp).next.store(new_node_ptr, Ordering::Release); // Line 4: tmp.next = newNode
            self.last.store(new_node_ptr, Ordering::Release); // Line 5: Last = newNode
        }
    }

    // readFronte() - Lines 17-23 in Figure 2
    pub fn read_fronte(&self) -> Option<T> {
        unsafe {
            let tmp = self.first.load(Ordering::Acquire); // Line 17: tmp = First
            if tmp == self.last.load(Ordering::Relaxed) {
                // Line 18: if (tmp == Last)
                return None; // return ⊥
            }
            self.announce.store(tmp, Ordering::Release); // Line 19: Announce = tmp
            if tmp != self.first.load(Ordering::Acquire) {
                // Line 20: if (tmp ≠ First)
                let help_item_ref = (*self.help).assume_init_ref();
                Some(help_item_ref.clone()) // Line 21: retval = Help
            } else {
                let item_ref = (*tmp).item.assume_init_ref();
                Some(item_ref.clone()) // Line 22: else retval = tmp.val
            }
        }
    }

    // dequeue() - Lines 6-16 in Figure 2
    pub fn dequeue2(&self, node_to_free_pool: &mut *mut Node<T>) -> Option<T> {
        unsafe {
            let tmp = self.first.load(Ordering::Relaxed); // Line 6: tmp = First
            if tmp == self.last.load(Ordering::Acquire) {
                // Line 7: if (tmp == Last)
                *node_to_free_pool = ptr::null_mut();
                return None; // return ⊥
            }

            let retval = (*(*tmp).item.as_ptr()).clone(); // Line 8: retval = tmp.val
            self.help.write(MaybeUninit::new(retval.clone())); // Line 9: Help = retval
            let next_ptr = (*tmp).next.load(Ordering::Acquire);

            if next_ptr.is_null() {
                *node_to_free_pool = ptr::null_mut();
                return None;
            }

            self.first.store(next_ptr, Ordering::Release); // Line 10: First = tmp.next

            if tmp == self.announce.load(Ordering::Acquire) {
                // Line 11: if (tmp == Announce)
                let tmp_prime = self.free_later.swap(tmp, Ordering::AcqRel); // Line 12: tmp' = FreeLater
                *node_to_free_pool = tmp_prime; // Line 13: FreeLater = tmp
            } else {
                *node_to_free_pool = tmp; // Line 15: else free(tmp)
            }

            Some(retval) // Line 16: return retval
        }
    }

    // readFrontd() - Lines 24-26 in Figure 2
    pub fn read_frontd(&self) -> Option<T> {
        unsafe {
            let tmp = self.first.load(Ordering::Relaxed); // Line 24: tmp = First
            if tmp == self.last.load(Ordering::Acquire) {
                // Line 25: if (tmp == Last)
                None // return ⊥
            } else {
                let item_ref = (*tmp).item.assume_init_ref();
                Some(item_ref.clone()) // Line 26: return tmp.val
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SesdPushError;

#[derive(Debug, PartialEq, Eq)]
pub struct SesdPopError;
