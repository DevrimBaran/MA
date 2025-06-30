// part of jayanti petrovic
use crate::SpscQueue;
use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

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

#[repr(C)]
pub struct SesdJpQueue<T: Send + Clone> {
    first: AtomicPtr<Node<T>>,
    last: AtomicPtr<Node<T>>,
    announce: AtomicPtr<Node<T>>,
    free_later: AtomicPtr<Node<T>>,
    help: *mut MaybeUninit<T>,
}

impl<T: Send + Clone> SesdJpQueue<T> {
    pub unsafe fn new_in_shm(
        shm_ptr_self: *mut Self,
        shm_ptr_initial_dummy_node: *mut Node<T>,
        shm_ptr_help_slot: *mut MaybeUninit<T>,
        shm_ptr_free_later_dummy: *mut Node<T>,
    ) -> &'static mut Self {
        Node::init_dummy(shm_ptr_initial_dummy_node);
        Node::init_dummy(shm_ptr_free_later_dummy);
        shm_ptr_help_slot.write(MaybeUninit::uninit());

        ptr::write(
            shm_ptr_self,
            SesdJpQueue {
                first: AtomicPtr::new(shm_ptr_initial_dummy_node),
                last: AtomicPtr::new(shm_ptr_initial_dummy_node),
                announce: AtomicPtr::new(ptr::null_mut()),
                free_later: AtomicPtr::new(shm_ptr_free_later_dummy),
                help: shm_ptr_help_slot,
            },
        );
        &mut *shm_ptr_self
    }

    pub fn enqueue2(&self, item_val: T, new_node_ptr: *mut Node<T>) {
        unsafe {
            Node::init_dummy(new_node_ptr);
            let tmp = self.last.load(Ordering::Relaxed);
            ptr::addr_of_mut!((*tmp).item).write(MaybeUninit::new(item_val));
            (*tmp).next.store(new_node_ptr, Ordering::Release);
            self.last.store(new_node_ptr, Ordering::Release);
        }
    }

    pub fn read_fronte(&self) -> Option<T> {
        unsafe {
            let tmp = self.first.load(Ordering::Acquire);
            if tmp == self.last.load(Ordering::Relaxed) {
                return None;
            }
            self.announce.store(tmp, Ordering::Release);
            if tmp != self.first.load(Ordering::Acquire) {
                let help_item_ref = (*self.help).assume_init_ref();
                Some(help_item_ref.clone())
            } else {
                let item_ref = (*tmp).item.assume_init_ref();
                Some(item_ref.clone())
            }
        }
    }

    pub fn dequeue2(&self, node_to_free_pool: &mut *mut Node<T>) -> Option<T> {
        unsafe {
            let tmp = self.first.load(Ordering::Relaxed);
            if tmp == self.last.load(Ordering::Acquire) {
                *node_to_free_pool = ptr::null_mut();
                return None;
            }

            let retval = (*(*tmp).item.as_ptr()).clone();
            self.help.write(MaybeUninit::new(retval.clone()));
            let next_ptr = (*tmp).next.load(Ordering::Acquire);

            if next_ptr.is_null() {
                *node_to_free_pool = ptr::null_mut();
                return None;
            }

            self.first.store(next_ptr, Ordering::Release);

            if tmp == self.announce.load(Ordering::Acquire) {
                let tmp_prime = self.free_later.swap(tmp, Ordering::AcqRel);
                *node_to_free_pool = tmp_prime;
            } else {
                *node_to_free_pool = tmp;
            }

            Some(retval)
        }
    }

    pub fn read_frontd(&self) -> Option<T> {
        unsafe {
            let tmp = self.first.load(Ordering::Relaxed);
            if tmp == self.last.load(Ordering::Acquire) {
                None
            } else {
                let item_ref = (*tmp).item.assume_init_ref();
                Some(item_ref.clone())
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct SesdPushError;

#[derive(Debug, PartialEq, Eq)]
pub struct SesdPopError;
