use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering, AtomicUsize};
use std::mem::MaybeUninit;

use crate::MpscQueue;

#[repr(C)]
struct Node<T> {
   item: MaybeUninit<T>,
   next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
   // Initializes a node in shared memory with an item.
   fn new_in_shm(item_val: T, shm_node_ptr: *mut Self) {
      unsafe {
         ptr::addr_of_mut!((*shm_node_ptr).item).write(MaybeUninit::new(item_val));
         let atomic_next_ptr = ptr::addr_of_mut!((*shm_node_ptr).next);
         (*atomic_next_ptr).store(ptr::null_mut(), Ordering::Relaxed);
      }
   }

   // Helper to initialize a dummy node in shared memory.
   fn new_dummy_in_shm(shm_node_ptr: *mut Self) {
      unsafe {
         ptr::addr_of_mut!((*shm_node_ptr).item).write(MaybeUninit::uninit());
         let atomic_next_ptr = ptr::addr_of_mut!((*shm_node_ptr).next);
         (*atomic_next_ptr).store(ptr::null_mut(), Ordering::Relaxed);
      }
   }
}

#[repr(C)]
pub struct DrescherQueue<T: Send + 'static> {
   head: AtomicPtr<Node<T>>,
   tail: AtomicPtr<Node<T>>,
   dummy_node: *mut Node<T>,
   node_pool_start: *mut Node<T>,
   node_pool_capacity: usize,
   next_free_node_idx: AtomicUsize, // For the bump allocator from the pool
}

unsafe impl<T: Send + 'static> Sync for DrescherQueue<T> {}
unsafe impl<T: Send + 'static> Send for DrescherQueue<T> {}

impl<T: Send + 'static> DrescherQueue<T> {
   pub fn shared_size(node_capacity: usize) -> usize {
      std::mem::size_of::<Self>() +
      std::mem::size_of::<Node<T>>() + // For the dummy node
      (node_capacity * std::mem::size_of::<Node<T>>()) // For the node pool
   }

   pub unsafe fn init_in_shared(mem_ptr: *mut u8, node_capacity: usize) -> &'static mut Self {
      let queue_ptr = mem_ptr as *mut Self;
      
      // Memory for the dummy node follows the queue struct
      let dummy_node_ptr = mem_ptr.add(std::mem::size_of::<Self>()) as *mut Node<T>;
      Node::<T>::new_dummy_in_shm(dummy_node_ptr);

      // Memory for the node pool follows the dummy node
      let node_pool_start_ptr = (dummy_node_ptr as *mut u8).add(std::mem::size_of::<Node<T>>()) as *mut Node<T>;

      ptr::write(queue_ptr, Self {
         head: AtomicPtr::new(dummy_node_ptr),
         tail: AtomicPtr::new(dummy_node_ptr),
         dummy_node: dummy_node_ptr,
         node_pool_start: node_pool_start_ptr,
         node_pool_capacity: node_capacity,
         next_free_node_idx: AtomicUsize::new(0),
      });

      &mut *queue_ptr
   }

   // Helper to allocate a raw node pointer from the shared pool.
   // Initialization happens after allocation.
   fn alloc_raw_node_from_pool(&self) -> Option<*mut Node<T>> {
      let current_idx = self.next_free_node_idx.fetch_add(1, Ordering::Relaxed);
      if current_idx < self.node_pool_capacity {
         let node_ptr = unsafe { self.node_pool_start.add(current_idx) };
         Some(node_ptr)
      } else {
         // Rollback if pool is full to prevent overflow and allow future retries if items are freed.
         self.next_free_node_idx.fetch_sub(1, Ordering::Relaxed); 
         None
      }
   }

   // Enqueue (Push) - For Multiple Producers
   // Based on Fig 4(b) from the paper
   pub fn push(&self, item_val: T) -> Result<(), T> {
      let new_node_ptr = match self.alloc_raw_node_from_pool() {
         Some(ptr) => ptr,
         None => return Err(item_val), // Cannot allocate node from pool
      };

      // Initialize the newly allocated node with the item.
      Node::new_in_shm(item_val, new_node_ptr); // item_val is moved here.

      // Fig 4(b), line 3: prev <- FAS(tail, item)
      // Atomically sets self.tail to new_node_ptr and returns the previous self.tail.
      let prev_tail_ptr = self.tail.swap(new_node_ptr, Ordering::AcqRel);

      // Fig 4(b), line 4: prev.next <- item (new_node_ptr)
      // Link the previous tail node to the new node.
      // This operation is safe because 'prev_tail_ptr' was uniquely obtained by this producer
      // from the atomic 'swap' on 'tail'. Only this producer will write to '(*prev_tail_ptr).next'
      // to link the new node. The dummy node ensures prev_tail_ptr is never null.
      unsafe {
         (*prev_tail_ptr).next.store(new_node_ptr, Ordering::Release);
      }
      Ok(())
   }

   // Dequeue (Pop) - For Single Consumer
   // Based on Fig 4(c) from the paper
   pub fn pop(&self) -> Option<T> {
      let current_head_node_ptr = self.head.load(Ordering::Relaxed); // Fig4 line 2: item <- head
      let mut next_node_ptr = unsafe { (*current_head_node_ptr).next.load(Ordering::Acquire) }; // Fig4 line 3: next <- head.next

      // Fig4 line 4-6: if next = 0 then return 0 (empty queue)
      if next_node_ptr.is_null() {
         return None;
      }

      // At this point, `next_node_ptr` points to the node containing the item to be dequeued,
      // or it's the first actual item if `current_head_node_ptr` was the dummy.

      if current_head_node_ptr == self.dummy_node { // Fig4 line 8: if item (current_head_node_ptr) = ref dummy
         // The dummy node was the current head. The item to return is in `next_node_ptr`.
         let node_with_data_ptr = next_node_ptr;

         // Fig4 line 9: ENQUEUE(item) -> ENQUEUE(dummy_node)
         // Re-enqueue the dummy node:
         // 1. Dummy's next should be null before it becomes the new tail.
         unsafe { (*self.dummy_node).next.store(ptr::null_mut(), Ordering::Relaxed); }
         // 2. Atomically swap tail to dummy_node, getting the node that was previously tail.
         let prev_tail_before_dummy_requeue = self.tail.swap(self.dummy_node, Ordering::AcqRel);
         // 3. Link that previous tail to the dummy_node.
         unsafe {
            (*prev_tail_before_dummy_requeue).next.store(self.dummy_node, Ordering::Release);
         }
         
         // Fig4 line 13: head <- head.next
         // The `head` variable in the paper at this stage (after line 7 which conceptually happened)
         // would refer to `node_with_data_ptr`. So `head.next` is `(*node_with_data_ptr).next`.
         // This is the new head for the queue structure.
         let new_actual_head_ptr = unsafe { (*node_with_data_ptr).next.load(Ordering::Acquire) };

         if new_actual_head_ptr.is_null() {
            // The item we are returning (`node_with_data_ptr`) was the last actual item.
            // The queue will now only contain the dummy node. So, head should point to dummy.
            self.head.store(self.dummy_node, Ordering::Relaxed);
         } else {
            // There are more actual items after the one we are returning.
            self.head.store(new_actual_head_ptr, Ordering::Relaxed);
         }

         // Fig4 line 14: return next (the value from `node_with_data_ptr`)
         let item_val = unsafe { ptr::read(&(*node_with_data_ptr).item).assume_init() };
         // Note: The node `node_with_data_ptr` can now be considered "freed" or returned to a pool.
         // The simple bump allocator in `alloc_raw_node_from_pool` doesn't support freeing.
         // For a production queue, you'd need a proper shared memory allocator.
         Some(item_val)

      } else { // Fig4 line 15: else (current_head_node_ptr was a data node, not the dummy)
         // The item to return is in `current_head_node_ptr`.
         // The new head of the queue is `next_node_ptr` (already loaded).
         // Fig4 line 7: head <- next
         self.head.store(next_node_ptr, Ordering::Relaxed);
         
         let item_val = unsafe { ptr::read(&(*current_head_node_ptr).item).assume_init() };
         // Note: The node `current_head_node_ptr` can now be considered "freed".
         Some(item_val) // Fig4 line 16: return item
      }
   }

   // Check if the queue is empty of actual items.
   // Based on Fig 4(d) from the paper
   pub fn is_empty(&self) -> bool {
      let head_ptr = self.head.load(Ordering::Acquire);
      // The queue is empty if the head is the dummy node AND the dummy node's next is null.
      // Or, more directly from paper: if head.next is null (where head might be dummy or an item just before dummy was re-enqueued)
      // A consistent view is that if head points to dummy, and dummy.next is null, queue is empty.
      if head_ptr == self.dummy_node {
         return unsafe { (*head_ptr).next.load(Ordering::Acquire).is_null() };
      }
      // If head is not dummy, it means there's at least one item (the one head points to, unless pop logic is flawed).
      // However, the paper's EMPTY is just `head.next == 0`.
      // Given the pop logic, if head points to dummy and dummy.next is null, it's empty.
      // If head points to the last actual item, its next will be the dummy, which is not null.
      // The paper's `EMPTY` seems to be `return head.next == 0`.
      // Let's stick to the paper's direct definition:
      unsafe {
         (*head_ptr).next.load(Ordering::Acquire).is_null()
      }
   }
}

impl<T: Send + 'static> MpscQueue<T> for DrescherQueue<T> {
   type PushError = T; // DrescherQueue's push returns Result<(), T>
   type PopError = ();   // DrescherQueue's pop returns Option<T>, maps to Result<T, ()>

   fn push(&self, item: T) -> Result<(), Self::PushError> {
      // This calls the inherent method `push` you've already defined
      self.push(item)
   }

   fn pop(&self) -> Result<T, Self::PopError> {
      // This calls the inherent method `pop`
      self.pop().ok_or(()) // Convert Option<T> to Result<T, ()>
   }

   fn is_empty(&self) -> bool {
      // This calls the inherent method `is_empty`
      self.is_empty()
   }

   fn is_full(&self) -> bool {
      // For DrescherQueue, "full" means the node pool is exhausted.
      // The `alloc_raw_node_from_pool` checks this.
      // We can check the current state of `next_free_node_idx`.
      // This is a best-effort check, as another producer might take the last slot
      // between this check and an actual push.
      self.next_free_node_idx.load(Ordering::Relaxed) >= self.node_pool_capacity
   }
}