use loom::sync::Arc;
use loom::thread;
use loom::model;

use std::collections::HashSet;

use queues::mpsc::drescher_queue::DrescherQueue; // Adjust this path

const NUM_PRODUCERS: usize = 2;
const ITEMS_PER_PRODUCER: usize = 3;
const TOTAL_ITEMS: usize = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
const NODE_POOL_CAPACITY: usize = TOTAL_ITEMS * 2;

#[test]
fn loom_drescher_mpsc_no_loss() {
   model(|| {

      let queue_shared_mem_size = DrescherQueue::<usize>::shared_size(NODE_POOL_CAPACITY);
      let mut queue_mem: Vec<u8> = vec![0; queue_shared_mem_size];
      let queue_mem_ptr = queue_mem.as_mut_ptr();

      let queue_static_ref: &'static mut DrescherQueue<usize> = unsafe {
         let q_mut_ref = DrescherQueue::init_in_shared(queue_mem_ptr, NODE_POOL_CAPACITY);
         std::mem::transmute::<&mut DrescherQueue<usize>, &'static mut DrescherQueue<usize>>(q_mut_ref)
      };
      
      let queue_arc = Arc::new(unsafe {
         std::ptr::NonNull::from(queue_static_ref)
      });


      let mut producers = Vec::new();

      for p_id in 0..NUM_PRODUCERS {
         let queue_clone = queue_arc.clone();
         producers.push(thread::spawn(move || {
               let q_ptr = queue_clone.as_ptr(); 
               let queue: &DrescherQueue<usize> = unsafe { &*q_ptr }; 

               for i in 0..ITEMS_PER_PRODUCER {
                  let item = p_id * ITEMS_PER_PRODUCER + i;
                  loop {
                     match queue.push(item) {
                           Ok(_) => {
                              break;
                           }
                           Err(_) => {
                              thread::yield_now(); 
                           }
                     }
                  }
               }
         }));
      }

      let consumer_queue_arc = queue_arc;
      let consumer = thread::spawn(move || {
         let q_ptr = consumer_queue_arc.as_ptr();
         let queue: &DrescherQueue<usize> = unsafe { &*q_ptr };

         let mut received_items = HashSet::new();
         let mut local_received_count = 0;

         for _ in 0..TOTAL_ITEMS {
               loop {
                  match queue.pop() {
                     Some(item) => {
                           assert!(received_items.insert(item), "Consumer: Duplicate item popped: {}", item);
                           local_received_count += 1;
                           break;
                     }
                     None => {
                           thread::yield_now();
                     }
                  }
               }
         }
         for _ in 0..5 {
               if queue.pop().is_some() {
                  panic!("Consumer: Popped more than TOTAL_ITEMS");
               }
               thread::yield_now();
         }

         assert!(queue.is_empty(), "Consumer: Queue should be empty after all items popped");
         
         (received_items, local_received_count)
      });

      for p in producers {
         p.join().unwrap();
      }

      let (mut final_received_items, final_received_count) = consumer.join().unwrap();

      assert_eq!(final_received_count, TOTAL_ITEMS, "Data loss: Not all items were consumed.");

      for p_id in 0..NUM_PRODUCERS {
         for i in 0..ITEMS_PER_PRODUCER {
               let expected_item = p_id * ITEMS_PER_PRODUCER + i;
               assert!(final_received_items.remove(&expected_item), "Expected item {} was not received", expected_item);
         }
      }
      assert!(final_received_items.is_empty(), "Extra items received: {:?}", final_received_items);
   });
}