use queues::{MpscQueue, mpsc::*};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};

// Import BenchMpscQueue trait
trait BenchMpscQueue<T: Send>: Send + Sync + 'static {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()>;
    fn bench_pop(&self) -> Result<T, ()>;
    fn bench_is_empty(&self) -> bool;
    fn bench_is_full(&self) -> bool;
}

// Implement BenchMpscQueue for DrescherQueue
impl<T: Send + 'static> BenchMpscQueue<T> for DrescherQueue<T> {
    fn bench_push(&self, item: T, _producer_id: usize) -> Result<(), ()> {
        self.push(item).map_err(|_| ())
    }
    
    fn bench_pop(&self) -> Result<T, ()> {
        self.pop().ok_or(())
    }
    
    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }
    
    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

// Implement BenchMpscQueue for JiffyQueue
impl<T: Send + 'static> BenchMpscQueue<T> for JiffyQueue<T> {
    fn bench_push(&self, item: T, _producer_id: usize) -> Result<(), ()> {
        self.push(item).map_err(|_| ())
    }
    
    fn bench_pop(&self) -> Result<T, ()> {
        self.pop()
    }
    
    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }
    
    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

// Implement BenchMpscQueue for JayantiPetrovicMpscQueue
impl<T: Send + Clone + 'static> BenchMpscQueue<T> for JayantiPetrovicMpscQueue<T> {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
        self.enqueue(producer_id, item)
    }
    
    fn bench_pop(&self) -> Result<T, ()> {
        self.dequeue().ok_or(())
    }
    
    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }
    
    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

// Implement BenchMpscQueue for DQueue
impl<T: Send + Clone + 'static> BenchMpscQueue<T> for DQueue<T> {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
        self.enqueue(producer_id, item)
    }
    
    fn bench_pop(&self) -> Result<T, ()> {
        self.dequeue().ok_or(())
    }
    
    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }
    
    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

const NUM_PRODUCERS: usize = 4;
const ITEMS_PER_PRODUCER: usize = 1000;

// Helper macro for testing basic MPSC operations
macro_rules! test_mpsc_basic {
    ($queue_type:ty, $init:expr, $test_name:ident) => {
        mod $test_name {
            use super::*;
            
            #[test]
            fn test_single_producer_basic() {
                let queue = $init;
                
                // Test empty queue
                assert!(queue.is_empty());
                
                // Push and pop single item
                queue.push(42).unwrap();
                assert!(!queue.is_empty());
                assert_eq!(queue.pop().unwrap(), 42);
                assert!(queue.is_empty());
                
                // Push multiple items
                for i in 0..10 {
                    queue.push(i).unwrap();
                }
                
                for i in 0..10 {
                    assert_eq!(queue.pop().unwrap(), i);
                }
                assert!(queue.is_empty());
            }
            
            #[test]
            fn test_multiple_producers_single_consumer() {
                let queue = Arc::new($init);
                let barrier = Arc::new(Barrier::new(NUM_PRODUCERS + 1));
                let mut handles = vec![];
                
                // Spawn producers
                for producer_id in 0..NUM_PRODUCERS {
                    let queue_clone = queue.clone();
                    let barrier_clone = barrier.clone();
                    
                    let handle = thread::spawn(move || {
                        barrier_clone.wait();
                        
                        for i in 0..ITEMS_PER_PRODUCER {
                            let value = producer_id * ITEMS_PER_PRODUCER + i;
                            loop {
                                match queue_clone.push(value) {
                                    Ok(_) => break,
                                    Err(_) => thread::yield_now(),
                                }
                            }
                        }
                    });
                    
                    handles.push(handle);
                }
                
                // Start all producers
                barrier.wait();
                
                // Wait for all producers to finish
                for handle in handles {
                    handle.join().unwrap();
                }
                
                // Collect all items
                let mut items = Vec::new();
                let mut attempts = 0;
                let expected_count = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
                
                while items.len() < expected_count && attempts < 1000000 {
                    match queue.pop() {
                        Ok(item) => {
                            items.push(item);
                            attempts = 0;
                        }
                        Err(_) => {
                            attempts += 1;
                            thread::yield_now();
                        }
                    }
                }
                
                assert_eq!(items.len(), expected_count);
                
                // Verify all items were received (order may vary due to concurrency)
                items.sort();
                for (i, &item) in items.iter().enumerate() {
                    assert_eq!(item, i);
                }
                
                assert!(queue.is_empty());
            }
            
            #[test]
            fn test_concurrent_push_pop() {
                let queue = Arc::new($init);
                let barrier = Arc::new(Barrier::new(NUM_PRODUCERS + 1));
                let stop_flag = Arc::new(AtomicBool::new(false));
                let total_produced = Arc::new(AtomicUsize::new(0));
                let total_consumed = Arc::new(AtomicUsize::new(0));
                
                let mut producer_handles = vec![];
                
                // Spawn producers
                for producer_id in 0..NUM_PRODUCERS {
                    let queue_clone = queue.clone();
                    let barrier_clone = barrier.clone();
                    let stop_clone = stop_flag.clone();
                    let produced_clone = total_produced.clone();
                    
                    let handle = thread::spawn(move || {
                        barrier_clone.wait();
                        let mut count = 0;
                        
                        while !stop_clone.load(Ordering::Relaxed) {
                            let value = producer_id * 100000 + count;
                            match queue_clone.push(value) {
                                Ok(_) => {
                                    count += 1;
                                    produced_clone.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => thread::yield_now(),
                            }
                            
                            if count % 100 == 0 {
                                thread::yield_now();
                            }
                        }
                        
                        count
                    });
                    
                    producer_handles.push(handle);
                }
                
                // Spawn consumer
                let queue_clone = queue.clone();
                let barrier_clone = barrier.clone();
                let stop_clone = stop_flag.clone();
                let consumed_clone = total_consumed.clone();
                
                let consumer_handle = thread::spawn(move || {
                    barrier_clone.wait();
                    let mut count = 0;
                    let mut empty_polls = 0;
                    
                    while !stop_clone.load(Ordering::Relaxed) || !queue_clone.is_empty() {
                        match queue_clone.pop() {
                            Ok(_item) => {
                                count += 1;
                                consumed_clone.fetch_add(1, Ordering::Relaxed);
                                empty_polls = 0;
                            }
                            Err(_) => {
                                empty_polls += 1;
                                if empty_polls > 1000 {
                                    thread::yield_now();
                                    empty_polls = 0;
                                }
                            }
                        }
                    }
                    
                    count
                });
                
                // Run test
                barrier.wait();
                thread::sleep(Duration::from_millis(100));
                stop_flag.store(true, Ordering::Relaxed);
                
                // Wait for all threads
                let mut total_produced_count = 0;
                for handle in producer_handles {
                    total_produced_count += handle.join().unwrap();
                }
                
                let total_consumed_count = consumer_handle.join().unwrap();
                
                // Verify counts match
                assert_eq!(total_produced_count, total_produced.load(Ordering::Relaxed));
                assert_eq!(total_consumed_count, total_consumed.load(Ordering::Relaxed));
                
                // All produced items should be consumed
                let remaining_items = total_produced_count.saturating_sub(total_consumed_count);
                assert!(remaining_items <= NUM_PRODUCERS, 
                    "Too many unconsumed items: {}", remaining_items);
            }
        }
    };
}

// DrescherQueue tests
mod drescher_tests {
    use super::*;
    
    #[test]
    fn test_drescher_basic() {
        let expected_nodes = 1000;
        let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
        let memory = create_aligned_memory_box(shared_size);  // FIX
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            DrescherQueue::init_in_shared(mem_ptr, expected_nodes) 
        };
        
        // Test empty queue
        assert!(queue.is_empty());
        assert!(queue.pop().is_none());
        
        // Push and pop
        queue.push(42).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());
    }
    
    #[test]
    fn test_drescher_capacity() {
        let expected_nodes = 100;
        let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            DrescherQueue::init_in_shared(mem_ptr, expected_nodes) 
        };
        
        let mut pushed = 0;
        for i in 0..expected_nodes + 10 {
            match queue.push(i) {
                Ok(_) => pushed += 1,
                Err(val) => {
                    assert_eq!(val, i);
                    break;
                }
            }
        }
        
        assert!(pushed > 0);
        assert!(pushed < expected_nodes);
        
        let items_to_pop = 10.min(pushed);
        for _ in 0..items_to_pop {
            queue.pop().unwrap();
        }
        
        for i in 0..items_to_pop {
            queue.push(1000 + i).unwrap();
        }
    }
    
    #[test]
    fn test_drescher_concurrent() {
        let expected_nodes = 10000;
        let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
        let memory = create_aligned_memory_box(shared_size);  // FIX
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            DrescherQueue::init_in_shared(mem_ptr, expected_nodes) 
        };
        
        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(NUM_PRODUCERS + 1));
        let mut handles = vec![];
        
        // Spawn producers
        for producer_id in 0..NUM_PRODUCERS {
            let queue_clone = queue.clone();
            let barrier_clone = barrier.clone();
            
            let handle = thread::spawn(move || {
                barrier_clone.wait();
                
                for i in 0..ITEMS_PER_PRODUCER {
                    let value = producer_id * ITEMS_PER_PRODUCER + i;
                    loop {
                        match queue_clone.push(value) {
                            Ok(_) => break,
                            Err(_) => thread::yield_now(),
                        }
                    }
                }
            });
            
            handles.push(handle);
        }
        
        // Start all producers
        barrier.wait();
        
        // Wait for all producers to finish
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Collect all items
        let mut items = Vec::new();
        while let Some(item) = queue.pop() {
            items.push(item);
        }
        
        assert_eq!(items.len(), NUM_PRODUCERS * ITEMS_PER_PRODUCER);
        
        // Verify all items were received
        items.sort();
        for (i, &item) in items.iter().enumerate() {
            assert_eq!(item, i);
        }
    }
}

// JayantiPetrovicMpscQueue tests
mod jayanti_petrovic_tests {
    use super::*;
    
    #[test]
    fn test_jp_initialization() {
        let num_producers = 4;
        let node_pool_capacity = 1000;
        
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(
            num_producers, 
            node_pool_capacity
        );
        let memory = create_aligned_memory_box(shared_size);  // FIX
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue: &mut JayantiPetrovicMpscQueue<usize> = unsafe { 
            JayantiPetrovicMpscQueue::init_in_shared(
                mem_ptr, 
                num_producers, 
                node_pool_capacity
            ) 
        };
        
        assert!(queue.is_empty());
    }
    
    #[test]
    fn test_jp_producer_specific_enqueue() {
        let num_producers = 4;
        let node_pool_capacity = 1000;
        
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(
            num_producers, 
            node_pool_capacity
        );
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JayantiPetrovicMpscQueue::init_in_shared(
                memory.as_mut_ptr(), 
                num_producers, 
                node_pool_capacity
            ) 
        };
        
        // Test enqueue with specific producer IDs
        for producer_id in 0..num_producers {
            for i in 0..10 {
                let value = producer_id * 100 + i;
                queue.enqueue(producer_id, value).unwrap();
            }
        }
        
        // Dequeue all items
        let mut items = Vec::new();
        while let Some(item) = queue.dequeue() {
            items.push(item);
        }
        
        assert_eq!(items.len(), num_producers * 10);
        
        // Verify all items are present
        items.sort();
        for producer_id in 0..num_producers {
            for i in 0..10 {
                let expected = producer_id * 100 + i;
                assert!(items.contains(&expected));
            }
        }
    }
    
    #[test]
    fn test_jp_invalid_producer_id() {
        let num_producers = 2;
        let node_pool_capacity = 100;
        
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(
            num_producers, 
            node_pool_capacity
        );
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JayantiPetrovicMpscQueue::init_in_shared(
                memory.as_mut_ptr(), 
                num_producers, 
                node_pool_capacity
            ) 
        };
        
        // Try to enqueue with invalid producer ID
        assert!(queue.enqueue(num_producers, 42).is_err());
        assert!(queue.enqueue(num_producers + 1, 42).is_err());
    }
    
    #[test]
    fn test_jp_concurrent_producers() {
        let num_producers = 4;
        let node_pool_capacity = 10000;
        
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(
            num_producers, 
            node_pool_capacity
        );
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JayantiPetrovicMpscQueue::init_in_shared(
                memory.as_mut_ptr(), 
                num_producers, 
                node_pool_capacity
            ) 
        };
        
        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(num_producers + 1));
        let mut handles = vec![];
        
        // Spawn producer threads
        for producer_id in 0..num_producers {
            let queue_clone = queue.clone();
            let barrier_clone = barrier.clone();
            
            let handle = thread::spawn(move || {
                barrier_clone.wait();
                
                for i in 0..ITEMS_PER_PRODUCER {
                    let value = producer_id * ITEMS_PER_PRODUCER + i;
                    queue_clone.enqueue(producer_id, value).unwrap();
                }
            });
            
            handles.push(handle);
        }
        
        // Start all producers
        barrier.wait();
        
        // Wait for producers
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Collect all items
        let mut items = Vec::new();
        while let Some(item) = queue.dequeue() {
            items.push(item);
        }
        
        assert_eq!(items.len(), num_producers * ITEMS_PER_PRODUCER);
        
        // Verify all items
        items.sort();
        for (i, &item) in items.iter().enumerate() {
            assert_eq!(item, i);
        }
    }
    
    #[test]
    #[should_panic]
    fn test_jp_zero_producers_panic() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(1, 100); // At least 1 producer for size calculation
        let mut memory = vec![0u8; shared_size];
        
        unsafe { 
            JayantiPetrovicMpscQueue::<usize>::init_in_shared(memory.as_mut_ptr(), 0, 100) 
        };
    }
}

// JiffyQueue tests
mod jiffy_tests {
    use super::*;
    
    #[test]
    fn test_jiffy_basic() {
        let buffer_capacity = 64;
        let max_buffers = 10;
        
        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);  // FIX
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) 
        };
        
        assert!(queue.is_empty());
        assert!(!queue.is_full());
        
        // Push and pop
        queue.push(42).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());
    }
    
    #[test]
    fn test_jiffy_buffer_transitions() {
        let buffer_capacity = 4;
        let max_buffers = 5;
        
        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) 
        };
        
        let total_items = buffer_capacity * 3;
        let mut pushed = 0;
        for i in 0..total_items {
            if queue.push(i).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }
        
        assert!(pushed >= buffer_capacity);
        
        for i in 0..pushed {
            match queue.pop() {
                Ok(val) => assert_eq!(val, i),
                Err(_) => panic!("Failed to pop item {}", i),
            }
        }
        
        assert!(queue.is_empty());
    }
    
    #[test]
    fn test_jiffy_concurrent_operations() {
        let buffer_capacity = 128;
        let max_buffers = 20;
        
        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);  // FIX
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) 
        };
        
        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(NUM_PRODUCERS + 1));
        // Rest of the test remains the same...
        let mut handles = vec![];
        
        // Spawn producers
        for producer_id in 0..NUM_PRODUCERS {
            let queue_clone = queue.clone();
            let barrier_clone = barrier.clone();
            
            let handle = thread::spawn(move || {
                barrier_clone.wait();
                
                for i in 0..ITEMS_PER_PRODUCER {
                    let value = producer_id * ITEMS_PER_PRODUCER + i;
                    loop {
                        match queue_clone.push(value) {
                            Ok(_) => break,
                            Err(_) => thread::yield_now(),
                        }
                    }
                }
            });
            
            handles.push(handle);
        }
        
        // Consumer thread
        let queue_clone = queue.clone();
        let barrier_clone = barrier.clone();
        let consumer = thread::spawn(move || {
            barrier_clone.wait();
            
            let mut items = Vec::new();
            let expected_count = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
            
            while items.len() < expected_count {
                match queue_clone.pop() {
                    Ok(item) => items.push(item),
                    Err(_) => thread::yield_now(),
                }
            }
            
            items
        });
        
        // Start all threads
        barrier.wait();
        
        // Wait for producers
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Get consumer results
        let items = consumer.join().unwrap();
        
        assert_eq!(items.len(), NUM_PRODUCERS * ITEMS_PER_PRODUCER);
        
        // Verify all items present
        let mut sorted = items.clone();
        sorted.sort();
        for (i, &item) in sorted.iter().enumerate() {
            assert_eq!(item, i);
        }
    }
    
    
    #[test]
    fn test_jiffy_out_of_order_operations() {
        let buffer_capacity = 8;
        let max_buffers = 10;
        
        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) 
        };
        
        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(3));
        
        let queue1 = queue.clone();
        let barrier1 = barrier.clone();
        let producer1 = thread::spawn(move || {
            barrier1.wait();
            for i in 0..50 {
                queue1.push(i * 2).unwrap();
                if i % 10 == 0 {
                    thread::sleep(Duration::from_micros(10));
                }
            }
        });
        
        let queue2 = queue.clone();
        let barrier2 = barrier.clone();
        let producer2 = thread::spawn(move || {
            barrier2.wait();
            for i in 0..50 {
                queue2.push(i * 2 + 1).unwrap();
                if i % 7 == 0 {
                    thread::sleep(Duration::from_micros(10));
                }
            }
        });
        
        barrier.wait();
        producer1.join().unwrap();
        producer2.join().unwrap();
        
        let mut items = Vec::new();
        while !queue.is_empty() {
            if let Ok(item) = queue.pop() {
                items.push(item);
            }
        }
        
        assert_eq!(items.len(), 100);
        items.sort();
        for (i, &item) in items.iter().enumerate() {
            assert_eq!(item, i);
        }
    }
    
    test_mpsc_basic!(
      JiffyQueue<usize>,
      {
          let size = JiffyQueue::<usize>::shared_size(256, 50);
          let memory = create_aligned_memory_box(size);
          let mem_ptr = Box::leak(memory).as_mut_ptr();
          unsafe { JiffyQueue::init_in_shared(mem_ptr, 256, 50) }
      },
      jiffy_mpsc_tests
  );
}

// Helper functions for dqueue

fn create_aligned_memory(size: usize, align: usize) -> Vec<u8> {
   // Allocate extra space for alignment
   let total_size = size + align - 1;
   let mut vec = vec![0u8; total_size];
   
   // Get the raw pointer and align it
   let ptr = vec.as_mut_ptr() as usize;
   let aligned_ptr = (ptr + align - 1) & !(align - 1);
   let offset = aligned_ptr - ptr;
   
   // Return a vec that starts at the aligned position
   if offset > 0 {
       vec.drain(0..offset);
   }
   vec.truncate(size);
   
   // Verify alignment
   assert_eq!(vec.as_ptr() as usize % align, 0);
   vec
}

fn create_aligned_memory_box(size: usize) -> Box<[u8]> {
   const ALIGN: usize = 64; // Match the alignment of our structures
   
   use std::alloc::{alloc_zeroed, Layout};
   
   unsafe {
       let layout = Layout::from_size_align(size, ALIGN).unwrap();
       let ptr = alloc_zeroed(layout);
       if ptr.is_null() {
           panic!("Failed to allocate aligned memory");
       }
       
       let slice = std::slice::from_raw_parts_mut(ptr, size);
       Box::from_raw(slice)
   }
}


// DQueue tests
mod dqueue_tests {
    use super::*;
    
    #[test]
    fn test_dqueue_initialization() {
        let num_producers = 4;
        let segment_pool_capacity = 10;
        
        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);  // FIX
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue: &mut DQueue<usize> = unsafe { 
            DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) 
        };
        
        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }
    
    #[test]
    fn test_dqueue_producer_specific() {
        let num_producers = 3;
        let segment_pool_capacity = 10;
        
        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);  // FIX
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) 
        };
        
        // Enqueue from each producer
        for producer_id in 0..num_producers {
            for i in 0..10 {
                queue.enqueue(producer_id, producer_id * 100 + i).unwrap();
            }
        }
        
        // Dequeue all
        let mut items = Vec::new();
        while let Some(item) = queue.dequeue() {
            items.push(item);
        }
        
        assert_eq!(items.len(), num_producers * 10);
    }
    
    #[test]
    fn test_dqueue_local_buffer_operations() {
        let num_producers = 2;
        let segment_pool_capacity = 20;
        
        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) 
        };
        
        // Fill local buffer without flushing
        let items_to_push = 100; // Much less than L_LOCAL_BUFFER_CAPACITY
        for i in 0..items_to_push {
            queue.enqueue(0, i).unwrap();
        }
        
        // Items might be in local buffer, dequeue should trigger help
        let mut count = 0;
        let mut attempts = 0;
        while count < items_to_push && attempts < 10000 {
            if queue.dequeue().is_some() {
                count += 1;
            } else {
                attempts += 1;
                thread::yield_now();
            }
        }
        
        assert!(count > 0, "Should have dequeued at least some items");
    }
    
    #[test]
    fn test_dqueue_gc_operations() {
        let num_producers = 2;
        let segment_pool_capacity = 5;
        
        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);  // USE ALIGNED MEMORY
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) 
        };
        
        // Push many items to test segment management
        let total_items = 1000;
        
        for i in 0..total_items {
            queue.enqueue(0, i).unwrap();
        }
        
        // Dequeue half the items
        for _ in 0..total_items / 2 {
            queue.dequeue();
        }
        
        // Run garbage collection
        unsafe { queue.run_gc(); }
        
        // Should still be able to dequeue remaining items
        let mut remaining = 0;
        while queue.dequeue().is_some() {
            remaining += 1;
        }
        
        assert!(remaining > 0);
    }
    
    #[test]
    fn test_dqueue_concurrent_with_helping() {
        let num_producers = 4;
        let segment_pool_capacity = 20;
        
        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            DQueue::init_in_shared(memory.as_mut_ptr(), num_producers, segment_pool_capacity) 
        };
        
        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(num_producers + 1));
        let mut handles = vec![];
        
        // Spawn producers that intentionally don't flush immediately
        for producer_id in 0..num_producers {
            let queue_clone = queue.clone();
            let barrier_clone = barrier.clone();
            
            let handle = thread::spawn(move || {
                barrier_clone.wait();
                
                for i in 0..100 {
                    queue_clone.enqueue(producer_id, producer_id * 1000 + i).unwrap();
                }
            });
            
            handles.push(handle);
        }
        
        // Consumer that will trigger help_enqueue
        let queue_clone = queue.clone();
        let barrier_clone = barrier.clone();
        let consumer = thread::spawn(move || {
            barrier_clone.wait();
            
            // Let producers fill their buffers
            thread::sleep(Duration::from_millis(10));
            
            let mut items = Vec::new();
            let mut attempts = 0;
            
            // Try to dequeue, which should trigger helping
            while attempts < 10000 {
                match queue_clone.dequeue() {
                    Some(item) => {
                        items.push(item);
                        attempts = 0;
                    }
                    None => {
                        attempts += 1;
                        if attempts > 1000 {
                            break;
                        }
                        thread::yield_now();
                    }
                }
            }
            
            items
        });
        
        barrier.wait();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let items = consumer.join().unwrap();
        assert!(!items.is_empty(), "Consumer should have dequeued some items with helping");
    }
    
    #[test]
    fn test_dqueue_invalid_producer() {
        let num_producers = 2;
        let segment_pool_capacity = 10;
        
        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            DQueue::init_in_shared(memory.as_mut_ptr(), num_producers, segment_pool_capacity) 
        };
        
        // Invalid producer IDs
        assert!(queue.enqueue(num_producers, 42).is_err());
        assert!(queue.enqueue(num_producers + 1, 42).is_err());
        assert!(queue.enqueue(usize::MAX, 42).is_err());
    }
}

// Benchmark wrapper tests
mod bench_wrapper_tests {
    use super::*;
    
    #[test]
    fn test_drescher_bench_interface() {
        let shared_size = DrescherQueue::<usize>::shared_size(1000);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            DrescherQueue::init_in_shared(memory.as_mut_ptr(), 1000) 
        };
        
        // Test through BenchMpscQueue interface
        for producer_id in 0..4 {
            for i in 0..10 {
                queue.bench_push(producer_id * 100 + i, producer_id).unwrap();
            }
        }
        
        let mut count = 0;
        while queue.bench_pop().is_ok() {
            count += 1;
        }
        
        assert_eq!(count, 40);
        assert!(queue.bench_is_empty());
    }
    
    #[test]
    fn test_jiffy_bench_interface() {
        let shared_size = JiffyQueue::<usize>::shared_size(128, 10);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JiffyQueue::init_in_shared(memory.as_mut_ptr(), 128, 10) 
        };
        
        // Test bench interface
        queue.bench_push(42, 0).unwrap();
        assert!(!queue.bench_is_empty());
        assert_eq!(queue.bench_pop().unwrap(), 42);
        assert!(queue.bench_is_empty());
    }
    
    #[test]
    fn test_jayanti_bench_interface() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(4, 1000);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JayantiPetrovicMpscQueue::init_in_shared(memory.as_mut_ptr(), 4, 1000) 
        };
        
        // Test bench interface with different producer IDs
        for producer_id in 0..4 {
            queue.bench_push(producer_id * 10, producer_id).unwrap();
        }
        
        let mut items = Vec::new();
        while let Ok(item) = queue.bench_pop() {
            items.push(item);
        }
        
        assert_eq!(items.len(), 4);
        items.sort();
        assert_eq!(items, vec![0, 10, 20, 30]);
    }
    
    #[test]
    fn test_dqueue_bench_interface() {
        let shared_size = DQueue::<usize>::shared_size(4, 10);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            DQueue::init_in_shared(memory.as_mut_ptr(), 4, 10) 
        };
        
        // Push through bench interface
        for i in 0..10 {
            queue.bench_push(i, 0).unwrap();
        }
        
        // Pop through bench interface after some time
        thread::sleep(Duration::from_millis(10));
        
        let mut count = 0;
        let mut attempts = 0;
        while attempts < 1000 {
            if queue.bench_pop().is_ok() {
                count += 1;
                attempts = 0;
            } else {
                attempts += 1;
                thread::yield_now();
            }
            
            if count >= 10 {
                break;
            }
        }
        
        assert!(count > 0, "Should have popped at least some items");
    }
}

// Edge cases and error handling
mod edge_cases {
    use super::*;
    
    #[test]
    fn test_zero_sized_type() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct ZeroSized;
        
        let shared_size = DrescherQueue::<ZeroSized>::shared_size(100);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            DrescherQueue::init_in_shared(memory.as_mut_ptr(), 100) 
        };
        
        queue.push(ZeroSized).unwrap();
        assert_eq!(queue.pop().unwrap(), ZeroSized);
    }
    
    #[test]
    fn test_large_type() {
        #[derive(Clone, Debug, PartialEq)]
        struct LargeType {
            data: [u64; 128],
        }
        
        let shared_size = JiffyQueue::<LargeType>::shared_size(16, 5);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JiffyQueue::init_in_shared(memory.as_mut_ptr(), 16, 5) 
        };
        
        let item = LargeType { data: [42; 128] };
        queue.push(item.clone()).unwrap();
        assert_eq!(queue.pop().unwrap(), item);
    }
    
    #[test]
    fn test_drop_semantics() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);
        
        #[derive(Clone, Debug)]
        struct DropCounter {
            _value: usize,
        }
        
        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }
        
        DROP_COUNT.store(0, Ordering::SeqCst);
        
        {
            let shared_size = JiffyQueue::<DropCounter>::shared_size(64, 5);
            let mut memory = vec![0u8; shared_size];
            
            let queue = unsafe { 
                JiffyQueue::init_in_shared(memory.as_mut_ptr(), 64, 5) 
            };
            
            // Push items
            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }
            
            // Pop half
            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }
            
            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
            
            // Remaining items should be dropped when queue is dropped
        }
        
        // Give time for drops
        thread::sleep(Duration::from_millis(10));
        
        // At least the 5 popped items should be dropped
        assert!(DROP_COUNT.load(Ordering::SeqCst) >= 5);
    }
}

// Memory safety and allocation tests
mod memory_tests {
    use super::*;
    
    #[test]
    fn test_shared_memory_alignment() {
        // Test that shared memory calculations handle alignment correctly
        
        // DrescherQueue
        let size1 = DrescherQueue::<u8>::shared_size(100);
        let size2 = DrescherQueue::<u64>::shared_size(100);
        assert!(size2 >= size1); // Larger type should need more space
        
        // JiffyQueue
        let size1 = JiffyQueue::<u8>::shared_size(64, 10);
        let size2 = JiffyQueue::<u64>::shared_size(64, 10);
        assert!(size2 >= size1);
        
        // JayantiPetrovicMpscQueue
        let size1 = JayantiPetrovicMpscQueue::<u8>::shared_size(4, 100);
        let size2 = JayantiPetrovicMpscQueue::<u64>::shared_size(4, 100);
        assert!(size2 >= size1);
        
        // DQueue
        let size1 = DQueue::<u8>::shared_size(4, 10);
        let size2 = DQueue::<u64>::shared_size(4, 10);
        assert!(size2 >= size1);
    }
    
    #[test]
    fn test_allocation_limits() {
        let shared_size = DrescherQueue::<usize>::shared_size(10);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            DrescherQueue::init_in_shared(mem_ptr, 10) 
        };
        
        let mut pushed = 0;
        for i in 0..20 {
            if queue.push(i).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }
        
        assert!(pushed > 0 && pushed < 10);
        
        queue.pop().unwrap();
        queue.push(100).unwrap();
    }
    
    #[test]
    fn test_jiffy_buffer_pool_exhaustion() {
        // Test JiffyQueue with minimal buffers
        let buffer_capacity = 2;
        let max_buffers = 2;
        
        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JiffyQueue::init_in_shared(memory.as_mut_ptr(), buffer_capacity, max_buffers) 
        };
        
        // Try to push more items than total capacity
        let mut pushed = 0;
        for i in 0..10 {
            if queue.push(i).is_ok() {
                pushed += 1;
            }
        }
        
        // Should be limited by buffer pool
        assert!(pushed <= buffer_capacity * max_buffers);
    }
}

// Stress tests
mod stress_tests {
    use super::*;
    
    #[test]
    fn stress_test_high_contention() {
        let num_producers = 8;
        let items_per_producer = 10000;
        
        let shared_size = DrescherQueue::<usize>::shared_size(num_producers * items_per_producer);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            DrescherQueue::init_in_shared(memory.as_mut_ptr(), num_producers * items_per_producer) 
        };
        
        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(num_producers + 1));
        let mut handles = vec![];
        
        // Spawn many producers
        for producer_id in 0..num_producers {
            let queue_clone = queue.clone();
            let barrier_clone = barrier.clone();
            
            let handle = thread::spawn(move || {
                barrier_clone.wait();
                
                let mut pushed = 0;
                for i in 0..items_per_producer {
                    let value = producer_id * items_per_producer + i;
                    if queue_clone.push(value).is_ok() {
                        pushed += 1;
                    }
                }
                pushed
            });
            
            handles.push(handle);
        }
        
        // Single fast consumer
        let queue_clone = queue.clone();
        let barrier_clone = barrier.clone();
        
        let consumer = thread::spawn(move || {
            barrier_clone.wait();
            
            let mut consumed = 0;
            let start = std::time::Instant::now();
            
            while start.elapsed() < Duration::from_secs(5) {
                if queue_clone.pop().is_some() {
                    consumed += 1;
                }
            }
            
            consumed
        });
        
        barrier.wait();
        
        let mut total_pushed = 0;
        for handle in handles {
            total_pushed += handle.join().unwrap();
        }
        
        let total_consumed = consumer.join().unwrap();
        
        println!("Stress test: pushed {}, consumed {}", total_pushed, total_consumed);
        assert!(total_consumed > 0);
        assert!(total_consumed <= total_pushed);
    }
    
    #[test]
    fn stress_test_rapid_push_pop() {
        let shared_size = JiffyQueue::<usize>::shared_size(1024, 20);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JiffyQueue::init_in_shared(memory.as_mut_ptr(), 1024, 20) 
        };
        
        let queue = Arc::new(queue);
        let stop = Arc::new(AtomicBool::new(false));
        let operations = Arc::new(AtomicUsize::new(0));
        
        let mut handles = vec![];
        
        // Rapid pusher
        let queue_clone = queue.clone();
        let stop_clone = stop.clone();
        let ops_clone = operations.clone();
        
        let pusher = thread::spawn(move || {
            let mut count = 0;
            while !stop_clone.load(Ordering::Relaxed) {
                if queue_clone.push(count).is_ok() {
                    count += 1;
                    ops_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
            count
        });
        handles.push(pusher);
        
        // Rapid popper
        let queue_clone = queue.clone();
        let stop_clone = stop.clone();
        let ops_clone = operations.clone();
        
        let popper = thread::spawn(move || {
            let mut count = 0;
            while !stop_clone.load(Ordering::Relaxed) {
                if queue_clone.pop().is_ok() {
                    count += 1;
                    ops_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
            count
        });
        handles.push(popper);
        
        // Run for a short time
        thread::sleep(Duration::from_millis(100));
        stop.store(true, Ordering::Relaxed);
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let total_ops = operations.load(Ordering::Relaxed);
        println!("Rapid push/pop operations: {}", total_ops);
        assert!(total_ops > 0);
    }
}

// Tests for MpscQueue trait implementation
mod trait_tests {
    use super::*;
    
    fn test_mpsc_trait<T>(queue: &T) 
    where 
        T: MpscQueue<usize>,
        T::PushError: std::fmt::Debug,
        T::PopError: std::fmt::Debug,
    {
        // Test through trait interface
        assert!(queue.is_empty());
        assert!(!queue.is_full());
        
        queue.push(42).unwrap();
        assert!(!queue.is_empty());
        
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());
    }
    
    #[test]
    fn test_all_queues_implement_trait() {
        // DrescherQueue
        let shared_size = DrescherQueue::<usize>::shared_size(100);
        let mut memory = vec![0u8; shared_size];
        let queue = unsafe { DrescherQueue::init_in_shared(memory.as_mut_ptr(), 100) };
        test_mpsc_trait(&*queue);
        
        // JiffyQueue
        let shared_size = JiffyQueue::<usize>::shared_size(64, 10);
        let mut memory = vec![0u8; shared_size];
        let queue = unsafe { JiffyQueue::init_in_shared(memory.as_mut_ptr(), 64, 10) };
        test_mpsc_trait(&*queue);
        
        // DQueue (special case - needs adapter)
        let shared_size = DQueue::<usize>::shared_size(1, 10);
        let mut memory = vec![0u8; shared_size];
        let queue: &mut DQueue<usize> = unsafe { DQueue::init_in_shared(memory.as_mut_ptr(), 1, 10) };
        
        // Can't use push directly, test other methods
        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }
    
    #[test]
    #[should_panic]
    fn test_dqueue_push_panics() {
        let shared_size = DQueue::<usize>::shared_size(2, 10);
        let mut memory = vec![0u8; shared_size];
        let queue = unsafe { DQueue::init_in_shared(memory.as_mut_ptr(), 2, 10) };
        
        // This should panic as per implementation
        queue.push(42).unwrap();
    }
    
    #[test]
    #[should_panic]
    fn test_jayanti_push_panics() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(2, 100);
        let mut memory = vec![0u8; shared_size];
        let queue = unsafe { 
            JayantiPetrovicMpscQueue::init_in_shared(memory.as_mut_ptr(), 2, 100) 
        };
        
        // This should panic as per implementation
        queue.push(42).unwrap();
    }
}

// Additional comprehensive tests for better coverage
mod comprehensive_tests {
    use super::*;
    
    #[test]
    fn test_drescher_node_recycling() {
        let nodes = 50;
        let shared_size = DrescherQueue::<String>::shared_size(nodes);
        let memory = create_aligned_memory_box(shared_size);  // FIX
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        
        let queue = unsafe { 
            DrescherQueue::init_in_shared(mem_ptr, nodes) 
        };
        
        // Push and pop repeatedly to test node recycling
        for cycle in 0..3 {
            for i in 0..20 {
                queue.push(format!("cycle_{}_item_{}", cycle, i)).unwrap();
            }
            
            for _ in 0..20 {
                assert!(queue.pop().is_some());
            }
        }
        
        assert!(queue.is_empty());
    }
    
    #[test]
    fn test_jayanti_multiple_producers_interleaved() {
        let num_producers = 4;
        let node_pool = 1000;
        
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JayantiPetrovicMpscQueue::init_in_shared(memory.as_mut_ptr(), num_producers, node_pool) 
        };
        
        // Interleaved enqueues from different producers
        for round in 0..10 {
            for producer_id in 0..num_producers {
                queue.enqueue(producer_id, producer_id * 1000 + round).unwrap();
            }
        }
        
        // Dequeue and verify all items are present
        let mut items = Vec::new();
        while let Some(item) = queue.dequeue() {
            items.push(item);
        }
        
        assert_eq!(items.len(), num_producers * 10);
    }
    
    #[test]
    fn test_jiffy_empty_buffer_handling() {
        let buffer_capacity = 8;
        let max_buffers = 3;
        
        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JiffyQueue::init_in_shared(memory.as_mut_ptr(), buffer_capacity, max_buffers) 
        };
        
        // Push items leaving gaps
        queue.push(1).unwrap();
        queue.push(2).unwrap();
        
        // Pop one
        assert_eq!(queue.pop().unwrap(), 1);
        
        // Push more
        for i in 3..10 {
            queue.push(i).unwrap();
        }
        
        // Pop remaining
        assert_eq!(queue.pop().unwrap(), 2);
        for i in 3..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }
    
    #[test]
    fn test_dqueue_segment_allocation() {
        let num_producers = 2;
        let segment_pool = 3;
        
        let shared_size = DQueue::<String>::shared_size(num_producers, segment_pool);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            DQueue::init_in_shared(memory.as_mut_ptr(), num_producers, segment_pool) 
        };
        
        // Test segment allocation with string data
        for i in 0..50 {
            queue.enqueue(0, format!("item_{}", i)).unwrap();
        }
        
        // Dequeue some items
        for _ in 0..25 {
            assert!(queue.dequeue().is_some());
        }
        
        // Enqueue more
        for i in 50..75 {
            queue.enqueue(1, format!("item_{}", i)).unwrap();
        }
        
        // Verify we can still dequeue
        let mut count = 0;
        while queue.dequeue().is_some() {
            count += 1;
        }
        assert!(count > 0);
    }
    
    #[test]
    fn test_queue_state_consistency() {
        // Test DrescherQueue state
        let size = DrescherQueue::<i32>::shared_size(100);
        let mut mem = vec![0u8; size];
        let drescher = unsafe { DrescherQueue::init_in_shared(mem.as_mut_ptr(), 100) };
        
        assert!(drescher.is_empty());
        assert!(!drescher.is_full());
        
        drescher.push(42).unwrap();
        assert!(!drescher.is_empty());
        
        // Test JiffyQueue state
        let size = JiffyQueue::<i32>::shared_size(64, 10);
        let mut mem = vec![0u8; size];
        let jiffy = unsafe { JiffyQueue::init_in_shared(mem.as_mut_ptr(), 64, 10) };
        
        assert!(jiffy.is_empty());
        assert!(!jiffy.is_full());
        
        jiffy.push(42).unwrap();
        assert!(!jiffy.is_empty());
    }
    
    #[test]
    fn test_error_propagation() {
        // Test push errors
        let size = DrescherQueue::<usize>::shared_size(2);
        let mem = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(mem).as_mut_ptr();
        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 2) };
        
        queue.push(1).unwrap();
        match queue.push(2) {
            Err(val) => assert_eq!(val, 2),
            Ok(_) => panic!("Expected push to fail"),
        }
        
        queue.pop().unwrap();
        assert!(queue.pop().is_none());
    }
    
    #[test]
    fn test_jayanti_tree_operations() {
        let num_producers = 8;
        let node_pool = 1000;
        
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JayantiPetrovicMpscQueue::init_in_shared(memory.as_mut_ptr(), num_producers, node_pool) 
        };
        
        // Test that tree properly tracks minimum across producers
        // Enqueue in reverse producer order
        for producer_id in (0..num_producers).rev() {
            queue.enqueue(producer_id, producer_id).unwrap();
        }
        
        // Should dequeue in timestamp order (which follows enqueue order)
        for expected in (0..num_producers).rev() {
            assert_eq!(queue.dequeue().unwrap(), expected);
        }
    }
    
    #[test]
    fn test_dqueue_help_mechanism() {
        let num_producers = 4;
        let segment_pool = 10;
        
        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            DQueue::init_in_shared(memory.as_mut_ptr(), num_producers, segment_pool) 
        };
        
        // Fill local buffers without immediate dequeue
        for prod in 0..num_producers {
            for i in 0..10 {
                queue.enqueue(prod, prod * 100 + i).unwrap();
            }
        }
        
        // Dequeue should trigger helping mechanism
        let mut dequeued = Vec::new();
        for _ in 0..20 {
            if let Some(val) = queue.dequeue() {
                dequeued.push(val);
            }
        }
        
        assert!(!dequeued.is_empty(), "Help mechanism should allow dequeuing");
    }
    
    #[test]
    fn test_concurrent_empty_checks() {
        let size = JiffyQueue::<usize>::shared_size(128, 10);
        let memory = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 128, 10) };
        
        assert!(queue.is_empty());
        
        queue.push(1).unwrap();
        assert!(!queue.is_empty());
        
        queue.push(2).unwrap();
        assert!(!queue.is_empty());
        
        assert_eq!(queue.pop().unwrap(), 1);
        assert!(!queue.is_empty());
        
        assert_eq!(queue.pop().unwrap(), 2);
        assert!(queue.is_empty());
    }
}

// Integration tests
mod integration_tests {
    use super::*;
    
    #[test]
    fn test_mixed_workload() {
        let shared_size = JiffyQueue::<String>::shared_size(256, 20);
        let mut memory = vec![0u8; shared_size];
        
        let queue = unsafe { 
            JiffyQueue::init_in_shared(memory.as_mut_ptr(), 256, 20) 
        };
        
        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(5));
        
        // Producer 1: Steady stream
        let q1 = queue.clone();
        let b1 = barrier.clone();
        let h1 = thread::spawn(move || {
            b1.wait();
            for i in 0..1000 {
                q1.push(format!("steady_{}", i)).unwrap();
                thread::sleep(Duration::from_micros(100));
            }
        });
        
        // Producer 2: Bursty
        let q2 = queue.clone();
        let b2 = barrier.clone();
        let h2 = thread::spawn(move || {
            b2.wait();
            for burst in 0..10 {
                for i in 0..100 {
                    q2.push(format!("burst_{}_{}", burst, i)).unwrap();
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        
        // Producer 3: Random delays
        let q3 = queue.clone();
        let b3 = barrier.clone();
        let h3 = thread::spawn(move || {
            b3.wait();
            for i in 0..500 {
                q3.push(format!("random_{}", i)).unwrap();
                thread::sleep(Duration::from_micros(i % 500));
            }
        });
        
        // Consumer: Variable speed
        let q4 = queue.clone();
        let b4 = barrier.clone();
        let h4 = thread::spawn(move || {
            b4.wait();
            let mut items = Vec::new();
            let mut batch_size = 1;
            
            for _ in 0..250 {
                for _ in 0..batch_size {
                    if let Ok(item) = q4.pop() {
                        items.push(item);
                    }
                }
                batch_size = (batch_size % 10) + 1;
                thread::sleep(Duration::from_micros(200));
            }
            
            items
        });
        
        barrier.wait();
        
        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
        let consumed = h4.join().unwrap();
        
        println!("Mixed workload consumed {} items", consumed.len());
        assert!(!consumed.is_empty());
        
        // Verify different types of items were consumed
        let steady_count = consumed.iter().filter(|s| s.starts_with("steady")).count();
        let burst_count = consumed.iter().filter(|s| s.starts_with("burst")).count();
        let random_count = consumed.iter().filter(|s| s.starts_with("random")).count();
        
        assert!(steady_count > 0);
        assert!(burst_count > 0);
        assert!(random_count > 0);
    }
}