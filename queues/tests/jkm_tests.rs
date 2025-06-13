#[cfg(test)]
mod jkm_queue_tests {
    use queues::JKMQueue;

    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;

    #[test]
    fn test_single_producer_single_consumer() {
        unsafe {
            let mem_size = JKMQueue::<usize>::shared_size(1, 1);
            let mem = libc::malloc(mem_size) as *mut u8;
            let queue = JKMQueue::init_in_shared(mem, 1, 1);

            // Enqueue 100 items
            for i in 0..100 {
                assert!(queue.enqueue(0, i).is_ok(), "Failed to enqueue item {}", i);
            }

            // Force sync to ensure visibility
            queue.force_sync();

            // Dequeue all items
            let mut dequeued = Vec::new();
            for _ in 0..100 {
                match queue.dequeue(0) {
                    Ok(val) => dequeued.push(val),
                    Err(_) => break,
                }
            }

            // Finalize to ensure no items are stuck
            queue.finalize_pending_dequeues();

            // Try to dequeue any remaining items
            while let Ok(val) = queue.dequeue(0) {
                dequeued.push(val);
            }

            assert_eq!(
                dequeued.len(),
                100,
                "Expected 100 items, got {}",
                dequeued.len()
            );

            // Verify all items are present
            let dequeued_set: HashSet<_> = dequeued.into_iter().collect();
            for i in 0..100 {
                assert!(dequeued_set.contains(&i), "Missing item {}", i);
            }

            libc::free(mem as *mut libc::c_void);
        }
    }

    #[test]
    fn test_multiple_producers_single_consumer() {
        unsafe {
            let mem_size = JKMQueue::<usize>::shared_size(2, 1);
            let mem = libc::malloc(mem_size) as *mut u8;
            let queue = JKMQueue::init_in_shared(mem, 2, 1);

            // Producer 0 enqueues 0-49
            for i in 0..50 {
                assert!(queue.enqueue(0, i).is_ok(), "P0 failed to enqueue {}", i);
            }

            // Producer 1 enqueues 50-99
            for i in 50..100 {
                assert!(queue.enqueue(1, i).is_ok(), "P1 failed to enqueue {}", i);
            }

            queue.force_sync();

            // Dequeue all items
            let mut dequeued = Vec::new();
            for _ in 0..100 {
                match queue.dequeue(0) {
                    Ok(val) => dequeued.push(val),
                    Err(_) => break,
                }
            }

            queue.finalize_pending_dequeues();

            // Try to get any remaining items
            while let Ok(val) = queue.dequeue(0) {
                dequeued.push(val);
            }

            assert_eq!(
                dequeued.len(),
                100,
                "Expected 100 items, got {}",
                dequeued.len()
            );

            libc::free(mem as *mut libc::c_void);
        }
    }

    #[test]
    fn test_dequeue_operation_ordering() {
        unsafe {
            let mem_size = JKMQueue::<usize>::shared_size(1, 2);
            let mem = libc::malloc(mem_size) as *mut u8;
            let queue = JKMQueue::init_in_shared(mem, 1, 2);

            // Enqueue 10 items
            for i in 0..10 {
                queue.enqueue(0, i).unwrap();
            }

            queue.force_sync();

            // Track dequeue operations
            let mut operations = Vec::new();

            // First consumer dequeues
            for _ in 0..5 {
                if let Ok(val) = queue.dequeue(0) {
                    operations.push((0, val));
                }
            }

            // Second consumer dequeues
            for _ in 0..5 {
                if let Ok(val) = queue.dequeue(1) {
                    operations.push((1, val));
                }
            }

            queue.finalize_pending_dequeues();

            // Try to get any remaining
            while let Ok(val) = queue.dequeue(0) {
                operations.push((0, val));
            }
            while let Ok(val) = queue.dequeue(1) {
                operations.push((1, val));
            }

            let total_dequeued = operations.len();
            assert_eq!(
                total_dequeued, 10,
                "Expected 10 items, got {}",
                total_dequeued
            );

            // Check all items are present
            let values: HashSet<_> = operations.iter().map(|(_, v)| *v).collect();
            for i in 0..10 {
                assert!(values.contains(&i), "Missing item {}", i);
            }

            libc::free(mem as *mut libc::c_void);
        }
    }

    #[test]
    fn test_tree_propagation() {
        unsafe {
            let mem_size = JKMQueue::<usize>::shared_size(2, 1);
            let mem = libc::malloc(mem_size) as *mut u8;
            let queue = JKMQueue::init_in_shared(mem, 2, 1);

            // Enqueue one item in each sub-queue
            queue.enqueue(0, 100).unwrap();
            queue.enqueue(1, 200).unwrap();

            // Check tree before propagation
            assert!(!queue.is_empty(), "Queue should not be empty after enqueue");

            // Force propagation
            queue.force_sync();

            // Dequeue and verify
            let val1 = queue.dequeue(0).unwrap();
            let val2 = queue.dequeue(0).unwrap();

            assert!(val1 == 100 || val1 == 200, "Unexpected value: {}", val1);
            assert!(val2 == 100 || val2 == 200, "Unexpected value: {}", val2);
            assert_ne!(val1, val2, "Got same value twice");

            // Should be empty now
            queue.finalize_pending_dequeues();
            assert!(queue.dequeue(0).is_err(), "Queue should be empty");

            libc::free(mem as *mut libc::c_void);
        }
    }

    #[test]
    fn test_concurrent_operations() {
        const NUM_ITEMS: usize = 1000;
        const NUM_PRODUCERS: usize = 4;
        const NUM_CONSUMERS: usize = 4;
        const ITEMS_PER_PRODUCER: usize = NUM_ITEMS / NUM_PRODUCERS;

        unsafe {
            let mem_size = JKMQueue::<usize>::shared_size(NUM_PRODUCERS, NUM_CONSUMERS);
            let mem = libc::malloc(mem_size) as *mut u8;
            let queue = JKMQueue::init_in_shared(mem, NUM_PRODUCERS, NUM_CONSUMERS);
            let queue_ptr = queue as *const JKMQueue<usize>;

            // Shared set to track dequeued items
            let dequeued = Arc::new(Mutex::new(HashSet::new()));

            // Spawn producers
            let mut producer_handles = vec![];
            for p in 0..NUM_PRODUCERS {
                let handle = thread::spawn(move || {
                    let q = &*queue_ptr;
                    let start = p * ITEMS_PER_PRODUCER;
                    let end = start + ITEMS_PER_PRODUCER;

                    for i in start..end {
                        while q.enqueue(p, i).is_err() {
                            thread::yield_now();
                        }
                    }
                });
                producer_handles.push(handle);
            }

            // Spawn consumers
            let mut consumer_handles = vec![];
            for c in 0..NUM_CONSUMERS {
                let dequeued_clone = dequeued.clone();
                let handle = thread::spawn(move || {
                    let q = &*queue_ptr;
                    let mut local_count = 0;
                    let mut empty_count = 0;

                    loop {
                        match q.dequeue(c) {
                            Ok(val) => {
                                dequeued_clone.lock().unwrap().insert(val);
                                local_count += 1;
                                empty_count = 0;
                            }
                            Err(_) => {
                                empty_count += 1;
                                if empty_count > 10000 {
                                    break;
                                }
                                thread::yield_now();
                            }
                        }
                    }
                    local_count
                });
                consumer_handles.push(handle);
            }

            // Wait for producers
            for handle in producer_handles {
                handle.join().unwrap();
            }

            // Give consumers time to finish
            thread::sleep(std::time::Duration::from_millis(100));

            // Force finalization
            queue.finalize_pending_dequeues();

            // Signal consumers to stop by checking if queue is truly empty
            thread::sleep(std::time::Duration::from_millis(100));

            // Wait for consumers
            let mut total_dequeued = 0;
            for handle in consumer_handles {
                total_dequeued += handle.join().unwrap();
            }

            let dequeued_set = dequeued.lock().unwrap();
            println!("Total dequeued: {}/{}", dequeued_set.len(), NUM_ITEMS);

            // Check for missing items
            for i in 0..NUM_ITEMS {
                if !dequeued_set.contains(&i) {
                    println!("Missing item: {}", i);
                }
            }

            assert_eq!(
                dequeued_set.len(),
                NUM_ITEMS,
                "Expected {} items, got {}",
                NUM_ITEMS,
                dequeued_set.len()
            );

            libc::free(mem as *mut libc::c_void);
        }
    }

    #[test]
    fn test_edge_case_last_dequeue() {
        unsafe {
            let mem_size = JKMQueue::<usize>::shared_size(1, 1);
            let mem = libc::malloc(mem_size) as *mut u8;
            let queue = JKMQueue::init_in_shared(mem, 1, 1);

            // Enqueue exactly one item
            queue.enqueue(0, 42).unwrap();
            queue.force_sync();

            // Dequeue it
            let val = queue.dequeue(0).unwrap();
            assert_eq!(val, 42);

            // Finalize
            queue.finalize_pending_dequeues();

            // Verify queue is truly empty
            assert!(queue.is_empty(), "Queue should be empty");
            assert!(
                queue.dequeue(0).is_err(),
                "Should not be able to dequeue from empty queue"
            );

            // Check internal state
            let total = queue.total_items();
            assert_eq!(total, 0, "Total items should be 0, got {}", total);

            libc::free(mem as *mut libc::c_void);
        }
    }

    #[test]
    fn test_dequeue_counter_initialization() {
        unsafe {
            let mem_size = JKMQueue::<usize>::shared_size(1, 1);
            let mem = libc::malloc(mem_size) as *mut u8;
            let queue = JKMQueue::init_in_shared(mem, 1, 1);

            // Verify dequeue counter starts at 0
            let initial_counter = queue.deq_counter.v.load(Ordering::Acquire);
            assert_eq!(initial_counter, 0, "Dequeue counter should start at 0");

            // First dequeue should get number 0
            queue.enqueue(0, 1).unwrap();
            queue.force_sync();

            let _ = queue.dequeue(0);

            let after_first = queue.deq_counter.v.load(Ordering::Acquire);
            assert_eq!(after_first, 1, "After first dequeue, counter should be 1");

            libc::free(mem as *mut libc::c_void);
        }
    }
}
