#[cfg(test)]
mod wcq_tests {
    use queues::mpmc::WCQueue;
    use queues::MpmcQueue;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_debug_stuck_consumer() {
        const ITEMS: usize = 100;

        unsafe {
            let mem = map_shared(WCQueue::<usize>::shared_size(2));
            let queue = Arc::new(WCQueue::init_in_shared(mem, 2));
            let queue_clone = queue.clone();

            let producer = thread::spawn(move || {
                for i in 0..ITEMS {
                    let mut retry = 0;
                    while queue.push(i, 0).is_err() {
                        retry += 1;
                        if retry > 1000 {
                            panic!("Producer stuck at item {}", i);
                        }
                        thread::yield_now();
                    }

                    // Add small delay to test synchronization
                    if i % 10 == 0 {
                        thread::sleep(Duration::from_micros(10));
                    }
                }
                println!("Producer finished");
            });

            let consumer = thread::spawn(move || {
                let mut received = Vec::with_capacity(ITEMS);
                let mut empty_count = 0;
                let mut last_received = 0;

                while received.len() < ITEMS {
                    match queue_clone.pop(1) {
                        Ok(val) => {
                            received.push(val);
                            last_received = val;
                            empty_count = 0;

                            if received.len() % 100 == 0 {
                                println!("Received {} items, last: {}", received.len(), val);
                            }
                        }
                        Err(_) => {
                            empty_count += 1;

                            if empty_count % 100_000 == 0 {
                                println!(
                                    "Consumer waiting... received so far: {}, last: {}",
                                    received.len(),
                                    last_received
                                );

                                // Debug queue state when stuck
                                queue_clone.debug_state();
                            }

                            if empty_count > 10_000_000 {
                                panic!("Consumer stuck after receiving {} items", received.len());
                            }
                            thread::yield_now();
                        }
                    }
                }

                received
            });

            producer.join().unwrap();
            let received = consumer.join().unwrap();

            assert_eq!(received.len(), ITEMS);

            unmap_shared(mem, WCQueue::<usize>::shared_size(2));
        }
    }

    #[test]
    fn test_debug_queue_state() {
        unsafe {
            let mem = map_shared(WCQueue::<usize>::shared_size(2));
            let queue = WCQueue::init_in_shared(mem, 2);

            // Test scenario that reproduces the issue
            println!("Initial state:");
            queue.debug_state();

            // Enqueue some items
            for i in 0..10 {
                if queue.push(i, 0).is_err() {
                    println!("Failed to push item {}", i);
                    break;
                }
            }

            println!("\nAfter enqueuing 10 items:");
            queue.debug_state();

            // Dequeue half
            for i in 0..5 {
                match queue.pop(1) {
                    Ok(v) => println!("Dequeued: {}", v),
                    Err(_) => println!("Failed to dequeue at {}", i),
                }
            }

            println!("\nAfter dequeuing 5 items:");
            queue.debug_state();

            unmap_shared(mem, WCQueue::<usize>::shared_size(2));
        }
    }

    #[test]
    fn test_single_thread_basic() {
        unsafe {
            let mem = map_shared(WCQueue::<usize>::shared_size(1));
            let queue = WCQueue::init_in_shared(mem, 1);

            // Test 1: Basic enqueue/dequeue
            assert!(queue.is_empty());
            assert!(queue.push(42, 0).is_ok());
            assert!(!queue.is_empty());
            assert_eq!(queue.pop(0), Ok(42));
            assert!(queue.is_empty());

            // Test 2: Multiple items
            for i in 0..10 {
                assert!(queue.push(i, 0).is_ok());
            }

            for i in 0..10 {
                assert_eq!(queue.pop(0), Ok(i));
            }

            assert!(queue.is_empty());
            assert!(queue.pop(0).is_err());

            unmap_shared(mem, WCQueue::<usize>::shared_size(1));
        }
    }

    #[test]
    fn test_fifo_order() {
        unsafe {
            let mem = map_shared(WCQueue::<usize>::shared_size(1));
            let queue = WCQueue::init_in_shared(mem, 1);

            let values = vec![1, 2, 3, 4, 5];
            for &v in &values {
                assert!(queue.push(v, 0).is_ok());
            }

            for &expected in &values {
                assert_eq!(queue.pop(0), Ok(expected));
            }

            unmap_shared(mem, WCQueue::<usize>::shared_size(1));
        }
    }

    #[test]
    fn test_queue_capacity() {
        unsafe {
            let mem = map_shared(WCQueue::<usize>::shared_size(1));
            let queue = WCQueue::init_in_shared(mem, 1);

            // Fill the queue
            let mut count = 0;
            while queue.push(count, 0).is_ok() {
                count += 1;
                if count > 100_000 {
                    panic!("Queue should have filled by now");
                }
            }

            println!("Queue capacity: {}", count);

            // Verify we can't add more
            assert!(queue.push(999999, 0).is_err());
            assert!(queue.is_full());

            // Dequeue all
            for i in 0..count {
                assert_eq!(queue.pop(0), Ok(i));
            }

            assert!(queue.is_empty());

            unmap_shared(mem, WCQueue::<usize>::shared_size(1));
        }
    }

    #[test]
    fn test_concurrent_single_producer_single_consumer() {
        const ITEMS: usize = 10_000;

        unsafe {
            let mem = map_shared(WCQueue::<usize>::shared_size(2));
            let queue = Arc::new(WCQueue::init_in_shared(mem, 2));
            let queue_clone = queue.clone();

            let producer = thread::spawn(move || {
                for i in 0..ITEMS {
                    while queue.push(i, 0).is_err() {
                        thread::yield_now();
                    }
                }
            });

            let consumer = thread::spawn(move || {
                let mut received = Vec::with_capacity(ITEMS);
                let mut empty_count = 0;

                while received.len() < ITEMS {
                    match queue_clone.pop(1) {
                        Ok(val) => {
                            received.push(val);
                            empty_count = 0;
                        }
                        Err(_) => {
                            empty_count += 1;
                            if empty_count > 1_000_000 {
                                panic!("Consumer stuck after receiving {} items", received.len());
                            }
                            thread::yield_now();
                        }
                    }
                }

                received
            });

            producer.join().unwrap();
            let received = consumer.join().unwrap();

            // Verify all items received
            assert_eq!(received.len(), ITEMS);
            for (i, &val) in received.iter().enumerate() {
                assert_eq!(val, i, "Wrong value at position {}", i);
            }

            unmap_shared(mem, WCQueue::<usize>::shared_size(2));
        }
    }

    #[test]
    fn test_concurrent_multiple_producers_consumers() {
        const NUM_PRODUCERS: usize = 2;
        const NUM_CONSUMERS: usize = 2;
        const ITEMS_PER_PRODUCER: usize = 1000;
        const TOTAL_ITEMS: usize = NUM_PRODUCERS * ITEMS_PER_PRODUCER;

        unsafe {
            let mem = map_shared(WCQueue::<usize>::shared_size(NUM_PRODUCERS + NUM_CONSUMERS));
            let queue = Arc::new(WCQueue::init_in_shared(mem, NUM_PRODUCERS + NUM_CONSUMERS));

            let produced = Arc::new(AtomicUsize::new(0));
            let consumed = Arc::new(AtomicUsize::new(0));

            let mut producers = vec![];
            for p_id in 0..NUM_PRODUCERS {
                let queue_clone = queue.clone();
                let produced_clone = produced.clone();
                let producer = thread::spawn(move || {
                    let base = p_id * ITEMS_PER_PRODUCER;

                    for i in 0..ITEMS_PER_PRODUCER {
                        let value = base + i;
                        let mut retry_count = 0;
                        while queue_clone.push(value, p_id).is_err() {
                            retry_count += 1;
                            if retry_count > 1_000_000 {
                                panic!("Producer {} failed to enqueue item {}", p_id, i);
                            }
                            thread::yield_now();
                        }
                        produced_clone.fetch_add(1, Ordering::SeqCst);
                    }
                });
                producers.push(producer);
            }

            let mut consumers = vec![];
            for c_id in 0..NUM_CONSUMERS {
                let queue_clone = queue.clone();
                let consumed_clone = consumed.clone();
                let produced_clone = produced.clone();
                let consumer = thread::spawn(move || {
                    let mut received = Vec::new();
                    let mut empty_count = 0;
                    let consumer_id = NUM_PRODUCERS + c_id;

                    loop {
                        match queue_clone.pop(consumer_id) {
                            Ok(val) => {
                                received.push(val);
                                consumed_clone.fetch_add(1, Ordering::SeqCst);
                                empty_count = 0;
                            }
                            Err(_) => {
                                // Check if all items have been consumed
                                if consumed_clone.load(Ordering::SeqCst) >= TOTAL_ITEMS {
                                    break;
                                }

                                empty_count += 1;
                                if empty_count > 10_000_000 {
                                    let consumed_so_far = consumed_clone.load(Ordering::SeqCst);
                                    let produced_so_far = produced_clone.load(Ordering::SeqCst);
                                    panic!(
                                        "Consumer {} stuck. Consumed: {}/{}, Produced: {}",
                                        c_id, consumed_so_far, TOTAL_ITEMS, produced_so_far
                                    );
                                }
                                thread::yield_now();
                            }
                        }
                    }

                    received
                });
                consumers.push(consumer);
            }

            // Wait for all producers
            for producer in producers {
                producer.join().unwrap();
            }

            // Give consumers time to finish
            thread::sleep(Duration::from_millis(100));

            // Collect results from consumers
            let mut all_received = Vec::new();
            for consumer in consumers {
                let mut received = consumer.join().unwrap();
                all_received.append(&mut received);
            }

            // Verify results
            assert_eq!(
                all_received.len(),
                TOTAL_ITEMS,
                "Lost {} items",
                TOTAL_ITEMS - all_received.len()
            );

            // Check for duplicates
            let mut seen = HashSet::new();
            for &val in &all_received {
                if !seen.insert(val) {
                    panic!("Duplicate value: {}", val);
                }
            }

            // Verify all values are present
            for p_id in 0..NUM_PRODUCERS {
                for i in 0..ITEMS_PER_PRODUCER {
                    let expected = p_id * ITEMS_PER_PRODUCER + i;
                    assert!(seen.contains(&expected), "Missing value: {}", expected);
                }
            }

            unmap_shared(
                mem,
                WCQueue::<usize>::shared_size(NUM_PRODUCERS + NUM_CONSUMERS),
            );
        }
    }

    #[test]
    fn test_stress_many_threads() {
        const NUM_THREADS: usize = 8;
        const ITEMS_PER_THREAD: usize = 100;

        unsafe {
            let mem = map_shared(WCQueue::<usize>::shared_size(NUM_THREADS));
            let queue = Arc::new(WCQueue::init_in_shared(mem, NUM_THREADS));

            let total_enqueued = Arc::new(AtomicUsize::new(0));
            let total_dequeued = Arc::new(AtomicUsize::new(0));

            let mut threads = vec![];

            for tid in 0..NUM_THREADS {
                let queue_clone = queue.clone();
                let enqueued = total_enqueued.clone();
                let dequeued = total_dequeued.clone();

                let handle = thread::spawn(move || {
                    let mut my_enqueued = 0;
                    let mut my_dequeued = 0;

                    // Mix enqueue and dequeue operations
                    for i in 0..ITEMS_PER_THREAD * 2 {
                        if i % 2 == 0 && my_enqueued < ITEMS_PER_THREAD {
                            // Try to enqueue
                            let value = tid * 1000 + my_enqueued;
                            if queue_clone.push(value, tid).is_ok() {
                                my_enqueued += 1;
                                enqueued.fetch_add(1, Ordering::SeqCst);
                            }
                        } else {
                            // Try to dequeue
                            if queue_clone.pop(tid).is_ok() {
                                my_dequeued += 1;
                                dequeued.fetch_add(1, Ordering::SeqCst);
                            }
                        }

                        if i % 10 == 0 {
                            thread::yield_now();
                        }
                    }

                    // Ensure all items are enqueued
                    while my_enqueued < ITEMS_PER_THREAD {
                        let value = tid * 1000 + my_enqueued;
                        if queue_clone.push(value, tid).is_ok() {
                            my_enqueued += 1;
                            enqueued.fetch_add(1, Ordering::SeqCst);
                        }
                        thread::yield_now();
                    }

                    (my_enqueued, my_dequeued)
                });

                threads.push(handle);
            }

            // Wait for all threads to finish enqueuing
            let mut total_enqueued_local = 0;
            let mut total_dequeued_local = 0;

            for handle in threads {
                let (enq, deq) = handle.join().unwrap();
                total_enqueued_local += enq;
                total_dequeued_local += deq;
            }

            println!(
                "Total enqueued: {}, Total dequeued so far: {}",
                total_enqueued_local, total_dequeued_local
            );

            // Now drain the queue
            let mut drain_threads = vec![];
            let remaining = total_enqueued_local - total_dequeued_local;
            let per_thread = (remaining + NUM_THREADS - 1) / NUM_THREADS;

            for tid in 0..NUM_THREADS {
                let queue_clone = queue.clone();
                let dequeued = total_dequeued.clone();
                let handle = thread::spawn(move || {
                    let mut my_dequeued = 0;
                    let mut empty_count = 0;

                    while my_dequeued < per_thread {
                        if queue_clone.pop(tid).is_ok() {
                            my_dequeued += 1;
                            dequeued.fetch_add(1, Ordering::SeqCst);
                            empty_count = 0;
                        } else {
                            empty_count += 1;
                            if empty_count > 1_000_000 {
                                break; // Probably done
                            }
                            thread::yield_now();
                        }
                    }

                    my_dequeued
                });
                drain_threads.push(handle);
            }

            let mut additional_dequeued = 0;
            for handle in drain_threads {
                additional_dequeued += handle.join().unwrap();
            }

            total_dequeued_local += additional_dequeued;

            println!(
                "Final: Enqueued: {}, Dequeued: {}",
                total_enqueued_local, total_dequeued_local
            );

            assert_eq!(
                total_enqueued_local,
                total_dequeued_local,
                "Lost {} items",
                total_enqueued_local - total_dequeued_local
            );

            unmap_shared(mem, WCQueue::<usize>::shared_size(NUM_THREADS));
        }
    }

    #[test]
    fn test_slow_path_triggers() {
        // Test that forces slow path by having multiple threads compete
        const NUM_THREADS: usize = 4;
        const ITEMS: usize = 100;

        unsafe {
            let mem = map_shared(WCQueue::<usize>::shared_size(NUM_THREADS));
            let queue = Arc::new(WCQueue::init_in_shared(mem, NUM_THREADS));

            let barrier = Arc::new(std::sync::Barrier::new(NUM_THREADS));
            let mut threads = vec![];

            for tid in 0..NUM_THREADS {
                let queue_clone = queue.clone();
                let barrier_clone = barrier.clone();
                let handle = thread::spawn(move || {
                    // Synchronize all threads to start at the same time
                    barrier_clone.wait();

                    // All threads try to enqueue at once
                    let mut successes = 0;
                    for i in 0..ITEMS {
                        let value = tid * 1000 + i;
                        if queue_clone.push(value, tid).is_ok() {
                            successes += 1;
                        }
                    }

                    successes
                });
                threads.push(handle);
            }

            let mut total_enqueued = 0;
            for handle in threads {
                total_enqueued += handle.join().unwrap();
            }

            println!("Total enqueued in slow path test: {}", total_enqueued);

            // Dequeue all
            let mut total_dequeued = 0;
            let mut empty_count = 0;
            while empty_count < 1_000_000 {
                if queue.pop(0).is_ok() {
                    total_dequeued += 1;
                    empty_count = 0;
                } else {
                    empty_count += 1;
                }
            }

            assert_eq!(
                total_enqueued,
                total_dequeued,
                "Lost {} items in slow path test",
                total_enqueued - total_dequeued
            );

            unmap_shared(mem, WCQueue::<usize>::shared_size(NUM_THREADS));
        }
    }

    // Helper functions
    unsafe fn map_shared(bytes: usize) -> *mut u8 {
        use std::ptr;
        let ptr = libc::mmap(
            ptr::null_mut(),
            bytes,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if ptr == libc::MAP_FAILED {
            panic!("mmap failed: {}", std::io::Error::last_os_error());
        }
        ptr.cast()
    }

    unsafe fn unmap_shared(ptr: *mut u8, len: usize) {
        if libc::munmap(ptr.cast(), len) == -1 {
            panic!("munmap failed: {}", std::io::Error::last_os_error());
        }
    }
}
