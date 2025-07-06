#![allow(clippy::cast_ptr_alignment)]
use queues::spmc::{DavidQueue, EnqueuerState};
use queues::SpmcQueue;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

// Helper function to allocate aligned shared memory
unsafe fn allocate_shared_memory(size: usize) -> *mut u8 {
    use std::alloc::{alloc_zeroed, Layout};
    let layout = Layout::from_size_align(size, 4096).unwrap();
    let ptr = alloc_zeroed(layout);
    if ptr.is_null() {
        panic!("Failed to allocate shared memory");
    }
    ptr
}

// Helper function to deallocate shared memory
unsafe fn deallocate_shared_memory(ptr: *mut u8, size: usize) {
    use std::alloc::{dealloc, Layout};
    let layout = Layout::from_size_align(size, 4096).unwrap();
    dealloc(ptr, layout);
}

#[test]
fn test_spmc_basic_operations() {
    unsafe {
        let num_consumers = 2;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::init_in_shared(mem, num_consumers, &mut enqueuer_state);

        // Test is_empty on new queue (always returns false for David queue)
        assert!(
            !queue.is_empty(),
            "David queue always returns false for is_empty"
        );

        // Test enqueue
        assert!(
            queue.enqueue(&mut enqueuer_state, 1).is_ok(),
            "Enqueue should succeed"
        );
        assert!(
            queue.enqueue(&mut enqueuer_state, 2).is_ok(),
            "Enqueue should succeed"
        );

        // Test dequeue
        match queue.dequeue(0) {
            Ok(val) => assert_eq!(val, 1, "First dequeued value should be 1"),
            Err(_) => panic!("Dequeue should succeed"),
        }

        match queue.dequeue(1) {
            Ok(val) => assert_eq!(val, 2, "Second dequeued value should be 2"),
            Err(_) => panic!("Dequeue should succeed"),
        }

        // Test dequeue from empty queue
        assert!(
            queue.dequeue(0).is_err(),
            "Dequeue from empty queue should fail"
        );

        deallocate_shared_memory(mem, size);
    }
}

#[test]
fn test_spmc_small_sequence() {
    unsafe {
        let num_consumers = 2;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::init_in_shared(mem, num_consumers, &mut enqueuer_state);

        // Enqueue a few items
        for i in 0..5 {
            assert!(
                queue.enqueue(&mut enqueuer_state, i).is_ok(),
                "Enqueue {} should succeed",
                i
            );
        }

        // Dequeue all items using different consumers
        for i in 0..5 {
            match queue.dequeue(i % num_consumers) {
                Ok(val) => assert_eq!(val, i, "Dequeued value should be {}", i),
                Err(_) => panic!("Dequeue {} should succeed", i),
            }
        }

        assert!(queue.dequeue(0).is_err(), "Queue should be empty");

        deallocate_shared_memory(mem, size);
    }
}

#[test]
fn test_spmc_multiple_operations() {
    unsafe {
        let num_consumers = 2;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::init_in_shared(mem, num_consumers, &mut enqueuer_state);

        // Enqueue multiple items (keeping it at 10 like other tests)
        for i in 0..10 {
            assert!(
                queue.enqueue(&mut enqueuer_state, i).is_ok(),
                "Enqueue {} should succeed",
                i
            );
        }

        // Dequeue all items alternating between consumers
        for i in 0..10 {
            match queue.dequeue(i % num_consumers) {
                Ok(val) => assert_eq!(val, i, "Dequeued value should be {}", i),
                Err(_) => panic!("Dequeue {} should succeed", i),
            }
        }

        assert!(queue.dequeue(0).is_err(), "Queue should be empty");

        deallocate_shared_memory(mem, size);
    }
}

#[test]
fn test_spmc_consumer_id_bounds() {
    unsafe {
        let num_consumers = 2;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::init_in_shared(mem, num_consumers, &mut enqueuer_state);

        // Test with valid consumer IDs first
        assert!(
            queue.enqueue(&mut enqueuer_state, 42).is_ok(),
            "Enqueue should work"
        );
        assert!(
            queue.enqueue(&mut enqueuer_state, 43).is_ok(),
            "Enqueue should work"
        );

        // Test dequeue with valid IDs
        match queue.dequeue(0) {
            Ok(val) => assert!(val == 42 || val == 43, "Should dequeue valid value"),
            Err(_) => panic!("Dequeue with valid consumer ID should work"),
        }

        // Test with invalid consumer ID (should still work due to recovery mechanism)
        let _ = queue.dequeue(num_consumers);
        let _ = queue.dequeue(num_consumers + 1);

        deallocate_shared_memory(mem, size);
    }
}

#[test]
fn test_spmc_is_full_basic() {
    unsafe {
        let num_consumers = 1;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::init_in_shared(mem, num_consumers, &mut enqueuer_state);

        // New queue should not be full
        assert!(!queue.is_full(), "New queue should not be full");

        // Push one item and check again
        let _ = queue.enqueue(&mut enqueuer_state, 1);
        assert!(!queue.is_full(), "Queue with one item should not be full");

        deallocate_shared_memory(mem, size);
    }
}

#[test]
fn test_spmc_simple_concurrent() {
    unsafe {
        let num_consumers = 2;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::<usize>::init_in_shared(mem, num_consumers, &mut enqueuer_state);
        let queue_ptr = queue as *const _ as usize;
        let enqueuer_state_ptr = &mut enqueuer_state as *mut EnqueuerState as usize;

        // Very small numbers for Miri
        let items_to_produce = 3;
        let consumed = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // One producer thread
        let handle = thread::spawn(move || {
            let q = &*(queue_ptr as *const DavidQueue<usize>);
            let state = &mut *(enqueuer_state_ptr as *mut EnqueuerState);

            for i in 0..items_to_produce {
                let mut retries = 0;
                while q.enqueue(state, i).is_err() && retries < 5 {
                    retries += 1;
                    thread::yield_now();
                }
            }
        });
        handles.push(handle);

        // Two consumer threads
        for consumer_id in 0..num_consumers {
            let c = Arc::clone(&consumed);
            let handle = thread::spawn(move || {
                let q = &*(queue_ptr as *const DavidQueue<usize>);
                let mut attempts = 0;

                while c.load(Ordering::Relaxed) < items_to_produce && attempts < 20 {
                    if q.dequeue(consumer_id).is_ok() {
                        c.fetch_add(1, Ordering::Relaxed);
                    }
                    attempts += 1;
                    thread::yield_now();
                }
            });
            handles.push(handle);
        }

        // Wait for threads
        for handle in handles {
            handle.join().unwrap();
        }

        let consumed_count = consumed.load(Ordering::Relaxed);
        assert_eq!(
            consumed_count, items_to_produce,
            "Should consume all items. Consumed: {}",
            consumed_count
        );

        deallocate_shared_memory(mem, size);
    }
}

#[test]
fn test_spmc_concurrent_multiple_consumers() {
    unsafe {
        let num_consumers = 3;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::<usize>::init_in_shared(mem, num_consumers, &mut enqueuer_state);
        let queue_ptr = queue as *const _ as usize;
        let enqueuer_state_ptr = &mut enqueuer_state as *mut EnqueuerState as usize;

        let items_per_consumer = 2;
        let total_items = items_per_consumer * num_consumers;
        let consumed_per_consumer = Arc::new(
            (0..num_consumers)
                .map(|_| AtomicUsize::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        let mut handles = vec![];

        // Producer thread
        let handle = thread::spawn(move || {
            let q = unsafe { &*(queue_ptr as *const DavidQueue<usize>) };
            let state = unsafe { &mut *(enqueuer_state_ptr as *mut EnqueuerState) };

            for i in 0..total_items {
                let mut retries = 0;
                while q.enqueue(state, i).is_err() && retries < 5 {
                    retries += 1;
                    thread::yield_now();
                }
            }
        });
        handles.push(handle);

        // Consumer threads
        for consumer_id in 0..num_consumers {
            let counts = Arc::clone(&consumed_per_consumer);
            let handle = thread::spawn(move || {
                let q = unsafe { &*(queue_ptr as *const DavidQueue<usize>) };
                let mut local_count = 0;
                let mut attempts = 0;

                while local_count < items_per_consumer && attempts < 50 {
                    if q.dequeue(consumer_id).is_ok() {
                        local_count += 1;
                        counts[consumer_id].fetch_add(1, Ordering::Relaxed);
                    }
                    attempts += 1;
                    thread::yield_now();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify total consumption
        let total_consumed: usize = consumed_per_consumer
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum();

        assert_eq!(total_consumed, total_items, "Should consume all items");

        deallocate_shared_memory(mem, size);
    }
}

// Test specific to David queue's recovery mechanism
#[test]
fn test_spmc_recovery_mechanism() {
    unsafe {
        let num_consumers = 2;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::init_in_shared(mem, num_consumers, &mut enqueuer_state);

        // Enqueue items in multiple rows
        for i in 0..5 {
            assert!(queue.enqueue(&mut enqueuer_state, i).is_ok());
        }

        // Force advancement to next row by filling current row
        // This tests the row transition logic
        while enqueuer_state.enq_row == 0 {
            if queue.enqueue(&mut enqueuer_state, 999).is_err() {
                break;
            }
        }

        // Now dequeue - should still get items from previous rows
        let mut found = Vec::new();
        for _ in 0..10 {
            if let Ok(val) = queue.dequeue(0) {
                if val < 5 {
                    found.push(val);
                }
            }
        }

        // Should have found at least some of the original items
        assert!(!found.is_empty(), "Should recover items from previous rows");

        deallocate_shared_memory(mem, size);
    }
}

#[test]
fn test_spmc_spsc_variant() {
    unsafe {
        let size = DavidQueue::<usize>::spsc_shared_size();
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::init_in_shared_spsc(mem, &mut enqueuer_state);

        // Test basic SPSC operations
        assert!(queue.enqueue(&mut enqueuer_state, 42).is_ok());
        assert!(queue.enqueue(&mut enqueuer_state, 43).is_ok());

        assert_eq!(queue.dequeue(0).unwrap(), 42);
        assert_eq!(queue.dequeue(0).unwrap(), 43);
        assert!(queue.dequeue(0).is_err());

        deallocate_shared_memory(mem, size);
    }
}

#[test]
fn test_spmc_stress_small() {
    unsafe {
        let num_consumers = 2;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::init_in_shared(mem, num_consumers, &mut enqueuer_state);

        // Small stress test suitable for Miri
        for cycle in 0..3 {
            // Enqueue batch
            for i in 0..5 {
                assert!(
                    queue.enqueue(&mut enqueuer_state, cycle * 100 + i).is_ok(),
                    "Enqueue should succeed"
                );
            }

            // Dequeue batch
            for i in 0..5 {
                let consumer_id = i % num_consumers;
                assert!(queue.dequeue(consumer_id).is_ok(), "Dequeue should succeed");
            }
        }

        // Queue should be empty
        assert!(queue.dequeue(0).is_err());

        deallocate_shared_memory(mem, size);
    }
}

#[test]
fn test_spmc_edge_cases() {
    unsafe {
        let num_consumers = 1;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::init_in_shared(mem, num_consumers, &mut enqueuer_state);

        // Test empty dequeue
        assert!(queue.dequeue(0).is_err());

        // Test single item
        assert!(queue.enqueue(&mut enqueuer_state, 1).is_ok());
        assert_eq!(queue.dequeue(0).unwrap(), 1);
        assert!(queue.dequeue(0).is_err());

        // Test alternating enqueue/dequeue
        for i in 0..5 {
            assert!(queue.enqueue(&mut enqueuer_state, i).is_ok());
            assert_eq!(queue.dequeue(0).unwrap(), i);
        }

        deallocate_shared_memory(mem, size);
    }
}
