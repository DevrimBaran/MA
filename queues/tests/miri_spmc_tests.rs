#![allow(clippy::cast_ptr_alignment)]
use queues::spmc::{DavidQueue, EnqueuerState};
use queues::SpmcQueue;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

unsafe fn allocate_shared_memory(size: usize) -> *mut u8 {
    use std::alloc::{alloc_zeroed, Layout};
    let layout = Layout::from_size_align(size, 4096).unwrap();
    let ptr = alloc_zeroed(layout);
    if ptr.is_null() {
        panic!("Failed to allocate shared memory");
    }
    ptr
}

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

        assert!(
            !queue.is_empty(),
            "David queue always returns false for is_empty"
        );

        assert!(
            queue.enqueue(&mut enqueuer_state, 1).is_ok(),
            "Enqueue should succeed"
        );
        assert!(
            queue.enqueue(&mut enqueuer_state, 2).is_ok(),
            "Enqueue should succeed"
        );

        match queue.dequeue(0) {
            Ok(val) => assert_eq!(val, 1, "First dequeued value should be 1"),
            Err(_) => panic!("Dequeue should succeed"),
        }

        match queue.dequeue(1) {
            Ok(val) => assert_eq!(val, 2, "Second dequeued value should be 2"),
            Err(_) => panic!("Dequeue should succeed"),
        }

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

        for i in 0..5 {
            assert!(
                queue.enqueue(&mut enqueuer_state, i).is_ok(),
                "Enqueue {} should succeed",
                i
            );
        }

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

        for i in 0..10 {
            assert!(
                queue.enqueue(&mut enqueuer_state, i).is_ok(),
                "Enqueue {} should succeed",
                i
            );
        }

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

        assert!(
            queue.enqueue(&mut enqueuer_state, 42).is_ok(),
            "Enqueue should work"
        );
        assert!(
            queue.enqueue(&mut enqueuer_state, 43).is_ok(),
            "Enqueue should work"
        );

        match queue.dequeue(0) {
            Ok(val) => assert!(val == 42 || val == 43, "Should dequeue valid value"),
            Err(_) => panic!("Dequeue with valid consumer ID should work"),
        }

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

        assert!(!queue.is_full(), "New queue should not be full");

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

        let items_to_produce = 3;
        let consumed = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

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

        for handle in handles {
            handle.join().unwrap();
        }

        let total_consumed: usize = consumed_per_consumer
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum();

        assert_eq!(total_consumed, total_items, "Should consume all items");

        deallocate_shared_memory(mem, size);
    }
}

#[test]
fn test_spmc_recovery_mechanism() {
    unsafe {
        let num_consumers = 2;
        let size = DavidQueue::<usize>::shared_size(num_consumers);
        let mem = allocate_shared_memory(size);

        let mut enqueuer_state = EnqueuerState::new();
        let queue = DavidQueue::init_in_shared(mem, num_consumers, &mut enqueuer_state);

        for i in 0..5 {
            assert!(queue.enqueue(&mut enqueuer_state, i).is_ok());
        }

        while enqueuer_state.enq_row == 0 {
            if queue.enqueue(&mut enqueuer_state, 999).is_err() {
                break;
            }
        }

        let mut found = Vec::new();
        for _ in 0..10 {
            if let Ok(val) = queue.dequeue(0) {
                if val < 5 {
                    found.push(val);
                }
            }
        }

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

        for cycle in 0..3 {
            for i in 0..5 {
                assert!(
                    queue.enqueue(&mut enqueuer_state, cycle * 100 + i).is_ok(),
                    "Enqueue should succeed"
                );
            }

            for i in 0..5 {
                let consumer_id = i % num_consumers;
                assert!(queue.dequeue(consumer_id).is_ok(), "Dequeue should succeed");
            }
        }

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

        assert!(queue.dequeue(0).is_err());

        assert!(queue.enqueue(&mut enqueuer_state, 1).is_ok());
        assert_eq!(queue.dequeue(0).unwrap(), 1);
        assert!(queue.dequeue(0).is_err());

        for i in 0..5 {
            assert!(queue.enqueue(&mut enqueuer_state, i).is_ok());
            assert_eq!(queue.dequeue(0).unwrap(), i);
        }

        deallocate_shared_memory(mem, size);
    }
}
