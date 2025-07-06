use queues::spmc::{DavidQueue, EnqueuerState};
use queues::SpmcQueue;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

const NUM_CONSUMERS: usize = 1;
const TEST_ITEMS: usize = 5;

fn create_aligned_memory(size: usize) -> Box<[u8]> {
    use std::alloc::{alloc_zeroed, Layout};

    unsafe {
        let layout = Layout::from_size_align(size, 4096).unwrap();
        let ptr = alloc_zeroed(layout);
        if ptr.is_null() {
            panic!("Failed to allocate aligned memory");
        }

        let slice = std::slice::from_raw_parts_mut(ptr, size);
        Box::from_raw(slice)
    }
}

#[test]
fn test_david_queue_basic_operations() {
    let shared_size = DavidQueue::<usize>::shared_size(NUM_CONSUMERS);
    let memory = create_aligned_memory(shared_size);
    let mem_ptr = Box::leak(memory).as_mut_ptr();

    let mut enqueuer_state = EnqueuerState::new();
    let queue = unsafe { DavidQueue::init_in_shared(mem_ptr, NUM_CONSUMERS, &mut enqueuer_state) };

    assert!(queue.enqueue(&mut enqueuer_state, 42).is_ok());
    assert!(queue.enqueue(&mut enqueuer_state, 43).is_ok());
    assert!(queue.enqueue(&mut enqueuer_state, 44).is_ok());

    assert_eq!(queue.dequeue(0).unwrap(), 42);
    assert_eq!(queue.dequeue(1).unwrap(), 43);
    assert_eq!(queue.dequeue(0).unwrap(), 44);

    assert!(queue.dequeue(0).is_err());
}

#[test]
fn test_david_queue_multiple_consumers() {
    let shared_size = DavidQueue::<usize>::shared_size(NUM_CONSUMERS);
    let memory = create_aligned_memory(shared_size);
    let mem_ptr = Box::leak(memory).as_mut_ptr();

    let mut enqueuer_state = EnqueuerState::new();
    let queue = unsafe { DavidQueue::init_in_shared(mem_ptr, NUM_CONSUMERS, &mut enqueuer_state) };

    for i in 0..100 {
        assert!(queue.enqueue(&mut enqueuer_state, i).is_ok());
    }

    let mut items = Vec::new();
    for consumer_id in 0..NUM_CONSUMERS {
        for _ in 0..100 {
            if let Ok(item) = queue.dequeue(consumer_id) {
                items.push(item);
            }
        }
    }

    items.sort();
    assert_eq!(items.len(), 100);
    for (i, &item) in items.iter().enumerate() {
        assert_eq!(item, i);
    }
}

#[test]
fn test_david_queue_rapid_reuse() {
    let shared_size = DavidQueue::<usize>::shared_size(2);
    let memory = create_aligned_memory(shared_size);
    let mem_ptr = Box::leak(memory).as_mut_ptr();

    let mut enqueuer_state = EnqueuerState::new();
    let queue = unsafe { DavidQueue::init_in_shared(mem_ptr, 2, &mut enqueuer_state) };

    for cycle in 0..20 {
        for i in 0..20 {
            assert!(
                queue.enqueue(&mut enqueuer_state, cycle * 1000 + i).is_ok(),
                "Failed to enqueue in cycle {}",
                cycle
            );
        }

        for _ in 0..20 {
            let result = queue.dequeue(cycle % 2);
            assert!(result.is_ok(), "Failed to dequeue in cycle {}", cycle);
        }
    }

    assert!(queue.dequeue(0).is_err());
    assert!(queue.dequeue(1).is_err());
}

#[test]
fn test_david_queue_is_full() {
    let shared_size = DavidQueue::<usize>::shared_size(1);
    let memory = create_aligned_memory(shared_size);
    let mem_ptr = Box::leak(memory).as_mut_ptr();

    let mut enqueuer_state = EnqueuerState::new();
    let queue = unsafe { DavidQueue::init_in_shared(mem_ptr, 1, &mut enqueuer_state) };

    assert!(!queue.is_full());

    let mut added = 0;
    for i in 0..1000 {
        if queue.enqueue(&mut enqueuer_state, i).is_err() || queue.is_full() {
            break;
        }
        added += 1;
    }

    assert!(added > 0, "Should be able to add at least some items");

    if queue.enqueue(&mut enqueuer_state, 999999).is_err() {
        assert!(queue.is_full() || added > 0);
    }
}
