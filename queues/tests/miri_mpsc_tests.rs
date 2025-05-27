#![cfg(miri)]

use queues::{mpsc::*, MpscQueue};
use std::sync::{Arc, Barrier};
use std::thread;

// Smaller sizes for Miri performance
const MIRI_PRODUCERS: usize = 2;
const MIRI_ITEMS_PER_PRODUCER: usize = 50;
const MIRI_NODE_POOL: usize = 200;

// Helper for aligned allocation
fn create_aligned_memory(size: usize, alignment: usize) -> Box<[u8]> {
    let layout = std::alloc::Layout::from_size_align(size, alignment).expect("Invalid layout");

    unsafe {
        let ptr = std::alloc::alloc_zeroed(layout);
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Box::from_raw(std::slice::from_raw_parts_mut(ptr, size))
    }
}

mod miri_drescher_tests {
    use super::*;

    #[test]
    fn test_drescher_basic() {
        let expected_nodes = 100;
        let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, expected_nodes) };

        // Basic operations
        assert!(queue.is_empty());
        queue.push(42).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());

        // Multiple items
        for i in 0..10 {
            queue.push(i).unwrap();
        }

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        // Clean up
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_drescher_node_recycling() {
        let nodes = 50;
        let shared_size = DrescherQueue::<String>::shared_size(nodes);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, nodes) };

        // Test node recycling
        for cycle in 0..3 {
            for i in 0..10 {
                queue.push(format!("cycle_{}_item_{}", cycle, i)).unwrap();
            }

            for _ in 0..10 {
                assert!(queue.pop().is_some());
            }
        }

        assert!(queue.is_empty());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_drescher_concurrent() {
        let expected_nodes = 500;
        let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, expected_nodes) };
        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(MIRI_PRODUCERS + 1));

        let mut handles = vec![];

        // Spawn producers
        for producer_id in 0..MIRI_PRODUCERS {
            let queue_clone = queue.clone();
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(move || {
                barrier_clone.wait();

                for i in 0..MIRI_ITEMS_PER_PRODUCER {
                    let value = producer_id * MIRI_ITEMS_PER_PRODUCER + i;
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

        barrier.wait();

        // Wait for producers
        for handle in handles {
            handle.join().unwrap();
        }

        // Collect all items
        let mut items = Vec::new();
        while let Some(item) = queue.pop() {
            items.push(item);
        }

        assert_eq!(items.len(), MIRI_PRODUCERS * MIRI_ITEMS_PER_PRODUCER);
        items.sort();

        for (i, &item) in items.iter().enumerate() {
            assert_eq!(item, i);
        }

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }
}

mod miri_jayanti_petrovic_tests {
    use super::*;

    #[test]
    fn test_jp_basic() {
        let num_producers = 2;
        let node_pool_capacity = 100;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe {
            JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
        };

        assert!(queue.is_empty());

        // Test producer-specific enqueue
        for producer_id in 0..num_producers {
            for i in 0..5 {
                queue.enqueue(producer_id, producer_id * 10 + i).unwrap();
            }
        }

        assert!(!queue.is_empty());

        let mut items = Vec::new();
        while let Some(item) = queue.dequeue() {
            items.push(item);
        }

        assert_eq!(items.len(), num_producers * 5);

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_jp_invalid_producer() {
        let num_producers = 2;
        let node_pool_capacity = 50;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe {
            JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
        };

        // Invalid producer IDs should fail
        assert!(queue.enqueue(num_producers, 42).is_err());
        assert!(queue.enqueue(num_producers + 1, 42).is_err());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    #[should_panic(expected = "Number of producers must be > 0")]
    fn test_jp_zero_producers() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(1, 100);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        unsafe {
            JayantiPetrovicMpscQueue::<usize>::init_in_shared(mem_ptr, 0, 100);
        }
    }
}

mod miri_jiffy_tests {
    use super::*;

    #[test]
    fn test_jiffy_basic() {
        let buffer_capacity = 8;
        let max_buffers = 5;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        assert!(queue.is_empty());

        // Basic push/pop
        queue.push(42).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());

        // Test buffer transitions
        for i in 0..20 {
            queue.push(i).unwrap();
        }

        for i in 0..20 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.is_empty());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_jiffy_out_of_order() {
        let buffer_capacity = 4;
        let max_buffers = 10;

        let shared_size = JiffyQueue::<String>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(3));

        // Producer 1
        let q1 = queue.clone();
        let b1 = barrier.clone();
        let p1 = thread::spawn(move || {
            b1.wait();
            let mut pushed = 0;
            for i in 0..25 {
                if q1.push(format!("p1_{}", i)).is_ok() {
                    pushed += 1;
                } else {
                    break;
                }
            }
            pushed
        });

        // Producer 2
        let q2 = queue.clone();
        let b2 = barrier.clone();
        let p2 = thread::spawn(move || {
            b2.wait();
            let mut pushed = 0;
            for i in 0..25 {
                if q2.push(format!("p2_{}", i)).is_ok() {
                    pushed += 1;
                } else {
                    break;
                }
            }
            pushed
        });

        barrier.wait();

        let pushed1 = p1.join().unwrap();
        let pushed2 = p2.join().unwrap();
        let total_pushed = pushed1 + pushed2;

        // Collect items
        let mut items = Vec::new();
        while !queue.is_empty() && items.len() < total_pushed {
            if let Ok(item) = queue.pop() {
                items.push(item);
            }
        }

        assert_eq!(items.len(), total_pushed);

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }
}

mod miri_dqueue_tests {
    use super::*;

    #[test]
    fn test_dqueue_basic() {
        let num_producers = 2;
        let segment_pool_capacity = 5;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

        assert!(queue.is_empty());

        // Test producer-specific enqueue
        for producer_id in 0..num_producers {
            for i in 0..10 {
                queue.enqueue(producer_id, producer_id * 100 + i).unwrap();
            }
        }

        // Force flush from local buffers
        unsafe {
            for producer_id in 0..num_producers {
                queue.dump_local_buffer(producer_id);
            }
        }

        let mut items = Vec::new();
        while let Some(item) = queue.dequeue() {
            items.push(item);
        }

        assert_eq!(items.len(), num_producers * 10);

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_dqueue_invalid_producer() {
        let num_producers = 2;
        let segment_pool_capacity = 5;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

        // Invalid producer IDs
        assert!(queue.enqueue(num_producers, 42).is_err());
        assert!(queue.enqueue(usize::MAX, 42).is_err());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }
}

// Test drop semantics
mod miri_drop_tests {
    use super::*;
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

    #[test]
    fn test_drescher_drops() {
        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let shared_size = DrescherQueue::<DropCounter>::shared_size(50);
            let memory = create_aligned_memory(shared_size, 64);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };

            // Push items
            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            // Pop half
            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }

        // Note: In Miri, we might not see all drops immediately
    }

    #[test]
    fn test_jiffy_drops() {
        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let shared_size = JiffyQueue::<DropCounter>::shared_size(16, 5);
            let memory = create_aligned_memory(shared_size, 64);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };

            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }
    }
}

// Type safety tests
mod miri_type_tests {
    use super::*;

    #[test]
    fn test_zero_sized_types() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct ZeroSized;

        let shared_size = DrescherQueue::<ZeroSized>::shared_size(50);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };

        queue.push(ZeroSized).unwrap();
        assert_eq!(queue.pop().unwrap(), ZeroSized);

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_large_types() {
        #[derive(Clone, Debug, PartialEq)]
        struct LargeType {
            data: [u64; 32],
        }

        let shared_size = JiffyQueue::<LargeType>::shared_size(4, 3);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 4, 3) };

        let item = LargeType { data: [42; 32] };
        queue.push(item.clone()).unwrap();
        assert_eq!(queue.pop().unwrap(), item);

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }
}

// Test trait implementations
mod miri_trait_tests {
    use super::*;

    fn test_mpsc_trait<T>(queue: &T)
    where
        T: MpscQueue<usize>,
        T::PushError: std::fmt::Debug,
        T::PopError: std::fmt::Debug,
    {
        assert!(queue.is_empty());
        assert!(!queue.is_full());

        queue.push(42).unwrap();
        assert!(!queue.is_empty());

        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_trait_implementations() {
        // Test DrescherQueue
        {
            let shared_size = DrescherQueue::<usize>::shared_size(50);
            let memory = create_aligned_memory(shared_size, 64);
            let mem_ptr = Box::leak(memory).as_mut_ptr();
            let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };
            test_mpsc_trait(&*queue);
            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }

        // Test JiffyQueue
        {
            let shared_size = JiffyQueue::<usize>::shared_size(16, 5);
            let memory = create_aligned_memory(shared_size, 64);
            let mem_ptr = Box::leak(memory).as_mut_ptr();
            let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };
            test_mpsc_trait(&*queue);
            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }
    }

    #[test]
    #[should_panic(expected = "DQueue::push on MpscQueue trait")]
    fn test_dqueue_push_panics() {
        let shared_size = DQueue::<usize>::shared_size(2, 5);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { DQueue::init_in_shared(mem_ptr, 2, 5) };

        // This should panic
        queue.push(42).unwrap();
    }
}
