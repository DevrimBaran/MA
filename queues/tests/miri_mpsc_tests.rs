#![cfg(miri)]

use queues::{mpsc::*, MpscQueue};
use std::sync::{Arc, Barrier};
use std::thread;

const MIRI_PRODUCERS: usize = 2;
const MIRI_ITEMS_PER_PRODUCER: usize = 50;
const MIRI_NODE_POOL: usize = 200;

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

// Define BenchMpscQueue trait for miri tests
trait BenchMpscQueue<T: Send>: Send + Sync + 'static {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()>;
    fn bench_pop(&self) -> Result<T, ()>;
    fn bench_is_empty(&self) -> bool;
    fn bench_is_full(&self) -> bool;
}

impl<T: Send + 'static + std::fmt::Debug> BenchMpscQueue<T> for DrescherQueue<T> {
    fn bench_push(&self, item: T, _producer_id: usize) -> Result<(), ()> {
        MpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        MpscQueue::pop(self).map_err(|_| ())
    }
    fn bench_is_empty(&self) -> bool {
        MpscQueue::is_empty(self)
    }
    fn bench_is_full(&self) -> bool {
        MpscQueue::is_full(self)
    }
}

impl<T: Send + Clone + 'static> BenchMpscQueue<T> for JayantiPetrovicMpscQueue<T> {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
        self.enqueue(producer_id, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        self.dequeue().ok_or(())
    }
    fn bench_is_empty(&self) -> bool {
        MpscQueue::is_empty(self)
    }
    fn bench_is_full(&self) -> bool {
        MpscQueue::is_full(self)
    }
}

impl<T: Send + 'static + Clone + std::fmt::Debug> BenchMpscQueue<T> for JiffyQueue<T> {
    fn bench_push(&self, item: T, _producer_id: usize) -> Result<(), ()> {
        MpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        MpscQueue::pop(self).map_err(|_| ())
    }
    fn bench_is_empty(&self) -> bool {
        MpscQueue::is_empty(self)
    }
    fn bench_is_full(&self) -> bool {
        MpscQueue::is_full(self)
    }
}

impl<T: Send + Clone + 'static> BenchMpscQueue<T> for DQueue<T> {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
        self.enqueue(producer_id, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        MpscQueue::pop(self).map_err(|_| ())
    }
    fn bench_is_empty(&self) -> bool {
        MpscQueue::is_empty(self)
    }
    fn bench_is_full(&self) -> bool {
        MpscQueue::is_full(self)
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

        assert!(queue.is_empty());
        queue.push(42).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());

        for i in 0..10 {
            queue.push(i).unwrap();
        }

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_drescher_capacity() {
        let expected_nodes = 10; // Smaller for miri
        let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, expected_nodes) };

        let mut pushed = 0;
        for i in 0..expected_nodes {
            match queue.push(i) {
                Ok(_) => pushed += 1,
                Err(_) => break,
            }
        }

        assert_eq!(
            pushed,
            expected_nodes - 1,
            "Should push exactly expected_nodes - 1 items"
        );
        assert!(queue.is_full());

        // Free some space and push more
        let items_to_pop = 3.min(pushed);
        for _ in 0..items_to_pop {
            queue.pop().unwrap();
        }

        for i in 0..items_to_pop {
            queue.push(100 + i).unwrap();
        }

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

        for handle in handles {
            handle.join().unwrap();
        }

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

    #[test]
    fn test_drescher_bench_interface() {
        let shared_size = DrescherQueue::<usize>::shared_size(100);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 100) };

        for producer_id in 0..4 {
            for i in 0..10 {
                queue
                    .bench_push(producer_id * 100 + i, producer_id)
                    .unwrap();
            }
        }

        let mut count = 0;
        while queue.bench_pop().is_ok() {
            count += 1;
        }

        assert_eq!(count, 40);
        assert!(queue.bench_is_empty());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_drescher_error_propagation() {
        let size = DrescherQueue::<usize>::shared_size(2);
        let mem = create_aligned_memory(size, 64);
        let mem_ptr = Box::leak(mem).as_mut_ptr();
        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 2) };

        queue.push(1).unwrap();

        let mut pushed = 1;
        for i in 2..10 {
            match queue.push(i) {
                Ok(_) => pushed += 1,
                Err(val) => {
                    assert_eq!(val, i);
                    break;
                }
            }
        }

        assert!(pushed >= 1);

        queue.pop().unwrap();
        queue.push(100).unwrap();

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, size));
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
    fn test_jp_producer_specific_enqueue() {
        let num_producers = 4;
        let node_pool_capacity = 200;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe {
            JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
        };

        for producer_id in 0..num_producers {
            for i in 0..10 {
                let value = producer_id * 100 + i;
                queue.enqueue(producer_id, value).unwrap();
            }
        }

        let mut items = Vec::new();
        while let Some(item) = queue.dequeue() {
            items.push(item);
        }

        assert_eq!(items.len(), num_producers * 10);

        items.sort();
        for producer_id in 0..num_producers {
            for i in 0..10 {
                let expected = producer_id * 100 + i;
                assert!(items.contains(&expected));
            }
        }

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

    #[test]
    fn test_jp_concurrent_producers() {
        let num_producers = 2; // Reduced for miri
        let node_pool_capacity = 200;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe {
            JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
        };

        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(num_producers + 1));
        let mut handles = vec![];

        for producer_id in 0..num_producers {
            let queue_clone = queue.clone();
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(move || {
                barrier_clone.wait();

                for i in 0..MIRI_ITEMS_PER_PRODUCER {
                    let value = producer_id * MIRI_ITEMS_PER_PRODUCER + i;
                    queue_clone.enqueue(producer_id, value).unwrap();
                }
            });

            handles.push(handle);
        }

        barrier.wait();

        for handle in handles {
            handle.join().unwrap();
        }

        let mut items = Vec::new();
        while let Some(item) = queue.dequeue() {
            items.push(item);
        }

        assert_eq!(items.len(), num_producers * MIRI_ITEMS_PER_PRODUCER);

        items.sort();
        for (i, &item) in items.iter().enumerate() {
            assert_eq!(item, i);
        }

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_jp_tree_operations() {
        let num_producers = 4; // Reduced from 8 for miri
        let node_pool = 200;

        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool) };

        // Insert in reverse order
        for producer_id in (0..num_producers).rev() {
            queue.enqueue(producer_id, producer_id).unwrap();
        }

        // Should dequeue in reverse order due to tree priority
        for expected in (0..num_producers).rev() {
            assert_eq!(queue.dequeue().unwrap(), expected);
        }

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_jp_multiple_producers_interleaved() {
        let num_producers = 2;
        let node_pool = 200;

        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool) };

        for round in 0..5 {
            // Reduced rounds for miri
            for producer_id in 0..num_producers {
                queue
                    .enqueue(producer_id, producer_id * 1000 + round)
                    .unwrap();
            }
        }

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
    fn test_jp_bench_interface() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(4, 200);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, 4, 200) };

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

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    #[should_panic(expected = "JayantiPetrovicMpscQueue::push from MpscQueue trait")]
    fn test_jp_push_panics() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(2, 100);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, 2, 100) };

        queue.push(42).unwrap();
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

        queue.push(42).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());

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
    fn test_jiffy_buffer_transitions() {
        let buffer_capacity = 4;
        let max_buffers = 10;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        let total_items = 20;
        for i in 0..total_items {
            queue.push(i).unwrap();
        }

        for i in 0..total_items {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(
            queue.is_empty(),
            "Queue should be empty after popping all items"
        );
        assert!(queue.pop().is_err(), "Pop should fail on empty queue");

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_jiffy_buffer_folding() {
        let buffer_capacity = 4;
        let max_buffers = 10;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        // Fill 3 buffers
        for i in 0..12 {
            queue.push(i).unwrap();
        }

        // Pop from first buffer
        for i in 0..4 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        // Add more items
        for i in 12..20 {
            queue.push(i).unwrap();
        }

        // Pop remaining from original items
        for i in 4..12 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        // Pop new items
        for i in 12..20 {
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

    #[test]
    fn test_jiffy_buffer_pool_exhaustion() {
        let buffer_capacity = 2;
        let max_buffers = 2;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        let mut pushed = 0;
        for i in 0..10 {
            if queue.push(i).is_ok() {
                pushed += 1;
            }
        }

        assert!(pushed <= buffer_capacity * max_buffers);

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_jiffy_empty_buffer_handling() {
        let buffer_capacity = 8;
        let max_buffers = 3;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        queue.push(1).unwrap();
        queue.push(2).unwrap();

        assert_eq!(queue.pop().unwrap(), 1);

        for i in 3..10 {
            queue.push(i).unwrap();
        }

        assert_eq!(queue.pop().unwrap(), 2);
        for i in 3..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_jiffy_bench_interface() {
        let shared_size = JiffyQueue::<usize>::shared_size(16, 5);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };

        queue.bench_push(42, 0).unwrap();
        assert!(!queue.bench_is_empty());
        assert_eq!(queue.bench_pop().unwrap(), 42);
        assert!(queue.bench_is_empty());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_jiffy_concurrent_empty_checks() {
        let size = JiffyQueue::<usize>::shared_size(16, 5);
        let memory = create_aligned_memory(size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };

        assert!(queue.is_empty(), "Queue should start empty");

        queue.push(1).unwrap();
        assert!(!queue.is_empty(), "Queue should not be empty after push");

        queue.push(2).unwrap();
        assert!(!queue.is_empty(), "Queue should not be empty with 2 items");

        assert_eq!(queue.pop().unwrap(), 1);
        assert!(
            !queue.is_empty(),
            "Queue should not be empty with 1 item remaining"
        );

        assert_eq!(queue.pop().unwrap(), 2);
        assert!(
            queue.is_empty(),
            "Queue should be empty after popping all items"
        );

        assert!(queue.pop().is_err(), "Pop should fail on empty queue");

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, size));
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

        for producer_id in 0..num_producers {
            for i in 0..10 {
                queue.enqueue(producer_id, producer_id * 100 + i).unwrap();
            }
        }

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

        assert!(queue.enqueue(num_producers, 42).is_err());
        assert!(queue.enqueue(usize::MAX, 42).is_err());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_dqueue_local_buffer_operations() {
        let num_producers = 2;
        let segment_pool_capacity = 10;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

        let items_to_push = 10;
        for i in 0..items_to_push {
            queue.enqueue(0, i).unwrap();
        }

        unsafe {
            queue.dump_local_buffer(0);
        }

        let mut count = 0;
        for _ in 0..items_to_push {
            if queue.dequeue().is_some() {
                count += 1;
            }
        }

        assert_eq!(count, items_to_push, "Should have dequeued all items");

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_dqueue_gc_operations() {
        let num_producers = 2;
        let segment_pool_capacity = 5;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

        let total_items = 20;

        for i in 0..total_items {
            queue.enqueue(0, i).unwrap();
        }

        unsafe {
            queue.dump_local_buffer(0);
        }

        for _ in 0..total_items / 2 {
            queue.dequeue();
        }

        unsafe {
            queue.run_gc();
        }

        let mut remaining = 0;
        while queue.dequeue().is_some() {
            remaining += 1;
        }

        assert!(remaining > 0);

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_dqueue_concurrent_with_helping() {
        let num_producers = 2;
        let segment_pool_capacity = 10;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

        for producer_id in 0..num_producers {
            for i in 0..10 {
                queue.enqueue(producer_id, producer_id * 1000 + i).unwrap();
            }
            unsafe {
                queue.dump_local_buffer(producer_id);
            }
        }

        let mut items = Vec::new();
        while let Some(item) = queue.dequeue() {
            items.push(item);
        }

        assert_eq!(
            items.len(),
            num_producers * 10,
            "Should have dequeued all items"
        );

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_dqueue_segment_allocation() {
        let num_producers = 2;
        let segment_pool = 5;

        let shared_size = DQueue::<String>::shared_size(num_producers, segment_pool);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool) };

        for i in 0..20 {
            queue.enqueue(0, format!("item_{}", i)).unwrap();
        }

        unsafe {
            queue.dump_local_buffer(0);
        }

        for _ in 0..10 {
            assert!(queue.dequeue().is_some());
        }

        for i in 20..30 {
            queue.enqueue(1, format!("item_{}", i)).unwrap();
        }

        unsafe {
            queue.dump_local_buffer(1);
        }

        let mut count = 0;
        while queue.dequeue().is_some() {
            count += 1;
        }
        assert!(count > 0);

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_dqueue_bench_interface() {
        let shared_size = DQueue::<usize>::shared_size(2, 10);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DQueue::init_in_shared(mem_ptr, 2, 10) };

        for i in 0..10 {
            queue.bench_push(i, 0).unwrap();
        }

        thread::yield_now();

        let mut count = 0;
        let mut attempts = 0;
        while attempts < 100 {
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

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    #[should_panic(expected = "DQueue::push on MpscQueue trait")]
    fn test_dqueue_push_panics() {
        let shared_size = DQueue::<usize>::shared_size(2, 5);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { DQueue::init_in_shared(mem_ptr, 2, 5) };

        queue.push(42).unwrap();
    }

    #[test]
    fn test_dqueue_help_mechanism() {
        let num_producers = 2;
        let segment_pool = 5;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool) };

        for prod in 0..num_producers {
            for i in 0..10 {
                queue.enqueue(prod, prod * 100 + i).unwrap();
            }
        }

        for prod in 0..num_producers {
            unsafe {
                queue.dump_local_buffer(prod);
            }
        }

        let mut dequeued = Vec::new();
        for _ in 0..num_producers * 10 {
            if let Some(val) = queue.dequeue() {
                dequeued.push(val);
            }
        }

        assert_eq!(
            dequeued.len(),
            num_producers * 10,
            "Should have dequeued all items"
        );

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }
}

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

    #[test]
    fn test_jp_drops() {
        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let shared_size = JayantiPetrovicMpscQueue::<DropCounter>::shared_size(2, 50);
            let memory = create_aligned_memory(shared_size, 64);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, 2, 50) };

            for i in 0..10 {
                queue.enqueue(0, DropCounter { _value: i }).unwrap();
            }

            for _ in 0..5 {
                drop(queue.dequeue().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }
    }

    #[test]
    fn test_dqueue_drops() {
        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let shared_size = DQueue::<DropCounter>::shared_size(2, 5);
            let memory = create_aligned_memory(shared_size, 64);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { DQueue::init_in_shared(mem_ptr, 2, 5) };

            for i in 0..10 {
                queue.enqueue(0, DropCounter { _value: i }).unwrap();
            }

            unsafe {
                queue.dump_local_buffer(0);
            }

            for _ in 0..5 {
                if let Some(item) = queue.dequeue() {
                    drop(item);
                }
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }
    }
}

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

    #[test]
    fn test_string_types() {
        let shared_size = DrescherQueue::<String>::shared_size(20);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 20) };

        for i in 0..10 {
            queue.push(format!("test_string_{}", i)).unwrap();
        }

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), format!("test_string_{}", i));
        }

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }
}

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

        queue.push(42).unwrap();
    }

    #[test]
    #[should_panic(expected = "JayantiPetrovicMpscQueue::push")]
    fn test_jayanti_push_panics() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(2, 100);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, 2, 100) };

        queue.push(42).unwrap();
    }
}

mod miri_memory_tests {
    use super::*;

    #[test]
    fn test_shared_memory_alignment() {
        let size1 = DrescherQueue::<u8>::shared_size(100);
        let size2 = DrescherQueue::<u64>::shared_size(100);
        assert!(size2 >= size1);

        let size1 = JiffyQueue::<u8>::shared_size(64, 10);
        let size2 = JiffyQueue::<u64>::shared_size(64, 10);
        assert!(size2 >= size1);

        let size1 = JayantiPetrovicMpscQueue::<u8>::shared_size(4, 100);
        let size2 = JayantiPetrovicMpscQueue::<u64>::shared_size(4, 100);
        assert!(size2 >= size1);

        let size1 = DQueue::<u8>::shared_size(4, 10);
        let size2 = DQueue::<u64>::shared_size(4, 10);
        assert!(size2 >= size1);
    }

    #[test]
    fn test_allocation_limits() {
        let shared_size = DrescherQueue::<usize>::shared_size(10);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 10) };

        let test_count = 9; // Max we can push
        for i in 0..test_count {
            queue.push(i).unwrap();
        }

        // Should fail
        assert!(queue.push(99).is_err());

        // Free some space
        for _ in 0..5 {
            queue.pop().unwrap();
        }

        // Now we can push more
        for i in 0..5 {
            queue.push(100 + i).unwrap();
        }

        let mut count = 0;
        while queue.pop().is_some() {
            count += 1;
        }

        assert_eq!(count, 9, "Should have the right number of items");

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }
}

mod miri_state_consistency_tests {
    use super::*;

    #[test]
    fn test_queue_state_consistency() {
        // Test DrescherQueue
        {
            let size = DrescherQueue::<i32>::shared_size(50);
            let mem = create_aligned_memory(size, 64);
            let mem_ptr = Box::leak(mem).as_mut_ptr();
            let drescher = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };

            assert!(drescher.is_empty());
            assert!(!drescher.is_full());

            drescher.push(42).unwrap();
            assert!(!drescher.is_empty());

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, size));
            }
        }

        // Test JiffyQueue
        {
            let size = JiffyQueue::<i32>::shared_size(16, 5);
            let mem = create_aligned_memory(size, 64);
            let mem_ptr = Box::leak(mem).as_mut_ptr();
            let jiffy = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };

            assert!(jiffy.is_empty());
            assert!(!jiffy.is_full());

            jiffy.push(42).unwrap();
            assert!(!jiffy.is_empty());

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, size));
            }
        }

        // Test JayantiPetrovicMpscQueue
        {
            let size = JayantiPetrovicMpscQueue::<i32>::shared_size(2, 50);
            let mem = create_aligned_memory(size, 64);
            let mem_ptr = Box::leak(mem).as_mut_ptr();
            let jp = unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, 2, 50) };

            assert!(jp.is_empty());
            assert!(!jp.is_full());

            jp.enqueue(0, 42).unwrap();
            assert!(!jp.is_empty());

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, size));
            }
        }

        // Test DQueue
        {
            let size = DQueue::<i32>::shared_size(2, 5);
            let mem = create_aligned_memory(size, 64);
            let mem_ptr = Box::leak(mem).as_mut_ptr();
            let dq = unsafe { DQueue::<i32>::init_in_shared(mem_ptr, 2, 5) };

            assert!(dq.is_empty());
            assert!(!dq.is_full());

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, size));
            }
        }
    }

    #[test]
    fn test_rapid_push_pop() {
        let shared_size = JiffyQueue::<usize>::shared_size(64, 10);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 64, 10) };

        // Rapid push/pop cycles
        for _ in 0..3 {
            for i in 0..50 {
                queue.push(i).unwrap();
            }

            for i in 0..50 {
                assert_eq!(queue.pop().unwrap(), i);
            }
        }

        assert!(queue.is_empty());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }
}

mod miri_edge_case_tests {
    use super::*;

    #[test]
    fn test_jiffy_fold_buffer_edge_cases() {
        let buffer_capacity = 3;
        let max_buffers = 15;

        let shared_size = JiffyQueue::<i32>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        // Fill several buffers
        for i in 0..30 {
            queue.push(i).unwrap();
        }

        // Pop first buffer
        for _ in 0..3 {
            queue.pop().unwrap();
        }

        // Add more items
        for i in 30..36 {
            queue.push(i).unwrap();
        }

        // Pop some more
        for _ in 0..6 {
            queue.pop().unwrap();
        }

        // Continue pattern
        for _ in 0..9 {
            queue.pop().unwrap();
        }

        // Add final items
        for i in 100..105 {
            queue.push(i).unwrap();
        }

        // Drain queue
        while queue.pop().is_ok() {}

        assert!(queue.is_empty());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }
}
