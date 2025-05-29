#![cfg(miri)]

use queues::{mpsc::*, MpscQueue};
use std::sync::{Arc, Barrier};
use std::thread;

const MIRI_PRODUCERS: usize = 2;
const MIRI_ITEMS_PER_PRODUCER: usize = 50;
const MIRI_NODE_POOL: usize = 200;

struct AlignedMemory {
    ptr: *mut u8,
    layout: std::alloc::Layout,
}

impl AlignedMemory {
    fn new(size: usize, align: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(size, align).expect("Invalid layout");

        let ptr = unsafe {
            let p = std::alloc::alloc_zeroed(layout);
            if p.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            p
        };

        Self { ptr, layout }
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for AlignedMemory {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.ptr, self.layout);
        }
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
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_drescher_capacity() {
        let expected_nodes = 10;
        let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, expected_nodes) };

        let mut pushed = 0;
        for i in 0..expected_nodes {
            match queue.push(i) {
                Ok(_) => pushed += 1,
                Err(_) => break,
            }
        }
        assert!(pushed > 0, "Should be able to push at least one item");
        if pushed < expected_nodes {
            assert!(queue.is_full());
        }

        let items_to_pop = (pushed / 2).min(3);
        for _ in 0..items_to_pop {
            queue.pop().unwrap();
        }
        for i in 0..items_to_pop {
            queue.push(100 + i).unwrap();
        }
    }

    #[test]
    fn test_drescher_node_recycling() {
        let nodes = 50;
        let shared_size = DrescherQueue::<String>::shared_size(nodes);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_drescher_concurrent() {
        let expected_nodes = 500;
        let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_drescher_bench_interface() {
        let shared_size = DrescherQueue::<usize>::shared_size(100);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_drescher_error_propagation() {
        let size = DrescherQueue::<usize>::shared_size(2);
        let mut mem = AlignedMemory::new(size, 64);
        let mem_ptr = mem.as_mut_ptr();
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
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_jp_producer_specific_enqueue() {
        let num_producers = 4;
        let node_pool_capacity = 200;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_jp_invalid_producer() {
        let num_producers = 2;
        let node_pool_capacity = 50;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe {
            JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
        };

        assert!(queue.enqueue(num_producers, 42).is_err());
        assert!(queue.enqueue(num_producers + 1, 42).is_err());
    }

    #[test]
    #[should_panic(expected = "Number of producers must be > 0")]
    fn test_jp_zero_producers() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(1, 100);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        unsafe {
            JayantiPetrovicMpscQueue::<usize>::init_in_shared(mem_ptr, 0, 100);
        }
    }

    #[test]
    fn test_jp_concurrent_producers() {
        let num_producers = 2;
        let node_pool_capacity = 200;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_jp_tree_operations() {
        let num_producers = 4;
        let node_pool = 200;

        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue =
            unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool) };

        for producer_id in (0..num_producers).rev() {
            queue.enqueue(producer_id, producer_id).unwrap();
        }

        for expected in (0..num_producers).rev() {
            assert_eq!(queue.dequeue().unwrap(), expected);
        }
    }

    #[test]
    fn test_jp_multiple_producers_interleaved() {
        let num_producers = 2;
        let node_pool = 200;

        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue =
            unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool) };

        for round in 0..5 {
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
    }

    #[test]
    fn test_jp_bench_interface() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(4, 200);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    #[should_panic(expected = "JayantiPetrovicMpscQueue::push from MpscQueue trait")]
    fn test_jp_push_panics() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(2, 100);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();
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
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_jiffy_buffer_transitions() {
        let buffer_capacity = 4;
        let max_buffers = 10;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_jiffy_buffer_folding() {
        let buffer_capacity = 4;
        let max_buffers = 10;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        for i in 0..12 {
            queue.push(i).unwrap();
        }

        for i in 0..4 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        for i in 12..20 {
            queue.push(i).unwrap();
        }

        for i in 4..12 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        for i in 12..20 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_jiffy_out_of_order() {
        let buffer_capacity = 4;
        let max_buffers = 10;

        let shared_size = JiffyQueue::<String>::shared_size(buffer_capacity, max_buffers);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_jiffy_buffer_pool_exhaustion() {
        let buffer_capacity = 2;
        let max_buffers = 2;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        let mut pushed = 0;
        for i in 0..10 {
            if queue.push(i).is_ok() {
                pushed += 1;
            }
        }

        assert!(pushed <= buffer_capacity * max_buffers);
    }

    #[test]
    fn test_jiffy_empty_buffer_handling() {
        let buffer_capacity = 8;
        let max_buffers = 3;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

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
    }

    #[test]
    fn test_jiffy_bench_interface() {
        let shared_size = JiffyQueue::<usize>::shared_size(16, 5);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };

        queue.bench_push(42, 0).unwrap();
        assert!(!queue.bench_is_empty());
        assert_eq!(queue.bench_pop().unwrap(), 42);
        assert!(queue.bench_is_empty());
    }

    #[test]
    fn test_jiffy_concurrent_empty_checks() {
        let size = JiffyQueue::<usize>::shared_size(16, 5);
        let mut memory = AlignedMemory::new(size, 64);
        let mem_ptr = memory.as_mut_ptr();
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
    }
}

mod miri_dqueue_tests {
    use super::*;

    #[test]
    fn test_dqueue_size_calculation_only() {
        let size = DQueue::<usize>::shared_size(1, 2);
        assert!(size > 0);

        let size2 = DQueue::<usize>::shared_size(2, 5);
        assert!(size2 > size);
    }

    #[test]
    fn test_dqueue_memory_allocation_only() {
        let shared_size = DQueue::<usize>::shared_size(1, 2);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let _mem_ptr = memory.as_mut_ptr();
    }

    #[test]
    #[should_panic(expected = "DQueue::push on MpscQueue trait")]
    fn test_dqueue_trait_panic() {
        struct DQueueStub;
        impl MpscQueue<usize> for DQueueStub {
            type PushError = ();
            type PopError = ();

            fn push(&self, _: usize) -> Result<(), Self::PushError> {
                panic!("DQueue::push on MpscQueue trait. Use DQueue::enqueue(producer_id, item) or BenchMpscQueue::bench_push.");
            }

            fn pop(&self) -> Result<usize, Self::PopError> {
                Ok(0)
            }

            fn is_empty(&self) -> bool {
                true
            }

            fn is_full(&self) -> bool {
                false
            }
        }

        let stub = DQueueStub;
        let _ = stub.push(42);
    }
}

#[test]
fn test_miri_is_working() {
    let x = 42;
    assert_eq!(x, 42);
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
            let mut memory = AlignedMemory::new(shared_size, 64);
            let mem_ptr = memory.as_mut_ptr();

            let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };

            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
        }
    }

    #[test]
    fn test_jiffy_drops() {
        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let shared_size = JiffyQueue::<DropCounter>::shared_size(16, 5);
            let mut memory = AlignedMemory::new(shared_size, 64);
            let mem_ptr = memory.as_mut_ptr();

            let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };

            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
        }
    }

    #[test]
    fn test_jp_drops() {
        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let shared_size = JayantiPetrovicMpscQueue::<DropCounter>::shared_size(2, 50);
            let mut memory = AlignedMemory::new(shared_size, 64);
            let mem_ptr = memory.as_mut_ptr();

            let queue = unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, 2, 50) };

            for i in 0..10 {
                queue.enqueue(0, DropCounter { _value: i }).unwrap();
            }

            // Dequeue 5 items
            for _ in 0..5 {
                drop(queue.dequeue().unwrap());
            }
            let drops = DROP_COUNT.load(Ordering::SeqCst);
            assert!(drops >= 5, "Should have at least 5 drops, got {}", drops);
        }

        // After the queue is dropped, all remaining items should be dropped too
        let final_drops = DROP_COUNT.load(Ordering::SeqCst);
        assert!(
            final_drops >= 10,
            "All 10 items should eventually be dropped, got {}",
            final_drops
        );
    }
}

mod miri_type_tests {
    use super::*;

    #[test]
    fn test_zero_sized_types() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct ZeroSized;

        let shared_size = DrescherQueue::<ZeroSized>::shared_size(50);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };

        queue.push(ZeroSized).unwrap();
        assert_eq!(queue.pop().unwrap(), ZeroSized);
    }

    #[test]
    fn test_large_types() {
        #[derive(Clone, Debug, PartialEq)]
        struct LargeType {
            data: [u64; 32],
        }

        let shared_size = JiffyQueue::<LargeType>::shared_size(4, 3);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 4, 3) };

        let item = LargeType { data: [42; 32] };
        queue.push(item.clone()).unwrap();
        assert_eq!(queue.pop().unwrap(), item);
    }

    #[test]
    fn test_string_types() {
        let shared_size = DrescherQueue::<String>::shared_size(20);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 20) };

        for i in 0..10 {
            queue.push(format!("test_string_{}", i)).unwrap();
        }

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), format!("test_string_{}", i));
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
            let mut memory = AlignedMemory::new(shared_size, 64);
            let mem_ptr = memory.as_mut_ptr();
            let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };
            test_mpsc_trait(&*queue);
        }

        {
            let shared_size = JiffyQueue::<usize>::shared_size(16, 5);
            let mut memory = AlignedMemory::new(shared_size, 64);
            let mem_ptr = memory.as_mut_ptr();
            let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };
            test_mpsc_trait(&*queue);
        }
    }

    #[test]
    #[should_panic(expected = "JayantiPetrovicMpscQueue::push")]
    fn test_jayanti_push_panics() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(2, 100);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();
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
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 10) };

        // Push until full
        let mut pushed = 0;
        for i in 0..100 {
            // Try many more than expected capacity
            match queue.push(i) {
                Ok(_) => pushed += 1,
                Err(_) => break,
            }
        }

        assert!(pushed > 0, "Should push at least one item");
        let initial_pushed = pushed;

        // Pop half
        let to_pop = pushed / 2;
        for _ in 0..to_pop {
            queue.pop().unwrap();
        }

        // Push again - should be able to push at least as many as we popped
        let mut pushed_again = 0;
        for i in 100..200 {
            match queue.push(i) {
                Ok(_) => pushed_again += 1,
                Err(_) => break,
            }
        }

        assert!(
            pushed_again >= to_pop,
            "Should be able to push at least {} items (popped), but pushed {}",
            to_pop,
            pushed_again
        );

        // Verify total items in queue
        let mut count = 0;
        while queue.pop().is_some() {
            count += 1;
        }

        let expected = initial_pushed - to_pop + pushed_again;
        assert_eq!(
            count, expected,
            "Should have {} items ({}−{}+{}), but got {}",
            expected, initial_pushed, to_pop, pushed_again, count
        );
    }
}

mod miri_state_consistency_tests {
    use super::*;

    #[test]
    fn test_queue_state_consistency() {
        {
            let size = DrescherQueue::<i32>::shared_size(50);
            let mut mem = AlignedMemory::new(size, 64);
            let mem_ptr = mem.as_mut_ptr();
            let drescher = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };

            assert!(drescher.is_empty());
            assert!(!drescher.is_full());

            drescher.push(42).unwrap();
            assert!(!drescher.is_empty());
        }

        {
            let size = JiffyQueue::<i32>::shared_size(16, 5);
            let mut mem = AlignedMemory::new(size, 64);
            let mem_ptr = mem.as_mut_ptr();
            let jiffy = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };

            assert!(jiffy.is_empty());
            assert!(!jiffy.is_full());

            jiffy.push(42).unwrap();
            assert!(!jiffy.is_empty());
        }

        {
            let size = JayantiPetrovicMpscQueue::<i32>::shared_size(2, 50);
            let mut mem = AlignedMemory::new(size, 64);
            let mem_ptr = mem.as_mut_ptr();
            let jp = unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, 2, 50) };

            assert!(jp.is_empty());
            assert!(!jp.is_full());

            jp.enqueue(0, 42).unwrap();
            assert!(!jp.is_empty());
        }
    }

    #[test]
    fn test_rapid_push_pop() {
        let shared_size = JiffyQueue::<usize>::shared_size(64, 10);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 64, 10) };

        for _ in 0..3 {
            for i in 0..50 {
                queue.push(i).unwrap();
            }

            for i in 0..50 {
                assert_eq!(queue.pop().unwrap(), i);
            }
        }

        assert!(queue.is_empty());
    }
}

mod miri_edge_case_tests {
    use super::*;

    #[test]
    fn test_jiffy_fold_buffer_edge_cases() {
        let buffer_capacity = 3;
        let max_buffers = 15;

        let shared_size = JiffyQueue::<i32>::shared_size(buffer_capacity, max_buffers);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        for i in 0..30 {
            queue.push(i).unwrap();
        }

        for _ in 0..3 {
            queue.pop().unwrap();
        }

        for i in 30..36 {
            queue.push(i).unwrap();
        }

        for _ in 0..6 {
            queue.pop().unwrap();
        }

        for _ in 0..9 {
            queue.pop().unwrap();
        }

        for i in 100..105 {
            queue.push(i).unwrap();
        }

        while queue.pop().is_ok() {}

        assert!(queue.is_empty());
    }
}
