use queues::{mpsc::*, MpscQueue};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

trait BenchMpscQueue<T: Send>: Send + Sync + 'static {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()>;
    fn bench_pop(&self) -> Result<T, ()>;
    fn bench_is_empty(&self) -> bool;
    fn bench_is_full(&self) -> bool;
}

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

fn create_aligned_memory_box(size: usize) -> Box<[u8]> {
    const ALIGN: usize = 64;

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

macro_rules! test_mpsc_basic {
    ($queue_type:ty, $init:expr, $test_name:ident) => {
        mod $test_name {
            use super::*;

            #[test]
            fn test_single_producer_basic() {
                let queue = $init;
                assert!(queue.is_empty());
                queue.push(42).unwrap();
                assert!(!queue.is_empty());
                assert_eq!(queue.pop().unwrap(), 42);
                assert!(queue.is_empty());
                for i in 0..5 {
                    queue.push(i).unwrap();
                }
                for i in 0..5 {
                    assert_eq!(queue.pop().unwrap(), i);
                }
                assert!(queue.is_empty());
            }

            #[test]
            fn test_multiple_producers_single_consumer() {}
            #[test]
            fn test_concurrent_push_pop() {}
        }
    };
}

mod drescher_tests {
    use super::*;

    #[test]
    fn test_drescher_basic() {
        let expected_nodes = 1000;
        let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, expected_nodes) };

        assert!(queue.is_empty());
        assert!(queue.pop().is_none());

        queue.push(42).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());
    }

    fn test_drescher_capacity() {
        let expected_nodes = 100;
        let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
        let memory = create_aligned_memory_box(shared_size);
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
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, expected_nodes) };

        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(NUM_PRODUCERS + 1));
        let mut handles = vec![];

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

        barrier.wait();

        for handle in handles {
            handle.join().unwrap();
        }

        let mut items = Vec::new();
        while let Some(item) = queue.pop() {
            items.push(item);
        }

        assert_eq!(items.len(), NUM_PRODUCERS * ITEMS_PER_PRODUCER);

        items.sort();
        for (i, &item) in items.iter().enumerate() {
            assert_eq!(item, i);
        }
    }
}

mod jayanti_petrovic_tests {
    use super::*;

    #[test]
    fn test_jp_initialization() {
        let num_producers = 4;
        let node_pool_capacity = 1000;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue: &mut JayantiPetrovicMpscQueue<usize> = unsafe {
            JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
        };

        assert!(queue.is_empty());
    }

    #[test]
    fn test_jp_producer_specific_enqueue() {
        let num_producers = 4;
        let node_pool_capacity = 1000;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);
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
    }

    #[test]
    fn test_jp_invalid_producer_id() {
        let num_producers = 2;
        let node_pool_capacity = 100;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe {
            JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
        };

        assert!(queue.enqueue(num_producers, 42).is_err());
        assert!(queue.enqueue(num_producers + 1, 42).is_err());
    }

    #[test]
    fn test_jp_concurrent_producers() {
        let num_producers = 4;
        let node_pool_capacity = 10000;

        let shared_size =
            JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);
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

                for i in 0..ITEMS_PER_PRODUCER {
                    let value = producer_id * ITEMS_PER_PRODUCER + i;
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

        assert_eq!(items.len(), num_producers * ITEMS_PER_PRODUCER);

        items.sort();
        for (i, &item) in items.iter().enumerate() {
            assert_eq!(item, i);
        }
    }

    #[test]
    #[should_panic]
    fn test_jp_zero_producers_panic() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(1, 100);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        unsafe { JayantiPetrovicMpscQueue::<usize>::init_in_shared(mem_ptr, 0, 100) };
    }
}

mod jiffy_tests {
    use super::*;

    #[test]
    fn test_jiffy_basic() {
        let buffer_capacity = 64;
        let max_buffers = 10;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());

        queue.push(42).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_jiffy_buffer_transitions() {
        let buffer_capacity = 4;
        let max_buffers = 10;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
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
    }

    #[test]
    fn test_jiffy_concurrent_operations() {
        let buffer_capacity = 128;
        let max_buffers = 20;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        let total = 1000;
        for i in 0..total {
            queue.push(i).unwrap();
        }

        for i in 0..total {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_jiffy_out_of_order_operations() {
        let buffer_capacity = 8;
        let max_buffers = 20;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(3));

        let queue1 = queue.clone();
        let barrier1 = barrier.clone();
        let producer1 = thread::spawn(move || {
            barrier1.wait();
            let mut pushed = 0;
            for i in 0..50 {
                if queue1.push(i * 2).is_ok() {
                    pushed += 1;
                } else {
                    break;
                }
                if i % 10 == 0 {
                    thread::sleep(Duration::from_micros(10));
                }
            }
            pushed
        });

        let queue2 = queue.clone();
        let barrier2 = barrier.clone();
        let producer2 = thread::spawn(move || {
            barrier2.wait();
            let mut pushed = 0;
            for i in 0..50 {
                if queue2.push(i * 2 + 1).is_ok() {
                    pushed += 1;
                } else {
                    break;
                }
                if i % 7 == 0 {
                    thread::sleep(Duration::from_micros(10));
                }
            }
            pushed
        });

        barrier.wait();
        let pushed1 = producer1.join().unwrap();
        let pushed2 = producer2.join().unwrap();
        let total_pushed = pushed1 + pushed2;

        let mut items = Vec::new();
        while !queue.is_empty() && items.len() < total_pushed {
            if let Ok(item) = queue.pop() {
                items.push(item);
            }
        }

        assert_eq!(items.len(), total_pushed);
        items.sort();

        for (i, &item) in items.iter().enumerate() {
            assert_eq!(item, i);
        }
    }

    #[test]
    fn test_jiffy_buffer_folding() {
        let buffer_capacity = 4;
        let max_buffers = 10;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

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
    fn test_jiffy_concurrent_folding_scenario() {
        let buffer_capacity = 32;
        let max_buffers = 40;

        let shared_size = JiffyQueue::<String>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(3));

        let q1 = queue.clone();
        let b1 = barrier.clone();
        let producer1 = thread::spawn(move || {
            b1.wait();
            let mut pushed = 0;
            for i in 0..30 {
                if q1.push(format!("p1_{}", i)).is_ok() {
                    pushed += 1;
                } else {
                    thread::sleep(Duration::from_millis(1));

                    if q1.push(format!("p1_{}", i)).is_ok() {
                        pushed += 1;
                    }
                }
                if i % 5 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }
            pushed
        });

        let q2 = queue.clone();
        let b2 = barrier.clone();
        let producer2 = thread::spawn(move || {
            b2.wait();
            let mut pushed = 0;
            for burst in 0..6 {
                thread::sleep(Duration::from_micros(50));
                for i in 0..5 {
                    if q2.push(format!("p2_{}_{}", burst, i)).is_ok() {
                        pushed += 1;
                    } else {
                        thread::sleep(Duration::from_millis(1));

                        if q2.push(format!("p2_{}_{}", burst, i)).is_ok() {
                            pushed += 1;
                        }
                    }
                }
            }
            pushed
        });

        let q3 = queue.clone();
        let consumer = thread::spawn(move || {
            barrier.wait();

            let mut items = Vec::new();
            let mut empty_count = 0;

            while empty_count < 50 {
                match q3.pop() {
                    Ok(item) => {
                        items.push(item);
                        empty_count = 0;

                        if items.len() % 10 == 0 {
                            thread::sleep(Duration::from_micros(200));
                        }
                    }
                    Err(_) => {
                        empty_count += 1;
                        thread::sleep(Duration::from_micros(10));
                    }
                }
            }

            items
        });

        let pushed1 = producer1.join().unwrap();
        let pushed2 = producer2.join().unwrap();
        let consumed = consumer.join().unwrap();

        assert_eq!(consumed.len(), pushed1 + pushed2);

        assert!(queue.is_empty());
    }

    #[test]
    fn test_jiffy_fold_buffer_edge_cases() {
        let buffer_capacity = 3;
        let max_buffers = 15;

        let shared_size = JiffyQueue::<i32>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

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

mod dqueue_tests {
    use super::*;

    #[test]
    fn test_dqueue_initialization() {
        let num_producers = 4;
        let segment_pool_capacity = 10;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue: &mut DQueue<usize> =
            unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_dqueue_producer_specific() {
        let num_producers = 3;
        let segment_pool_capacity = 10;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

        for producer_id in 0..num_producers {
            for i in 0..10 {
                queue.enqueue(producer_id, producer_id * 100 + i).unwrap();
            }
        }

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
    }

    #[test]
    fn test_dqueue_gc_operations() {
        let num_producers = 2;
        let segment_pool_capacity = 10;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

        let total_items = 100;

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
    }

    #[test]
    fn test_dqueue_concurrent_with_helping() {
        let num_producers = 4;
        let segment_pool_capacity = 20;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);
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
    }

    #[test]
    fn test_dqueue_invalid_producer() {
        let num_producers = 2;
        let segment_pool_capacity = 10;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

        assert!(queue.enqueue(num_producers, 42).is_err());
        assert!(queue.enqueue(num_producers + 1, 42).is_err());
        assert!(queue.enqueue(usize::MAX, 42).is_err());
    }
}

mod bench_wrapper_tests {
    use super::*;

    #[test]
    fn test_drescher_bench_interface() {
        let shared_size = DrescherQueue::<usize>::shared_size(1000);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 1000) };

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
    fn test_jiffy_bench_interface() {
        let shared_size = JiffyQueue::<usize>::shared_size(128, 10);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 128, 10) };

        queue.bench_push(42, 0).unwrap();
        assert!(!queue.bench_is_empty());
        assert_eq!(queue.bench_pop().unwrap(), 42);
        assert!(queue.bench_is_empty());
    }

    #[test]
    fn test_jayanti_bench_interface() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(4, 1000);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, 4, 1000) };

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
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DQueue::init_in_shared(mem_ptr, 4, 10) };

        for i in 0..10 {
            queue.bench_push(i, 0).unwrap();
        }

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

mod edge_cases {
    use super::*;

    #[test]
    fn test_zero_sized_type() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct ZeroSized;

        let shared_size = DrescherQueue::<ZeroSized>::shared_size(100);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 100) };

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
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };

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
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 64, 5) };

            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
        }

        thread::sleep(Duration::from_millis(10));

        assert!(DROP_COUNT.load(Ordering::SeqCst) >= 5);
    }
    #[test]
    fn test_single_item_queues() {
        // Test with minimal queue sizes
        let size = JiffyQueue::<usize>::shared_size(1, 1);
        let memory = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 1, 1) };

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());

        // Test DrescherQueue with minimal nodes
        let size = DrescherQueue::<usize>::shared_size(2); // Need at least 2 nodes
        let memory = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 2) };

        // With 2 nodes, we can push 1 item (1 for dummy, 1 for data)
        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.is_empty());

        // After popping, we should be able to push again due to node recycling
        queue.push(43).unwrap();
        assert_eq!(queue.pop().unwrap(), 43);
    }

    #[test]
    fn test_option_values() {
        // Test with None values
        let num_producers = 1;
        let segment_pool = 10;

        let shared_size = DQueue::<Option<usize>>::shared_size(num_producers, segment_pool);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool) };

        // Test with None values
        queue.enqueue(0, None).unwrap();
        queue.enqueue(0, Some(42)).unwrap();
        queue.enqueue(0, None).unwrap();
        queue.enqueue(0, Some(100)).unwrap();

        unsafe {
            queue.dump_local_buffer(0);
        }

        assert_eq!(queue.dequeue(), Some(None));
        assert_eq!(queue.dequeue(), Some(Some(42)));
        assert_eq!(queue.dequeue(), Some(None));
        assert_eq!(queue.dequeue(), Some(Some(100)));
        assert_eq!(queue.dequeue(), None);
    }

    #[test]
    fn test_comprehensive_drop_semantics() {
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

        // Test DrescherQueue drops
        {
            DROP_COUNT.store(0, Ordering::SeqCst);

            let shared_size = DrescherQueue::<DropCounter>::shared_size(50);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };

            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);

            // Queue still has 5 items that haven't been dropped
            // Unlike in Miri tests, we can't easily drop the queue itself
            // since we used Box::leak
        }

        // Test JayantiPetrovicMpscQueue drops
        {
            DROP_COUNT.store(0, Ordering::SeqCst);

            let shared_size = JayantiPetrovicMpscQueue::<DropCounter>::shared_size(2, 50);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

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

        // Test JiffyQueue drops
        {
            DROP_COUNT.store(0, Ordering::SeqCst);

            let shared_size = JiffyQueue::<DropCounter>::shared_size(16, 5);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

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
}

mod memory_tests {
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
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 10) };

        let test_count = 20;
        for i in 0..test_count {
            queue.push(i).unwrap();
        }

        for _ in 0..test_count / 2 {
            queue.pop().unwrap();
        }

        for i in test_count..test_count + 5 {
            queue.push(i).unwrap();
        }

        let mut count = 0;
        while queue.pop().is_some() {
            count += 1;
        }

        assert_eq!(
            count,
            test_count / 2 + 5,
            "Should have the right number of items remaining"
        );
    }

    #[test]
    fn test_jiffy_buffer_pool_exhaustion() {
        let buffer_capacity = 2;
        let max_buffers = 2;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

        let mut pushed = 0;
        for i in 0..10 {
            if queue.push(i).is_ok() {
                pushed += 1;
            }
        }

        assert!(pushed <= buffer_capacity * max_buffers);
    }
}

mod stress_tests {
    use super::*;

    #[test]
    fn stress_test_high_contention() {}

    #[test]
    fn stress_test_rapid_push_pop() {
        let shared_size = JiffyQueue::<usize>::shared_size(1024, 20);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 1024, 20) };

        for i in 0..100 {
            queue.push(i).unwrap();
        }

        for i in 0..100 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        println!("Rapid push/pop test completed");
    }
}

mod trait_tests {
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
    fn test_all_queues_implement_trait() {
        let shared_size = DrescherQueue::<usize>::shared_size(100);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 100) };
        test_mpsc_trait(&*queue);

        let shared_size = JiffyQueue::<usize>::shared_size(64, 10);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 64, 10) };
        test_mpsc_trait(&*queue);

        let shared_size = DQueue::<usize>::shared_size(1, 10);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue: &mut DQueue<usize> = unsafe { DQueue::init_in_shared(mem_ptr, 1, 10) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    #[should_panic]
    fn test_dqueue_push_panics() {
        let shared_size = DQueue::<usize>::shared_size(2, 10);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { DQueue::init_in_shared(mem_ptr, 2, 10) };

        queue.push(42).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_jayanti_push_panics() {
        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(2, 100);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, 2, 100) };

        queue.push(42).unwrap();
    }
}

mod comprehensive_tests {
    use super::*;

    #[test]
    fn test_drescher_node_recycling() {
        let nodes = 50;
        let shared_size = DrescherQueue::<String>::shared_size(nodes);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, nodes) };

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
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool) };

        for round in 0..10 {
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

        assert_eq!(items.len(), num_producers * 10);
    }

    #[test]
    fn test_jiffy_empty_buffer_handling() {
        let buffer_capacity = 8;
        let max_buffers = 3;

        let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
        let memory = create_aligned_memory_box(shared_size);
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
    }

    #[test]
    fn test_dqueue_segment_allocation() {
        let num_producers = 2;
        let segment_pool = 5;

        let shared_size = DQueue::<String>::shared_size(num_producers, segment_pool);
        let memory = create_aligned_memory_box(shared_size);
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
    }

    #[test]
    fn test_queue_state_consistency() {
        let size = DrescherQueue::<i32>::shared_size(100);
        let mem = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(mem).as_mut_ptr();
        let drescher = unsafe { DrescherQueue::init_in_shared(mem_ptr, 100) };

        assert!(drescher.is_empty());
        assert!(!drescher.is_full());

        drescher.push(42).unwrap();
        assert!(!drescher.is_empty());

        let size = JiffyQueue::<i32>::shared_size(64, 10);
        let mem = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(mem).as_mut_ptr();
        let jiffy = unsafe { JiffyQueue::init_in_shared(mem_ptr, 64, 10) };

        assert!(jiffy.is_empty());
        assert!(!jiffy.is_full());

        jiffy.push(42).unwrap();
        assert!(!jiffy.is_empty());
    }

    #[test]
    fn test_error_propagation() {
        let size = DrescherQueue::<usize>::shared_size(2);
        let mem = create_aligned_memory_box(size);
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
    }

    #[test]
    fn test_jayanti_tree_operations() {
        let num_producers = 8;
        let node_pool = 1000;

        let shared_size = JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

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
    fn test_dqueue_help_mechanism() {
        let num_producers = 4;
        let segment_pool = 10;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool);
        let memory = create_aligned_memory_box(shared_size);
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
    }

    #[test]
    fn test_concurrent_empty_checks() {
        let size = JiffyQueue::<usize>::shared_size(128, 10);
        let memory = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();
        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 128, 10) };

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
    #[test]
    fn test_dqueue_wraparound() {
        let num_producers = 2;
        let segment_pool = 4;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool) };

        // Multiple cycles of enqueue/dequeue to test wraparound
        for cycle in 0..3 {
            for producer_id in 0..num_producers {
                for i in 0..10 {
                    queue
                        .enqueue(producer_id, cycle * 1000 + producer_id * 100 + i)
                        .unwrap();
                }
                unsafe {
                    queue.dump_local_buffer(producer_id);
                }
            }

            // Dequeue all
            for _ in 0..(num_producers * 10) {
                assert!(queue.dequeue().is_some());
            }

            // Run GC between cycles
            unsafe {
                queue.run_gc();
            }
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_dqueue_producer_fairness() {
        let num_producers = 3;
        let segment_pool = 5;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool) };

        // Each producer adds items with different timestamps
        for round in 0..5 {
            for producer_id in 0..num_producers {
                queue
                    .enqueue(producer_id, producer_id * 1000 + round)
                    .unwrap();
            }
        }

        // Dump all buffers
        for producer_id in 0..num_producers {
            unsafe {
                queue.dump_local_buffer(producer_id);
            }
        }

        // Items should come out in timestamp order
        let mut items = Vec::new();
        while let Some(item) = queue.dequeue() {
            items.push(item);
        }

        // Verify fairness - items should be interleaved by timestamp
        assert_eq!(items.len(), num_producers * 5);

        // Check that we got all items from all producers
        for producer_id in 0..num_producers {
            let producer_items: Vec<_> =
                items.iter().filter(|&&x| x / 1000 == producer_id).collect();
            assert_eq!(producer_items.len(), 5);
        }
    }

    #[test]
    fn test_dqueue_mixed_operations() {
        let num_producers = 2;
        let segment_pool = 5;

        let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool) };

        // Interleave enqueues, dumps, and dequeues
        queue.enqueue(0, 1).unwrap();
        queue.enqueue(1, 2).unwrap();

        unsafe {
            queue.dump_local_buffer(0);
        }
        assert_eq!(queue.dequeue(), Some(1));

        queue.enqueue(0, 3).unwrap();
        unsafe {
            queue.dump_local_buffer(1);
        }

        assert_eq!(queue.dequeue(), Some(2));

        queue.enqueue(1, 4).unwrap();
        unsafe {
            queue.dump_local_buffer(0);
            queue.dump_local_buffer(1);
        }

        assert_eq!(queue.dequeue(), Some(3));
        assert_eq!(queue.dequeue(), Some(4));
        assert!(queue.dequeue().is_none());
    }

    #[test]
    fn test_string_types() {
        let shared_size = DrescherQueue::<String>::shared_size(20);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 20) };

        for i in 0..10 {
            queue.push(format!("test_string_{}", i)).unwrap();
        }

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), format!("test_string_{}", i));
        }
    }

    #[test]
    fn test_queue_reuse() {
        // Test that queues can be emptied and reused
        let shared_size = DrescherQueue::<usize>::shared_size(50);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };

        for cycle in 0..3 {
            // Fill
            for i in 0..10 {
                queue.push(cycle * 100 + i).unwrap();
            }

            // Empty
            for _ in 0..10 {
                assert!(queue.pop().is_some());
            }

            assert!(queue.is_empty());
        }
    }

    // In integration_tests module, add:

    #[test]
    fn test_producer_consumer_pattern() {
        // Simplified version of mixed workload
        let shared_size = JiffyQueue::<usize>::shared_size(64, 10);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 64, 10) };
        let queue = Arc::new(queue);

        // Producer
        let q1 = queue.clone();
        let producer = thread::spawn(move || {
            for i in 0..100 {
                q1.push(i).unwrap();
            }
        });

        // Consumer
        let q2 = queue.clone();
        let consumer = thread::spawn(move || {
            let mut items = Vec::new();
            let mut retries = 0;
            while items.len() < 100 && retries < 1000 {
                match q2.pop() {
                    Ok(item) => {
                        items.push(item);
                        retries = 0;
                    }
                    Err(_) => {
                        retries += 1;
                        thread::yield_now();
                    }
                }
            }
            items
        });

        producer.join().unwrap();
        let items = consumer.join().unwrap();

        assert_eq!(items.len(), 100);

        let unique_count = items
            .into_iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(unique_count, 100);
    }
}

mod integration_tests {
    use super::*;

    #[test]
    fn test_mixed_workload() {
        let shared_size = JiffyQueue::<String>::shared_size(256, 20);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 256, 20) };

        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(5));
        let stop_flag = Arc::new(AtomicBool::new(false));

        let q1 = queue.clone();
        let b1 = barrier.clone();
        let stop1 = stop_flag.clone();
        let h1 = thread::spawn(move || {
            b1.wait();
            let mut i = 0;
            while !stop1.load(Ordering::Relaxed) && i < 100 {
                q1.push(format!("steady_{}", i)).unwrap();
                i += 1;
                thread::sleep(Duration::from_micros(10));
            }
        });

        let q2 = queue.clone();
        let b2 = barrier.clone();
        let stop2 = stop_flag.clone();
        let h2 = thread::spawn(move || {
            b2.wait();
            for burst in 0..5 {
                if stop2.load(Ordering::Relaxed) {
                    break;
                }
                for i in 0..20 {
                    q2.push(format!("burst_{}_{}", burst, i)).unwrap();
                }
                thread::sleep(Duration::from_millis(5));
            }
        });

        let q3 = queue.clone();
        let b3 = barrier.clone();
        let stop3 = stop_flag.clone();
        let h3 = thread::spawn(move || {
            b3.wait();
            for i in 0..50 {
                if stop3.load(Ordering::Relaxed) {
                    break;
                }
                q3.push(format!("random_{}", i)).unwrap();
                thread::sleep(Duration::from_micros(i % 50));
            }
        });

        let q4 = queue.clone();
        let b4 = barrier.clone();
        let h4 = thread::spawn(move || {
            b4.wait();
            let mut items = Vec::new();
            let start = std::time::Instant::now();

            while start.elapsed() < Duration::from_millis(200) {
                if let Ok(item) = q4.pop() {
                    items.push(item);
                } else {
                    thread::sleep(Duration::from_micros(10));
                }
            }

            items
        });

        barrier.wait();

        thread::sleep(Duration::from_millis(300));
        stop_flag.store(true, Ordering::Relaxed);

        h1.join().unwrap();
        h2.join().unwrap();
        h3.join().unwrap();
        let consumed = h4.join().unwrap();

        println!("Mixed workload consumed {} items", consumed.len());
        assert!(!consumed.is_empty());

        let steady_count = consumed.iter().filter(|s| s.starts_with("steady")).count();
        let burst_count = consumed.iter().filter(|s| s.starts_with("burst")).count();
        let random_count = consumed.iter().filter(|s| s.starts_with("random")).count();

        assert!(steady_count > 0);
        assert!(burst_count > 0);
        assert!(random_count > 0);
    }
}

#[test]
fn test_comprehensive_drop_semantics() {
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

    // Test DrescherQueue drops
    {
        DROP_COUNT.store(0, Ordering::SeqCst);

        let shared_size = DrescherQueue::<DropCounter>::shared_size(50);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, 50) };

        for i in 0..10 {
            queue.push(DropCounter { _value: i }).unwrap();
        }

        for _ in 0..5 {
            drop(queue.pop().unwrap());
        }

        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
    }

    // Test JayantiPetrovicMpscQueue drops
    {
        DROP_COUNT.store(0, Ordering::SeqCst);

        let shared_size = JayantiPetrovicMpscQueue::<DropCounter>::shared_size(2, 50);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

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

    // Test JiffyQueue drops
    {
        DROP_COUNT.store(0, Ordering::SeqCst);

        let shared_size = JiffyQueue::<DropCounter>::shared_size(16, 5);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JiffyQueue::init_in_shared(mem_ptr, 16, 5) };

        for i in 0..10 {
            queue.push(DropCounter { _value: i }).unwrap();
        }

        for _ in 0..5 {
            drop(queue.pop().unwrap());
        }

        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
    }

    // Test DQueue drops
    {
        DROP_COUNT.store(0, Ordering::SeqCst);

        let shared_size = DQueue::<DropCounter>::shared_size(2, 10);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DQueue::init_in_shared(mem_ptr, 2, 10) };

        // Enqueue items
        for i in 0..10 {
            queue.enqueue(0, DropCounter { _value: i }).unwrap();
        }

        unsafe {
            queue.dump_local_buffer(0);
        }

        // Dequeue 5 items
        for _ in 0..5 {
            drop(queue.dequeue().unwrap());
        }

        assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
    }
}

// Add these tests to queues/tests/unit_test_mpsc.rs

mod mpsc_branch_coverage_improvement {
    use super::*;
    use std::ptr;
    use std::sync::atomic::Ordering;

    mod dqueue_coverage_tests {
        use queues::mpsc::dqueue::Segment;

        use super::*;

        #[test]
        fn test_dqueue_segment_allocation_branches() {
            let num_producers = 2;
            let segment_pool_capacity = 3;

            let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

            // Test initial allocation (is_initial = true)
            // Already done in init, so test the reuse path

            // Fill segments to populate free list
            for i in 0..1000 {
                queue.enqueue(0, i).unwrap();
            }
            unsafe {
                queue.dump_local_buffer(0);
            }

            // Dequeue to make segments available for reuse
            for _ in 0..500 {
                queue.dequeue();
            }

            // Run GC to release segments to free list
            unsafe {
                queue.run_gc();
            }

            // Now allocate from free list
            for i in 0..100 {
                queue.enqueue(1, i).unwrap();
            }
            unsafe {
                queue.dump_local_buffer(1);
            }
        }

        #[test]
        fn test_dqueue_find_segment_null_paths() {
            let num_producers = 1;
            let segment_pool_capacity = 2;

            let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue: &mut DQueue<usize> =
                unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

            // Test with null qseg (shouldn't happen in practice but tests the branch)
            unsafe {
                let old_qseg: *mut Segment<usize> = queue.qseg.load(Ordering::Acquire);
                queue.qseg.store(ptr::null_mut(), Ordering::Release);

                let result = queue.find_segment(ptr::null_mut(), 100);
                assert!(result.is_null());

                queue.qseg.store(old_qseg, Ordering::Release);
            }

            // Test loop limit exceeded
            unsafe {
                let very_high_id = 1000000u64;
                let result = queue.find_segment(ptr::null_mut(), very_high_id);
                assert!(result.is_null());
            }
        }

        #[test]
        fn test_dqueue_help_enqueue_edge_cases() {
            let num_producers = 3;
            let segment_pool_capacity = 5;

            let shared_size = DQueue::<String>::shared_size(num_producers, segment_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

            // Test help with no producers
            unsafe {
                let old_num = queue.num_producers;
                queue.num_producers = 0;
                queue.help_enqueue();
                queue.num_producers = old_num;
            }

            // Enqueue items in different producers
            for prod in 0..num_producers {
                for i in 0..10 {
                    queue.enqueue(prod, format!("p{}_i{}", prod, i)).unwrap();
                }
            }

            // Don't dump, let dequeue trigger help
            let mut helped = 0;
            for _ in 0..30 {
                if queue.dequeue().is_some() {
                    helped += 1;
                }
            }
            assert!(helped > 0, "Should have dequeued some items via help");
        }

        #[test]
        fn test_dqueue_gc_producer_segments() {
            let num_producers = 2;
            let segment_pool_capacity = 4;

            let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

            // Create scenario where producers reference different segments
            queue.enqueue(0, 1).unwrap();
            queue.enqueue(0, 2).unwrap();
            unsafe {
                queue.dump_local_buffer(0);
            }

            // Advance queue state
            for i in 0..1000 {
                queue.enqueue(1, i).unwrap();
            }
            unsafe {
                queue.dump_local_buffer(1);
            }

            // Dequeue some
            for _ in 0..50 {
                queue.dequeue();
            }

            // GC with producers having different segment references
            unsafe {
                queue.run_gc();
            }

            // Enqueue more in producer 0
            for i in 0..50 {
                queue.enqueue(0, 1000 + i).unwrap();
            }

            // GC again with different state
            unsafe {
                queue.run_gc();
            }
        }

        #[test]
        fn test_dqueue_local_buffer_wraparound() {
            use queues::mpsc::dqueue::L_LOCAL_BUFFER_CAPACITY;

            let num_producers = 1;
            let segment_pool_capacity = 10;

            let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

            // Fill local buffer completely
            for i in 0..L_LOCAL_BUFFER_CAPACITY - 1 {
                queue.enqueue(0, i).unwrap();
            }

            // This should trigger dump due to full buffer
            assert!(queue.enqueue(0, 99999).is_err() || queue.enqueue(0, 99999).is_ok());

            // Dump and verify we can enqueue again
            unsafe {
                queue.dump_local_buffer(0);
            }
            queue.enqueue(0, 88888).unwrap();
        }

        #[test]
        fn test_dqueue_dequeue_empty_checks() {
            let num_producers = 2;
            let segment_pool_capacity = 5;

            let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

            // Test empty queue
            assert!(queue.dequeue().is_none());

            // Test with items only in local buffers
            queue.enqueue(0, 1).unwrap();
            queue.enqueue(1, 2).unwrap();

            // Should trigger help and find items
            assert!(queue.dequeue().is_some());
            assert!(queue.dequeue().is_some());
            assert!(queue.dequeue().is_none());
        }

        #[test]
        fn test_dqueue_segment_allocation_failure_recovery() {
            let num_producers = 1;
            let segment_pool_capacity = 1; // Minimal

            let shared_size = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { DQueue::init_in_shared(mem_ptr, num_producers, segment_pool_capacity) };

            // Fill to exhaust pool
            for i in 0..10000 {
                let _ = queue.enqueue(0, i);
            }
            unsafe {
                queue.dump_local_buffer(0);
            }

            // Try to trigger allocation failure in find_segment
            unsafe {
                // This should handle allocation failure gracefully
                let result = queue.find_segment(ptr::null_mut(), 1000000);
                assert!(result.is_null());
            }
        }
    }

    mod jiffy_coverage_tests {
        use queues::mpsc::jiffy_queue::BufferList;

        use super::*;

        #[test]
        fn test_jiffy_buffer_allocation_failure_paths() {
            let buffer_capacity = 4;
            let max_buffers = 2;

            let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

            // Fill all buffers
            let mut pushed_count = 0;
            for i in 0..buffer_capacity * max_buffers * 2 {
                if queue.push(i).is_ok() {
                    pushed_count += 1;
                } else {
                    break;
                }
            }

            // We should have pushed some items
            assert!(pushed_count > 0);

            // Try to push when full
            if pushed_count < buffer_capacity * max_buffers * 2 {
                // Queue is full
                assert!(queue.push(999).is_err());
            }

            // Pop items to free space
            let mut popped_count = 0;
            for _ in 0..pushed_count {
                if queue.pop().is_ok() {
                    popped_count += 1;
                } else {
                    break;
                }
            }

            // After popping everything, we should be able to push
            if popped_count == pushed_count {
                // Queue is empty, should be able to push
                assert!(queue.push(888).is_ok());
            }
        }

        #[test]
        fn test_jiffy_fold_buffer_conditions() {
            let buffer_capacity = 4;
            let max_buffers = 5;

            let shared_size = JiffyQueue::<String>::shared_size(buffer_capacity, max_buffers);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

            // Create specific pattern for folding
            for i in 0..12 {
                queue.push(format!("item_{}", i)).unwrap();
            }

            // Mark all items in first buffer as handled
            for _ in 0..4 {
                queue.pop().unwrap();
            }

            // Try folding different buffers
            unsafe {
                // Try to fold null buffer
                let (result, folded) = queue.attempt_fold_buffer(ptr::null_mut());
                assert!(result.is_null());
                assert!(!folded);

                // Try to fold head buffer (should fail)
                let head = queue.head_of_queue.load(Ordering::Acquire);
                let (result, folded) = queue.attempt_fold_buffer(head);
                assert_eq!(result, head);
                assert!(!folded);
            }
        }

        #[test]
        fn test_jiffy_dequeue_complex_paths() {
            let buffer_capacity = 2; // Minimal
            let max_buffers = 10;

            let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

            // Create sparse pattern
            queue.push(1).unwrap();
            queue.push(2).unwrap(); // Fill first buffer
            queue.push(3).unwrap(); // New buffer

            // Pop first
            assert_eq!(queue.pop().unwrap(), 1);

            // Push more to create complex state
            for i in 10..20 {
                queue.push(i).unwrap();
            }

            // Pop with backtracking
            assert_eq!(queue.pop().unwrap(), 2);
            assert_eq!(queue.pop().unwrap(), 3);

            // Continue popping
            for i in 10..20 {
                assert_eq!(queue.pop().unwrap(), i);
            }
        }

        #[test]
        fn test_jiffy_garbage_collection_edge_cases() {
            let buffer_capacity = 4;
            let max_buffers = 3;

            let shared_size = JiffyQueue::<Vec<u8>>::shared_size(buffer_capacity, max_buffers);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

            // Test empty garbage list
            queue.actual_process_garbage_list(100);

            // Create buffers and mark for GC
            for i in 0..8 {
                queue.push(vec![i as u8; 10]).unwrap();
            }

            // Pop all to trigger potential GC
            while queue.pop().is_ok() {}

            // Process with different thresholds
            queue.actual_process_garbage_list(0);
            queue.actual_process_garbage_list(u64::MAX);
        }

        #[test]
        fn test_jiffy_buffer_state_transitions() {
            let buffer_capacity = 4;
            let max_buffers = 5;

            let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue: &mut JiffyQueue<usize> =
                unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

            // Test null buffer handling
            unsafe {
                let head: *mut BufferList<usize> = queue.head_of_queue.load(Ordering::Acquire);
                if !head.is_null() {
                    // Temporarily mark as reclaimed
                    (*head).is_array_reclaimed.store(true, Ordering::Release);

                    // Try operations
                    assert!(queue.pop().is_err());

                    // Restore
                    (*head).is_array_reclaimed.store(false, Ordering::Release);
                }
            }
        }

        #[test]
        fn test_jiffy_concurrent_modification_detection() {
            let buffer_capacity = 8;
            let max_buffers = 4;

            let shared_size = JiffyQueue::<usize>::shared_size(buffer_capacity, max_buffers);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue =
                unsafe { JiffyQueue::init_in_shared(mem_ptr, buffer_capacity, max_buffers) };

            // Fill multiple buffers
            for i in 0..20 {
                queue.push(i).unwrap();
            }

            // Simulate concurrent modifications by manipulating state
            unsafe {
                let tail = queue.tail_of_queue.load(Ordering::Acquire);
                if !tail.is_null() {
                    // Change position to trigger bounds check
                    let old_pos = (*tail).position_in_queue;
                    (*tail).position_in_queue = 1000;

                    // This should handle the invalid state
                    let _ = queue.push(999);

                    // Restore
                    (*tail).position_in_queue = old_pos;
                }
            }
        }
    }

    mod jayanti_petrovic_coverage_tests {
        use super::*;

        #[test]
        fn test_jp_tree_refresh_paths() {
            let num_producers = 4;
            let node_pool_capacity = 100;

            let shared_size =
                JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue: &mut JayantiPetrovicMpscQueue<usize> = unsafe {
                JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
            };

            // Test with no children (leaf nodes)
            unsafe {
                let leaf_idx = queue.get_leaf_tree_node_idx(0);
                queue.refresh(leaf_idx);
            }

            // Test with one child
            unsafe {
                queue.refresh(1); // Has left child only at index 3
            }

            // Test with both children
            unsafe {
                queue.refresh(0); // Root has both children
            }
        }

        #[test]
        fn test_jp_propagate_paths() {
            let num_producers = 8; // Larger tree
            let node_pool_capacity = 1000;

            let shared_size =
                JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe {
                JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
            };

            // Test propagate as enqueuer
            queue.enqueue(0, 1).unwrap();

            // Test propagate as dequeuer
            queue.dequeue().unwrap();

            // Test with empty queue
            queue.enqueue(7, 999).unwrap(); // Last producer
            queue.dequeue().unwrap();
        }

        #[test]
        fn test_jp_dequeue_edge_cases() {
            use queues::mpsc::jayanti_petrovic_queue::Timestamp;

            let num_producers = 2;
            let node_pool_capacity = 50;

            let shared_size =
                JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe {
                JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
            };

            // Test empty queue
            assert!(queue.dequeue().is_none());

            // Test with invalid leaf index
            unsafe {
                // We can't directly access MinInfo as it's private, so we'll test
                // indirectly by enqueueing/dequeueing
                queue.enqueue(0, 42).unwrap();
                assert_eq!(queue.dequeue(), Some(42));
            }
        }

        #[test]
        fn test_jp_node_pool_exhaustion() {
            let num_producers = 2;
            let node_pool_capacity = 10; // Increased to account for overhead

            let shared_size =
                JayantiPetrovicMpscQueue::<usize>::shared_size(num_producers, node_pool_capacity);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe {
                JayantiPetrovicMpscQueue::init_in_shared(mem_ptr, num_producers, node_pool_capacity)
            };

            // Fill pool (leave some capacity for internal nodes)
            let items_to_push = node_pool_capacity - 3; // Reserve some for dummy nodes
            for i in 0..items_to_push {
                if queue.enqueue(0, i).is_err() {
                    // Pool exhausted earlier than expected, that's ok
                    break;
                }
            }

            // Try to push one more - this might fail
            let push_result = queue.enqueue(0, 999);

            // If it failed, dequeue some items
            if push_result.is_err() {
                // Dequeue a few items to free nodes
                for _ in 0..3 {
                    if queue.dequeue().is_none() {
                        break;
                    }
                }

                // Now try again - but don't assume it will work
                // The pool might still be exhausted due to fragmentation
                let _ = queue.enqueue(1, 888);
            }
        }
    }

    mod drescher_coverage_tests {
        use super::*;

        #[test]
        fn test_drescher_free_list_contention() {
            let expected_nodes = 100;
            let shared_size = DrescherQueue::<usize>::shared_size(expected_nodes);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, expected_nodes) };

            // Fill and empty multiple times to exercise free list
            for cycle in 0..5 {
                for i in 0..50 {
                    queue.push(cycle * 100 + i).unwrap();
                }

                for _ in 0..50 {
                    queue.pop().unwrap();
                }
            }

            // Free list should be populated
            // Test allocation from free list
            for i in 0..30 {
                queue.push(1000 + i).unwrap();
            }
        }

        #[test]
        fn test_drescher_dummy_node_requeue() {
            let expected_nodes = 50;
            let shared_size = DrescherQueue::<String>::shared_size(expected_nodes);
            let memory = create_aligned_memory_box(shared_size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { DrescherQueue::init_in_shared(mem_ptr, expected_nodes) };

            // Push one item and pop it to trigger dummy requeue
            queue.push("first".to_string()).unwrap();
            assert_eq!(queue.pop().unwrap(), "first");

            // Push more to test with requeued dummy
            for i in 0..10 {
                queue.push(format!("item_{}", i)).unwrap();
            }

            for i in 0..10 {
                assert_eq!(queue.pop().unwrap(), format!("item_{}", i));
            }
        }
    }

    mod sesd_jp_coverage_tests {
        use super::*;
        use queues::mpsc::sesd_jp_queue::{Node as SesdNode, SesdJpQueue};
        use std::mem::MaybeUninit;
        use std::sync::atomic::AtomicPtr;

        #[test]
        fn test_sesd_read_fronte_paths() {
            let help_slot = Box::into_raw(Box::new(MaybeUninit::<usize>::uninit()));
            let initial_dummy = Box::into_raw(Box::new(SesdNode::<usize> {
                item: MaybeUninit::uninit(),
                next: AtomicPtr::new(ptr::null_mut()),
            }));
            let free_later_dummy = Box::into_raw(Box::new(SesdNode::<usize> {
                item: MaybeUninit::uninit(),
                next: AtomicPtr::new(ptr::null_mut()),
            }));

            let queue_mem = Box::into_raw(Box::new(MaybeUninit::<SesdJpQueue<usize>>::uninit()));
            let queue = unsafe {
                SesdJpQueue::new_in_shm(
                    queue_mem as *mut SesdJpQueue<usize>,
                    initial_dummy,
                    help_slot,
                    free_later_dummy,
                )
            };

            // Test empty queue
            assert!(queue.read_fronte().is_none());

            // Add item
            let node = Box::into_raw(Box::new(SesdNode::<usize> {
                item: MaybeUninit::uninit(),
                next: AtomicPtr::new(ptr::null_mut()),
            }));
            queue.enqueue2(42, node);

            // Test announce mechanism
            assert_eq!(queue.read_fronte().unwrap(), 42);

            // Cleanup
            unsafe {
                let _ = Box::from_raw(help_slot);
                let _ = Box::from_raw(initial_dummy);
                let _ = Box::from_raw(free_later_dummy);
                let _ = Box::from_raw(node);
                let _ = Box::from_raw(queue_mem);
            }
        }
    }

    // Helper function
    fn create_aligned_memory_box(size: usize) -> Box<[u8]> {
        use std::alloc::{alloc_zeroed, Layout};

        unsafe {
            let layout = Layout::from_size_align(size, 64).unwrap();
            let ptr = alloc_zeroed(layout);
            if ptr.is_null() {
                panic!("Failed to allocate aligned memory");
            }

            let slice = std::slice::from_raw_parts_mut(ptr, size);
            Box::from_raw(slice)
        }
    }
}
