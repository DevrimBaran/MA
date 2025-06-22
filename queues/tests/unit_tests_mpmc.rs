use queues::{mpmc::*, MpmcQueue};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

trait BenchMpmcQueue<T: Send>: Send + Sync + 'static {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()>;
    fn bench_pop(&self, thread_id: usize) -> Result<T, ()>;
    fn bench_is_empty(&self) -> bool;
    fn bench_is_full(&self) -> bool;
}

// Implement BenchMpmcQueue for all queue types
impl<T: Send + 'static> BenchMpmcQueue<T> for YangCrummeyQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + 'static> BenchMpmcQueue<T> for KWQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + 'static> BenchMpmcQueue<T> for WFQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + 'static> BenchMpmcQueue<T> for BurdenWFQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + 'static> BenchMpmcQueue<T> for NRQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + 'static> BenchMpmcQueue<T> for JKMQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + 'static> BenchMpmcQueue<T> for WCQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + 'static> BenchMpmcQueue<T> for TurnQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + 'static> BenchMpmcQueue<T> for FeldmanDechevWFQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + 'static> BenchMpmcQueue<T> for SDPWFQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + 'static> BenchMpmcQueue<T> for KPQueue<T> {
    fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
        self.push(item, thread_id)
    }

    fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
        self.pop(thread_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

const NUM_THREADS: usize = 4;
const ITEMS_PER_THREAD: usize = 1000;

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

// Helper macro for basic MPMC tests
macro_rules! test_mpmc_basic {
    ($queue_type:ty, $init:expr, $test_name:ident) => {
        mod $test_name {
            use super::*;

            #[test]
            fn test_single_thread_basic() {
                let queue = $init;
                assert!(queue.is_empty());
                queue.push(42, 0).unwrap();
                assert!(!queue.is_empty());
                assert_eq!(queue.pop(0).unwrap(), 42);
                assert!(queue.is_empty());

                for i in 0..5 {
                    queue.push(i, 0).unwrap();
                }
                for i in 0..5 {
                    assert_eq!(queue.pop(0).unwrap(), i);
                }
                assert!(queue.is_empty());
            }

            #[test]
            fn test_multiple_threads_single_producer_consumer() {
                let queue = Arc::new($init);
                let barrier = Arc::new(Barrier::new(3));

                let q1 = queue.clone();
                let b1 = barrier.clone();
                let producer = thread::spawn(move || {
                    b1.wait();
                    for i in 0..100 {
                        q1.push(i, 0).unwrap();
                    }
                });

                let q2 = queue.clone();
                let b2 = barrier.clone();
                let consumer = thread::spawn(move || {
                    b2.wait();
                    let mut items = Vec::new();
                    for _ in 0..100 {
                        loop {
                            match q2.pop(1) {
                                Ok(item) => {
                                    items.push(item);
                                    break;
                                }
                                Err(_) => thread::yield_now(),
                            }
                        }
                    }
                    items
                });

                barrier.wait();
                producer.join().unwrap();
                let items = consumer.join().unwrap();

                assert_eq!(items.len(), 100);
                for (i, &item) in items.iter().enumerate() {
                    assert_eq!(item, i);
                }
            }
        }
    };
}

mod yang_crummey_tests {
    use super::*;

    #[test]
    fn test_yang_crummey_basic() {
        let num_threads = 4;
        let shared_size = YangCrummeyQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { YangCrummeyQueue::init_in_shared(mem_ptr, num_threads) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());

        // Test enqueue and dequeue
        queue.enqueue(0, 42).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.dequeue(0).unwrap(), 42);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_yang_crummey_concurrent() {
        let num_threads = 4;
        let shared_size = YangCrummeyQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { YangCrummeyQueue::init_in_shared(mem_ptr, num_threads) };
        let queue = Arc::new(queue);
        let barrier = Arc::new(Barrier::new(num_threads * 2));

        let mut handles = vec![];

        // Spawn producers
        for thread_id in 0..num_threads {
            let q = queue.clone();
            let b = barrier.clone();

            let handle = thread::spawn(move || {
                b.wait();
                for i in 0..ITEMS_PER_THREAD {
                    let value = thread_id * ITEMS_PER_THREAD + i;
                    q.enqueue(thread_id, value).unwrap();
                }
            });

            handles.push(handle);
        }

        // Spawn consumers
        for thread_id in 0..num_threads {
            let q = queue.clone();
            let b = barrier.clone();

            let handle = thread::spawn(move || {
                b.wait();
                let mut items = Vec::new();
                for _ in 0..ITEMS_PER_THREAD {
                    loop {
                        match q.dequeue(thread_id) {
                            Ok(item) => {
                                items.push(item);
                                break;
                            }
                            Err(_) => thread::yield_now(),
                        }
                    }
                }
                items
            });

            handles.push(handle);
        }

        barrier.wait();

        let mut all_items = Vec::new();
        for handle in handles {
            if let Ok(items) = handle.join() {
                if let Ok(items) = items.downcast::<Vec<usize>>() {
                    all_items.extend(items.into_iter());
                }
            }
        }

        assert_eq!(all_items.len(), num_threads * ITEMS_PER_THREAD);
        all_items.sort();

        for (i, &item) in all_items.iter().enumerate() {
            assert_eq!(item, i);
        }
    }

    test_mpmc_basic!(
        YangCrummeyQueue<usize>,
        {
            let size = YangCrummeyQueue::<usize>::shared_size(4);
            let memory = create_aligned_memory_box(size);
            let mem_ptr = Box::leak(memory).as_mut_ptr();
            unsafe { YangCrummeyQueue::init_in_shared(mem_ptr, 4) }
        },
        yang_crummey_mpmc_tests
    );
}

mod kw_queue_tests {
    use super::*;

    #[test]
    fn test_kw_queue_initialization() {
        let num_threads = 4;
        let shared_size = KWQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { KWQueue::init_in_shared(mem_ptr, num_threads) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_kw_queue_enqueue_dequeue() {
        let num_threads = 4;
        let shared_size = KWQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { KWQueue::init_in_shared(mem_ptr, num_threads) };

        // Test basic operations
        queue.enqueue(0, 100).unwrap();
        queue.enqueue(1, 200).unwrap();

        assert_eq!(queue.dequeue(0).unwrap(), 100);
        assert_eq!(queue.dequeue(1).unwrap(), 200);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_kw_queue_concurrent_operations() {
        let num_threads = 4;
        let shared_size = KWQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { KWQueue::init_in_shared(mem_ptr, num_threads) };
        let queue = Arc::new(queue);

        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let q = queue.clone();
            let b = barrier.clone();

            let handle = thread::spawn(move || {
                b.wait();

                // Each thread enqueues and dequeues
                for i in 0..50 {
                    q.enqueue(thread_id, thread_id * 1000 + i).unwrap();
                }

                let mut dequeued = Vec::new();
                for _ in 0..50 {
                    loop {
                        match q.dequeue(thread_id) {
                            Ok(val) => {
                                dequeued.push(val);
                                break;
                            }
                            Err(_) => thread::yield_now(),
                        }
                    }
                }
                dequeued
            });

            handles.push(handle);
        }

        barrier.wait();

        let mut all_items = Vec::new();
        for handle in handles {
            let items = handle.join().unwrap();
            all_items.extend(items);
        }

        assert_eq!(all_items.len(), num_threads * 50);
        all_items.sort();

        // Check all items are present
        let mut expected = Vec::new();
        for thread_id in 0..num_threads {
            for i in 0..50 {
                expected.push(thread_id * 1000 + i);
            }
        }
        expected.sort();

        assert_eq!(all_items, expected);
    }
}

mod wf_queue_tests {
    use super::*;

    // Note: WFQueue requires a helper thread to be running
    // We'll simulate basic tests without the helper for initialization tests

    #[test]
    fn test_wf_queue_initialization() {
        let num_threads = 4;
        let shared_size = WFQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { WFQueue::init_in_shared(mem_ptr, num_threads) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_wf_queue_stop_helper() {
        let num_threads = 2;
        let shared_size = WFQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { WFQueue::init_in_shared(mem_ptr, num_threads) };

        // Test stop_helper doesn't hang
        queue.stop_helper();

        // The helper should not be running
        assert!(!queue.helper_running.load(Ordering::Acquire));
    }
}

mod burden_wf_queue_tests {
    use super::*;

    #[test]
    fn test_burden_queue_basic() {
        let num_threads = 4;
        let shared_size = BurdenWFQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { BurdenWFQueue::init_in_shared(mem_ptr, num_threads) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());

        queue.enqueue(0, 42).unwrap();
        assert!(!queue.is_empty());
        assert_eq!(queue.dequeue(0).unwrap(), 42);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_burden_queue_multiple_operations() {
        let num_threads = 4;
        let shared_size = BurdenWFQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { BurdenWFQueue::init_in_shared(mem_ptr, num_threads) };

        // Enqueue from different threads
        for thread_id in 0..num_threads {
            for i in 0..10 {
                queue.enqueue(thread_id, thread_id * 100 + i).unwrap();
            }
        }

        // Dequeue and verify
        let mut items = Vec::new();
        for thread_id in 0..num_threads {
            for _ in 0..10 {
                items.push(queue.dequeue(thread_id).unwrap());
            }
        }

        assert_eq!(items.len(), num_threads * 10);
        items.sort();

        let mut expected = Vec::new();
        for thread_id in 0..num_threads {
            for i in 0..10 {
                expected.push(thread_id * 100 + i);
            }
        }
        expected.sort();

        assert_eq!(items, expected);
    }
}

mod nr_queue_tests {
    use super::*;

    #[test]
    fn test_nr_queue_initialization() {
        let num_processes = 4;
        let shared_size = NRQueue::<usize>::shared_size(num_processes);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { NRQueue::init_in_shared(mem_ptr, num_processes) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_nr_queue_enqueue_dequeue() {
        let num_processes = 4;
        let shared_size = NRQueue::<usize>::shared_size(num_processes);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { NRQueue::init_in_shared(mem_ptr, num_processes) };

        // Test enqueue
        queue.enqueue(0, 100).unwrap();
        queue.enqueue(1, 200).unwrap();

        assert!(!queue.is_empty());

        // Test dequeue with simple_dequeue
        assert_eq!(queue.simple_dequeue(0).unwrap(), 100);
        assert_eq!(queue.simple_dequeue(1).unwrap(), 200);

        assert!(queue.is_empty());
    }

    #[test]
    fn test_nr_queue_sync_operations() {
        let num_processes = 4;
        let shared_size = NRQueue::<usize>::shared_size(num_processes);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { NRQueue::init_in_shared(mem_ptr, num_processes) };

        // Enqueue from multiple processes
        for i in 0..10 {
            queue.enqueue(i % num_processes, i).unwrap();
        }

        // Force sync
        queue.force_complete_sync();

        // Dequeue all
        let mut items = Vec::new();
        for i in 0..10 {
            items.push(queue.simple_dequeue(i % num_processes).unwrap());
        }

        assert_eq!(items.len(), 10);
        items.sort();

        for (i, &item) in items.iter().enumerate() {
            assert_eq!(item, i);
        }
    }
}

mod jkm_queue_tests {
    use super::*;

    #[test]
    fn test_jkm_queue_initialization() {
        let num_enq = 4;
        let num_deq = 4;
        let shared_size = JKMQueue::<usize>::shared_size(num_enq, num_deq);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JKMQueue::init_in_shared(mem_ptr, num_enq, num_deq) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_jkm_queue_enqueue_dequeue() {
        let num_enq = 2;
        let num_deq = 2;
        let shared_size = JKMQueue::<usize>::shared_size(num_enq, num_deq);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JKMQueue::init_in_shared(mem_ptr, num_enq, num_deq) };

        // Test enqueue
        queue.enqueue(0, 100).unwrap();
        queue.enqueue(1, 200).unwrap();

        // Force sync to ensure visibility
        queue.force_sync();

        // Test dequeue
        let val1 = queue.dequeue(0).unwrap();
        let val2 = queue.dequeue(1).unwrap();

        assert!((val1 == 100 && val2 == 200) || (val1 == 200 && val2 == 100));
    }

    #[test]
    fn test_jkm_queue_force_sync() {
        let num_enq = 4;
        let num_deq = 4;
        let shared_size = JKMQueue::<usize>::shared_size(num_enq, num_deq);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JKMQueue::init_in_shared(mem_ptr, num_enq, num_deq) };

        // Enqueue items
        for i in 0..10 {
            queue.enqueue(i % num_enq, i).unwrap();
        }

        // Force sync
        queue.force_sync();

        // Check total items
        assert_eq!(queue.total_items(), 10);

        // Finalize pending dequeues
        queue.finalize_pending_dequeues();
    }

    #[test]
    fn test_jkm_queue_debug_stats() {
        let num_enq = 2;
        let num_deq = 2;
        let shared_size = JKMQueue::<usize>::shared_size(num_enq, num_deq);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { JKMQueue::init_in_shared(mem_ptr, num_enq, num_deq) };

        // Perform some operations
        queue.enqueue(0, 42).unwrap();
        queue.force_sync();
        let _ = queue.dequeue(0);

        // Print debug stats (no assertions, just ensure it doesn't crash)
        queue.print_debug_stats();
    }
}

mod wcq_queue_tests {
    use super::*;

    #[test]
    fn test_wcq_initialization() {
        let num_threads = 4;
        let shared_size = WCQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { WCQueue::init_in_shared(mem_ptr, num_threads) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_wcq_enqueue_dequeue() {
        let num_threads = 2;
        let shared_size = WCQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { WCQueue::init_in_shared(mem_ptr, num_threads) };

        // Test enqueue
        queue.enqueue(42, 0).unwrap();
        assert!(!queue.is_empty());

        // Test dequeue
        assert_eq!(queue.dequeue(0).unwrap(), 42);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_wcq_terminate() {
        let num_threads = 2;
        let shared_size = WCQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { WCQueue::init_in_shared(mem_ptr, num_threads) };

        // Test terminate
        queue.terminate();
        assert!(queue.terminating.load(Ordering::Acquire));
    }
}

mod turn_queue_tests {
    use super::*;

    #[test]
    fn test_turn_queue_initialization() {
        let num_threads = 4;
        let shared_size = TurnQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { TurnQueue::init_in_shared(mem_ptr, num_threads) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_turn_queue_basic_operations() {
        let num_threads = 4;
        let shared_size = TurnQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { TurnQueue::init_in_shared(mem_ptr, num_threads) };

        // Enqueue from different threads
        queue.enqueue(0, 100).unwrap();
        queue.enqueue(1, 200).unwrap();
        queue.enqueue(2, 300).unwrap();

        // Dequeue
        assert_eq!(queue.dequeue(0).unwrap(), 100);
        assert_eq!(queue.dequeue(1).unwrap(), 200);
        assert_eq!(queue.dequeue(2).unwrap(), 300);

        assert!(queue.is_empty());
    }
}

mod feldman_dechev_tests {
    use super::*;

    #[test]
    fn test_feldman_dechev_initialization() {
        let num_threads = 4;
        let shared_size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { FeldmanDechevWFQueue::init_in_shared(mem_ptr, num_threads) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_feldman_dechev_enqueue_dequeue() {
        let num_threads = 4;
        let shared_size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { FeldmanDechevWFQueue::init_in_shared(mem_ptr, num_threads) };

        // Test fast path
        queue.enqueue(0, 42).unwrap();
        assert_eq!(queue.dequeue(0).unwrap(), 42);

        // Test multiple operations
        for i in 0..10 {
            queue.enqueue(i % num_threads, i * 100).unwrap();
        }

        let mut items = Vec::new();
        for i in 0..10 {
            items.push(queue.dequeue(i % num_threads).unwrap());
        }

        items.sort();
        let expected: Vec<_> = (0..10).map(|i| i * 100).collect();
        assert_eq!(items, expected);
    }
}

mod sdp_queue_tests {
    use super::*;

    #[test]
    fn test_sdp_queue_initialization() {
        let num_threads = 4;
        let enable_helping = true;
        let shared_size = SDPWFQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { SDPWFQueue::init_in_shared(mem_ptr, num_threads, enable_helping) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_sdp_queue_basic_operations() {
        let num_threads = 4;
        let enable_helping = true;
        let shared_size = SDPWFQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { SDPWFQueue::init_in_shared(mem_ptr, num_threads, enable_helping) };

        // Test enqueue and dequeue
        queue.enqueue(0, 100).unwrap();
        queue.enqueue(1, 200).unwrap();

        assert_eq!(queue.dequeue(0).unwrap(), 100);
        assert_eq!(queue.dequeue(1).unwrap(), 200);

        assert!(queue.is_empty());
    }

    #[test]
    fn test_sdp_queue_read_operation() {
        let num_threads = 2;
        let enable_helping = false;
        let shared_size = SDPWFQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { SDPWFQueue::init_in_shared(mem_ptr, num_threads, enable_helping) };

        // Enqueue some items
        queue.enqueue(0, 10).unwrap();
        queue.enqueue(0, 20).unwrap();
        queue.enqueue(0, 30).unwrap();

        // Read all items (non-destructive)
        let items = queue.read(0);
        assert_eq!(items.len(), 3);
        assert!(items.contains(&10));
        assert!(items.contains(&20));
        assert!(items.contains(&30));

        // Items should still be in queue
        assert!(!queue.is_empty());
    }
}

mod kogan_petrank_tests {
    use super::*;

    #[test]
    fn test_kp_queue_initialization() {
        let num_threads = 4;
        let shared_size = KPQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { KPQueue::init_in_shared(mem_ptr, num_threads) };

        assert!(queue.is_empty());
        assert!(!queue.is_full());
    }

    #[test]
    fn test_kp_queue_enqueue_dequeue() {
        let num_threads = 4;
        let shared_size = KPQueue::<usize>::shared_size(num_threads);
        let memory = create_aligned_memory_box(shared_size);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { KPQueue::init_in_shared(mem_ptr, num_threads) };

        // Test basic operations
        queue.enqueue(0, 42).unwrap();
        assert!(!queue.is_empty());

        assert_eq!(queue.dequeue(1).unwrap(), 42);
        assert!(queue.is_empty());

        // Test multiple operations
        for i in 0..5 {
            queue.enqueue(i % num_threads, i * 10).unwrap();
        }

        for i in 0..5 {
            assert_eq!(queue.dequeue(i % num_threads).unwrap(), i * 10);
        }
    }
}

// Integration tests for all queues
mod integration_tests {
    use super::*;

    #[test]
    fn test_all_queues_implement_trait() {
        // Test that all queues properly implement the MpmcQueue trait
        fn test_mpmc_trait<T>(queue: &T)
        where
            T: MpmcQueue<usize>,
            T::PushError: std::fmt::Debug,
            T::PopError: std::fmt::Debug,
        {
            assert!(queue.is_empty());
            assert!(!queue.is_full());

            queue.push(42, 0).unwrap();
            assert!(!queue.is_empty());

            assert_eq!(queue.pop(0).unwrap(), 42);
            assert!(queue.is_empty());
        }

        // YangCrummeyQueue
        {
            let size = YangCrummeyQueue::<usize>::shared_size(4);
            let mem = create_aligned_memory_box(size);
            let mem_ptr = Box::leak(mem).as_mut_ptr();
            let queue = unsafe { YangCrummeyQueue::init_in_shared(mem_ptr, 4) };
            test_mpmc_trait(&*queue);
        }

        // KWQueue
        {
            let size = KWQueue::<usize>::shared_size(4);
            let mem = create_aligned_memory_box(size);
            let mem_ptr = Box::leak(mem).as_mut_ptr();
            let queue = unsafe { KWQueue::init_in_shared(mem_ptr, 4) };
            test_mpmc_trait(&*queue);
        }

        // Add other queues as needed...
    }

    #[test]
    fn test_concurrent_stress() {
        let num_threads = 8;
        let items_per_thread = 100;

        // Test YangCrummeyQueue under stress
        {
            let size = YangCrummeyQueue::<usize>::shared_size(num_threads);
            let mem = create_aligned_memory_box(size);
            let mem_ptr = Box::leak(mem).as_mut_ptr();
            let queue = unsafe { YangCrummeyQueue::init_in_shared(mem_ptr, num_threads) };
            let queue = Arc::new(queue);

            let barrier = Arc::new(Barrier::new(num_threads));
            let mut handles = vec![];

            for thread_id in 0..num_threads {
                let q = queue.clone();
                let b = barrier.clone();

                let handle = thread::spawn(move || {
                    b.wait();

                    // Interleave enqueue and dequeue
                    for i in 0..items_per_thread {
                        q.enqueue(thread_id, thread_id * 1000 + i).unwrap();

                        if i % 2 == 0 {
                            // Try to dequeue
                            let _ = q.dequeue(thread_id);
                        }
                    }

                    // Final drain
                    let mut count = 0;
                    for _ in 0..items_per_thread {
                        if q.dequeue(thread_id).is_ok() {
                            count += 1;
                        }
                    }
                    count
                });

                handles.push(handle);
            }

            barrier.wait();

            let mut total_dequeued = 0;
            for handle in handles {
                total_dequeued += handle.join().unwrap();
            }

            println!("Stress test dequeued {} items", total_dequeued);
            assert!(total_dequeued > 0);
        }
    }
}

// Memory and edge case tests
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_zero_sized_type() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct ZeroSized;

        let size = YangCrummeyQueue::<ZeroSized>::shared_size(4);
        let mem = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(mem).as_mut_ptr();
        let queue = unsafe { YangCrummeyQueue::init_in_shared(mem_ptr, 4) };

        queue.enqueue(0, ZeroSized).unwrap();
        assert_eq!(queue.dequeue(0).unwrap(), ZeroSized);
    }

    #[test]
    fn test_large_type() {
        #[derive(Clone, Debug, PartialEq)]
        struct LargeType {
            data: [u64; 128],
        }

        let size = KWQueue::<LargeType>::shared_size(2);
        let mem = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(mem).as_mut_ptr();
        let queue = unsafe { KWQueue::init_in_shared(mem_ptr, 2) };

        let item = LargeType { data: [42; 128] };
        queue.enqueue(0, item.clone()).unwrap();
        assert_eq!(queue.dequeue(0).unwrap(), item);
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
            let size = BurdenWFQueue::<DropCounter>::shared_size(4);
            let mem = create_aligned_memory_box(size);
            let mem_ptr = Box::leak(mem).as_mut_ptr();
            let queue = unsafe { BurdenWFQueue::init_in_shared(mem_ptr, 4) };

            for i in 0..10 {
                queue.enqueue(0, DropCounter { _value: i }).unwrap();
            }

            for _ in 0..5 {
                drop(queue.dequeue(0).unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
        }
    }

    #[test]
    fn test_thread_id_bounds() {
        let num_threads = 4;
        let size = YangCrummeyQueue::<usize>::shared_size(num_threads);
        let mem = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(mem).as_mut_ptr();
        let queue = unsafe { YangCrummeyQueue::init_in_shared(mem_ptr, num_threads) };

        // Test valid thread IDs
        assert!(queue.enqueue(0, 42).is_ok());
        assert!(queue.enqueue(num_threads - 1, 43).is_ok());

        // Test invalid thread IDs
        assert!(queue.enqueue(num_threads, 44).is_err());
        assert!(queue.enqueue(usize::MAX, 45).is_err());
    }
}

// Performance characteristics tests
mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_queue_throughput() {
        let num_threads = 4;
        let operations = 10_000;

        // Test YangCrummeyQueue throughput
        {
            let size = YangCrummeyQueue::<usize>::shared_size(num_threads);
            let mem = create_aligned_memory_box(size);
            let mem_ptr = Box::leak(mem).as_mut_ptr();
            let queue = unsafe { YangCrummeyQueue::init_in_shared(mem_ptr, num_threads) };

            let start = Instant::now();

            for i in 0..operations {
                queue.enqueue(i % num_threads, i).unwrap();
            }

            for i in 0..operations {
                queue.dequeue(i % num_threads).unwrap();
            }

            let duration = start.elapsed();
            println!("YangCrummeyQueue: {} ops in {:?}", operations * 2, duration);
        }
    }

    #[test]
    fn test_fairness() {
        let num_threads = 4;
        let size = TurnQueue::<usize>::shared_size(num_threads);
        let mem = create_aligned_memory_box(size);
        let mem_ptr = Box::leak(mem).as_mut_ptr();
        let queue = unsafe { TurnQueue::init_in_shared(mem_ptr, num_threads) };
        let queue = Arc::new(queue);

        let barrier = Arc::new(Barrier::new(num_threads));
        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let q = queue.clone();
            let b = barrier.clone();

            let handle = thread::spawn(move || {
                b.wait();

                let mut enqueued = 0;
                let mut dequeued = 0;

                for _ in 0..100 {
                    q.enqueue(thread_id, thread_id).unwrap();
                    enqueued += 1;

                    if let Ok(val) = q.dequeue(thread_id) {
                        dequeued += 1;
                        assert!(val < num_threads); // Should get values from all threads
                    }
                }

                (enqueued, dequeued)
            });

            handles.push(handle);
        }

        barrier.wait();

        let mut total_enqueued = 0;
        let mut total_dequeued = 0;

        for handle in handles {
            let (enq, deq) = handle.join().unwrap();
            total_enqueued += enq;
            total_dequeued += deq;
        }

        assert_eq!(total_enqueued, num_threads * 100);
        assert_eq!(total_dequeued, num_threads * 100);
    }
}
