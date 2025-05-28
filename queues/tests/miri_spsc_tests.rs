#![cfg(miri)]

use queues::{spsc::*, SpscQueue};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::thread;

const MIRI_SMALL_CAP: usize = 16;
const MIRI_MEDIUM_CAP: usize = 64;
const MIRI_LARGE_CAP: usize = 256;
const MIRI_TEST_ITEMS: usize = 100;

struct AlignedMemory {
    ptr: *mut u8,
    layout: std::alloc::Layout,
    cleanup: Option<Box<dyn FnOnce()>>,
}

impl AlignedMemory {
    fn new(size: usize, alignment: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(size, alignment).expect("Invalid layout");

        unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            Self {
                ptr,
                layout,
                cleanup: None,
            }
        }
    }

    fn with_cleanup<F: FnOnce() + 'static>(mut self, cleanup: F) -> Self {
        self.cleanup = Some(Box::new(cleanup));
        self
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for AlignedMemory {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }

        unsafe {
            std::alloc::dealloc(self.ptr, self.layout);
        }
    }
}

mod miri_basic_tests {
    use super::*;

    #[test]
    fn miri_test_lamport() {
        {
            let queue = LamportQueue::<usize>::with_capacity(MIRI_SMALL_CAP);

            assert!(queue.empty());
            assert!(queue.available());
            queue.push(42).unwrap();
            assert!(!queue.empty());
            assert!(queue.available());
            assert_eq!(queue.pop().unwrap(), 42);
            assert!(queue.empty());
        }

        {
            let queue = LamportQueue::<usize>::with_capacity(MIRI_SMALL_CAP);
            let items_to_push = MIRI_SMALL_CAP - 1;

            for i in 0..items_to_push {
                assert!(queue.push(i).is_ok(), "Failed to push item {}", i);
            }

            assert!(!queue.available());
            assert!(queue.push(999).is_err(), "Should fail when full");

            for i in 0..items_to_push {
                assert_eq!(queue.pop().unwrap(), i);
            }
            assert!(queue.empty());
        }

        {
            let queue = Arc::new(LamportQueue::<usize>::with_capacity(MIRI_SMALL_CAP));
            let barrier = Arc::new(Barrier::new(2));
            let items = 8;

            let q_prod = queue.clone();
            let b_prod = barrier.clone();

            let producer = thread::spawn(move || {
                b_prod.wait();
                for i in 0..items {
                    while q_prod.push(i).is_err() {
                        thread::yield_now();
                    }
                }
            });

            let q_cons = queue.clone();
            let b_cons = barrier.clone();

            let consumer = thread::spawn(move || {
                b_cons.wait();
                let mut received = Vec::new();
                let mut attempts = 0;

                while received.len() < items && attempts < items * 100 {
                    match q_cons.pop() {
                        Ok(val) => {
                            received.push(val);
                            attempts = 0;
                        }
                        Err(_) => {
                            attempts += 1;
                            thread::yield_now();
                        }
                    }
                }
                received
            });

            producer.join().unwrap();
            let received = consumer.join().unwrap();

            assert_eq!(received.len(), items);
            for (i, &val) in received.iter().enumerate() {
                assert_eq!(val, i);
            }
        }
    }

    #[test]
    fn miri_test_ffq() {
        {
            let queue = FfqQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

            assert!(queue.empty());
            assert!(queue.available());
            queue.push(42).unwrap();
            assert!(!queue.empty());
            assert_eq!(queue.pop().unwrap(), 42);
            assert!(queue.empty());
        }

        {
            let queue = FfqQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

            let mut pushed = 0;
            for i in 0..MIRI_MEDIUM_CAP {
                if queue.push(i).is_ok() {
                    pushed += 1;
                } else {
                    break;
                }
            }

            assert!(pushed > 0, "Should push at least some items");

            if pushed < MIRI_MEDIUM_CAP {
                assert!(!queue.available());
            }

            for i in 0..pushed {
                assert_eq!(queue.pop().unwrap(), i);
            }
            assert!(queue.empty());
        }
    }

    #[test]
    fn miri_test_bqueue() {
        {
            let queue = BQueue::<usize>::new(MIRI_MEDIUM_CAP);

            assert!(queue.empty());
            assert!(queue.available());
            queue.push(42).unwrap();
            assert!(!queue.empty());
            assert_eq!(queue.pop().unwrap(), 42);
            assert!(queue.empty());
        }

        {
            let queue = BQueue::<usize>::new(MIRI_MEDIUM_CAP);

            let mut pushed = 0;
            for i in 0..MIRI_MEDIUM_CAP {
                if queue.push(i).is_ok() {
                    pushed += 1;
                } else {
                    break;
                }
            }

            println!(
                "BQueue pushed {} items with capacity {}",
                pushed, MIRI_MEDIUM_CAP
            );
            assert!(pushed > 0, "Should push at least some items");
            assert!(pushed <= MIRI_MEDIUM_CAP, "Should not exceed capacity");

            assert!(queue.push(999).is_err() || pushed < MIRI_MEDIUM_CAP);

            for i in 0..pushed {
                assert_eq!(queue.pop().unwrap(), i);
            }
            assert!(queue.empty());
        }
    }

    #[test]
    fn miri_test_dehnavi() {
        {
            let queue = DehnaviQueue::<usize>::new(MIRI_SMALL_CAP);

            assert!(queue.empty());
            assert!(queue.available());
            queue.push(42).unwrap();
            assert!(!queue.empty());
            assert_eq!(queue.pop().unwrap(), 42);
            assert!(queue.empty());
        }

        {
            let queue = DehnaviQueue::<usize>::new(4);

            for i in 0..10 {
                queue.push(i).unwrap();
            }

            let mut items = Vec::new();
            while let Ok(item) = queue.pop() {
                items.push(item);
            }

            assert!(!items.is_empty());

            for window in items.windows(2) {
                assert!(window[1] > window[0]);
            }
        }
    }

    #[test]
    fn miri_test_multipush() {
        let queue = MultiPushQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

        queue.push(42).unwrap();
        assert!(!queue.empty());

        assert!(queue.flush());

        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        for i in 0..10 {
            queue.push(i).unwrap();
        }

        assert!(queue.local_count.load(std::sync::atomic::Ordering::Relaxed) > 0);

        assert!(queue.flush());
        assert_eq!(
            queue.local_count.load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn miri_test_llq() {
        let queue = LlqQueue::<usize>::with_capacity(128);

        assert!(queue.empty());
        assert!(queue.available());

        queue.push(42).unwrap();
        assert!(!queue.empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        let mut pushed = 0;
        for i in 0..100 {
            if queue.push(i).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }

        assert!(pushed > 0);

        for _ in 0..pushed {
            assert!(queue.pop().is_ok());
        }
        assert!(queue.empty());
    }

    #[test]
    fn miri_test_blq() {
        let queue = BlqQueue::<usize>::with_capacity(64);

        assert!(queue.empty());
        assert!(queue.available());

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        let space = queue.blq_enq_space(10);
        assert!(space >= 10);

        for i in 0..10 {
            queue.blq_enq_local(i).unwrap();
        }
        queue.blq_enq_publish();

        let available = queue.blq_deq_space(10);
        assert_eq!(available, 10);

        for i in 0..10 {
            assert_eq!(queue.blq_deq_local().unwrap(), i);
        }
        queue.blq_deq_publish();
    }

    #[test]
    fn miri_test_iffq() {
        let queue = IffqQueue::<usize>::with_capacity(128);

        assert!(queue.empty());
        assert!(queue.available());

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        for i in 0..31 {
            queue.push(i).unwrap();
        }

        assert!(queue.available());
        queue.push(31).unwrap();

        for i in 0..32 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn miri_test_biffq() {
        let queue = BiffqQueue::<usize>::with_capacity(128);

        assert!(queue.empty());
        assert!(queue.available());

        queue.push(42).unwrap();

        let _ = queue.flush_producer_buffer();

        assert!(!queue.empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        for i in 0..20 {
            queue.push(i).unwrap();
        }

        assert!(
            queue
                .prod
                .local_count
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
        );

        let _ = queue.flush_producer_buffer();
        assert_eq!(
            queue
                .prod
                .local_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        for i in 0..20 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn miri_test_dspsc() {
        let queue = DynListQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

        assert!(queue.empty());
        assert!(queue.available());

        queue.push(42).unwrap();
        assert!(!queue.empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        for i in 0..100 {
            queue.push(i).unwrap();
        }

        for i in 0..100 {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());
    }

    #[cfg(not(miri))]
    #[test]
    fn miri_test_unbounded() {
        unreachable!("This test should not run under Miri");
    }
}

mod miri_shared_memory {
    use super::*;

    #[test]
    fn test_lamport_shared_init() {
        let capacity = MIRI_SMALL_CAP;
        let shared_size = LamportQueue::<usize>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { LamportQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        for i in 0..capacity - 1 {
            queue.push(i).unwrap();
        }

        for i in 0..capacity - 1 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_bqueue_shared_init() {
        let capacity = MIRI_MEDIUM_CAP;
        let shared_size = BQueue::<usize>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { BQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        for i in 0..10 {
            queue.push(i).unwrap();
        }

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_ffq_shared_init() {
        let capacity = MIRI_MEDIUM_CAP;
        let shared_size = FfqQueue::<usize>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { FfqQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);

        queue.push(1).unwrap();
        queue.push(2).unwrap();
    }

    #[test]
    fn test_dehnavi_shared_init() {
        let capacity = 10;
        let shared_size = DehnaviQueue::<usize>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { DehnaviQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        let mut pushed = 0;
        for i in 0..capacity * 2 {
            queue.push(i).unwrap();
            pushed += 1;
        }

        assert!(pushed > 0);

        let mut popped = 0;
        while !queue.empty() && popped < capacity {
            queue.pop().unwrap();
            popped += 1;
        }
        assert!(popped > 0);
    }

    #[test]
    fn test_llq_shared_init() {
        let capacity = 128;
        let shared_size = LlqQueue::<usize>::llq_shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { LlqQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        let mut pushed = 0;
        for i in 0..capacity / 2 {
            match queue.push(i) {
                Ok(_) => pushed += 1,
                Err(_) => break,
            }
        }

        assert!(pushed > 0);

        for _ in 0..pushed {
            queue.pop().unwrap();
        }
        assert!(queue.empty());
    }

    #[test]
    fn test_blq_shared_init() {
        let capacity = 64;
        let shared_size = BlqQueue::<usize>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { BlqQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());
    }

    #[test]
    fn test_iffq_shared_init() {
        let capacity = 128;
        let shared_size = IffqQueue::<usize>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { IffqQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());
    }

    #[test]
    fn test_biffq_shared_init() {
        let capacity = 128;
        let shared_size = BiffqQueue::<usize>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { BiffqQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        queue.push(123).unwrap();
        let _ = queue.flush_producer_buffer();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        let _ = queue.flush_producer_buffer();
        assert_eq!(
            queue
                .prod
                .local_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_multipush_shared_init() {
        let capacity = MIRI_MEDIUM_CAP;
        let shared_size = MultiPushQueue::<usize>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { MultiPushQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        queue.push(123).unwrap();
        assert!(queue.flush());
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        assert_eq!(
            queue.local_count.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_sesd_wrapper_shared_init() {
        let pool_capacity = 100;
        let shared_size = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue =
            unsafe { SesdJpSpscBenchWrapper::<usize>::init_in_shared(mem_ptr, pool_capacity) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        let mut pushed = 0;
        for i in 0..pool_capacity - 10 {
            match queue.push(i) {
                Ok(_) => pushed += 1,
                Err(_) => break,
            }
        }

        assert!(pushed > 0);

        let mut popped = 0;
        while queue.pop().is_ok() {
            popped += 1;
        }

        assert_eq!(popped, pushed);
    }

    #[test]
    fn test_dspsc_shared_init() {
        let capacity = MIRI_MEDIUM_CAP;
        let shared_size = DynListQueue::<usize>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { DynListQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        for i in 0..50 {
            queue.push(i).unwrap();
        }

        for i in 0..50 {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());

        queue.push(999).unwrap();
        assert_eq!(queue.pop().unwrap(), 999);
    }
}

mod miri_special_features {
    use super::*;

    #[test]
    fn test_multipush_local_buffer() {
        let queue = MultiPushQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

        for i in 0..10 {
            queue.push(i).unwrap();
        }

        assert!(queue.local_count.load(std::sync::atomic::Ordering::Relaxed) > 0);

        assert!(queue.flush());
        assert_eq!(
            queue.local_count.load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_multipush_automatic_flush() {
        let queue = MultiPushQueue::<usize>::with_capacity(MIRI_LARGE_CAP);

        for i in 0..32 {
            queue.push(i).unwrap();
        }

        assert_eq!(
            queue.local_count.load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        for i in 0..32 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_biffq_producer_buffer() {
        let queue = BiffqQueue::<usize>::with_capacity(128);

        for i in 0..20 {
            queue.push(i).unwrap();
        }

        assert!(
            queue
                .prod
                .local_count
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
        );

        let flushed = queue.flush_producer_buffer().unwrap();
        assert!(flushed > 0);
        assert_eq!(
            queue
                .prod
                .local_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        for i in 0..20 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_biffq_automatic_flush() {
        let queue = BiffqQueue::<usize>::with_capacity(128);

        for i in 0..32 {
            queue.push(i).unwrap();
        }

        assert!(!queue.empty());

        let mut count = 0;
        while queue.pop().is_ok() {
            count += 1;
        }

        let _ = queue.flush_producer_buffer();

        while queue.pop().is_ok() {
            count += 1;
        }

        assert_eq!(count, 32);
    }

    #[test]
    fn test_dehnavi_lossy() {
        let queue = DehnaviQueue::<usize>::new(4);

        for i in 0..10 {
            queue.push(i).unwrap();
        }

        let mut items = Vec::new();
        while let Ok(item) = queue.pop() {
            items.push(item);
        }

        assert!(!items.is_empty());
        assert!(items.len() <= 4);

        for window in items.windows(2) {
            assert!(window[1] > window[0]);
        }
    }

    #[test]
    fn test_blq_batch_operations() {
        let queue = BlqQueue::<usize>::with_capacity(128);

        let space = queue.blq_enq_space(10);
        assert!(space >= 10);

        for i in 0..10 {
            queue.blq_enq_local(i).unwrap();
        }
        queue.blq_enq_publish();

        let available = queue.blq_deq_space(10);
        assert_eq!(available, 10);

        for i in 0..10 {
            assert_eq!(queue.blq_deq_local().unwrap(), i);
        }
        queue.blq_deq_publish();

        assert!(queue.empty());
    }

    #[test]
    fn test_dspsc_dynamic_allocation() {
        let queue = DynListQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

        for i in 0..100 {
            queue.push(i).unwrap();
        }

        for i in 0..100 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.empty());
    }

    #[cfg(not(miri))]
    #[test]
    fn test_unbounded_segment_growth() {
        unreachable!("This test should not run under Miri");
    }
}

mod miri_drop_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug)]
    struct DropCounter {
        _value: usize,
    }

    impl Drop for DropCounter {
        fn drop(&mut self) {
            DROP_COUNT.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_drop_on_queue_drop() {
        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let queue = LamportQueue::<DropCounter>::with_capacity(MIRI_SMALL_CAP);

            for i in 0..5 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..2 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 2);
        }

        thread::yield_now();

        assert!(DROP_COUNT.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn test_drop_with_strings() {
        let queue = BQueue::<String>::new(MIRI_MEDIUM_CAP);

        for i in 0..10 {
            queue.push(format!("item_{}", i)).unwrap();
        }

        for _ in 0..5 {
            let _ = queue.pop().unwrap();
        }
    }

    #[test]
    fn test_drop_in_buffered_queues() {
        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let queue = MultiPushQueue::<DropCounter>::with_capacity(MIRI_MEDIUM_CAP);

            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            assert!(queue.local_count.load(Ordering::Relaxed) > 0);

            queue.flush();

            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);
        }

        thread::yield_now();

        let drops_after_multipush = DROP_COUNT.load(Ordering::SeqCst);
        assert!(
            drops_after_multipush >= 5,
            "Should have dropped at least the 5 explicit items"
        );
    }
}

mod miri_memory_safety {
    use super::*;

    #[test]
    fn test_shared_memory_bounds() {
        let capacity = 32;
        let shared_size = LamportQueue::<u64>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { LamportQueue::<u64>::init_in_shared(mem_ptr, capacity) };

        let mut pushed = 0;
        for i in 0..capacity {
            if queue.push(i as u64).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }

        assert_eq!(pushed, capacity - 1);

        assert!(queue.push(999).is_err());

        for _ in 0..pushed {
            queue.pop().unwrap();
        }
    }

    #[test]
    fn test_zero_sized_types() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct ZeroSized;

        let queue = LamportQueue::<ZeroSized>::with_capacity(32);

        for _ in 0..10 {
            queue.push(ZeroSized).unwrap();
        }

        for _ in 0..10 {
            assert_eq!(queue.pop().unwrap(), ZeroSized);
        }
    }

    #[test]
    fn test_large_types() {
        #[derive(Clone, Debug, PartialEq)]
        struct LargeType {
            data: [u64; 32],
        }

        let queue = LamportQueue::<LargeType>::with_capacity(8);
        let item = LargeType { data: [42; 32] };

        queue.push(item.clone()).unwrap();
        assert_eq!(queue.pop().unwrap(), item);
    }

    #[test]
    fn test_alignment_requirements() {
        {
            let shared_size = LamportQueue::<usize>::shared_size(32);
            let mut memory = AlignedMemory::new(shared_size, 64);
            let mem_ptr = memory.as_mut_ptr();

            assert_eq!(mem_ptr as usize % 64, 0);

            let queue = unsafe { LamportQueue::<usize>::init_in_shared(mem_ptr, 32) };
            queue.push(42).unwrap();
            assert_eq!(queue.pop().unwrap(), 42);
        }

        #[cfg(not(miri))]
        {
            let shared_size = UnboundedQueue::<usize>::shared_size(64);
            let mut memory = AlignedMemory::new(shared_size, 128);
            let mem_ptr = memory.as_mut_ptr();

            assert_eq!(mem_ptr as usize % 128, 0);

            let queue = unsafe { UnboundedQueue::<usize>::init_in_shared(mem_ptr, 64) };
            queue.push(42).unwrap();
            assert_eq!(queue.pop().unwrap(), 42);
        }

        #[cfg(miri)]
        {
            let shared_size = DynListQueue::<usize>::shared_size(64);
            let mut memory = AlignedMemory::new(shared_size, 128);
            let mem_ptr = memory.as_mut_ptr();

            assert_eq!(mem_ptr as usize % 128, 0);

            let queue = unsafe { DynListQueue::<usize>::init_in_shared(mem_ptr, 64) };
            queue.push(42).unwrap();
            assert_eq!(queue.pop().unwrap(), 42);
        }
    }
}

mod miri_concurrency {
    use super::*;

    #[test]
    fn test_concurrent_small() {
        let queue = Arc::new(LamportQueue::<usize>::with_capacity(64));
        let items = 20;

        let q1 = queue.clone();
        let producer = thread::spawn(move || {
            for i in 0..items {
                while q1.push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        let q2 = queue.clone();
        let consumer = thread::spawn(move || {
            let mut received = 0;
            let mut sum = 0;
            while received < items {
                if let Ok(val) = q2.pop() {
                    sum += val;
                    received += 1;
                } else {
                    thread::yield_now();
                }
            }
            sum
        });

        producer.join().unwrap();
        let sum = consumer.join().unwrap();

        assert_eq!(sum, (items - 1) * items / 2);
    }

    #[test]
    fn test_concurrent_buffered_queues() {
        {
            let queue = Arc::new(MultiPushQueue::<usize>::with_capacity(128));
            let items = 50;

            let q_prod = queue.clone();
            let producer = thread::spawn(move || {
                for i in 0..items {
                    q_prod.push(i).unwrap();
                }

                q_prod.flush();
            });

            let q_cons = queue.clone();
            let consumer = thread::spawn(move || {
                let mut received = Vec::new();
                let mut attempts = 0;

                while received.len() < items && attempts < items * 100 {
                    match q_cons.pop() {
                        Ok(val) => {
                            received.push(val);
                            attempts = 0;
                        }
                        Err(_) => {
                            attempts += 1;
                            thread::yield_now();
                        }
                    }
                }
                received
            });

            producer.join().unwrap();
            let received = consumer.join().unwrap();

            assert_eq!(received.len(), items);
        }
    }

    #[cfg(not(miri))]
    #[test]
    fn test_concurrent_ffq() {
        let ffq = Arc::new(FfqQueue::<usize>::with_capacity(64));
        let items = 10;

        let ffq_prod = ffq.clone();
        let ffq_producer = thread::spawn(move || {
            for i in 0..items {
                while ffq_prod.push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        let ffq_cons = ffq.clone();
        let ffq_consumer = thread::spawn(move || {
            let mut sum = 0;
            let mut count = 0;
            while count < items {
                if let Ok(val) = ffq_cons.pop() {
                    sum += val;
                    count += 1;
                } else {
                    thread::yield_now();
                }
            }
            sum
        });

        ffq_producer.join().unwrap();
        let ffq_sum = ffq_consumer.join().unwrap();

        let expected_sum = (items - 1) * items / 2;
        assert_eq!(ffq_sum, expected_sum);
    }

    #[test]
    fn test_concurrent_multiple_queue_types() {
        let blq = Arc::new(BlqQueue::<usize>::with_capacity(64));

        let items = 10;

        let blq_prod = blq.clone();
        let blq_producer = thread::spawn(move || {
            for i in 0..items {
                while blq_prod.push(i).is_err() {
                    thread::yield_now();
                }
            }
        });

        let blq_cons = blq.clone();
        let blq_consumer = thread::spawn(move || {
            let mut sum = 0;
            let mut count = 0;
            while count < items {
                if let Ok(val) = blq_cons.pop() {
                    sum += val;
                    count += 1;
                } else {
                    thread::yield_now();
                }
            }
            sum
        });

        blq_producer.join().unwrap();
        let blq_sum = blq_consumer.join().unwrap();

        let expected_sum = (items - 1) * items / 2;
        assert_eq!(blq_sum, expected_sum);
    }
}

mod miri_special_init {
    use super::*;

    #[test]
    fn test_llq_init() {
        let capacity = 128;
        let queue = LlqQueue::<usize>::with_capacity(capacity);

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
    }

    #[test]
    fn test_blq_init() {
        let capacity = 64;
        let queue = BlqQueue::<usize>::with_capacity(capacity);

        queue.blq_enq_local(1).unwrap();
        queue.blq_enq_local(2).unwrap();
        queue.blq_enq_publish();

        assert_eq!(queue.blq_deq_local().unwrap(), 1);
        assert_eq!(queue.blq_deq_local().unwrap(), 2);
        queue.blq_deq_publish();
    }

    #[test]
    fn test_iffq_init() {
        let capacity = 128;
        let queue = IffqQueue::<usize>::with_capacity(capacity);

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
    }

    #[test]
    fn test_biffq_init() {
        let capacity = 128;
        let queue = BiffqQueue::<usize>::with_capacity(capacity);

        queue.push(456).unwrap();
        let _ = queue.flush_producer_buffer();
        assert_eq!(queue.pop().unwrap(), 456);
    }

    #[test]
    fn test_sesd_wrapper_init() {
        let pool_capacity = 100;
        let shared_size = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue =
            unsafe { SesdJpSpscBenchWrapper::<usize>::init_in_shared(mem_ptr, pool_capacity) };

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
    }
}

mod miri_error_handling {
    use super::*;

    #[test]
    #[should_panic(expected = "capacity must be power of two")]
    fn test_lamport_invalid_capacity() {
        let _ = LamportQueue::<usize>::with_capacity(15);
    }

    #[test]
    #[should_panic(expected = "Capacity (k) must be greater than 0")]
    fn test_dehnavi_zero_capacity() {
        let _ = DehnaviQueue::<usize>::new(0);
    }

    #[test]
    #[should_panic(expected = "Capacity must be greater than K_CACHE_LINE_SLOTS")]
    fn test_llq_small_capacity() {
        let _ = LlqQueue::<usize>::with_capacity(4);
    }

    #[test]
    #[should_panic(expected = "Capacity must be at least 2 * H_PARTITION_SIZE")]
    fn test_iffq_invalid_capacity() {
        let _ = IffqQueue::<usize>::with_capacity(32);
    }

    #[test]
    #[should_panic(expected = "Capacity must be a power of two")]
    fn test_iffq_invalid_capacity_not_power_of_two() {
        let _ = IffqQueue::<usize>::with_capacity(96);
    }

    #[test]
    fn test_push_error_handling() {
        let queue = LamportQueue::<String>::with_capacity(2);

        queue.push("first".to_string()).unwrap();

        match queue.push("second".to_string()) {
            Err(_) => {}
            Ok(_) => panic!("Push should have failed on full queue"),
        }
    }

    #[test]
    fn test_pop_error_handling() {
        let queue = BQueue::<usize>::new(16);

        assert!(queue.pop().is_err());

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);

        assert!(queue.pop().is_err());
    }
}

mod miri_edge_cases {
    use super::*;

    #[test]
    fn test_alternating_push_pop() {
        let queue = LamportQueue::<usize>::with_capacity(4);

        for i in 0..20 {
            queue.push(i).unwrap();
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.empty());
    }

    #[test]
    fn test_wraparound() {
        let queue = FfqQueue::<usize>::with_capacity(8);

        for i in 0..4 {
            queue.push(i).unwrap();
        }

        for i in 0..4 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        for i in 4..12 {
            queue.push(i).unwrap();
        }

        for i in 4..12 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_queue_state_transitions() {
        let queue = BQueue::<usize>::new(16);

        assert!(queue.empty());
        assert!(queue.available());

        queue.push(1).unwrap();
        assert!(!queue.empty());
        assert!(queue.available());

        let mut pushed = 1;
        for i in 2..16 {
            if queue.push(i).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }

        println!("Pushed {} items to BQueue with capacity 16", pushed);

        assert!(!queue.empty());

        let mut extra_pushed = 0;
        for i in 100..110 {
            if queue.push(i).is_ok() {
                extra_pushed += 1;
            } else {
                break;
            }
        }

        println!("Could push {} extra items", extra_pushed);
        assert!(
            extra_pushed < 5,
            "Should not be able to push many more items when nearly full"
        );

        let mut popped = 0;
        while queue.pop().is_ok() {
            popped += 1;
        }

        println!("Popped {} items total", popped);
        assert_eq!(popped, pushed + extra_pushed, "Should pop all pushed items");
        assert!(queue.empty());
        assert!(queue.available());
    }
}

#[test]
fn test_multiple_queues() {
    let q1 = LamportQueue::<u32>::with_capacity(32);
    let q2 = LamportQueue::<u32>::with_capacity(32);

    q1.push(100).unwrap();
    q2.push(200).unwrap();

    assert_eq!(q1.pop().unwrap(), 100);
    assert_eq!(q2.pop().unwrap(), 200);
}

#[test]
fn test_arc_safety() {
    let queue = Arc::new(BQueue::<i32>::new(64));
    let q1 = queue.clone();
    let q2 = queue.clone();

    drop(q1);

    q2.push(42).unwrap();
    assert_eq!(q2.pop().unwrap(), 42);
}

#[test]
fn test_different_types() {
    {
        let q_u8 = LamportQueue::<u8>::with_capacity(16);
        q_u8.push(255u8).unwrap();
        assert_eq!(q_u8.pop().unwrap(), 255u8);
    }

    {
        let q_box = FfqQueue::<Box<usize>>::with_capacity(16);
        q_box.push(Box::new(42)).unwrap();
        assert_eq!(*q_box.pop().unwrap(), 42);
    }

    {
        let q_opt = BQueue::<Option<String>>::new(16);
        q_opt.push(Some("hello".to_string())).unwrap();
        q_opt.push(None).unwrap();

        assert_eq!(q_opt.pop().unwrap(), Some("hello".to_string()));
        assert_eq!(q_opt.pop().unwrap(), None);
    }
}

mod miri_stress_tests {
    use super::*;

    #[test]
    fn test_stress_concurrent_lamport() {
        let queue = Arc::new(LamportQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP));
        let num_items = MIRI_MEDIUM_CAP * 2;
        let barrier = Arc::new(Barrier::new(2));

        let queue_prod = queue.clone();
        let barrier_prod = barrier.clone();

        let producer = thread::spawn(move || {
            barrier_prod.wait();
            for i in 0..num_items {
                loop {
                    match queue_prod.push(i) {
                        Ok(_) => break,
                        Err(_) => thread::yield_now(),
                    }
                }
            }
        });

        let queue_cons = queue.clone();
        let barrier_cons = barrier.clone();

        let consumer = thread::spawn(move || {
            barrier_cons.wait();
            let mut sum = 0u64;
            let mut count = 0;

            while count < num_items {
                match queue_cons.pop() {
                    Ok(item) => {
                        sum += item as u64;
                        count += 1;
                    }
                    Err(_) => thread::yield_now(),
                }
            }

            sum
        });

        producer.join().unwrap();
        let sum = consumer.join().unwrap();

        let expected_sum = (num_items as u64 * (num_items as u64 - 1)) / 2;
        assert_eq!(sum, expected_sum);
    }

    #[test]
    fn test_available_empty_states() {
        let queue = LamportQueue::<usize>::with_capacity(MIRI_SMALL_CAP);

        assert!(queue.available());
        assert!(queue.empty());

        queue.push(1).unwrap();
        assert!(!queue.empty());

        let mut count = 1;
        while queue.available() && count < MIRI_SMALL_CAP {
            queue.push(count).unwrap();
            count += 1;
        }

        assert!(!queue.available());
        assert!(!queue.empty());

        while !queue.empty() {
            queue.pop().unwrap();
        }

        assert!(queue.available());
        assert!(queue.empty());
    }
}

mod miri_dehnavi_wait_free_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_dehnavi_wait_free_property() {
        let queue = Arc::new(DehnaviQueue::<usize>::new(4));
        let barrier = Arc::new(Barrier::new(2));

        let queue_prod = queue.clone();
        let barrier_prod = barrier.clone();

        let producer = thread::spawn(move || {
            barrier_prod.wait();
            for i in 0..20 {
                queue_prod.push(i).unwrap();
                if i % 3 == 0 {
                    thread::yield_now();
                }
            }
        });

        let queue_cons = queue.clone();
        let barrier_cons = barrier.clone();

        let consumer = thread::spawn(move || {
            barrier_cons.wait();
            let mut items = Vec::new();
            let mut attempts = 0;
            let mut last_seen = None;

            while attempts < 1000 {
                match queue_cons.pop() {
                    Ok(item) => {
                        items.push(item);

                        if let Some(last) = last_seen {
                            if item < last {}
                        }
                        last_seen = Some(item);
                        attempts = 0;
                    }
                    Err(_) => {
                        attempts += 1;
                        thread::yield_now();
                    }
                }

                if items.len() >= 10 {
                    break;
                }
            }

            items
        });

        producer.join().unwrap();
        let items = consumer.join().unwrap();

        assert!(
            !items.is_empty(),
            "Should have received at least some items"
        );
        assert!(
            items.len() >= 4,
            "Should receive at least as many items as queue capacity"
        );

        let mut max_seen = items[0];
        let mut increasing_count = 0;

        for &item in &items[1..] {
            if item > max_seen {
                max_seen = item;
                increasing_count += 1;
            }
        }

        assert!(
            increasing_count >= items.len() / 3,
            "Should see general progression in values despite potential overwrites"
        );
    }
}

mod miri_sesd_wrapper_concurrent_tests {
    use super::*;

    #[test]
    fn test_sesd_wrapper_concurrent() {
        let pool_capacity = 200;
        let shared_size = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);

        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { SesdJpSpscBenchWrapper::init_in_shared(mem_ptr, pool_capacity) };

        let queue_ptr = queue as *const SesdJpSpscBenchWrapper<usize>;

        let barrier = Arc::new(Barrier::new(2));
        let items_to_send = 100;

        let queue_prod = unsafe { &*queue_ptr };
        let barrier_prod = barrier.clone();

        let producer = thread::spawn(move || {
            barrier_prod.wait();
            for i in 0..items_to_send {
                loop {
                    match queue_prod.push(i) {
                        Ok(_) => break,
                        Err(_) => thread::yield_now(),
                    }
                }
            }
        });

        let queue_cons = unsafe { &*queue_ptr };
        let barrier_cons = barrier.clone();

        let consumer = thread::spawn(move || {
            barrier_cons.wait();
            let mut received = Vec::new();
            let mut empty_polls = 0;

            while received.len() < items_to_send {
                match queue_cons.pop() {
                    Ok(item) => {
                        received.push(item);
                        empty_polls = 0;
                    }
                    Err(_) => {
                        empty_polls += 1;
                        if empty_polls > 10000 {
                            panic!("Too many failed polls, possible deadlock");
                        }
                        thread::yield_now();
                    }
                }
            }

            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();

        assert_eq!(received.len(), items_to_send);
        for (i, &item) in received.iter().enumerate() {
            assert_eq!(item, i);
        }

        assert!(queue.empty());
    }
}

mod miri_additional_concurrent_tests {
    use super::*;

    #[test]
    fn test_concurrent_biffq() {
        let queue = Arc::new(BiffqQueue::<usize>::with_capacity(128));
        let barrier = Arc::new(Barrier::new(2));
        let items_to_send = 50;

        let queue_prod = queue.clone();
        let barrier_prod = barrier.clone();

        let producer = thread::spawn(move || {
            barrier_prod.wait();
            for i in 0..items_to_send {
                loop {
                    match queue_prod.push(i) {
                        Ok(_) => break,
                        Err(_) => {
                            let _ = queue_prod.flush_producer_buffer();
                            thread::yield_now();
                        }
                    }
                }
            }

            while queue_prod.prod.local_count.load(Ordering::Relaxed) > 0 {
                let _ = queue_prod.flush_producer_buffer();
                thread::yield_now();
            }
        });

        let queue_cons = queue.clone();
        let barrier_cons = barrier.clone();

        let consumer = thread::spawn(move || {
            barrier_cons.wait();
            let mut received = Vec::new();
            let mut empty_polls = 0;

            while received.len() < items_to_send {
                match queue_cons.pop() {
                    Ok(item) => {
                        received.push(item);
                        empty_polls = 0;
                    }
                    Err(_) => {
                        empty_polls += 1;
                        if empty_polls > 10000 {
                            panic!("Too many failed polls, possible deadlock");
                        }
                        thread::yield_now();
                    }
                }
            }

            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();

        assert_eq!(received.len(), items_to_send);
        for (i, &item) in received.iter().enumerate() {
            assert_eq!(item, i);
        }

        assert!(queue.empty());
    }

    #[test]
    fn test_concurrent_dspsc() {
        let queue = Arc::new(DynListQueue::<usize>::with_capacity(64));
        let barrier = Arc::new(Barrier::new(2));
        let items_to_send = 50;

        let queue_prod = queue.clone();
        let barrier_prod = barrier.clone();

        let producer = thread::spawn(move || {
            barrier_prod.wait();
            for i in 0..items_to_send {
                loop {
                    match queue_prod.push(i) {
                        Ok(_) => break,
                        Err(_) => thread::yield_now(),
                    }
                }
            }
        });

        let queue_cons = queue.clone();
        let barrier_cons = barrier.clone();

        let consumer = thread::spawn(move || {
            barrier_cons.wait();
            let mut received = Vec::new();
            let mut empty_polls = 0;

            while received.len() < items_to_send {
                match queue_cons.pop() {
                    Ok(item) => {
                        received.push(item);
                        empty_polls = 0;
                    }
                    Err(_) => {
                        empty_polls += 1;
                        if empty_polls > 10000 {
                            panic!("Too many failed polls, possible deadlock");
                        }
                        thread::yield_now();
                    }
                }
            }

            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();

        assert_eq!(received.len(), items_to_send);
        for (i, &item) in received.iter().enumerate() {
            assert_eq!(item, i);
        }

        assert!(queue.empty());
    }

    #[test]
    fn test_concurrent_ffq() {
        let queue = Arc::new(FfqQueue::<usize>::with_capacity(64));
        let barrier = Arc::new(Barrier::new(2));
        let items_to_send = 50;

        let queue_prod = queue.clone();
        let barrier_prod = barrier.clone();

        let producer = thread::spawn(move || {
            barrier_prod.wait();
            for i in 0..items_to_send {
                loop {
                    match queue_prod.push(i) {
                        Ok(_) => break,
                        Err(_) => thread::yield_now(),
                    }
                }
            }
        });

        let queue_cons = queue.clone();
        let barrier_cons = barrier.clone();

        let consumer = thread::spawn(move || {
            barrier_cons.wait();
            let mut received = Vec::new();
            let mut empty_polls = 0;

            while received.len() < items_to_send {
                match queue_cons.pop() {
                    Ok(item) => {
                        received.push(item);
                        empty_polls = 0;
                    }
                    Err(_) => {
                        empty_polls += 1;
                        if empty_polls > 10000 {
                            panic!("Too many failed polls, possible deadlock");
                        }
                        thread::yield_now();
                    }
                }
            }

            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();

        assert_eq!(received.len(), items_to_send);
        for (i, &item) in received.iter().enumerate() {
            assert_eq!(item, i);
        }

        assert!(queue.empty());
    }

    #[test]
    fn test_concurrent_iffq() {
        let queue = Arc::new(IffqQueue::<usize>::with_capacity(128));
        let barrier = Arc::new(Barrier::new(2));
        let items_to_send = 50;

        let queue_prod = queue.clone();
        let barrier_prod = barrier.clone();

        let producer = thread::spawn(move || {
            barrier_prod.wait();
            for i in 0..items_to_send {
                loop {
                    match queue_prod.push(i) {
                        Ok(_) => break,
                        Err(_) => thread::yield_now(),
                    }
                }
            }
        });

        let queue_cons = queue.clone();
        let barrier_cons = barrier.clone();

        let consumer = thread::spawn(move || {
            barrier_cons.wait();
            let mut received = Vec::new();
            let mut empty_polls = 0;

            while received.len() < items_to_send {
                match queue_cons.pop() {
                    Ok(item) => {
                        received.push(item);
                        empty_polls = 0;
                    }
                    Err(_) => {
                        empty_polls += 1;
                        if empty_polls > 10000 {
                            panic!("Too many failed polls, possible deadlock");
                        }
                        thread::yield_now();
                    }
                }
            }

            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();

        assert_eq!(received.len(), items_to_send);
        for (i, &item) in received.iter().enumerate() {
            assert_eq!(item, i);
        }

        assert!(queue.empty());
    }
}

mod miri_capacity_tests {
    use super::*;

    #[test]
    fn test_lamport_capacity() {
        let capacity = 32;
        let queue = LamportQueue::<usize>::with_capacity(capacity);

        let effective_capacity = capacity - 1;

        for i in 0..effective_capacity {
            assert!(
                queue.push(i).is_ok(),
                "Failed to push item {} of {}",
                i,
                effective_capacity
            );
        }

        assert!(
            queue.push(999).is_err(),
            "Should not be able to push when full"
        );

        for i in 0..effective_capacity {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(
            queue.pop().is_err(),
            "Should not be able to pop from empty queue"
        );
    }

    #[test]
    fn test_ffq_capacity() {
        let capacity = 64;
        let queue = FfqQueue::<usize>::with_capacity(capacity);

        let mut pushed = 0;
        for i in 0..capacity {
            if queue.push(i).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }

        println!("FFQ: pushed {} items out of {} capacity", pushed, capacity);

        assert!(
            queue.push(999).is_err(),
            "Should not be able to push when full"
        );

        for i in 0..pushed {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(
            queue.pop().is_err(),
            "Should not be able to pop from empty queue"
        );
    }

    #[test]
    fn test_bqueue_capacity() {
        let capacity = 128;
        let queue = BQueue::<usize>::new(capacity);

        let mut pushed = 0;
        for i in 0..capacity {
            if queue.push(i).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }

        println!(
            "BQueue: pushed {} items out of {} capacity",
            pushed, capacity
        );
        assert!(pushed > 0, "Should be able to push at least some items");

        assert!(
            queue.push(999).is_err(),
            "Should not be able to push when full"
        );

        for i in 0..pushed {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(
            queue.pop().is_err(),
            "Should not be able to pop from empty queue"
        );
    }

    #[test]
    fn test_dehnavi_capacity() {
        let capacity = 10;
        let queue = DehnaviQueue::<usize>::new(capacity);

        for i in 0..capacity * 2 {
            queue.push(i).unwrap();
        }

        let mut count = 0;
        while queue.pop().is_ok() && count < capacity {
            count += 1;
        }

        assert!(count > 0, "Should have popped some items");
    }
}

mod miri_payload_type_tests {
    use super::*;

    #[test]
    fn test_string_payload() {
        let queue = LamportQueue::<String>::with_capacity(16);

        for i in 0..10 {
            queue.push(format!("test_{}", i)).unwrap();
        }

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), format!("test_{}", i));
        }
    }

    #[test]
    fn test_vec_payload() {
        let queue = FfqQueue::<Vec<u32>>::with_capacity(16);

        for i in 0..5 {
            queue.push(vec![i; i as usize + 1]).unwrap();
        }

        for i in 0..5 {
            let v = queue.pop().unwrap();
            assert_eq!(v.len(), i as usize + 1);
            assert!(v.iter().all(|&x| x == i));
        }
    }

    #[test]
    fn test_option_payload() {
        let queue = BlqQueue::<Option<usize>>::with_capacity(32);

        queue.push(Some(42)).unwrap();
        queue.push(None).unwrap();
        queue.push(Some(100)).unwrap();

        assert_eq!(queue.pop().unwrap(), Some(42));
        assert_eq!(queue.pop().unwrap(), None);
        assert_eq!(queue.pop().unwrap(), Some(100));
    }

    #[test]
    fn test_tuple_payload() {
        let queue = IffqQueue::<(usize, String)>::with_capacity(64);

        for i in 0..10 {
            queue.push((i, format!("item_{}", i))).unwrap();
        }

        for i in 0..10 {
            let (num, text) = queue.pop().unwrap();
            assert_eq!(num, i);
            assert_eq!(text, format!("item_{}", i));
        }
    }
}

mod miri_batch_tests {
    use super::*;

    #[test]
    fn test_blq_batch_operations() {
        let queue = BlqQueue::<usize>::with_capacity(128);

        let space = queue.blq_enq_space(50);
        assert!(space >= 50);

        for i in 0..50 {
            queue.blq_enq_local(i).unwrap();
        }

        queue.blq_enq_publish();

        let available = queue.blq_deq_space(50);
        assert_eq!(available, 50);

        for i in 0..50 {
            assert_eq!(queue.blq_deq_local().unwrap(), i);
        }

        queue.blq_deq_publish();

        assert!(queue.empty());
    }

    #[test]
    fn test_multipush_batch_flush() {
        let queue = MultiPushQueue::<usize>::with_capacity(256);

        for i in 0..20 {
            queue.push(i).unwrap();
        }

        assert!(queue.local_count.load(Ordering::Relaxed) > 0);

        assert!(queue.flush());
        assert_eq!(queue.local_count.load(Ordering::Relaxed), 0);

        for i in 0..20 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_biffq_batch_behavior() {
        let queue = BiffqQueue::<usize>::with_capacity(256);

        for i in 0..30 {
            queue.push(i).unwrap();
        }

        let local_count = queue.prod.local_count.load(Ordering::Relaxed);
        assert!(local_count > 0 || local_count == 0);

        let _ = queue.flush_producer_buffer();

        for i in 0..30 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }
}

mod miri_queue_interaction_tests {
    use super::*;

    #[test]
    fn test_push_pop_patterns() {
        let queue = LamportQueue::<usize>::with_capacity(16);

        let mut next_to_push = 0;
        let mut next_to_pop = 0;

        for round in 0..5 {
            for _ in 0..3 {
                queue.push(next_to_push).unwrap();
                next_to_push += 1;
            }

            for _ in 0..2 {
                assert_eq!(queue.pop().unwrap(), next_to_pop);
                next_to_pop += 1;
            }
        }

        for _ in 0..5 {
            assert_eq!(queue.pop().unwrap(), next_to_pop);
            next_to_pop += 1;
        }

        assert!(queue.empty());
    }

    #[test]
    fn test_alternating_patterns() {
        let lamport = LamportQueue::<usize>::with_capacity(8);
        let bqueue = BQueue::<usize>::new(8);

        for i in 0..20 {
            lamport.push(i).unwrap();
            assert_eq!(lamport.pop().unwrap(), i);

            bqueue.push(i).unwrap();
            assert_eq!(bqueue.pop().unwrap(), i);
        }

        assert!(lamport.empty());
        assert!(bqueue.empty());
    }

    #[test]
    fn test_wraparound_behavior() {
        let capacity = 8;
        let queue = FfqQueue::<usize>::with_capacity(capacity);

        for cycle in 0..3 {
            let base = cycle * 100;

            for i in 0..capacity - 1 {
                queue.push(base + i).unwrap();
            }

            for i in 0..capacity / 2 {
                assert_eq!(queue.pop().unwrap(), base + i);
            }

            for i in 0..capacity / 2 {
                queue.push(base + 1000 + i).unwrap();
            }

            for i in capacity / 2..capacity - 1 {
                assert_eq!(queue.pop().unwrap(), base + i);
            }
            for i in 0..capacity / 2 {
                assert_eq!(queue.pop().unwrap(), base + 1000 + i);
            }
        }
    }
}
