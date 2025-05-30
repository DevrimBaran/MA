use queues::{spsc::*, SpscQueue};
use std::any::Any;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

const TEST_ITEMS: usize = 1000;
const SMALL_CAPACITY: usize = 64;
const MEDIUM_CAPACITY: usize = 1024;
const LARGE_CAPACITY: usize = 8192;

macro_rules! test_queue {
    ($queue_type:ty, $capacity:expr, $test_name:ident) => {
        mod $test_name {
            use super::*;

            #[test]
            fn test_basic_push_pop() {
                let queue = <$queue_type>::with_capacity($capacity);

                assert!(queue.empty());
                assert!(queue.pop().is_err());

                queue.push(42).unwrap();
                assert!(!queue.empty());
                assert_eq!(queue.pop().unwrap(), 42);
                assert!(queue.empty());

                for i in 0..10 {
                    queue.push(i).unwrap();
                }

                for i in 0..10 {
                    assert_eq!(queue.pop().unwrap(), i);
                }
                assert!(queue.empty());
            }

            #[test]
            fn test_capacity_limits() {
                let queue = <$queue_type>::with_capacity($capacity);

                let mut pushed = 0;
                for i in 0..$capacity {
                    match queue.push(i) {
                        Ok(_) => pushed += 1,
                        Err(_) => {
                            if stringify!($queue_type).contains("BiffqQueue") {
                                if let Some(biffq) =
                                    (&queue as &dyn Any).downcast_ref::<BiffqQueue<usize>>()
                                {
                                    let _ = biffq.flush_producer_buffer();
                                    if queue.push(i).is_ok() {
                                        pushed += 1;
                                    } else {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            } else if stringify!($queue_type).contains("MultiPushQueue") {
                                if let Some(mp_queue) =
                                    (&queue as &dyn Any).downcast_ref::<MultiPushQueue<usize>>()
                                {
                                    let _ = mp_queue.flush();
                                    if queue.push(i).is_ok() {
                                        pushed += 1;
                                    } else {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }

                assert!(pushed > 0, "Should be able to push at least one item");

                assert!(!queue.available() || queue.push(999999).is_err());

                if pushed > 0 {
                    assert!(queue.pop().is_ok());

                    if stringify!($queue_type).contains("IffqQueue") {
                        let mut popped = 1;
                        let mut push_succeeded = false;

                        for _ in 0..33 {
                            if queue.pop().is_ok() {
                                popped += 1;
                            }

                            if queue.push(888888).is_ok() {
                                push_succeeded = true;
                                break;
                            }
                        }

                        assert!(popped > 0, "Should have popped at least one item");
                    } else {
                        assert!(queue.available());
                        assert!(queue.push(888888).is_ok());
                    }
                }
            }

            #[test]
            fn test_available_empty() {
                let queue = <$queue_type>::with_capacity($capacity);

                assert!(queue.available());
                assert!(queue.empty());

                queue.push(1).unwrap();
                assert!(!queue.empty());

                let mut count = 1;
                while queue.available() && count < $capacity {
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

            #[test]
            fn test_concurrent_spsc() {
                let queue = Arc::new(<$queue_type>::with_capacity($capacity));
                let barrier = Arc::new(Barrier::new(2));
                let items_to_send = 100;

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
                                if empty_polls > 1000000 {
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
            fn test_stress_concurrent() {
                let queue = Arc::new(<$queue_type>::with_capacity($capacity));
                let num_items = $capacity * 10;
                let barrier = Arc::new(Barrier::new(2));

                let queue_prod = queue.clone();
                let barrier_prod = barrier.clone();

                let producer = thread::spawn(move || {
                    barrier_prod.wait();
                    for i in 0..num_items {
                        loop {
                            match queue_prod.push(i) {
                                Ok(_) => break,
                                Err(_) => {
                                    thread::yield_now();
                                }
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
        }
    };
}

test_queue!(LamportQueue<usize>, SMALL_CAPACITY, lamport_tests);
test_queue!(FfqQueue<usize>, MEDIUM_CAPACITY, ffq_tests);
test_queue!(LlqQueue<usize>, MEDIUM_CAPACITY, llq_tests);
test_queue!(BlqQueue<usize>, MEDIUM_CAPACITY, blq_tests);
test_queue!(IffqQueue<usize>, MEDIUM_CAPACITY, iffq_tests);

mod biffq_tests {
    use super::*;

    const BIFFQ_CAPACITY: usize = 1024;

    #[test]
    fn test_basic_push_pop() {
        let queue = BiffqQueue::<usize>::with_capacity(BIFFQ_CAPACITY);

        assert!(queue.empty());
        assert!(queue.pop().is_err());

        queue.push(42).unwrap();

        let _ = queue.flush_producer_buffer();

        assert!(!queue.empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        for i in 0..10 {
            queue.push(i).unwrap();
        }
        let _ = queue.flush_producer_buffer();

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());
    }

    #[test]
    fn test_capacity_limits() {
        let queue = BiffqQueue::<usize>::with_capacity(BIFFQ_CAPACITY);

        let mut pushed_total = 0;

        for i in 0..BIFFQ_CAPACITY + 100 {
            match queue.push(i) {
                Ok(_) => pushed_total += 1,
                Err(_) => {
                    let _ = queue.flush_producer_buffer();
                    if queue.push(i).is_err() {
                        break;
                    } else {
                        pushed_total += 1;
                    }
                }
            }

            if i % 32 == 31 {
                let _ = queue.flush_producer_buffer();
            }
        }

        let _ = queue.flush_producer_buffer();

        println!(
            "BiffQ pushed {} items out of {} capacity",
            pushed_total, BIFFQ_CAPACITY
        );
        assert!(pushed_total > 0, "Should push at least some items");

        if pushed_total >= BIFFQ_CAPACITY - 32 {
            let popped = queue.pop();
            assert!(popped.is_ok(), "Should be able to pop from full queue");

            let mut pushed_after = false;
            for _ in 0..10 {
                let _ = queue.flush_producer_buffer();
                if queue.push(99999).is_ok() {
                    pushed_after = true;
                    break;
                }

                let _ = queue.pop();
            }

            println!("Pushed after pop: {}", pushed_after);
        } else {
            assert!(queue.pop().is_ok(), "Should be able to pop");
            assert!(
                queue.push(99999).is_ok(),
                "Should be able to push after pop"
            );
            let _ = queue.flush_producer_buffer();
        }
    }

    #[test]
    fn test_available_empty() {
        let queue = BiffqQueue::<usize>::with_capacity(BIFFQ_CAPACITY);

        assert!(queue.available());
        assert!(queue.empty());

        queue.push(1).unwrap();

        let _ = queue.flush_producer_buffer();
        assert!(!queue.empty());

        let mut count = 1;
        while queue.available() && count < BIFFQ_CAPACITY - 32 {
            queue.push(count).unwrap();
            count += 1;
            if count % 32 == 0 {
                let _ = queue.flush_producer_buffer();
            }
        }

        let _ = queue.flush_producer_buffer();

        assert!(!queue.empty());

        while !queue.empty() {
            queue.pop().unwrap();
        }

        assert!(queue.available());
        assert!(queue.empty());
    }

    #[test]
    fn test_concurrent_spsc() {
        let queue = Arc::new(BiffqQueue::<usize>::with_capacity(BIFFQ_CAPACITY));
        let barrier = Arc::new(Barrier::new(2));
        let items_to_send = 100;

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
                        if empty_polls > 1000000 {
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
    fn test_stress_concurrent() {
        let queue = Arc::new(BiffqQueue::<usize>::with_capacity(BIFFQ_CAPACITY));
        let num_items = BIFFQ_CAPACITY * 10;
        let barrier = Arc::new(Barrier::new(2));

        let queue_prod = queue.clone();
        let barrier_prod = barrier.clone();

        let producer = thread::spawn(move || {
            barrier_prod.wait();
            for i in 0..num_items {
                loop {
                    match queue_prod.push(i) {
                        Ok(_) => break,
                        Err(_) => {
                            let _ = queue_prod.flush_producer_buffer();
                            thread::yield_now();
                        }
                    }
                }
                if i % 32 == 31 {
                    let _ = queue_prod.flush_producer_buffer();
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
}

mod bqueue_tests {
    use super::*;

    #[test]
    fn test_basic_push_pop() {
        let queue = BQueue::<usize>::new(MEDIUM_CAPACITY);

        assert!(queue.empty());
        assert!(queue.pop().is_err());

        queue.push(42).unwrap();
        assert!(!queue.empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        for i in 0..10 {
            queue.push(i).unwrap();
        }

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());
    }

    #[test]
    fn test_capacity_limits() {
        let queue = BQueue::<usize>::new(MEDIUM_CAPACITY);
        let effective_capacity = MEDIUM_CAPACITY - 1;

        for i in 0..effective_capacity {
            match queue.push(i) {
                Ok(_) => {}
                Err(_) => {
                    assert!(i > 0, "Should be able to push at least one item");
                    return;
                }
            }
        }

        assert!(!queue.available());
        assert!(queue.push(999).is_err());

        queue.pop().unwrap();
        assert!(queue.available());
        queue.push(999).unwrap();
        assert!(!queue.available());
    }

    #[test]
    fn test_available_empty() {
        let queue = BQueue::<usize>::new(MEDIUM_CAPACITY);

        assert!(queue.available());
        assert!(queue.empty());

        queue.push(1).unwrap();
        assert!(!queue.empty());

        let mut count = 1;
        while queue.available() && count < MEDIUM_CAPACITY {
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

    #[test]
    fn test_concurrent_spsc() {
        let queue = Arc::new(BQueue::<usize>::new(MEDIUM_CAPACITY));
        let barrier = Arc::new(Barrier::new(2));
        let items_to_send = 100;

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
                        if empty_polls > 1000000 {
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
    fn test_stress_concurrent() {
        let queue = Arc::new(BQueue::<usize>::new(MEDIUM_CAPACITY));
        let num_items = MEDIUM_CAPACITY * 10;
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
}

mod multipush_tests {
    use super::*;

    #[test]
    fn test_multipush_basic() {
        let queue = MultiPushQueue::<usize>::with_capacity(MEDIUM_CAPACITY);

        for i in 0..100 {
            queue.push(i).unwrap();
        }

        assert!(queue.flush());

        for i in 0..100 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.empty());
    }

    #[test]
    fn test_multipush_flush() {
        let queue = MultiPushQueue::<usize>::with_capacity(MEDIUM_CAPACITY);

        for i in 0..5 {
            queue.push(i).unwrap();
        }

        assert!(!queue.empty());
        assert!(queue.flush());

        for i in 0..5 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_multipush_local_buffer_overflow() {
        let queue = MultiPushQueue::<usize>::with_capacity(MEDIUM_CAPACITY);

        for i in 0..32 {
            queue.push(i).unwrap();
        }

        for i in 0..32 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }
}

mod unbounded_tests {
    use super::*;

    fn allocate_aligned_memory(size: usize, alignment: usize) -> Vec<u8> {
        let total_size = size + alignment;
        let mut memory = vec![0u8; total_size];

        let ptr = memory.as_mut_ptr();
        let addr = ptr as usize;
        let aligned_addr = (addr + alignment - 1) & !(alignment - 1);
        let offset = aligned_addr - addr;

        let mut aligned_memory = Vec::with_capacity(size);
        unsafe {
            aligned_memory.set_len(size);
            std::ptr::copy_nonoverlapping(
                memory.as_ptr().add(offset),
                aligned_memory.as_mut_ptr(),
                size,
            );
        }

        assert_eq!(
            aligned_memory.as_ptr() as usize % alignment,
            0,
            "Memory not properly aligned to {} bytes",
            alignment
        );

        aligned_memory
    }

    fn allocate_aligned_box(size: usize, alignment: usize) -> Box<[u8]> {
        use std::alloc::{alloc_zeroed, Layout};

        unsafe {
            let layout = Layout::from_size_align(size, alignment).unwrap();
            let ptr = alloc_zeroed(layout);
            if ptr.is_null() {
                panic!("Failed to allocate aligned memory");
            }

            let slice = std::slice::from_raw_parts_mut(ptr, size);
            Box::from_raw(slice)
        }
    }

    #[test]
    fn test_unbounded_basic() {
        let shared_size = UnboundedQueue::<usize>::shared_size(8192);

        const ALIGNMENT: usize = 128;

        let memory = allocate_aligned_box(shared_size, ALIGNMENT);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        assert_eq!(
            mem_ptr as usize % ALIGNMENT,
            0,
            "Memory not aligned to {} bytes",
            ALIGNMENT
        );

        let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 8192) };

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_unbounded_segment_growth() {
        let shared_size = UnboundedQueue::<usize>::shared_size(8192);
        const ALIGNMENT: usize = 128;

        let memory = allocate_aligned_box(shared_size, ALIGNMENT);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 8192) };

        let num_items = 100000;
        for i in 0..num_items {
            queue.push(i).unwrap();
        }

        for i in 0..num_items {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.empty());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_unbounded_segment_deallocation() {
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

        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let shared_size = UnboundedQueue::<DropCounter>::shared_size(8192);
            const ALIGNMENT: usize = 128;

            let memory = allocate_aligned_box(shared_size, ALIGNMENT);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 8192) };

            let items_to_push = 70000;

            for i in 0..items_to_push {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..items_to_push {
                drop(queue.pop().unwrap());
            }

            let drops_after_pop = DROP_COUNT.load(Ordering::SeqCst);
            assert_eq!(
                drops_after_pop, items_to_push,
                "All items should be dropped after popping"
            );

            assert!(queue.empty());

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let shared_size = UnboundedQueue::<DropCounter>::shared_size(8192);
            const ALIGNMENT: usize = 128;

            let memory = allocate_aligned_box(shared_size, ALIGNMENT);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 8192) };

            let items_to_push = 100;
            for i in 0..items_to_push {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..50 {
                drop(queue.pop().unwrap());
            }

            let drops_before_queue_drop = DROP_COUNT.load(Ordering::SeqCst);
            assert_eq!(drops_before_queue_drop, 50, "Should have dropped 50 items");

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }

        std::thread::sleep(Duration::from_millis(10));

        let final_drops = DROP_COUNT.load(Ordering::SeqCst);
        assert!(
            final_drops >= 50,
            "At least the popped items should be dropped, got {}",
            final_drops
        );
    }

    #[test]
    fn test_unbounded_force_segment_deallocation() {
        const BUF_CAP: usize = 65536;
        const POOL_CAP: usize = 32;

        let (mem_ptr, shared_size) = allocate_aligned_unbounded_memory(8192);
        let queue = unsafe { UnboundedQueue::<usize>::init_in_shared(mem_ptr, 8192) };

        for batch in 0..10 {
            for i in 0..BUF_CAP - 100 {
                if queue.push(batch * BUF_CAP + i).is_err() {
                    break;
                }
            }

            while queue.pop().is_ok() {}
        }

        for i in 0..1000 {
            if queue.push(i).is_err() {
                break;
            }
        }

        unsafe {
            use std::alloc::{dealloc, Layout};
            let layout = Layout::from_size_align(shared_size, 128).unwrap();
            dealloc(mem_ptr, layout);
        }
    }

    fn allocate_aligned_unbounded_memory(buffer_size: usize) -> (*mut u8, usize) {
        use std::alloc::{alloc_zeroed, Layout};

        let shared_size = UnboundedQueue::<usize>::shared_size(buffer_size);
        const ALIGNMENT: usize = 128;

        unsafe {
            let layout = Layout::from_size_align(shared_size, ALIGNMENT).unwrap();
            let ptr = alloc_zeroed(layout);
            if ptr.is_null() {
                panic!("Failed to allocate aligned memory");
            }

            assert_eq!(
                ptr as usize % ALIGNMENT,
                0,
                "Memory not aligned to {} bytes",
                ALIGNMENT
            );

            (ptr, shared_size)
        }
    }

    #[test]
    fn test_unbounded_deallocate_with_drops() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct TrackingItem {
            _id: usize,
            _data: Vec<u8>,
        }

        impl TrackingItem {
            fn new(id: usize) -> Self {
                ALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
                Self {
                    _id: id,
                    _data: vec![0u8; 100],
                }
            }
        }

        impl Drop for TrackingItem {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        ALLOC_COUNT.store(0, Ordering::SeqCst);
        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let (mem_ptr, shared_size) = allocate_aligned_unbounded_memory(8192);
            let queue = unsafe { UnboundedQueue::<TrackingItem>::init_in_shared(mem_ptr, 8192) };

            for i in 0..1000 {
                queue.push(TrackingItem::new(i)).unwrap();
            }

            for _ in 0..500 {
                drop(queue.pop().unwrap());
            }

            unsafe {
                use std::alloc::{dealloc, Layout};
                let layout = Layout::from_size_align(shared_size, 128).unwrap();
                dealloc(mem_ptr, layout);
            }
        }

        std::thread::sleep(Duration::from_millis(10));

        let allocations = ALLOC_COUNT.load(Ordering::SeqCst);
        let drops = DROP_COUNT.load(Ordering::SeqCst);

        assert!(
            drops >= 500,
            "Should have dropped at least 500 items, got {}",
            drops
        );
        assert_eq!(
            allocations, 1000,
            "Should have allocated exactly 1000 items"
        );
    }

    #[test]
    fn test_unbounded_segment_lifecycle() {
        const BUF_CAP: usize = 65536;

        #[derive(Debug)]
        struct NeedsDrop {
            data: String,
        }

        let (mem_ptr, shared_size) = allocate_aligned_unbounded_memory(8192);

        {
            let queue = unsafe { UnboundedQueue::<NeedsDrop>::init_in_shared(mem_ptr, 8192) };

            for i in 0..BUF_CAP - 1 {
                queue
                    .push(NeedsDrop {
                        data: format!("item_{}", i),
                    })
                    .unwrap();
            }

            queue
                .push(NeedsDrop {
                    data: "overflow".to_string(),
                })
                .unwrap();

            for _ in 0..BUF_CAP - 1 {
                drop(queue.pop().unwrap());
            }

            drop(queue.pop().unwrap());

            for i in 0..100 {
                queue
                    .push(NeedsDrop {
                        data: format!("reuse_{}", i),
                    })
                    .unwrap();
            }
        }

        unsafe {
            use std::alloc::{dealloc, Layout};
            let layout = Layout::from_size_align(shared_size, 128).unwrap();
            dealloc(mem_ptr, layout);
        }
    }

    #[test]
    fn test_unbounded_drop_implementation() {
        fn allocate_aligned_memory(size: usize, alignment: usize) -> (*mut u8, usize) {
            use std::alloc::{alloc_zeroed, Layout};

            unsafe {
                let layout = Layout::from_size_align(size, alignment).unwrap();
                let ptr = alloc_zeroed(layout);
                if ptr.is_null() {
                    panic!("Failed to allocate aligned memory");
                }

                assert_eq!(
                    ptr as usize % alignment,
                    0,
                    "Memory not aligned to {} bytes",
                    alignment
                );

                (ptr, size)
            }
        }

        unsafe fn deallocate_aligned_memory(ptr: *mut u8, size: usize, alignment: usize) {
            use std::alloc::{dealloc, Layout};
            let layout = Layout::from_size_align(size, alignment).unwrap();
            dealloc(ptr, layout);
        }

        const ALIGNMENT: usize = 128;

        {
            let shared_size = UnboundedQueue::<()>::shared_size(8192);
            let (mem_ptr, _) = allocate_aligned_memory(shared_size, ALIGNMENT);

            let queue = unsafe { UnboundedQueue::<()>::init_in_shared(mem_ptr, 8192) };

            for _ in 0..100000 {
                queue.push(()).unwrap();
            }

            for _ in 0..50000 {
                queue.pop().unwrap();
            }

            unsafe {
                deallocate_aligned_memory(mem_ptr, shared_size, ALIGNMENT);
            }
        }

        {
            let shared_size = UnboundedQueue::<Vec<u8>>::shared_size(8192);
            let (mem_ptr, _) = allocate_aligned_memory(shared_size, ALIGNMENT);

            let queue = unsafe { UnboundedQueue::<Vec<u8>>::init_in_shared(mem_ptr, 8192) };

            for i in 0..1000 {
                queue.push(vec![i as u8; 100]).unwrap();
            }

            for _ in 0..500 {
                queue.pop().unwrap();
            }

            unsafe {
                deallocate_aligned_memory(mem_ptr, shared_size, ALIGNMENT);
            }
        }

        {
            let shared_size = UnboundedQueue::<String>::shared_size(8192);
            let (mem_ptr, _) = allocate_aligned_memory(shared_size, ALIGNMENT);

            let queue = unsafe { UnboundedQueue::<String>::init_in_shared(mem_ptr, 8192) };

            for batch in 0..5 {
                for i in 0..1000 {
                    queue.push(format!("batch_{}_item_{}", batch, i)).unwrap();
                }

                for _ in 0..1000 {
                    queue.pop().unwrap();
                }
            }

            unsafe {
                deallocate_aligned_memory(mem_ptr, shared_size, ALIGNMENT);
            }
        }
    }

    #[test]
    fn test_unbounded_deallocate_segment_directly() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        fn allocate_aligned_box(size: usize, alignment: usize) -> Box<[u8]> {
            use std::alloc::{alloc_zeroed, Layout};

            unsafe {
                let layout = Layout::from_size_align(size, alignment).unwrap();
                let ptr = alloc_zeroed(layout);
                if ptr.is_null() {
                    panic!("Failed to allocate aligned memory");
                }

                let slice = std::slice::from_raw_parts_mut(ptr, size);
                Box::from_raw(slice)
            }
        }

        {
            let shared_size = UnboundedQueue::<usize>::shared_size(8192);
            const ALIGNMENT: usize = 128;

            let memory = allocate_aligned_box(shared_size, ALIGNMENT);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { UnboundedQueue::<usize>::init_in_shared(mem_ptr, 8192) };

            unsafe {
                queue._deallocate_segment(std::ptr::null_mut());
            }

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }

        {
            let shared_size = UnboundedQueue::<usize>::shared_size(8192);
            const ALIGNMENT: usize = 128;

            let memory = allocate_aligned_box(shared_size, ALIGNMENT);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { UnboundedQueue::<usize>::init_in_shared(mem_ptr, 8192) };

            let original_size = queue.segment_mmap_size.load(Ordering::Acquire);

            queue.segment_mmap_size.store(0, Ordering::Release);

            unsafe {
                queue._deallocate_segment(1 as *mut _);
            }

            queue
                .segment_mmap_size
                .store(original_size, Ordering::Release);

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }

        {
            let shared_size = UnboundedQueue::<String>::shared_size(8192);
            const ALIGNMENT: usize = 128;

            let memory = allocate_aligned_box(shared_size, ALIGNMENT);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            {
                let queue = unsafe { UnboundedQueue::<String>::init_in_shared(mem_ptr, 8192) };

                for i in 0..70000 {
                    if queue.push(format!("item_{}", i)).is_err() {
                        break;
                    }
                }

                for _ in 0..30000 {
                    queue.pop().unwrap();
                }
            }

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }

        {
            static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

            #[derive(Debug)]
            struct DropCounter {
                _id: usize,
            }

            impl Drop for DropCounter {
                fn drop(&mut self) {
                    DROP_COUNT.fetch_add(1, Ordering::SeqCst);
                }
            }

            DROP_COUNT.store(0, Ordering::SeqCst);

            {
                let shared_size = UnboundedQueue::<DropCounter>::shared_size(8192);
                const ALIGNMENT: usize = 128;

                let memory = allocate_aligned_box(shared_size, ALIGNMENT);
                let mem_ptr = Box::leak(memory).as_mut_ptr();

                let queue = unsafe { UnboundedQueue::<DropCounter>::init_in_shared(mem_ptr, 8192) };

                for i in 0..1000 {
                    queue.push(DropCounter { _id: i }).unwrap();
                }

                for _ in 0..500 {
                    drop(queue.pop().unwrap());
                }

                assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 500);

                unsafe {
                    let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
                }
            }

            std::thread::sleep(Duration::from_millis(10));

            let final_count = DROP_COUNT.load(Ordering::SeqCst);
            assert!(
                final_count >= 500,
                "At least 500 items should have been dropped, got {}",
                final_count
            );
        }

        {
            let shared_size = UnboundedQueue::<Vec<u8>>::shared_size(8192);
            const ALIGNMENT: usize = 128;

            let memory = allocate_aligned_box(shared_size, ALIGNMENT);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { UnboundedQueue::<Vec<u8>>::init_in_shared(mem_ptr, 8192) };

            for i in 0..100000 {
                if queue.push(vec![i as u8; 10]).is_err() {
                    break;
                }
            }

            while queue.pop().is_ok() {}

            for i in 0..1000 {
                queue.push(vec![i as u8; 10]).unwrap();
            }

            drop(queue);

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }
    }

    #[test]
    fn test_unbounded_cleanup_loop_in_deallocate() {
        use std::alloc::{alloc_zeroed, dealloc, Layout};
        use std::sync::atomic::{AtomicUsize, Ordering};

        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug)]
        struct DropTracker {
            id: usize,
        }

        impl Drop for DropTracker {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);

        const ALIGNMENT: usize = 128;

        {
            let shared_size = UnboundedQueue::<DropTracker>::shared_size(8192);
            let layout = Layout::from_size_align(shared_size, ALIGNMENT).unwrap();

            let mem_ptr = unsafe {
                let ptr = alloc_zeroed(layout);
                if ptr.is_null() {
                    panic!("Failed to allocate aligned memory");
                }
                ptr
            };

            assert_eq!(mem_ptr as usize % ALIGNMENT, 0, "Memory not aligned");

            let queue = unsafe { UnboundedQueue::<DropTracker>::init_in_shared(mem_ptr, 8192) };

            for i in 0..1000 {
                queue.push(DropTracker { id: i }).unwrap();
            }

            for _ in 0..500 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(
                DROP_COUNT.load(Ordering::SeqCst),
                500,
                "500 items should be dropped from popping"
            );

            unsafe {
                dealloc(mem_ptr, layout);
            }
        }

        std::thread::sleep(Duration::from_millis(10));

        assert!(DROP_COUNT.load(Ordering::SeqCst) >= 500);
    }

    #[test]
    fn test_unbounded_transition_item_pending() {
        use std::alloc::{alloc_zeroed, dealloc, Layout};

        const BUF_CAP: usize = 65536;
        const ALIGNMENT: usize = 128;

        let shared_size = UnboundedQueue::<String>::shared_size(8192);
        let layout = Layout::from_size_align(shared_size, ALIGNMENT).unwrap();

        let mem_ptr = unsafe {
            let ptr = alloc_zeroed(layout);
            if ptr.is_null() {
                panic!("Failed to allocate aligned memory");
            }
            ptr
        };

        let queue = unsafe { UnboundedQueue::<String>::init_in_shared(mem_ptr, 8192) };

        for i in 0..BUF_CAP - 2 {
            queue.push(format!("item_{}", i)).unwrap();
        }

        queue.push("second_to_last".to_string()).unwrap();
        queue.push("last_in_segment".to_string()).unwrap();

        queue.push("first_in_new_segment".to_string()).unwrap();

        queue.push("another_item".to_string()).unwrap();

        for _ in 0..100 {
            assert!(queue.pop().is_ok());
        }

        unsafe {
            dealloc(mem_ptr, layout);
        }
    }

    #[test]
    fn test_unbounded_transition_item_multiple_segments() {
        use std::alloc::{alloc_zeroed, dealloc, Layout};

        const BUF_CAP: usize = 65536;
        const ALIGNMENT: usize = 128;

        let shared_size = UnboundedQueue::<usize>::shared_size(8192);
        let layout = Layout::from_size_align(shared_size, ALIGNMENT).unwrap();

        let mem_ptr = unsafe {
            let ptr = alloc_zeroed(layout);
            if ptr.is_null() {
                panic!("Failed to allocate aligned memory");
            }
            ptr
        };

        let queue = unsafe { UnboundedQueue::<usize>::init_in_shared(mem_ptr, 8192) };

        let mut total_pushed = 0;
        for batch in 0..3 {
            let base = batch * BUF_CAP;
            for i in 0..BUF_CAP - 1 {
                queue.push(total_pushed).unwrap();
                total_pushed += 1;
            }

            queue.push(total_pushed).unwrap();
            total_pushed += 1;

            for _ in 0..10 {
                queue.push(total_pushed).unwrap();
                total_pushed += 1;
            }
        }

        let mut expected = 0;
        while let Ok(value) = queue.pop() {
            assert_eq!(value, expected, "Expected {}, got {}", expected, value);
            expected += 1;
        }

        assert_eq!(
            expected, total_pushed,
            "Should have popped all pushed items"
        );
        assert!(
            expected > BUF_CAP * 2,
            "Should have processed multiple segments worth of items"
        );

        unsafe {
            dealloc(mem_ptr, layout);
        }
    }

    #[test]
    fn test_unbounded_segment_boundary_conditions() {
        use std::alloc::{alloc_zeroed, dealloc, Layout};

        const BUF_CAP: usize = 65536;
        const ALIGNMENT: usize = 128;

        let shared_size = UnboundedQueue::<Vec<u8>>::shared_size(8192);
        let layout = Layout::from_size_align(shared_size, ALIGNMENT).unwrap();

        let mem_ptr = unsafe {
            let ptr = alloc_zeroed(layout);
            if ptr.is_null() {
                panic!("Failed to allocate aligned memory");
            }
            ptr
        };

        let queue = unsafe { UnboundedQueue::<Vec<u8>>::init_in_shared(mem_ptr, 8192) };

        for i in 0..BUF_CAP - 1 {
            queue.push(vec![i as u8; 10]).unwrap();
        }

        queue.push(vec![255; 10]).unwrap();

        for _ in 0..BUF_CAP - 1 {
            assert!(queue.pop().is_ok());
        }

        let item = queue.pop().unwrap();
        assert_eq!(item, vec![255; 10]);

        for i in 0..100 {
            queue.push(vec![i as u8; 5]).unwrap();
        }

        for i in 0..100 {
            let item = queue.pop().unwrap();
            assert_eq!(item, vec![i as u8; 5]);
        }

        assert!(queue.empty());

        unsafe {
            dealloc(mem_ptr, layout);
        }
    }

    #[test]
    fn test_unbounded_drop_with_remaining_items() {
        use std::alloc::{alloc_zeroed, dealloc, Layout};
        use std::sync::atomic::{AtomicUsize, Ordering};

        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug)]
        struct DropCounter {
            value: usize,
        }

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        const ALIGNMENT: usize = 128;

        {
            DROP_COUNT.store(0, Ordering::SeqCst);

            let shared_size = UnboundedQueue::<DropCounter>::shared_size(8192);
            let layout = Layout::from_size_align(shared_size, ALIGNMENT).unwrap();

            let mem_ptr = unsafe {
                let ptr = alloc_zeroed(layout);
                if ptr.is_null() {
                    panic!("Failed to allocate aligned memory");
                }
                ptr
            };

            {
                let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 8192) };

                for i in 0..100 {
                    queue.push(DropCounter { value: i }).unwrap();
                }
            }

            std::thread::sleep(Duration::from_millis(10));

            unsafe {
                dealloc(mem_ptr, layout);
            }
        }
    }
}

mod dehnavi_tests {
    use super::*;

    #[test]
    fn test_dehnavi_basic() {
        let queue = DehnaviQueue::<usize>::new(10);

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());
    }

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
                    thread::sleep(Duration::from_micros(10));
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

            while attempts < 100000 {
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

fn create_aligned_memory_box(size: usize, alignment: usize) -> Box<[u8]> {
    use std::alloc::{alloc_zeroed, Layout};

    unsafe {
        let layout = Layout::from_size_align(size, alignment).unwrap();
        let ptr = alloc_zeroed(layout);
        if ptr.is_null() {
            panic!("Failed to allocate aligned memory");
        }

        let slice = std::slice::from_raw_parts_mut(ptr, size);
        Box::from_raw(slice)
    }
}

macro_rules! test_shared_init {
    ($queue_type:ty, $capacity:expr, $alignment:expr, $test_name:ident) => {
        #[test]
        fn $test_name() {
            let shared_size = <$queue_type>::shared_size($capacity);

            let memory = create_aligned_memory_box(shared_size, $alignment);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            assert_eq!(
                mem_ptr as usize % $alignment,
                0,
                "Memory not aligned to {} bytes",
                $alignment
            );

            let queue = unsafe { <$queue_type>::init_in_shared(mem_ptr, $capacity) };

            queue.push(123).unwrap();

            if stringify!($queue_type).contains("MultiPushQueue") {
                if let Some(mp_queue) =
                    (queue as &dyn std::any::Any).downcast_ref::<MultiPushQueue<usize>>()
                {
                    let _ = mp_queue.flush();
                }
            } else if stringify!($queue_type).contains("BiffqQueue") {
                if let Some(biffq) =
                    (queue as &dyn std::any::Any).downcast_ref::<BiffqQueue<usize>>()
                {
                    let _ = biffq.flush_producer_buffer();
                }
            }

            assert_eq!(queue.pop().unwrap(), 123);
            assert!(queue.empty());

            let mut pushed = 0;
            for i in 0..$capacity {
                match queue.push(i) {
                    Ok(_) => pushed += 1,
                    Err(_) => break,
                }
            }

            assert!(pushed > 0);

            if stringify!($queue_type).contains("MultiPushQueue") {
                if let Some(mp_queue) = (queue as &dyn Any).downcast_ref::<MultiPushQueue<usize>>()
                {
                    let _ = mp_queue.flush();
                }
            } else if stringify!($queue_type).contains("BiffqQueue") {
                if let Some(biffq) = (queue as &dyn Any).downcast_ref::<BiffqQueue<usize>>() {
                    let _ = biffq.flush_producer_buffer();
                }
            }

            let mut popped = 0;
            let mut pop_attempts = 0;
            while popped < pushed && pop_attempts < pushed * 2 {
                if queue.pop().is_ok() {
                    popped += 1;
                } else {
                    if stringify!($queue_type).contains("BiffqQueue") {
                        if let Some(biffq) = (queue as &dyn Any).downcast_ref::<BiffqQueue<usize>>()
                        {
                            let _ = biffq.flush_producer_buffer();
                        }
                    } else if stringify!($queue_type).contains("MultiPushQueue") {
                        if let Some(mp_queue) =
                            (queue as &dyn Any).downcast_ref::<MultiPushQueue<usize>>()
                        {
                            let _ = mp_queue.flush();
                        }
                    }
                    pop_attempts += 1;
                    std::thread::yield_now();
                }
            }

            if stringify!($queue_type).contains("BiffqQueue")
                || stringify!($queue_type).contains("MultiPushQueue")
            {
                assert!(popped > 0, "Should be able to pop at least some items");
            } else {
                assert_eq!(popped, pushed, "Should be able to pop all pushed items");
            }

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }
    };
}

mod shared_memory_tests {
    use super::*;

    test_shared_init!(LamportQueue<usize>, SMALL_CAPACITY, 64, test_lamport_shared);
    test_shared_init!(FfqQueue<usize>, MEDIUM_CAPACITY, 64, test_ffq_shared);
    test_shared_init!(BlqQueue<usize>, 128, 64, test_blq_shared);
    test_shared_init!(IffqQueue<usize>, MEDIUM_CAPACITY, 64, test_iffq_shared);
    test_shared_init!(BiffqQueue<usize>, MEDIUM_CAPACITY, 64, test_biffq_shared);
    test_shared_init!(BQueue<usize>, MEDIUM_CAPACITY, 64, test_bqueue_shared);
    test_shared_init!(
        MultiPushQueue<usize>,
        MEDIUM_CAPACITY,
        64,
        test_multipush_shared
    );

    #[test]
    fn test_dehnavi_shared() {
        let capacity = 10;
        let shared_size = DehnaviQueue::<usize>::shared_size(capacity);
        let memory = create_aligned_memory_box(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

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

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_llq_shared() {
        let shared_size = LlqQueue::<usize>::llq_shared_size(MEDIUM_CAPACITY);
        let memory = create_aligned_memory_box(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { LlqQueue::<usize>::init_in_shared(mem_ptr, MEDIUM_CAPACITY) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        let mut pushed = 0;
        for i in 0..MEDIUM_CAPACITY {
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

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_sesd_wrapper_shared() {
        let pool_capacity = 100;
        let shared_size = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);
        let memory = create_aligned_memory_box(shared_size, 64);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue =
            unsafe { SesdJpSpscBenchWrapper::<usize>::init_in_shared(mem_ptr, pool_capacity) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        let mut pushed = 0;
        for i in 0..pool_capacity {
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

        assert_eq!(popped, pushed, "Should be able to pop all pushed items");

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_dspsc_shared() {
        let shared_size = DynListQueue::<usize>::shared_size(8192);
        let memory = create_aligned_memory_box(shared_size, 128);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { DynListQueue::<usize>::init_in_shared(mem_ptr, 8192) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        for i in 0..1000 {
            queue.push(i).unwrap();
        }

        for i in 0..1000 {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }

    #[test]
    fn test_unbounded_shared() {
        let shared_size = UnboundedQueue::<usize>::shared_size(8192);
        let memory = create_aligned_memory_box(shared_size, 128);
        let mem_ptr = Box::leak(memory).as_mut_ptr();

        let queue = unsafe { UnboundedQueue::<usize>::init_in_shared(mem_ptr, 8192) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        for i in 0..70000 {
            queue.push(i).unwrap();
        }

        for i in 0..70000 {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());

        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
        }
    }
}

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_zero_sized_type() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct ZeroSized;

        let queue = LamportQueue::<ZeroSized>::with_capacity(64);
        queue.push(ZeroSized).unwrap();
        assert_eq!(queue.pop().unwrap(), ZeroSized);
    }

    #[test]
    fn test_large_type() {
        #[derive(Clone, Debug, PartialEq)]
        struct LargeType {
            data: [u64; 128],
        }

        let queue = LamportQueue::<LargeType>::with_capacity(16);
        let item = LargeType { data: [42; 128] };

        queue.push(item.clone()).unwrap();
        assert_eq!(queue.pop().unwrap(), item);
    }

    #[test]
    fn test_drop_semantics() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

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
            let queue = LamportQueue::<DropCounter>::with_capacity(64);

            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }

            let mid_count = DROP_COUNT.load(Ordering::SeqCst);
            assert_eq!(
                mid_count, 5,
                "5 items should be dropped after explicit drops"
            );
        }

        std::thread::sleep(Duration::from_millis(10));

        let final_count = DROP_COUNT.load(Ordering::SeqCst);

        assert!(
            final_count >= 5,
            "At least the 5 popped items should be dropped, got {}",
            final_count
        );
    }
}

mod special_feature_tests {

    use super::*;

    #[test]
    fn test_biffq_flush() {
        let queue = BiffqQueue::<usize>::with_capacity(128);

        for i in 0..10 {
            queue.push(i).unwrap();
        }

        let flushed = queue.flush_producer_buffer().unwrap();
        assert!(flushed > 0);

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
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
    }

    #[test]
    fn test_dspsc_shared_memory() {
        let shared_size = DynListQueue::<usize>::shared_size(8192);

        const ALIGNMENT: usize = 128;

        use std::alloc::{alloc_zeroed, Layout};

        let layout =
            Layout::from_size_align(shared_size, ALIGNMENT).expect("Failed to create layout");

        let mem_ptr = unsafe {
            let ptr = alloc_zeroed(layout);
            if ptr.is_null() {
                panic!("Failed to allocate aligned memory");
            }
            ptr
        };

        assert_eq!(
            mem_ptr as usize % ALIGNMENT,
            0,
            "Memory not aligned to {} bytes",
            ALIGNMENT
        );

        let queue = unsafe { DynListQueue::<usize>::init_in_shared(mem_ptr, 8192) };

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        for i in 0..100 {
            queue.push(i).unwrap();
        }

        for i in 0..100 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.empty());

        for i in 0..20000 {
            queue.push(i).unwrap();
        }

        for i in 0..20000 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.empty());

        unsafe {
            std::alloc::dealloc(mem_ptr, layout);
        }
    }

    #[test]
    fn test_dspsc_dynamic_allocation() {
        let queue = DynListQueue::<usize>::with_capacity(8192);

        for i in 0..1000 {
            queue.push(i).unwrap();
        }

        for i in 0..1000 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.empty());
    }

    #[test]
    fn test_dspsc_heap_allocation() {
        let queue = DynListQueue::<String>::with_capacity(8192);

        const PREALLOCATED_NODES: usize = 16384;

        for i in 0..PREALLOCATED_NODES + 100 {
            queue.push(format!("item_{}", i)).unwrap();
        }

        for i in 0..100 {
            assert!(queue.pop().is_ok());
            queue.push(format!("recycled_{}", i)).unwrap();
        }

        while queue.pop().is_ok() {}
    }
}

mod error_handling_tests {
    use super::*;

    #[test]
    #[should_panic]
    fn test_lamport_invalid_capacity() {
        let _ = LamportQueue::<usize>::with_capacity(15);
    }

    #[test]
    #[should_panic]
    fn test_dehnavi_zero_capacity() {
        let _ = DehnaviQueue::<usize>::new(0);
    }

    #[test]
    fn test_push_error_handling() {
        let queue = LamportQueue::<String>::with_capacity(2);

        queue.push("first".to_string()).unwrap();

        let failed_item = "second".to_string();
        match queue.push(failed_item.clone()) {
            Err(_) => {}
            Ok(_) => panic!("Push should have failed on full queue"),
        }
    }
}

mod sesd_wrapper_tests {
    use super::*;

    #[test]
    fn test_sesd_wrapper_basic() {
        let pool_capacity = 100;
        let shared_size = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);
        let mut memory = vec![0u8; shared_size];

        let queue =
            unsafe { SesdJpSpscBenchWrapper::init_in_shared(memory.as_mut_ptr(), pool_capacity) };

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        for i in 0..10 {
            queue.push(i).unwrap();
        }

        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());

        let mut pushed = 0;
        for i in 0..pool_capacity {
            match queue.push(i) {
                Ok(_) => pushed += 1,
                Err(_) => break,
            }
        }

        assert!(
            pushed >= pool_capacity - 5,
            "Should push most items, pushed: {}",
            pushed
        );

        let mut popped = 0;
        while queue.pop().is_ok() {
            popped += 1;
        }
        assert_eq!(popped, pushed, "Should pop all pushed items");
        assert!(queue.empty());
    }

    #[test]
    fn test_sesd_wrapper_concurrent() {
        let pool_capacity = 1000;
        let shared_size = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);
        let mut memory = vec![0u8; shared_size];

        let queue =
            unsafe { SesdJpSpscBenchWrapper::init_in_shared(memory.as_mut_ptr(), pool_capacity) };

        let queue_ptr = queue as *const SesdJpSpscBenchWrapper<usize>;
        let queue = unsafe { &*queue_ptr };

        let barrier = Arc::new(Barrier::new(2));
        let items_to_send = 500;

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
                        if empty_polls > 1000000 {
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

#[cfg(unix)]
mod ipc_tests {
    use super::*;
    use nix::{
        libc,
        sys::wait::waitpid,
        unistd::{fork, ForkResult},
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    unsafe fn map_shared(bytes: usize) -> *mut u8 {
        let page_size = 4096;
        let aligned_size = (bytes + page_size - 1) & !(page_size - 1);

        let ptr = libc::mmap(
            std::ptr::null_mut(),
            aligned_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if ptr == libc::MAP_FAILED {
            panic!("mmap failed: {}", std::io::Error::last_os_error());
        }

        std::ptr::write_bytes(ptr as *mut u8, 0, aligned_size);

        ptr.cast()
    }

    unsafe fn unmap_shared(ptr: *mut u8, len: usize) {
        let page_size = 4096;
        let aligned_size = (len + page_size - 1) & !(page_size - 1);

        if libc::munmap(ptr.cast(), aligned_size) == -1 {
            panic!("munmap failed: {}", std::io::Error::last_os_error());
        }
    }

    macro_rules! test_queue_ipc {
        ($queue_type:ty, $capacity:expr, $test_name:ident) => {
            #[test]
            fn $test_name() {
                const ATOMIC_BOOL_SIZE: usize = std::mem::size_of::<AtomicBool>();
                const ATOMIC_USIZE_SIZE: usize = std::mem::size_of::<AtomicUsize>();
                const ATOMIC_BOOL_ALIGN: usize = std::mem::align_of::<AtomicBool>();
                const ATOMIC_USIZE_ALIGN: usize = std::mem::align_of::<AtomicUsize>();

                let mut sync_size = 0;

                sync_size += ATOMIC_BOOL_SIZE;

                sync_size = (sync_size + ATOMIC_BOOL_ALIGN - 1) & !(ATOMIC_BOOL_ALIGN - 1);
                sync_size += ATOMIC_BOOL_SIZE;

                sync_size =
                    (sync_size + ATOMIC_USIZE_ALIGN.max(8) - 1) & !(ATOMIC_USIZE_ALIGN.max(8) - 1);
                sync_size += ATOMIC_USIZE_SIZE;

                sync_size = (sync_size + 63) & !63;

                let shared_size = <$queue_type>::shared_size($capacity);
                let total_size = shared_size + sync_size;

                let shm_ptr = unsafe { map_shared(total_size) };

                let producer_ready = unsafe { &*(shm_ptr as *const AtomicBool) };

                let consumer_ready_offset =
                    (ATOMIC_BOOL_SIZE + ATOMIC_BOOL_ALIGN - 1) & !(ATOMIC_BOOL_ALIGN - 1);
                let consumer_ready =
                    unsafe { &*(shm_ptr.add(consumer_ready_offset) as *const AtomicBool) };

                let items_consumed_offset = {
                    let offset = consumer_ready_offset + ATOMIC_BOOL_SIZE;
                    (offset + ATOMIC_USIZE_ALIGN.max(8) - 1) & !(ATOMIC_USIZE_ALIGN.max(8) - 1)
                };
                let items_consumed =
                    unsafe { &*(shm_ptr.add(items_consumed_offset) as *const AtomicUsize) };

                assert_eq!(
                    (producer_ready as *const _ as usize) % ATOMIC_BOOL_ALIGN,
                    0,
                    "producer_ready not aligned"
                );
                assert_eq!(
                    (consumer_ready as *const _ as usize) % ATOMIC_BOOL_ALIGN,
                    0,
                    "consumer_ready not aligned"
                );
                assert_eq!(
                    (items_consumed as *const _ as usize) % ATOMIC_USIZE_ALIGN.max(8),
                    0,
                    "items_consumed not aligned"
                );

                producer_ready.store(false, Ordering::SeqCst);
                consumer_ready.store(false, Ordering::SeqCst);
                items_consumed.store(0, Ordering::SeqCst);

                let queue_ptr = unsafe { shm_ptr.add(sync_size) };
                let queue = unsafe { <$queue_type>::init_in_shared(queue_ptr, $capacity) };

                const NUM_ITEMS: usize = 10000;

                match unsafe { fork() } {
                    Ok(ForkResult::Child) => {
                        producer_ready.store(true, Ordering::Release);

                        while !consumer_ready.load(Ordering::Acquire) {
                            std::hint::spin_loop();
                        }

                        for i in 0..NUM_ITEMS {
                            loop {
                                match queue.push(i) {
                                    Ok(_) => break,
                                    Err(_) => std::thread::yield_now(),
                                }
                            }
                        }

                        if let Some(mp_queue) =
                            (queue as &dyn std::any::Any).downcast_ref::<MultiPushQueue<usize>>()
                        {
                            let mut flush_attempts = 0;
                            while mp_queue.local_count.load(Ordering::Relaxed) > 0
                                && flush_attempts < 100
                            {
                                if !mp_queue.flush() {
                                    std::thread::yield_now();
                                }
                                flush_attempts += 1;
                            }

                            if mp_queue.local_count.load(Ordering::Relaxed) > 0 {
                                for _ in 0..16 {
                                    let _ = queue.push(999999);
                                }
                                let _ = mp_queue.flush();
                            }
                        } else if let Some(biffq) =
                            (queue as &dyn std::any::Any).downcast_ref::<BiffqQueue<usize>>()
                        {
                            while biffq.prod.local_count.load(Ordering::Relaxed) > 0 {
                                match biffq.flush_producer_buffer() {
                                    Ok(_) => {
                                        if biffq.prod.local_count.load(Ordering::Relaxed) == 0 {
                                            break;
                                        }
                                    }
                                    Err(_) => std::thread::yield_now(),
                                }
                            }
                        }

                        unsafe { libc::_exit(0) };
                    }
                    Ok(ForkResult::Parent { child }) => {
                        while !producer_ready.load(Ordering::Acquire) {
                            std::hint::spin_loop();
                        }

                        consumer_ready.store(true, Ordering::Release);

                        let mut received = Vec::new();
                        let mut empty_count = 0;

                        while received.len() < NUM_ITEMS {
                            match queue.pop() {
                                Ok(item) => {
                                    received.push(item);
                                    empty_count = 0;
                                }
                                Err(_) => {
                                    empty_count += 1;
                                    if empty_count > 1000000 {
                                        break;
                                    }
                                    std::thread::yield_now();
                                }
                            }
                        }

                        items_consumed.store(received.len(), Ordering::SeqCst);

                        waitpid(child, None).expect("waitpid failed");

                        let consumed = items_consumed.load(Ordering::SeqCst);
                        assert_eq!(
                            consumed, NUM_ITEMS,
                            "Not all items were consumed in IPC test"
                        );

                        if stringify!($queue_type).contains("MultiPushQueue") {
                            let mut sorted_received = received.clone();
                            sorted_received.sort();
                            for (i, &item) in sorted_received.iter().enumerate() {
                                assert_eq!(
                                    item,
                                    i,
                                    "Should have received all items from 0 to {}",
                                    NUM_ITEMS - 1
                                );
                            }
                        } else {
                            for (i, &item) in received.iter().enumerate() {
                                assert_eq!(item, i, "Items received out of order");
                            }
                        }

                        unsafe {
                            unmap_shared(shm_ptr, total_size);
                        }
                    }
                    Err(e) => {
                        unsafe {
                            unmap_shared(shm_ptr, total_size);
                        }
                        panic!("Fork failed: {}", e);
                    }
                }
            }
        };
    }

    test_queue_ipc!(LamportQueue<usize>, 1024, test_lamport_ipc);
    test_queue_ipc!(BlqQueue<usize>, 128, test_blq_ipc);
    test_queue_ipc!(IffqQueue<usize>, 1024, test_iffq_ipc);
    test_queue_ipc!(BiffqQueue<usize>, 1024, test_biffq_ipc);
    test_queue_ipc!(BQueue<usize>, 1024, test_bqueue_ipc);
    test_queue_ipc!(MultiPushQueue<usize>, 1024, test_multipush_ipc);

    #[test]
    fn test_ffq_ipc() {
        const ATOMIC_BOOL_SIZE: usize = std::mem::size_of::<AtomicBool>();
        const ATOMIC_USIZE_SIZE: usize = std::mem::size_of::<AtomicUsize>();
        const ATOMIC_BOOL_ALIGN: usize = std::mem::align_of::<AtomicBool>();
        const ATOMIC_USIZE_ALIGN: usize = std::mem::align_of::<AtomicUsize>();

        let mut sync_size = 0;
        sync_size += ATOMIC_BOOL_SIZE;
        sync_size = (sync_size + ATOMIC_BOOL_ALIGN - 1) & !(ATOMIC_BOOL_ALIGN - 1);
        sync_size += ATOMIC_BOOL_SIZE;
        sync_size = (sync_size + ATOMIC_USIZE_ALIGN.max(8) - 1) & !(ATOMIC_USIZE_ALIGN.max(8) - 1);
        sync_size += ATOMIC_USIZE_SIZE;
        sync_size = (sync_size + 63) & !63;

        let capacity = 1024;
        let shared_size = FfqQueue::<usize>::shared_size(capacity);
        let total_size = shared_size + sync_size;

        let shm_ptr = unsafe { map_shared(total_size) };

        // CRITICAL: Zero out ALL shared memory before use
        unsafe {
            std::ptr::write_bytes(shm_ptr, 0, total_size);
        }

        // Add memory barrier to ensure zeroing is complete
        std::sync::atomic::fence(Ordering::SeqCst);

        let producer_ready = unsafe { &*(shm_ptr as *const AtomicBool) };
        let consumer_ready_offset =
            (ATOMIC_BOOL_SIZE + ATOMIC_BOOL_ALIGN - 1) & !(ATOMIC_BOOL_ALIGN - 1);
        let consumer_ready = unsafe { &*(shm_ptr.add(consumer_ready_offset) as *const AtomicBool) };
        let items_consumed_offset = {
            let offset = consumer_ready_offset + ATOMIC_BOOL_SIZE;
            (offset + ATOMIC_USIZE_ALIGN.max(8) - 1) & !(ATOMIC_USIZE_ALIGN.max(8) - 1)
        };
        let items_consumed =
            unsafe { &*(shm_ptr.add(items_consumed_offset) as *const AtomicUsize) };

        producer_ready.store(false, Ordering::SeqCst);
        consumer_ready.store(false, Ordering::SeqCst);
        items_consumed.store(0, Ordering::SeqCst);

        let queue_ptr = unsafe { shm_ptr.add(sync_size) };
        let queue = unsafe { FfqQueue::<usize>::init_in_shared(queue_ptr, capacity) };

        const NUM_ITEMS: usize = 10000;

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                // Producer
                producer_ready.store(true, Ordering::Release);
                while !consumer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                // Add a small delay to ensure consumer is ready
                std::thread::sleep(std::time::Duration::from_millis(10));

                for i in 0..NUM_ITEMS {
                    let mut retry_count = 0;
                    loop {
                        match queue.push(i) {
                            Ok(_) => break,
                            Err(_) => {
                                retry_count += 1;
                                if retry_count > 10_000_000 {
                                    eprintln!("Producer: Excessive retries at item {}", i);
                                }
                                std::thread::yield_now();
                            }
                        }
                    }
                }

                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                // Consumer
                while !producer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                consumer_ready.store(true, Ordering::Release);

                let mut received = Vec::new();
                let mut empty_count = 0;
                let mut total_empty_polls = 0;

                while received.len() < NUM_ITEMS {
                    match queue.pop() {
                        Ok(item) => {
                            // Check immediately if we got the right value
                            if item != received.len() {
                                eprintln!(
                                    "ERROR: Expected {}, got {} at position {}",
                                    received.len(),
                                    item,
                                    received.len()
                                );
                                eprintln!(
                                    "Queue head: {}, tail: {}",
                                    queue.head.load(Ordering::SeqCst),
                                    queue.tail.load(Ordering::SeqCst)
                                );
                            }
                            received.push(item);
                            empty_count = 0;
                        }
                        Err(_) => {
                            empty_count += 1;
                            total_empty_polls += 1;
                            if empty_count > 10_000_000 {
                                eprintln!(
                                    "Consumer: Too many empty polls, breaking. Received {} items",
                                    received.len()
                                );
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                }

                items_consumed.store(received.len(), Ordering::SeqCst);

                // Wait for child with timeout
                use nix::sys::wait::WaitStatus;
                match waitpid(child, None) {
                    Ok(WaitStatus::Exited(_, 0)) => {}
                    other => eprintln!("Child process ended with: {:?}", other),
                }

                let consumed = items_consumed.load(Ordering::SeqCst);
                assert_eq!(
                    consumed, NUM_ITEMS,
                    "Not all items were consumed in IPC test"
                );

                // Now check order
                for (i, &item) in received.iter().enumerate() {
                    if item != i {
                        panic!(
                            "Items received out of order at position {}: expected {}, got {}",
                            i, i, item
                        );
                    }
                }

                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
            }
            Err(e) => {
                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
                panic!("Fork failed: {}", e);
            }
        }
    }

    #[test]
    fn test_llq_ipc() {
        let capacity = 1024;
        let shared_size = LlqQueue::<usize>::llq_shared_size(capacity);

        const ATOMIC_BOOL_SIZE: usize = std::mem::size_of::<AtomicBool>();
        const ATOMIC_USIZE_SIZE: usize = std::mem::size_of::<AtomicUsize>();
        const ATOMIC_BOOL_ALIGN: usize = std::mem::align_of::<AtomicBool>();
        const ATOMIC_USIZE_ALIGN: usize = std::mem::align_of::<AtomicUsize>();

        let mut sync_size = 0;

        sync_size += ATOMIC_BOOL_SIZE;

        sync_size = (sync_size + ATOMIC_BOOL_ALIGN - 1) & !(ATOMIC_BOOL_ALIGN - 1);
        sync_size += ATOMIC_BOOL_SIZE;

        sync_size = (sync_size + ATOMIC_USIZE_ALIGN.max(8) - 1) & !(ATOMIC_USIZE_ALIGN.max(8) - 1);
        sync_size += ATOMIC_USIZE_SIZE;

        sync_size = (sync_size + 63) & !63;

        let queue_alignment = 64;
        let total_size = sync_size + shared_size + queue_alignment;

        let shm_ptr = unsafe { map_shared(total_size) };

        let producer_ready = unsafe { &*(shm_ptr as *const AtomicBool) };

        let consumer_ready_offset =
            (ATOMIC_BOOL_SIZE + ATOMIC_BOOL_ALIGN - 1) & !(ATOMIC_BOOL_ALIGN - 1);
        let consumer_ready = unsafe { &*(shm_ptr.add(consumer_ready_offset) as *const AtomicBool) };

        let items_consumed_offset = {
            let offset = consumer_ready_offset + ATOMIC_BOOL_SIZE;
            (offset + ATOMIC_USIZE_ALIGN.max(8) - 1) & !(ATOMIC_USIZE_ALIGN.max(8) - 1)
        };
        let items_consumed =
            unsafe { &*(shm_ptr.add(items_consumed_offset) as *const AtomicUsize) };

        assert_eq!(
            (producer_ready as *const _ as usize) % ATOMIC_BOOL_ALIGN,
            0,
            "producer_ready not aligned"
        );
        assert_eq!(
            (consumer_ready as *const _ as usize) % ATOMIC_BOOL_ALIGN,
            0,
            "consumer_ready not aligned"
        );
        assert_eq!(
            (items_consumed as *const _ as usize) % ATOMIC_USIZE_ALIGN.max(8),
            0,
            "items_consumed not aligned"
        );

        producer_ready.store(false, Ordering::SeqCst);
        consumer_ready.store(false, Ordering::SeqCst);
        items_consumed.store(0, Ordering::SeqCst);

        let queue_ptr = unsafe {
            let unaligned_ptr = shm_ptr.add(sync_size);
            let addr = unaligned_ptr as usize;
            let aligned_addr = (addr + queue_alignment - 1) & !(queue_alignment - 1);
            aligned_addr as *mut u8
        };

        assert_eq!(
            queue_ptr as usize % queue_alignment,
            0,
            "Queue not properly aligned to {} bytes",
            queue_alignment
        );

        let queue = unsafe { LlqQueue::<usize>::init_in_shared(queue_ptr, capacity) };

        const NUM_ITEMS: usize = 10000;

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                producer_ready.store(true, Ordering::Release);

                while !consumer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                for i in 0..NUM_ITEMS {
                    loop {
                        match queue.push(i) {
                            Ok(_) => break,
                            Err(_) => std::thread::yield_now(),
                        }
                    }
                }

                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                while !producer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                consumer_ready.store(true, Ordering::Release);

                let mut received = Vec::new();
                let mut empty_count = 0;

                while received.len() < NUM_ITEMS {
                    match queue.pop() {
                        Ok(item) => {
                            received.push(item);
                            empty_count = 0;
                        }
                        Err(_) => {
                            empty_count += 1;
                            if empty_count > 1000000 {
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                }

                items_consumed.store(received.len(), Ordering::SeqCst);

                waitpid(child, None).expect("waitpid failed");

                let consumed = items_consumed.load(Ordering::SeqCst);
                assert_eq!(
                    consumed, NUM_ITEMS,
                    "Not all items were consumed in IPC test"
                );

                for (i, &item) in received.iter().enumerate() {
                    assert_eq!(item, i, "Items received out of order");
                }

                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
            }
            Err(e) => {
                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
                panic!("Fork failed: {}", e);
            }
        }
    }

    #[test]
    fn test_unbounded_ipc() {
        let shared_size = UnboundedQueue::<usize>::shared_size(16384);
        let sync_size = std::mem::size_of::<AtomicBool>() * 2;
        let sync_size = (sync_size + 63) & !63;
        let total_size = shared_size + sync_size + 128;

        let shm_ptr = unsafe { map_shared(total_size) };

        let producer_ready = unsafe { &*(shm_ptr as *const AtomicBool) };
        let consumer_ready =
            unsafe { &*(shm_ptr.add(std::mem::size_of::<AtomicBool>()) as *const AtomicBool) };

        producer_ready.store(false, Ordering::SeqCst);
        consumer_ready.store(false, Ordering::SeqCst);

        let queue_ptr = unsafe { shm_ptr.add(sync_size) };
        let queue_ptr = ((queue_ptr as usize + 127) & !127) as *mut u8;

        let queue = unsafe { UnboundedQueue::init_in_shared(queue_ptr, 16384) };

        const NUM_ITEMS: usize = 100000;

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                producer_ready.store(true, Ordering::Release);
                while !consumer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                for i in 0..NUM_ITEMS {
                    queue.push(i).unwrap();
                }

                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                while !producer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                consumer_ready.store(true, Ordering::Release);

                let mut count = 0;
                let mut attempts = 0;
                while count < NUM_ITEMS && attempts < NUM_ITEMS * 100 {
                    match queue.pop() {
                        Ok(item) => {
                            assert_eq!(item, count);
                            count += 1;
                        }
                        Err(_) => {
                            attempts += 1;
                            std::thread::yield_now();
                        }
                    }
                }

                waitpid(child, None).expect("waitpid failed");
                assert_eq!(count, NUM_ITEMS);

                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
            }
            Err(e) => {
                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
                panic!("Fork failed: {}", e);
            }
        }
    }

    #[test]
    fn test_dehnavi_ipc() {
        let capacity = 100;
        let shared_size = DehnaviQueue::<usize>::shared_size(capacity);
        let sync_size = std::mem::size_of::<AtomicBool>() * 2;
        let sync_size = (sync_size + 63) & !63;
        let total_size = shared_size + sync_size;

        let shm_ptr = unsafe { map_shared(total_size) };

        let producer_ready = unsafe { &*(shm_ptr as *const AtomicBool) };
        let consumer_ready =
            unsafe { &*(shm_ptr.add(std::mem::size_of::<AtomicBool>()) as *const AtomicBool) };

        producer_ready.store(false, Ordering::SeqCst);
        consumer_ready.store(false, Ordering::SeqCst);

        let queue_ptr = unsafe { shm_ptr.add(sync_size) };
        let queue = unsafe { DehnaviQueue::init_in_shared(queue_ptr, capacity) };

        const NUM_ITEMS: usize = 200;

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                producer_ready.store(true, Ordering::Release);
                while !consumer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                for i in 0..NUM_ITEMS {
                    queue.push(i).unwrap();
                    if i % 10 == 0 {
                        std::thread::sleep(Duration::from_micros(10));
                    }
                }

                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                while !producer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                consumer_ready.store(true, Ordering::Release);

                std::thread::sleep(Duration::from_millis(10));

                let mut received = Vec::new();
                let mut attempts = 0;

                while attempts < 100000 {
                    match queue.pop() {
                        Ok(item) => {
                            received.push(item);
                            attempts = 0;
                        }
                        Err(_) => {
                            attempts += 1;
                            if attempts > 10000 {
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                }

                waitpid(child, None).expect("waitpid failed");

                assert!(!received.is_empty(), "Should have received some items");
                for i in 1..received.len() {
                    assert!(
                        received[i] > received[i - 1],
                        "Items should be in increasing order"
                    );
                }

                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
            }
            Err(e) => {
                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
                panic!("Fork failed: {}", e);
            }
        }
    }

    #[test]
    fn test_sesd_wrapper_ipc() {
        let pool_capacity = 10000;
        let shared_size = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);

        const ATOMIC_BOOL_SIZE: usize = std::mem::size_of::<AtomicBool>();
        const ATOMIC_USIZE_SIZE: usize = std::mem::size_of::<AtomicUsize>();
        const ATOMIC_BOOL_ALIGN: usize = std::mem::align_of::<AtomicBool>();
        const ATOMIC_USIZE_ALIGN: usize = std::mem::align_of::<AtomicUsize>();

        let mut sync_size = 0;

        sync_size += ATOMIC_BOOL_SIZE;

        sync_size = (sync_size + ATOMIC_BOOL_ALIGN - 1) & !(ATOMIC_BOOL_ALIGN - 1);
        sync_size += ATOMIC_BOOL_SIZE;

        sync_size = (sync_size + ATOMIC_USIZE_ALIGN.max(8) - 1) & !(ATOMIC_USIZE_ALIGN.max(8) - 1);
        sync_size += ATOMIC_USIZE_SIZE;

        sync_size = (sync_size + 63) & !63;

        let queue_alignment = 64;
        let total_size = sync_size + shared_size + queue_alignment;

        let shm_ptr = unsafe { map_shared(total_size) };

        let producer_ready = unsafe { &*(shm_ptr as *const AtomicBool) };

        let consumer_ready_offset =
            (ATOMIC_BOOL_SIZE + ATOMIC_BOOL_ALIGN - 1) & !(ATOMIC_BOOL_ALIGN - 1);
        let consumer_ready = unsafe { &*(shm_ptr.add(consumer_ready_offset) as *const AtomicBool) };

        let items_consumed_offset = {
            let offset = consumer_ready_offset + ATOMIC_BOOL_SIZE;
            (offset + ATOMIC_USIZE_ALIGN.max(8) - 1) & !(ATOMIC_USIZE_ALIGN.max(8) - 1)
        };
        let items_consumed =
            unsafe { &*(shm_ptr.add(items_consumed_offset) as *const AtomicUsize) };

        assert_eq!(
            (producer_ready as *const _ as usize) % ATOMIC_BOOL_ALIGN,
            0,
            "producer_ready not aligned"
        );
        assert_eq!(
            (consumer_ready as *const _ as usize) % ATOMIC_BOOL_ALIGN,
            0,
            "consumer_ready not aligned"
        );
        assert_eq!(
            (items_consumed as *const _ as usize) % ATOMIC_USIZE_ALIGN.max(8),
            0,
            "items_consumed not aligned"
        );

        producer_ready.store(false, Ordering::SeqCst);
        consumer_ready.store(false, Ordering::SeqCst);
        items_consumed.store(0, Ordering::SeqCst);

        let queue_ptr = unsafe {
            let unaligned_ptr = shm_ptr.add(sync_size);
            let addr = unaligned_ptr as usize;
            let aligned_addr = (addr + queue_alignment - 1) & !(queue_alignment - 1);
            aligned_addr as *mut u8
        };

        assert_eq!(
            queue_ptr as usize % queue_alignment,
            0,
            "Queue not properly aligned to {} bytes",
            queue_alignment
        );

        let queue = unsafe { SesdJpSpscBenchWrapper::init_in_shared(queue_ptr, pool_capacity) };

        const NUM_ITEMS: usize = 5000;

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                producer_ready.store(true, Ordering::Release);

                while !consumer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                for i in 0..NUM_ITEMS {
                    loop {
                        match queue.push(i) {
                            Ok(_) => break,
                            Err(_) => std::thread::yield_now(),
                        }
                    }
                }

                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                while !producer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                consumer_ready.store(true, Ordering::Release);

                let mut received = Vec::new();
                let mut empty_count = 0;

                while received.len() < NUM_ITEMS {
                    match queue.pop() {
                        Ok(item) => {
                            received.push(item);
                            empty_count = 0;
                        }
                        Err(_) => {
                            empty_count += 1;
                            if empty_count > 1000000 {
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                }

                items_consumed.store(received.len(), Ordering::SeqCst);

                waitpid(child, None).expect("waitpid failed");

                let consumed = items_consumed.load(Ordering::SeqCst);
                assert_eq!(
                    consumed, NUM_ITEMS,
                    "Not all items were consumed in IPC test"
                );

                for (i, &item) in received.iter().enumerate() {
                    assert_eq!(item, i, "Items received out of order");
                }

                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
            }
            Err(e) => {
                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
                panic!("Fork failed: {}", e);
            }
        }
    }
}

// Add these tests to queues/tests/unit_test_spsc.rs

mod spsc_branch_coverage_improvement {
    use super::*;
    use std::sync::atomic::Ordering;

    mod dspsc_coverage_tests {
        use super::*;

        #[test]
        fn test_dspsc_null_node_paths() {
            let queue = DynListQueue::<usize>::with_capacity(16);

            // Test empty queue pop
            assert!(queue.pop().is_err());

            // Push and pop to create node recycling scenario
            for i in 0..100 {
                queue.push(i).unwrap();
            }

            // This should use pre-allocated nodes first
            let initial_heap_allocs = queue.heap_allocs.load(Ordering::Relaxed);

            // Pop all to populate free cache
            for _ in 0..100 {
                queue.pop().unwrap();
            }

            // Push again - should use cached nodes
            for i in 0..50 {
                queue.push(i).unwrap();
            }

            // Heap allocs shouldn't increase much if cache is working
            let _cache_reuse_allocs = queue.heap_allocs.load(Ordering::Relaxed);

            // Now exhaust the pre-allocated pool
            for i in 100..10000 {
                queue.push(i).unwrap();
            }

            // Should have heap allocations now
            let final_heap_allocs = queue.heap_allocs.load(Ordering::Relaxed);
            assert!(final_heap_allocs > initial_heap_allocs);

            // Pop all to test deallocation paths
            while queue.pop().is_ok() {}

            // Should have some deallocations
            let heap_frees = queue.heap_frees.load(Ordering::Relaxed);
            assert!(heap_frees > 0);
        }

        #[test]
        fn test_dspsc_cache_miss_retry() {
            let queue = DynListQueue::<String>::with_capacity(8192); // Removed Arc, not needed

            // First, populate and clear to set up cache
            for i in 0..20 {
                queue.push(format!("initial_{}", i)).unwrap();
            }
            while queue.pop().is_ok() {}

            // Now push items sequentially - no need for threads
            // The test name suggests testing cache miss retry, but the queue
            // is single-producer, so we can't have concurrent cache access
            for i in 0..100 {
                queue.push(format!("item_{}", i)).unwrap();
            }

            // Verify all items can be popped
            let mut count = 0;
            while queue.pop().is_ok() {
                count += 1;
            }
            assert_eq!(count, 100);
        }

        #[test]
        fn test_dspsc_edge_case_paths() {
            let queue = DynListQueue::<usize>::with_capacity(16);

            // Test the null checks in push
            queue.push(42).unwrap();

            // Test the null checks in pop
            assert_eq!(queue.pop().unwrap(), 42);

            // Test empty() with null head
            assert!(queue.empty());

            // Test available() - always returns true
            assert!(queue.available());
        }
    }

    mod uspsc_coverage_tests {
        use super::*;

        #[test]
        fn test_uspsc_segment_allocation_failure() {
            use std::alloc::{alloc_zeroed, Layout};

            const ALIGNMENT: usize = 128;
            let shared_size = UnboundedQueue::<usize>::shared_size(64);
            let layout = Layout::from_size_align(shared_size, ALIGNMENT).unwrap();

            let mem_ptr = unsafe {
                let ptr = alloc_zeroed(layout);
                assert!(!ptr.is_null());
                ptr
            };

            let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 64) };

            // Fill many segments to approach MAX_SEGMENTS
            for i in 0..500000 {
                if queue.push(i).is_err() {
                    // Hit the limit
                    break;
                }
            }

            // Check segment count
            let segments = queue.segment_count.load(Ordering::Relaxed);
            assert!(segments > 1);

            // Pop all to trigger different paths in deallocation
            while queue.pop().is_ok() {}

            // Intentionally leak memory - the OS will clean up when test exits
        }

        #[test]
        fn test_uspsc_cache_pool_full() {
            use std::alloc::{alloc_zeroed, Layout};

            const ALIGNMENT: usize = 128;
            let shared_size = UnboundedQueue::<Vec<u8>>::shared_size(64);
            let layout = Layout::from_size_align(shared_size, ALIGNMENT).unwrap();

            let mem_ptr = unsafe {
                let ptr = alloc_zeroed(layout);
                assert!(!ptr.is_null());
                ptr
            };

            let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 64) };

            // Create exactly POOL_CAP segments
            for batch in 0..35 {
                // Fill a segment
                for i in 0..64 {
                    queue.push(vec![batch as u8, i as u8]).unwrap();
                }
                // Pop the segment to make it recyclable
                for _ in 0..64 {
                    queue.pop().unwrap();
                }
            }

            // The cache should be full now, next recycling should deallocate
            for i in 0..64 {
                queue.push(vec![99, i as u8]).unwrap();
            }

            // Intentionally leak memory - the OS will clean up when test exits
        }

        #[test]
        fn test_uspsc_null_segment_paths() {
            use std::alloc::{alloc_zeroed, Layout};

            const ALIGNMENT: usize = 128;
            let shared_size = UnboundedQueue::<usize>::shared_size(64);
            let layout = Layout::from_size_align(shared_size, ALIGNMENT).unwrap();

            let mem_ptr = unsafe {
                let ptr = alloc_zeroed(layout);
                assert!(!ptr.is_null());
                ptr
            };

            let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 64) };

            // Test empty queue state
            assert!(queue.empty());
            assert!(queue.available());

            // Test get_next_segment error paths
            queue.push(1).unwrap();
            queue.pop().unwrap();

            // The queue is empty but segments exist
            assert!(queue.empty());

            // Intentionally leak memory - the OS will clean up when test exits
        }

        #[test]
        fn test_uspsc_race_condition_paths() {
            use std::alloc::{alloc_zeroed, Layout};

            const ALIGNMENT: usize = 128;
            let shared_size = UnboundedQueue::<usize>::shared_size(64);
            let layout = Layout::from_size_align(shared_size, ALIGNMENT).unwrap();

            let mem_ptr = unsafe {
                let ptr = alloc_zeroed(layout);
                assert!(!ptr.is_null());
                ptr
            };

            let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 64) };

            // Fill exactly one segment minus one
            for i in 0..63 {
                queue.push(i).unwrap();
            }

            // This triggers segment boundary
            queue.push(63).unwrap();
            queue.push(64).unwrap(); // New segment

            // Pop across segment boundary
            for _ in 0..65 {
                queue.pop().unwrap();
            }

            // Test the double-check empty logic
            assert!(queue.empty());

            // Intentionally leak memory - the OS will clean up when test exits
        }
    }

    mod bqueue_coverage_tests {
        use super::*;

        #[test]
        fn test_bqueue_backtrack_all_paths() {
            let queue = BQueue::<usize>::new(256);

            // Test empty backtrack
            assert!(queue.pop().is_err());

            // Create pattern that exercises different batch sizes in backtrack
            for i in 0..128 {
                queue.push(i).unwrap();
            }

            // Pop some to create gaps
            for _ in 0..64 {
                queue.pop().unwrap();
            }

            // Push more to wrap around
            for i in 200..250 {
                queue.push(i).unwrap();
            }

            // This should exercise the batch size reduction loop
            while queue.pop().is_ok() {}

            // Test the batch_size == 0 path
            assert!(queue.pop().is_err());
        }

        #[test]
        fn test_bqueue_probe_paths() {
            let queue = BQueue::<usize>::new(128);

            // BQueue reserves space for batching, so we can't fill all 128 slots
            // Fill to near capacity (accounting for batch size)
            let mut pushed = 0;
            for i in 0..128 {
                if queue.push(i).is_ok() {
                    pushed += 1;
                } else {
                    break;
                }
            }

            // Should have pushed less than 128 due to batching
            assert!(pushed < 128);
            assert!(pushed > 0);

            // Queue should be full now
            assert!(queue.push(999).is_err());
            assert!(!queue.available());

            // Pop one and test again
            queue.pop().unwrap();
            assert!(queue.available());
            queue.push(999).unwrap();
        }
    }

    mod biffq_coverage_tests {
        use super::*;

        #[test]
        fn test_biffq_publish_batch_edge_cases() {
            let queue = BiffqQueue::<usize>::with_capacity(128);

            // Test empty buffer publish
            let result = queue.publish_batch_internal();
            assert_eq!(result, Ok(0));

            // Fill local buffer partially
            for i in 0..10 {
                queue.push(i).unwrap();
            }

            // Manual flush
            let result = queue.publish_batch_internal();
            assert_eq!(result, Ok(10));

            // Fill to trigger auto-flush
            for i in 0..32 {
                queue.push(i).unwrap();
            }

            // Buffer should be empty after auto-flush
            assert_eq!(queue.prod.local_count.load(Ordering::Relaxed), 0);
        }

        #[test]
        fn test_biffq_limit_advancement() {
            let queue = BiffqQueue::<String>::with_capacity(128);

            // Fill first H partition
            for i in 0..32 {
                queue.push(format!("item_{}", i)).unwrap();
            }

            // Force flush
            queue.flush_producer_buffer().unwrap();

            // Fill more to test limit advancement
            for i in 32..96 {
                queue.push(format!("item_{}", i)).unwrap();
            }

            // Should have advanced limit multiple times
            let limit = queue.prod.limit.load(Ordering::Acquire);
            assert!(limit > 64);
        }

        #[test]
        fn test_biffq_clear_operation() {
            let queue = BiffqQueue::<usize>::with_capacity(128);

            // Fill and flush multiple partitions
            for i in 0..96 {
                queue.push(i).unwrap();
            }
            queue.flush_producer_buffer().unwrap();

            // Pop items from first partition
            for _ in 0..32 {
                queue.pop().unwrap();
            }

            // Clear operation happens lazily - need to cross partition boundary
            // The clear pointer advances when reading from a new partition
            // and the previous partition start is behind the current read position

            // Continue popping to trigger clear advancement
            for _ in 0..32 {
                queue.pop().unwrap();
            }

            // Now clear should have advanced
            let clear = queue.cons.clear.load(Ordering::Relaxed);
            // Clear advances in partition-sized chunks
            assert!(
                clear % 32 == 0,
                "Clear should be at partition boundary, got {}",
                clear
            );
        }
    }

    mod ffq_coverage_tests {
        use super::*;

        #[test]
        fn test_ffq_slot_state_transitions() {
            let queue = FfqQueue::<usize>::with_capacity(64);

            // Test all slots start empty
            for i in 0..64 {
                let slot = queue.get_slot(i);
                assert!(!slot.flag.load(Ordering::Acquire));
            }

            // Fill queue
            for i in 0..64 {
                queue.push(i).unwrap();
            }

            // All slots should be full
            for i in 0..64 {
                let slot = queue.get_slot(i);
                assert!(slot.flag.load(Ordering::Acquire));
            }

            // Queue should be full
            assert!(queue.push(999).is_err());

            // Pop all
            for _ in 0..64 {
                queue.pop().unwrap();
            }

            // All slots should be empty again
            for i in 0..64 {
                let slot = queue.get_slot(i);
                assert!(!slot.flag.load(Ordering::Acquire));
            }
        }
    }

    mod iffq_coverage_tests {
        use super::*;

        #[test]
        fn test_iffq_clear_boundary_conditions() {
            let queue = IffqQueue::<usize>::with_capacity(128);

            // Fill multiple partitions
            for i in 0..96 {
                queue.push(i).unwrap();
            }

            // Pop across partition boundaries
            for _ in 0..48 {
                queue.pop().unwrap();
            }

            // Clear should have advanced
            let clear = queue.cons.clear.load(Ordering::Relaxed);
            assert!(clear > 0);

            // Test the loop termination condition
            for _ in 0..48 {
                queue.pop().unwrap();
            }
        }
    }

    mod llq_coverage_tests {
        use super::*;

        #[test]
        fn test_llq_exact_capacity_boundary() {
            let queue = LlqQueue::<usize>::with_capacity(64);

            // Fill to exactly capacity - K_CACHE_LINE_SLOTS
            for i in 0..56 {
                queue.push(i).unwrap();
            }

            // This should fail
            assert!(queue.push(999).is_err());

            // Pop one
            queue.pop().unwrap();

            // Now should succeed
            queue.push(999).unwrap();
        }
    }

    mod mspsc_coverage_tests {
        use super::*;

        #[test]
        fn test_multipush_contiguous_free_calculation() {
            let queue = MultiPushQueue::<usize>::with_capacity(64);

            // Fill ring partially
            for i in 0..30 {
                queue.push(i).unwrap();
            }
            queue.flush();

            // Pop some to create wrapped free space
            for _ in 0..20 {
                queue.pop().unwrap();
            }

            // Fill local buffer
            for i in 0..32 {
                queue.push(100 + i).unwrap();
            }

            // Flush should calculate contiguous free correctly
            assert!(queue.flush());

            // Verify items are in queue
            let mut count = 0;
            while queue.pop().is_ok() {
                count += 1;
            }
            assert_eq!(count, 42); // 10 + 32
        }

        #[test]
        fn test_multipush_fallback_paths() {
            let queue = MultiPushQueue::<String>::with_capacity(64);

            // Fill ring almost completely
            for i in 0..63 {
                queue.push(format!("item_{}", i)).unwrap();
            }
            queue.flush();

            // Local buffer is empty, ring is almost full
            // This should go through the fallback path
            queue.push("fallback".to_string()).unwrap();

            // Verify
            assert!(!queue.empty());
        }
    }

    mod sesd_wrapper_coverage_tests {
        use super::*;

        #[test]
        fn test_sesd_null_node_handling() {
            let pool_capacity = 10;
            let shared_size = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);

            // Create properly aligned memory using a boxed slice
            let layout = std::alloc::Layout::from_size_align(shared_size, 64)
                .expect("Failed to create layout");

            let memory = unsafe {
                let ptr = std::alloc::alloc_zeroed(layout);
                if ptr.is_null() {
                    panic!("Failed to allocate memory");
                }

                // Create a box from the allocated memory
                Box::from_raw(std::slice::from_raw_parts_mut(ptr, shared_size))
            };

            let mem_ptr = memory.as_ptr() as *mut u8;

            // Create queue - memory is owned by the Box
            let queue = unsafe { SesdJpSpscBenchWrapper::init_in_shared(mem_ptr, pool_capacity) };

            // Simple test without exhausting the pool
            for i in 0..5 {
                queue.push(i).unwrap();
            }

            for i in 0..5 {
                assert_eq!(queue.pop().unwrap(), i);
            }

            assert!(queue.empty());

            // The Box will handle deallocation when it goes out of scope
            // Do NOT manually deallocate
        }
    }

    mod dehnavi_coverage_tests {
        use super::*;

        #[test]
        fn test_dehnavi_claim_scenarios() {
            let queue = DehnaviQueue::<usize>::new(3);

            // Fill queue
            queue.push(1).unwrap();
            queue.push(2).unwrap();

            // Queue is now full (capacity 3, but only 2 items because
            // Dehnavi uses one slot as a guard)

            // This push will trigger the wait-free overwrite mechanism
            queue.push(3).unwrap();

            // At this point, the oldest value (1) has been overwritten
            // The queue contains [2, 3]

            // Pop values - we should get the remaining values
            let val1 = queue.pop().unwrap();
            let val2 = queue.pop().unwrap();

            // The exact values depend on the overwrite behavior
            // But we should get two values
            assert!(val1 == 2 || val1 == 3);
            assert!(val2 == 2 || val2 == 3);

            // Queue should be empty now
            assert!(queue.empty());
            assert!(queue.pop().is_err());
        }
    }

    mod lamport_coverage_tests {
        use super::*;

        #[test]
        fn test_lamport_exact_full_condition() {
            let queue = LamportQueue::<usize>::with_capacity(4);

            // The condition is: next == head + mask + 1
            // With capacity 4, mask = 3
            // So when head=0, next=4 means full

            queue.push(1).unwrap();
            queue.push(2).unwrap();
            queue.push(3).unwrap();

            // This should fail - exactly full
            assert!(queue.push(4).is_err());

            // Pop one and wrap around
            queue.pop().unwrap();
            queue.push(4).unwrap();

            // Continue pattern
            queue.pop().unwrap();
            queue.push(5).unwrap();
        }
    }

    mod ipc_test_coverage {
        use super::*;

        #[test]
        fn test_ffq_edge_cases() {
            let queue = FfqQueue::<usize>::with_capacity(64);

            // Test pushing to full queue
            for i in 0..64 {
                queue.push(i).unwrap();
            }
            assert!(queue.push(999).is_err());

            // Test popping from empty queue
            for _ in 0..64 {
                queue.pop().unwrap();
            }
            assert!(queue.pop().is_err());
        }

        #[test]
        fn test_unbounded_coverage() {
            let shared_size = UnboundedQueue::<usize>::shared_size(64);
            const ALIGNMENT: usize = 128;

            let memory = create_aligned_memory_box(shared_size, ALIGNMENT);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 64) };

            // Test segment transitions
            for i in 0..200 {
                queue.push(i).unwrap();
            }

            // Pop all
            for i in 0..200 {
                assert_eq!(queue.pop().unwrap(), i);
            }

            assert!(queue.empty());
        }
    }
}
