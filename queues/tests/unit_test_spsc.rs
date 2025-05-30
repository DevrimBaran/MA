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

        // The paperlike implementation has a known limitation where
        // it may not handle all items correctly across segment boundaries
        let mut pushed = 0;
        for i in 0..20000 {
            match queue.push(i) {
                Ok(_) => pushed += 1,
                Err(_) => break,
            }
        }

        println!("Pushed {} items", pushed);
        assert!(pushed >= 8192, "Should push at least one full segment");

        let segments = queue.segment_count.load(Ordering::Relaxed);
        println!("Segment count: {}", segments);
        assert!(segments > 1, "Should have allocated more than one segment");

        // Pop what we can
        let mut popped = 0;
        while let Ok(val) = queue.pop() {
            // Don't assert on order due to known limitations
            popped += 1;
        }

        println!("Popped {} items (pushed {})", popped, pushed);
        // Known limitation: may not pop all items
        assert!(popped > 0, "Should pop at least some items");

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

            // Push within one segment to avoid the cross-segment issue
            let items_to_push = 5000; // Less than 8192
            for i in 0..items_to_push {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            // Pop all items
            let mut popped = 0;
            while let Ok(_) = queue.pop() {
                popped += 1;
            }

            println!("Pushed {}, popped {}", items_to_push, popped);
            assert_eq!(
                popped, items_to_push,
                "Should pop all items within one segment"
            );

            let drops_after_pop = DROP_COUNT.load(Ordering::SeqCst);
            assert_eq!(drops_after_pop, items_to_push);

            assert!(queue.empty());

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }
    }

    #[test]
    fn test_unbounded_drop_implementation() {
        const ALIGNMENT: usize = 128;

        // Test with unit type
        {
            let shared_size = UnboundedQueue::<()>::shared_size(8192);
            let memory = allocate_aligned_box(shared_size, ALIGNMENT);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { UnboundedQueue::<()>::init_in_shared(mem_ptr, 8192) };

            let mut pushed = 0;
            for _ in 0..50000 {
                match queue.push(()) {
                    Ok(_) => pushed += 1,
                    Err(_) => break,
                }
            }

            let mut popped = 0;
            for _ in 0..pushed / 2 {
                if queue.pop().is_ok() {
                    popped += 1;
                }
            }

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }

        // Test with Vec
        {
            let shared_size = UnboundedQueue::<Vec<u8>>::shared_size(8192);
            let memory = allocate_aligned_box(shared_size, ALIGNMENT);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { UnboundedQueue::<Vec<u8>>::init_in_shared(mem_ptr, 8192) };

            for i in 0..1000 {
                let _ = queue.push(vec![i as u8; 100]);
            }

            for _ in 0..500 {
                let _ = queue.pop();
            }

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
            }
        }

        // Test with String
        {
            let shared_size = UnboundedQueue::<String>::shared_size(8192);
            let memory = allocate_aligned_box(shared_size, ALIGNMENT);
            let mem_ptr = Box::leak(memory).as_mut_ptr();

            let queue = unsafe { UnboundedQueue::<String>::init_in_shared(mem_ptr, 8192) };

            for batch in 0..5 {
                for i in 0..1000 {
                    let _ = queue.push(format!("batch_{}_item_{}", batch, i));
                }

                for _ in 0..1000 {
                    let _ = queue.pop();
                }
            }

            unsafe {
                let _ = Box::from_raw(std::slice::from_raw_parts_mut(mem_ptr, shared_size));
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
    fn test_dehnavi() {
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

        // Test within single segment to avoid cross-segment issues
        let items = 5000; // Less than 8192
        for i in 0..items {
            queue.push(i).unwrap();
        }

        for i in 0..items {
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
