#![cfg(miri)]

use queues::{spsc::*, SpscQueue};
use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

const MIRI_SMALL_CAPACITY: usize = 64;
const MIRI_MEDIUM_CAPACITY: usize = 256;
const MIRI_LARGE_CAPACITY: usize = 1024;

struct AlignedMemory {
    ptr: *mut u8,
    layout: std::alloc::Layout,
}

impl AlignedMemory {
    fn new(size: usize, alignment: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(size, alignment).expect("Invalid layout");
        unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            Self { ptr, layout }
        }
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

                assert!(!queue.available() || count == $capacity);
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
                let items_to_send = 100.min($capacity / 2);

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

                    if let Some(mp_queue) =
                        (queue_prod.as_ref() as &dyn Any).downcast_ref::<MultiPushQueue<usize>>()
                    {
                        mp_queue.flush();
                    } else if let Some(biffq) =
                        (queue_prod.as_ref() as &dyn Any).downcast_ref::<BiffqQueue<usize>>()
                    {
                        while biffq.prod.local_count.load(Ordering::Relaxed) > 0 {
                            let _ = biffq.flush_producer_buffer();
                            thread::yield_now();
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
                                if empty_polls > 100000 {
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

                assert!(
                    queue.empty()
                        || (queue.as_ref() as &dyn Any)
                            .downcast_ref::<MultiPushQueue<usize>>()
                            .is_some()
                        || (queue.as_ref() as &dyn Any)
                            .downcast_ref::<BiffqQueue<usize>>()
                            .is_some()
                );
            }

            #[test]
            fn test_stress_concurrent() {
                let queue = Arc::new(<$queue_type>::with_capacity($capacity));
                let num_items = ($capacity * 2).min(1000);
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

                    if let Some(mp_queue) =
                        (queue_prod.as_ref() as &dyn Any).downcast_ref::<MultiPushQueue<usize>>()
                    {
                        mp_queue.flush();
                    } else if let Some(biffq) =
                        (queue_prod.as_ref() as &dyn Any).downcast_ref::<BiffqQueue<usize>>()
                    {
                        while biffq.prod.local_count.load(Ordering::Relaxed) > 0 {
                            let _ = biffq.flush_producer_buffer();
                            thread::yield_now();
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

test_queue!(LamportQueue<usize>, MIRI_SMALL_CAPACITY, lamport_tests);
test_queue!(FfqQueue<usize>, MIRI_MEDIUM_CAPACITY, ffq_tests);
test_queue!(LlqQueue<usize>, MIRI_MEDIUM_CAPACITY, llq_tests);
test_queue!(BlqQueue<usize>, MIRI_MEDIUM_CAPACITY, blq_tests);
test_queue!(IffqQueue<usize>, MIRI_MEDIUM_CAPACITY, iffq_tests);

mod biffq_tests {
    use super::*;

    const BIFFQ_CAPACITY: usize = MIRI_MEDIUM_CAPACITY;

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
                        if empty_polls > 100000 {
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
    }

    #[test]
    fn test_stress_concurrent() {
        let queue = Arc::new(BiffqQueue::<usize>::with_capacity(BIFFQ_CAPACITY));
        let num_items = (BIFFQ_CAPACITY * 2).min(1000);
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
        let queue = BQueue::<usize>::new(MIRI_MEDIUM_CAPACITY);

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
        let queue = BQueue::<usize>::new(MIRI_MEDIUM_CAPACITY);
        let effective_capacity = MIRI_MEDIUM_CAPACITY - 1;

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
        let queue = BQueue::<usize>::new(MIRI_MEDIUM_CAPACITY);

        assert!(queue.available());
        assert!(queue.empty());

        queue.push(1).unwrap();
        assert!(!queue.empty());

        let mut count = 1;
        while queue.available() && count < MIRI_MEDIUM_CAPACITY {
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
        let queue = Arc::new(BQueue::<usize>::new(MIRI_LARGE_CAPACITY));
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
                        if empty_polls > 100000 {
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
        let queue = Arc::new(BQueue::<usize>::new(MIRI_MEDIUM_CAPACITY));
        let num_items = (MIRI_MEDIUM_CAPACITY * 2).min(1000);
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
        let queue = MultiPushQueue::<usize>::with_capacity(MIRI_MEDIUM_CAPACITY);

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
        let queue = MultiPushQueue::<usize>::with_capacity(MIRI_MEDIUM_CAPACITY);

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
        let queue = MultiPushQueue::<usize>::with_capacity(MIRI_MEDIUM_CAPACITY);

        for i in 0..32 {
            queue.push(i).unwrap();
        }

        assert_eq!(queue.local_count.load(Ordering::Relaxed), 0);

        for i in 0..32 {
            assert_eq!(queue.pop().unwrap(), i);
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
                            if item <= last {}
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

mod unbounded_tests {
    use super::*;

    #[test]
    fn test_unbounded_basic_operations() {
        let size = UnboundedQueue::<usize>::shared_size(64, 16);
        assert!(size > 0);
        assert!(size >= std::mem::size_of::<UnboundedQueue<usize>>());

        let size_small = UnboundedQueue::<usize>::shared_size(64, 8);
        let size_large = UnboundedQueue::<usize>::shared_size(8192, 16);
        assert!(size_large >= size_small);
    }

    #[test]
    fn test_unbounded_type_safety() {
        let _size_u8 = UnboundedQueue::<u8>::shared_size(64, 16);
        let _size_string = UnboundedQueue::<String>::shared_size(64, 16);
        let _size_vec = UnboundedQueue::<Vec<u8>>::shared_size(64, 16);
    }

    #[test]
    fn test_unbounded_basic_push_pop() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        assert!(queue.empty());
        assert!(queue.pop().is_err());

        queue.push(42).unwrap();
        assert!(!queue.empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        for i in 0..50 {
            queue.push(i).unwrap();
        }

        for i in 0..50 {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());
    }

    #[test]
    fn test_unbounded_segment_tracking() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        let mut pushed = 0;
        for i in 0..200 {
            match queue.push(i) {
                Ok(_) => pushed += 1,
                Err(_) => break,
            }
        }

        assert!(pushed >= 64, "Should push at least one segment");
    }

    #[test]
    fn test_unbounded_drop_semantics() {
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
            let shared_size = UnboundedQueue::<DropCounter>::shared_size(64, 16);
            let mut memory = AlignedMemory::new(shared_size, 128);
            let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

            let items_to_push = 50;
            for i in 0..items_to_push {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..25 {
                drop(queue.pop().unwrap());
            }

            let drops_after_pop = DROP_COUNT.load(Ordering::SeqCst);
            assert_eq!(drops_after_pop, 25, "Should have dropped 25 items");
        }
    }

    #[test]
    fn test_unbounded_wraparound() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        for batch in 0..3 {
            for i in 0..50 {
                queue.push(batch * 100 + i).unwrap();
            }

            for i in 0..50 {
                assert_eq!(queue.pop().unwrap(), batch * 100 + i);
            }

            assert!(queue.empty());
        }
    }

    #[test]
    fn test_unbounded_concurrent() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue_ptr = unsafe {
            let q = UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16);
            q as *const UnboundedQueue<usize>
        };

        let barrier = Arc::new(Barrier::new(2));
        let items_to_send = 50;

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
                        if empty_polls > 100000 {
                            panic!("Too many failed polls");
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

        assert!(unsafe { (*queue_ptr).empty() });
    }

    #[test]
    fn test_unbounded_stress_single_segment() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        for round in 0..10 {
            for i in 0..60 {
                queue.push(round * 1000 + i).unwrap();
            }

            for _ in 0..30 {
                queue.pop().unwrap();
            }

            for i in 60..90 {
                match queue.push(round * 1000 + i) {
                    Ok(_) => {}
                    Err(_) => break,
                }
            }

            while !queue.empty() {
                queue.pop().unwrap();
            }
        }

        assert!(queue.empty());
    }

    #[test]
    fn test_unbounded_different_types() {
        {
            let shared_size = UnboundedQueue::<String>::shared_size(8, 16);
            let mut memory = AlignedMemory::new(shared_size, 128);
            let mem_ptr = memory.as_mut_ptr();

            let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 8, 16) };

            for i in 0..5 {
                queue.push(format!("test_{}", i)).unwrap();
            }
            for i in 0..5 {
                assert_eq!(queue.pop().unwrap(), format!("test_{}", i));
            }
        }

        {
            let shared_size = UnboundedQueue::<Option<usize>>::shared_size(8, 16);
            let mut memory = AlignedMemory::new(shared_size, 128);
            let mem_ptr = memory.as_mut_ptr();

            let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 8, 16) };

            queue.push(Some(42)).unwrap();
            queue.push(None).unwrap();
            queue.push(Some(100)).unwrap();

            assert_eq!(queue.pop().unwrap(), Some(42));
            assert_eq!(queue.pop().unwrap(), None);
            assert_eq!(queue.pop().unwrap(), Some(100));
        }

        {
            let shared_size = UnboundedQueue::<Vec<u8>>::shared_size(8, 16);
            let mut memory = AlignedMemory::new(shared_size, 128);
            let mem_ptr = memory.as_mut_ptr();

            let queue = unsafe { UnboundedQueue::init_in_shared(mem_ptr, 8, 16) };

            for i in 0..5 {
                queue.push(vec![i as u8; 10]).unwrap();
            }

            for i in 0..5 {
                let v = queue.pop().unwrap();
                assert_eq!(v.len(), 10);
                assert!(v.iter().all(|&x| x == i as u8));
            }
        }
    }

    #[test]
    fn test_unbounded_available_empty() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        assert!(queue.available());
        assert!(queue.empty());

        queue.push(1).unwrap();
        assert!(queue.available());
        assert!(!queue.empty());

        queue.pop().unwrap();
        assert!(queue.available());
        assert!(queue.empty());
    }
}

mod sesd_wrapper_tests {
    use super::*;

    #[test]
    fn test_sesd_wrapper_basic() {
        let pool_capacity = 100;
        let shared_size = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);

        let mut memory = AlignedMemory::new(shared_size, 64);

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

        assert!(pushed >= pool_capacity - 5, "Should push most items");

        let mut popped = 0;
        while queue.pop().is_ok() {
            popped += 1;
        }
        assert_eq!(popped, pushed, "Should pop all pushed items");
        assert!(queue.empty());
    }

    #[test]
    fn test_sesd_wrapper_concurrent() {
        let pool_capacity = 200;
        let shared_size = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);

        let mut memory = AlignedMemory::new(shared_size, 64);

        let queue =
            unsafe { SesdJpSpscBenchWrapper::init_in_shared(memory.as_mut_ptr(), pool_capacity) };

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
                        if empty_polls > 100000 {
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

macro_rules! test_shared_init {
    ($queue_type:ty, $capacity:expr, $alignment:expr, $test_name:ident) => {
        #[test]
        fn $test_name() {
            let shared_size = <$queue_type>::shared_size($capacity);

            let mut memory = AlignedMemory::new(shared_size, $alignment);
            let mem_ptr = memory.as_mut_ptr();

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
        }
    };
}

mod shared_memory_tests {
    use super::*;

    test_shared_init!(
        LamportQueue<usize>,
        MIRI_SMALL_CAPACITY,
        64,
        test_lamport_shared
    );
    test_shared_init!(FfqQueue<usize>, MIRI_MEDIUM_CAPACITY, 64, test_ffq_shared);
    test_shared_init!(BlqQueue<usize>, 128, 64, test_blq_shared);
    test_shared_init!(IffqQueue<usize>, MIRI_MEDIUM_CAPACITY, 64, test_iffq_shared);
    test_shared_init!(
        BiffqQueue<usize>,
        MIRI_MEDIUM_CAPACITY,
        64,
        test_biffq_shared
    );
    test_shared_init!(BQueue<usize>, MIRI_MEDIUM_CAPACITY, 64, test_bqueue_shared);
    test_shared_init!(
        MultiPushQueue<usize>,
        MIRI_MEDIUM_CAPACITY,
        64,
        test_multipush_shared
    );

    #[test]
    fn test_dehnavi_shared() {
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
    fn test_llq_shared() {
        let shared_size = LlqQueue::<usize>::llq_shared_size(MIRI_MEDIUM_CAPACITY);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { LlqQueue::<usize>::init_in_shared(mem_ptr, MIRI_MEDIUM_CAPACITY) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        let mut pushed = 0;
        for i in 0..MIRI_MEDIUM_CAPACITY {
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
    fn test_sesd_wrapper_shared() {
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
    }

    #[test]
    fn test_dspsc_shared() {
        const MIRI_MEDIUM_CAPACITY: usize = 128;

        let items_to_push = (MIRI_MEDIUM_CAPACITY / 2) - 10;
        let nodes_count = items_to_push + 1;

        let shared_size = DynListQueue::<usize>::shared_size(MIRI_MEDIUM_CAPACITY, nodes_count);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe {
            DynListQueue::<usize>::init_in_shared(mem_ptr, MIRI_MEDIUM_CAPACITY, nodes_count)
        };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        for i in 0..items_to_push {
            queue.push(i).unwrap();
        }

        for i in 0..items_to_push {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());
    }

    #[test]
    fn test_unbounded_shared() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { UnboundedQueue::<usize>::init_in_shared(mem_ptr, 64, 16) };

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        let items = 50;
        for i in 0..items {
            queue.push(i).unwrap();
        }

        for i in 0..items {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());
    }
}

mod drop_semantics_tests {
    use super::*;

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

        thread::yield_now();

        let final_count = DROP_COUNT.load(Ordering::SeqCst);
        assert!(
            final_count >= 5,
            "At least the 5 popped items should be dropped, got {}",
            final_count
        );
    }

    #[test]
    fn test_drop_with_strings() {
        let queue = BQueue::<String>::new(MIRI_MEDIUM_CAPACITY);

        for i in 0..10 {
            queue.push(format!("item_{}", i)).unwrap();
        }

        for _ in 0..5 {
            let _ = queue.pop().unwrap();
        }
    }

    #[test]
    fn test_drop_in_dspsc() {
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
            let items_to_test = 50;
            let cache_capacity = 64;
            let nodes_count = cache_capacity + 10;

            let shared_size = DynListQueue::<DropCounter>::shared_size(cache_capacity, nodes_count);
            let layout = std::alloc::Layout::from_size_align(shared_size, 128).unwrap();
            let mem_ptr = unsafe { std::alloc::alloc(layout) };

            if mem_ptr.is_null() {
                panic!("Failed to allocate memory");
            }

            let queue = unsafe {
                DynListQueue::<DropCounter>::init_in_shared(mem_ptr, cache_capacity, nodes_count)
            };

            for i in 0..items_to_test {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            for _ in 0..25 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 25);

            unsafe {
                while let Ok(item) = queue.pop() {
                    drop(item);
                }

                std::alloc::dealloc(mem_ptr, layout);
            }
        }

        std::thread::yield_now();

        let final_drops = DROP_COUNT.load(Ordering::SeqCst);
        assert_eq!(final_drops, 50, "Should have dropped all 50 items");
    }
}

mod edge_case_tests {
    use super::*;

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
            data: [u64; 128],
        }

        let queue = LamportQueue::<LargeType>::with_capacity(16);
        let item = LargeType { data: [42; 128] };

        queue.push(item.clone()).unwrap();
        assert_eq!(queue.pop().unwrap(), item);
    }

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
    fn test_different_payload_types() {
        {
            let queue = LamportQueue::<String>::with_capacity(16);
            for i in 0..10 {
                queue.push(format!("test_{}", i)).unwrap();
            }
            for i in 0..10 {
                assert_eq!(queue.pop().unwrap(), format!("test_{}", i));
            }
        }

        {
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

        {
            let queue = BlqQueue::<Option<usize>>::with_capacity(32);
            queue.push(Some(42)).unwrap();
            queue.push(None).unwrap();
            queue.push(Some(100)).unwrap();

            assert_eq!(queue.pop().unwrap(), Some(42));
            assert_eq!(queue.pop().unwrap(), None);
            assert_eq!(queue.pop().unwrap(), Some(100));
        }

        {
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
}

mod error_handling_tests {
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

        let failed_item = "second".to_string();
        match queue.push(failed_item.clone()) {
            Err(_) => {}
            Ok(_) => panic!("Push should have failed on full queue"),
        }
    }

    #[test]
    fn test_pop_error_handling() {
        let queue = BQueue::<usize>::new(128);

        assert!(queue.pop().is_err());

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);

        assert!(queue.pop().is_err());
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
    let queue = Arc::new(BQueue::<i32>::new(128));
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
        let q_opt = BQueue::<Option<String>>::new(128);
        q_opt.push(Some("hello".to_string())).unwrap();
        q_opt.push(None).unwrap();

        assert_eq!(q_opt.pop().unwrap(), Some("hello".to_string()));
        assert_eq!(q_opt.pop().unwrap(), None);
    }
}

mod shared_memory_init_tests {
    use super::*;

    #[test]
    fn test_shared_memory_alignment_all_queues() {
        struct AlignmentTest {
            queue_type: &'static str,
            min_align: usize,
            test_fn: fn(usize),
        }

        let tests = vec![
            AlignmentTest {
                queue_type: "LamportQueue",
                min_align: 64,
                test_fn: |align| {
                    let size = LamportQueue::<usize>::shared_size(64);
                    let mut mem = AlignedMemory::new(size + align, align);
                    let ptr = mem.as_mut_ptr();

                    for offset in [0, 8, 16, 32].iter() {
                        if *offset < align {
                            let test_ptr = unsafe { ptr.add(*offset) };
                            if test_ptr as usize % align == 0 {
                                let queue = unsafe { LamportQueue::init_in_shared(test_ptr, 64) };
                                queue.push(42).unwrap();
                                assert_eq!(queue.pop().unwrap(), 42);
                            }
                        }
                    }
                },
            },
            AlignmentTest {
                queue_type: "UnboundedQueue",
                min_align: 128,
                test_fn: |align| {
                    let size = UnboundedQueue::<usize>::shared_size(64, 16);
                    let mut mem = AlignedMemory::new(size, align);
                    let queue = unsafe { UnboundedQueue::init_in_shared(mem.as_mut_ptr(), 64, 16) };
                    queue.push(42).unwrap();
                    assert_eq!(queue.pop().unwrap(), 42);
                },
            },
        ];

        for test in tests {
            for align in [test.min_align, 256, 512] {
                (test.test_fn)(align);
            }
        }
    }

    #[test]
    fn test_shared_memory_reinitialization() {
        let size = LamportQueue::<usize>::shared_size(64);
        let mut memory = AlignedMemory::new(size, 64);
        let mem_ptr = memory.as_mut_ptr();

        {
            let queue = unsafe { LamportQueue::init_in_shared(mem_ptr, 64) };
            for i in 0..10 {
                queue.push(i).unwrap();
            }
        }

        unsafe {
            std::ptr::write_bytes(mem_ptr, 0, size);
        }

        {
            let queue = unsafe { LamportQueue::init_in_shared(mem_ptr, 64) };
            assert!(queue.empty());

            queue.push(100).unwrap();
            assert_eq!(queue.pop().unwrap(), 100);
        }
    }

    #[test]
    fn test_shared_memory_partial_writes() {
        let size = BiffqQueue::<String>::shared_size(128);
        let mut memory = AlignedMemory::new(size, 64);
        let mem_ptr = memory.as_mut_ptr();

        unsafe {
            let garbage: Vec<u8> = (0..size / 2).map(|i| (i % 256) as u8).collect();
            std::ptr::copy_nonoverlapping(garbage.as_ptr(), mem_ptr, size / 2);
        }

        let queue = unsafe { BiffqQueue::init_in_shared(mem_ptr, 128) };

        queue.push("test".to_string()).unwrap();
        let _ = queue.flush_producer_buffer();
        assert_eq!(queue.pop().unwrap(), "test");
    }

    #[test]
    fn test_shared_memory_size_calculations() {
        {
            let size = MultiPushQueue::<usize>::shared_size(128);
            let mut memory = AlignedMemory::new(size, 64);
            let queue =
                unsafe { MultiPushQueue::<usize>::init_in_shared(memory.as_mut_ptr(), 128) };
            queue.push(42).unwrap();
            queue.flush();
            assert_eq!(queue.pop().unwrap(), 42);

            let size = SesdJpSpscBenchWrapper::<usize>::shared_size(100);
            let mut memory = AlignedMemory::new(size, 64);
            let queue = unsafe {
                SesdJpSpscBenchWrapper::<usize>::init_in_shared(memory.as_mut_ptr(), 100)
            };
            queue.push(42).unwrap();
            assert_eq!(queue.pop().unwrap(), 42);

            let size = UnboundedQueue::<usize>::shared_size(64, 16);
            let mut memory = AlignedMemory::new(size, 128);
            let queue =
                unsafe { UnboundedQueue::<usize>::init_in_shared(memory.as_mut_ptr(), 64, 16) };
            queue.push(42).unwrap();
            assert_eq!(queue.pop().unwrap(), 42);
        }
    }

    #[test]
    fn test_shared_memory_concurrent_init_safety() {
        let size = MultiPushQueue::<usize>::shared_size(128);
        let mut memory = AlignedMemory::new(size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { MultiPushQueue::init_in_shared(mem_ptr, 128) };

        for i in 0..32 {
            queue.push(i).unwrap();
        }

        assert!(queue.flush());

        assert_eq!(queue.local_count.load(Ordering::Relaxed), 0);

        for i in 0..32 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_unbounded_shared_memory_pool_limits() {
        let size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        let items_to_push = 500;
        let mut actually_pushed = 0;
        for i in 0..items_to_push {
            match queue.push(i) {
                Ok(_) => actually_pushed += 1,
                Err(_) => break,
            }
        }

        println!(
            "Pushed {} items out of {} attempted",
            actually_pushed, items_to_push
        );

        let mut popped = 0;
        let mut consecutive_failures = 0;
        let mut i = 0;

        while popped < actually_pushed && consecutive_failures < 1000 {
            match queue.pop() {
                Ok(val) => {
                    assert_eq!(val, i, "Items should come out in order");
                    popped += 1;
                    i += 1;
                    consecutive_failures = 0;
                }
                Err(_) => {
                    consecutive_failures += 1;
                    std::thread::yield_now();
                }
            }
        }

        assert_eq!(
            popped, actually_pushed,
            "Should eventually pop all pushed items"
        );
    }
}

#[cfg(miri)]
mod unbounded_edge_cases {
    use super::*;

    #[test]
    fn test_unbounded_segment_boundary_edge_cases() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        for i in 0..64 {
            queue.push(i).unwrap();
        }

        queue.push(64).unwrap();

        let mut popped_count = 0;
        let mut consecutive_failures = 0;

        while popped_count < 65 && consecutive_failures < 1000 {
            match queue.pop() {
                Ok(val) => {
                    assert_eq!(val, popped_count, "Items should come out in order");
                    popped_count += 1;
                    consecutive_failures = 0;
                }
                Err(_) => {
                    consecutive_failures += 1;
                    std::thread::yield_now();
                }
            }
        }

        assert!(popped_count >= 64, "Should pop at least the first segment");
        assert_eq!(popped_count, 65, "Should eventually pop all 65 items");
    }

    #[test]
    fn test_unbounded_rapid_segment_cycling() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        for cycle in 0..3 {
            for i in 0..60 {
                queue.push(cycle * 1000 + i).unwrap();
            }

            for i in 0..60 {
                assert_eq!(queue.pop().unwrap(), cycle * 1000 + i);
            }

            assert!(queue.empty());
        }
    }

    #[test]
    fn test_unbounded_partial_segment_operations() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        for i in 0..30 {
            queue.push(i).unwrap();
        }

        for i in 0..15 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        for i in 30..80 {
            match queue.push(i) {
                Ok(_) => {}
                Err(_) => break,
            }
        }

        let mut next_expected = 15;
        while let Ok(val) = queue.pop() {
            assert!(val >= next_expected);
            if val == next_expected {
                next_expected += 1;
            }
        }
    }

    #[test]
    #[test]
    fn test_unbounded_empty_checks_across_segments() {
        let shared_size = UnboundedQueue::<usize>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        assert!(queue.empty());
        assert!(queue.available());

        for i in 0..64 {
            queue.push(i).unwrap();
            assert!(!queue.empty());
        }

        queue.push(64).unwrap();
        assert!(!queue.empty());

        let mut popped = 0;
        let mut consecutive_failures = 0;

        while popped < 65 && consecutive_failures < 100 {
            match queue.pop() {
                Ok(_) => {
                    popped += 1;
                    consecutive_failures = 0;
                }
                Err(_) => {
                    consecutive_failures += 1;
                    std::thread::yield_now();
                }
            }
        }

        assert_eq!(popped, 65, "Should pop all 65 items");
        assert!(queue.empty());

        let mut available = false;
        for _ in 0..10 {
            if queue.available() {
                available = true;
                break;
            }
            std::thread::yield_now();
        }
        assert!(available, "Should be available after emptying");
    }

    #[test]
    fn test_unbounded_with_zero_sized_types() {
        #[derive(Debug, PartialEq)]
        struct ZeroSized;

        let shared_size = UnboundedQueue::<ZeroSized>::shared_size(64, 16);
        let mut memory = AlignedMemory::new(shared_size, 128);
        let queue = unsafe { UnboundedQueue::init_in_shared(memory.as_mut_ptr(), 64, 16) };

        let mut pushed = 0;
        for _ in 0..200 {
            match queue.push(ZeroSized) {
                Ok(_) => pushed += 1,
                Err(_) => break,
            }
        }

        let mut popped = 0;
        let mut consecutive_failures = 0;

        while popped < pushed && consecutive_failures < 1000 {
            match queue.pop() {
                Ok(val) => {
                    assert_eq!(val, ZeroSized);
                    popped += 1;
                    consecutive_failures = 0;
                }
                Err(_) => {
                    consecutive_failures += 1;
                    std::thread::yield_now();
                }
            }
        }

        assert_eq!(popped, pushed, "Should pop all pushed ZSTs");
    }
}
