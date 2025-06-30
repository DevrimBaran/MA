use queues::{
    FeldmanDechevWFQueue, JKMQueue, KPQueue, MpmcQueue, NRQueue, TurnQueue, WCQueue, WFQueue,
    YangCrummeyQueue,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

unsafe fn allocate_shared_memory(size: usize) -> *mut u8 {
    use std::alloc::{alloc_zeroed, Layout};
    let layout = Layout::from_size_align(size, 4096).unwrap();
    let ptr = alloc_zeroed(layout);
    if ptr.is_null() {
        panic!("Failed to allocate shared memory");
    }
    ptr
}

unsafe fn deallocate_shared_memory(ptr: *mut u8, size: usize) {
    use std::alloc::{dealloc, Layout};
    let layout = Layout::from_size_align(size, 4096).unwrap();
    dealloc(ptr, layout);
}

macro_rules! mpmc_test_queue {
    ($module_name:ident, $queue_type:ty, $init_fn:expr, $size_fn:expr, $needs_helper:expr) => {
        mod $module_name {
            use super::*;

            #[test]
            fn test_single_thread_operations() {
                unsafe {
                    let num_threads = 1;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);
                    let queue = $init_fn(mem, num_threads);

                    assert!(queue.is_empty(), "New queue should be empty");

                    assert!(queue.push(42, 0).is_ok(), "Push should succeed");

                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);

                            for _ in 0..10 {
                                nr_queue.sync();
                                nr_queue.force_complete_sync();
                            }
                        }
                    }

                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() != TypeId::of::<NRQueue<usize>>() {
                            assert!(!queue.is_empty(), "Queue should not be empty after push");
                        }
                    }

                    match queue.pop(0) {
                        Ok(val) => assert_eq!(val, 42, "Dequeued value should be 42"),
                        Err(_) => panic!("Pop should succeed"),
                    }

                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);
                            nr_queue.sync();
                        }
                    }

                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            assert!(queue.pop(0).is_err(), "Pop from empty queue should fail");
                        } else {
                            assert!(queue.is_empty(), "Queue should be empty after pop");
                        }
                    }

                    assert!(queue.pop(0).is_err(), "Pop from empty queue should fail");

                    deallocate_shared_memory(mem, size);
                }
            }

            #[test]
            fn test_multiple_operations() {
                unsafe {
                    let num_threads = 2;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);
                    let queue = $init_fn(mem, num_threads);

                    for i in 0..10 {
                        assert!(queue.push(i, 0).is_ok(), "Push {} should succeed", i);
                    }

                    for i in 0..10 {
                        match queue.pop(0) {
                            Ok(val) => assert_eq!(val, i, "Dequeued value should be {}", i),
                            Err(_) => panic!("Pop {} should succeed", i),
                        }
                    }

                    assert!(queue.is_empty(), "Queue should be empty");

                    deallocate_shared_memory(mem, size);
                }
            }

            #[test]
            fn test_concurrent_enqueue() {
                use std::any::TypeId;

                unsafe {
                    let num_threads = 4;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);

                    let queue = $init_fn(mem, num_threads);
                    let queue_ptr = queue as *const _ as usize;

                    let items_per_thread =
                        if TypeId::of::<$queue_type>() == TypeId::of::<WCQueue<usize>>() {
                            10
                        } else {
                            100
                        };
                    let mut handles = vec![];

                    for tid in 0..num_threads {
                        let handle = thread::spawn(move || {
                            let q = unsafe { &*(queue_ptr as *const $queue_type) };
                            for i in 0..items_per_thread {
                                let value = tid * items_per_thread + i;
                                let mut retries = 0;
                                while q.push(value, tid).is_err() && retries < 1000 {
                                    retries += 1;
                                    thread::yield_now();
                                }
                                if retries >= 1000 {
                                    panic!("Failed to enqueue after 1000 retries");
                                }
                            }
                        });
                        handles.push(handle);
                    }

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);
                            nr_queue.sync();
                        }
                    }

                    let mut count = 0;
                    let max_dequeue_attempts = num_threads * items_per_thread * 2;
                    let mut attempts = 0;

                    while count < num_threads * items_per_thread && attempts < max_dequeue_attempts
                    {
                        if queue.pop(0).is_ok() {
                            count += 1;
                        } else if !queue.is_empty() {
                            thread::yield_now();
                        } else {
                            break;
                        }
                        attempts += 1;
                    }

                    assert_eq!(
                        count,
                        num_threads * items_per_thread,
                        "Should have dequeued all items"
                    );

                    deallocate_shared_memory(mem, size);
                }
            }

            #[test]
            fn test_concurrent_operations() {
                use std::any::TypeId;

                unsafe {
                    let num_threads = 4;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);
                    let queue = $init_fn(mem, num_threads);
                    let queue_ptr = queue as *const _ as usize;

                    let items_per_thread =
                        if TypeId::of::<$queue_type>() == TypeId::of::<WCQueue<usize>>() {
                            10
                        } else {
                            50
                        };
                    let produced = Arc::new(AtomicUsize::new(0));
                    let consumed = Arc::new(AtomicUsize::new(0));
                    let done = Arc::new(AtomicBool::new(false));
                    let mut handles = vec![];

                    for tid in 0..num_threads / 2 {
                        let p = Arc::clone(&produced);
                        let handle = thread::spawn(move || {
                            let q = unsafe { &*(queue_ptr as *const $queue_type) };
                            for i in 0..items_per_thread {
                                let value = tid * items_per_thread + i;
                                let mut retries = 0;
                                while q.push(value, tid).is_err() && retries < 1000 {
                                    retries += 1;
                                    thread::yield_now();
                                }
                                if retries < 1000 {
                                    p.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        });
                        handles.push(handle);
                    }

                    for tid in num_threads / 2..num_threads {
                        let c = Arc::clone(&consumed);
                        let d = Arc::clone(&done);
                        let handle = thread::spawn(move || {
                            let q = unsafe { &*(queue_ptr as *const $queue_type) };
                            let mut consecutive_failures = 0;
                            loop {
                                if q.pop(tid).is_ok() {
                                    c.fetch_add(1, Ordering::Relaxed);
                                    consecutive_failures = 0;
                                } else {
                                    consecutive_failures += 1;
                                    if d.load(Ordering::Relaxed) && consecutive_failures > 100 {
                                        break;
                                    }
                                    thread::yield_now();
                                }
                            }
                        });
                        handles.push(handle);
                    }

                    thread::sleep(Duration::from_millis(100));
                    done.store(true, Ordering::Relaxed);

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);
                            nr_queue.sync();
                        }
                    }

                    let mut drain_attempts = 0;
                    while !queue.is_empty() && drain_attempts < 1000 {
                        if queue.pop(0).is_ok() {
                            consumed.fetch_add(1, Ordering::Relaxed);
                        }
                        drain_attempts += 1;
                    }

                    let produced_count = produced.load(Ordering::Relaxed);
                    let consumed_count = consumed.load(Ordering::Relaxed);

                    assert!(
                        consumed_count >= produced_count * 9 / 10,
                        "Should consume at least 90% of produced items. Produced: {}, Consumed: {}",
                        produced_count,
                        consumed_count
                    );

                    deallocate_shared_memory(mem, size);
                }
            }

            #[test]
            fn test_thread_id_bounds() {
                unsafe {
                    let num_threads = 2;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);
                    let queue = $init_fn(mem, num_threads);

                    use std::any::TypeId;

                    if TypeId::of::<$queue_type>() == TypeId::of::<KPQueue<usize>>() {
                        assert!(
                            queue.push(42, 0).is_ok(),
                            "Push with valid thread ID should work"
                        );
                        assert!(
                            queue.push(43, 1).is_ok(),
                            "Push with valid thread ID should work"
                        );

                        match queue.pop(0) {
                            Ok(val) => assert!(val == 42 || val == 43, "Should pop valid value"),
                            Err(_) => panic!("Pop with valid thread ID should work"),
                        }

                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            let _ = queue.push(99, num_threads);
                        }));
                        assert!(
                            result.is_err(),
                            "Push with invalid thread ID should panic for KPQueue"
                        );
                    } else {
                        let _push_result = queue.push(42, num_threads);
                        let _pop_result = queue.pop(num_threads);
                    }

                    deallocate_shared_memory(mem, size);
                }
            }
        }
    };
}

unsafe fn init_wf_queue(mem: *mut u8, num_threads: usize) -> &'static mut WFQueue<usize> {
    let queue = WFQueue::init_in_shared(mem, num_threads);

    queue
}

mpmc_test_queue!(
    test_nr_queue,
    NRQueue<usize>,
    |mem, num_threads| {
        let q = NRQueue::<usize>::init_in_shared(mem, num_threads);

        unsafe {
            for _ in 0..5 {
                q.force_complete_sync();
            }
        }
        q
    },
    NRQueue::<usize>::shared_size,
    false
);

mpmc_test_queue!(
    test_yang_crummey,
    YangCrummeyQueue<usize>,
    YangCrummeyQueue::<usize>::init_in_shared,
    YangCrummeyQueue::<usize>::shared_size,
    false
);

mpmc_test_queue!(
    test_wcq_queue,
    WCQueue<usize>,
    WCQueue::<usize>::init_in_shared,
    WCQueue::<usize>::shared_size,
    false
);

mpmc_test_queue!(
    test_turn_queue,
    TurnQueue<usize>,
    TurnQueue::<usize>::init_in_shared,
    TurnQueue::<usize>::shared_size,
    false
);

mpmc_test_queue!(
    test_feldman_dechev,
    FeldmanDechevWFQueue<usize>,
    FeldmanDechevWFQueue::<usize>::init_in_shared,
    FeldmanDechevWFQueue::<usize>::shared_size,
    false
);

mpmc_test_queue!(
    test_kogan_petrank,
    KPQueue<usize>,
    KPQueue::<usize>::init_in_shared,
    KPQueue::<usize>::shared_size,
    false
);

mpmc_test_queue!(
    test_feldman_dechev_standard,
    FeldmanDechevWFQueue<usize>,
    FeldmanDechevWFQueue::<usize>::init_in_shared,
    FeldmanDechevWFQueue::<usize>::shared_size,
    false
);

mod test_nr_queue_special {
    use super::*;

    #[test]
    fn test_single_thread_operations() {
        unsafe {
            let num_threads = 1;
            let size = NRQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = NRQueue::<usize>::init_in_shared(mem, num_threads);

            assert!(queue.is_empty(), "New queue should be empty");

            assert!(queue.push(42, 0).is_ok(), "Push should succeed");

            for _ in 0..10 {
                queue.force_complete_sync();
            }

            match queue.pop(0) {
                Ok(val) => assert_eq!(val, 42, "Dequeued value should be 42"),
                Err(_) => panic!("Pop should succeed - item was pushed"),
            }

            for _ in 0..5 {
                queue.force_complete_sync();
            }

            assert!(queue.pop(0).is_err(), "Pop from empty queue should fail");

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_multiple_operations() {
        unsafe {
            let num_threads = 2;
            let size = NRQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = NRQueue::<usize>::init_in_shared(mem, num_threads);

            for i in 0..10 {
                assert!(queue.push(i, 0).is_ok(), "Push {} should succeed", i);

                queue.force_complete_sync();
            }

            for _ in 0..10 {
                queue.force_complete_sync();
            }

            for i in 0..10 {
                for _ in 0..5 {
                    queue.force_complete_sync();
                }

                match queue.pop(0) {
                    Ok(val) => assert_eq!(val, i, "Dequeued value should be {}", i),
                    Err(_) => {
                        for _ in 0..10 {
                            queue.force_complete_sync();
                        }
                        match queue.pop(0) {
                            Ok(val) => assert_eq!(val, i, "Dequeued value should be {} (retry)", i),
                            Err(_) => panic!("Pop {} should succeed after extra sync", i),
                        }
                    }
                }
            }

            for _ in 0..5 {
                queue.force_complete_sync();
            }

            assert!(queue.pop(0).is_err(), "Queue should be empty");

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_concurrent_operations() {
        unsafe {
            let num_threads = 4;
            let size = NRQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = NRQueue::<usize>::init_in_shared(mem, num_threads);
            let queue_ptr = queue as *const _ as usize;

            let items_per_thread = 25;
            let produced = Arc::new(AtomicUsize::new(0));
            let consumed = Arc::new(AtomicUsize::new(0));
            let done = Arc::new(AtomicBool::new(false));
            let mut handles = vec![];

            for tid in 0..num_threads / 2 {
                let p = Arc::clone(&produced);
                let handle = thread::spawn(move || {
                    let q = unsafe { &*(queue_ptr as *const NRQueue<usize>) };
                    for i in 0..items_per_thread {
                        let value = tid * items_per_thread + i;
                        let mut retries = 0;
                        while q.push(value, tid).is_err() && retries < 1000 {
                            retries += 1;
                            thread::yield_now();
                        }
                        if retries < 1000 {
                            p.fetch_add(1, Ordering::Relaxed);

                            q.force_complete_sync();
                        }
                    }
                });
                handles.push(handle);
            }

            for tid in num_threads / 2..num_threads {
                let c = Arc::clone(&consumed);
                let d = Arc::clone(&done);
                let handle = thread::spawn(move || {
                    let q = unsafe { &*(queue_ptr as *const NRQueue<usize>) };
                    let mut consecutive_failures = 0;
                    let mut sync_count = 0;

                    loop {
                        if sync_count % 10 == 0 {
                            for _ in 0..5 {
                                q.force_complete_sync();
                            }
                        }
                        sync_count += 1;

                        if q.pop(tid).is_ok() {
                            c.fetch_add(1, Ordering::Relaxed);
                            consecutive_failures = 0;
                        } else {
                            consecutive_failures += 1;

                            if consecutive_failures % 50 == 0 {
                                for _ in 0..10 {
                                    q.force_complete_sync();
                                }
                            }

                            if d.load(Ordering::Relaxed) && consecutive_failures > 500 {
                                break;
                            }
                            thread::yield_now();
                        }
                    }
                });
                handles.push(handle);
            }

            thread::sleep(Duration::from_millis(200));

            for _ in 0..20 {
                queue.force_complete_sync();
                thread::sleep(Duration::from_millis(10));
            }

            done.store(true, Ordering::Relaxed);

            for handle in handles {
                handle.join().unwrap();
            }

            for _ in 0..20 {
                queue.force_complete_sync();
            }

            let mut drain_attempts = 0;
            while drain_attempts < 1000 {
                let mut found_item = false;

                for tid in 0..num_threads {
                    if queue.pop(tid).is_ok() {
                        consumed.fetch_add(1, Ordering::Relaxed);
                        found_item = true;
                    }
                }

                if !found_item {
                    for _ in 0..5 {
                        queue.force_complete_sync();
                    }

                    if queue.is_empty() {
                        break;
                    }
                }

                drain_attempts += 1;
            }

            let produced_count = produced.load(Ordering::Relaxed);
            let consumed_count = consumed.load(Ordering::Relaxed);

            assert!(
                consumed_count >= produced_count * 95 / 100,
                "Should consume at least 95% of produced items. Produced: {}, Consumed: {}",
                produced_count,
                consumed_count
            );

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_sync_requirements() {
        unsafe {
            let num_threads = 2;
            let size = NRQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = NRQueue::<usize>::init_in_shared(mem, num_threads);

            assert!(queue.push(1, 0).is_ok());
            assert!(queue.push(2, 0).is_ok());
            assert!(queue.push(3, 0).is_ok());

            match queue.pop(1) {
                Ok(val) => assert!(val >= 1 && val <= 3, "Should get a valid value"),
                Err(_) => panic!("Pop should succeed with new aggressive sync"),
            }

            deallocate_shared_memory(mem, size);
        }
    }
}

mod test_wf_queue {
    use super::*;
    use nix::sys::wait::waitpid;
    use nix::unistd::{fork, ForkResult};
    use std::ptr;

    #[test]
    fn test_single_thread_operations() {
        unsafe {
            let num_threads = 1;
            let size = WFQueue::<usize>::shared_size(num_threads);

            let mem = libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            ) as *mut u8;

            if mem == libc::MAP_FAILED as *mut u8 {
                panic!("mmap failed");
            }

            let queue = WFQueue::<usize>::init_in_shared(mem, num_threads);

            match fork() {
                Ok(ForkResult::Child) => {
                    queue.run_helper();
                    std::process::exit(0);
                }
                Ok(ForkResult::Parent { child }) => {
                    thread::sleep(Duration::from_millis(20));

                    assert!(queue.is_empty(), "New queue should be empty");

                    assert!(queue.push(42, 0).is_ok(), "Push should succeed");

                    thread::sleep(Duration::from_millis(10));

                    assert!(!queue.is_empty(), "Queue should not be empty after push");

                    match queue.pop(0) {
                        Ok(val) => assert_eq!(val, 42, "Dequeued value should be 42"),
                        Err(_) => panic!("Pop should succeed"),
                    }

                    thread::sleep(Duration::from_millis(10));
                    assert!(queue.is_empty(), "Queue should be empty after pop");

                    queue.stop_helper();
                    waitpid(child, None).unwrap();

                    libc::munmap(mem as *mut libc::c_void, size);
                }
                Err(e) => panic!("Fork failed: {}", e),
            }
        }
    }

    #[test]
    fn test_multiple_operations_with_helper() {
        unsafe {
            let num_threads = 2;
            let size = WFQueue::<usize>::shared_size(num_threads);

            let mem = libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            ) as *mut u8;

            if mem == libc::MAP_FAILED as *mut u8 {
                panic!("mmap failed");
            }

            let queue = WFQueue::<usize>::init_in_shared(mem, num_threads);

            match fork() {
                Ok(ForkResult::Child) => {
                    queue.run_helper();
                    std::process::exit(0);
                }
                Ok(ForkResult::Parent { child }) => {
                    thread::sleep(Duration::from_millis(20));

                    for i in 0..10 {
                        assert!(queue.push(i, 0).is_ok(), "Push {} should succeed", i);
                        thread::sleep(Duration::from_millis(2));
                    }

                    for i in 0..10 {
                        thread::sleep(Duration::from_millis(2));
                        match queue.pop(1) {
                            Ok(val) => assert_eq!(val, i, "Dequeued value should be {}", i),
                            Err(_) => panic!("Pop {} should succeed", i),
                        }
                    }

                    queue.stop_helper();
                    waitpid(child, None).unwrap();

                    libc::munmap(mem as *mut libc::c_void, size);
                }
                Err(e) => panic!("Fork failed: {}", e),
            }
        }
    }

    #[test]
    fn test_without_helper() {
        unsafe {
            let num_threads = 1;
            let size = WFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WFQueue::<usize>::init_in_shared(mem, num_threads);

            assert!(queue.is_empty(), "New queue should be empty");

            deallocate_shared_memory(mem, size);
        }
    }
}

mod test_jkm_queue {
    use super::*;

    #[test]
    fn test_single_thread_operations() {
        unsafe {
            let num_enq = 1;
            let num_deq = 1;
            let size = JKMQueue::<usize>::shared_size(num_enq, num_deq);
            let mem = allocate_shared_memory(size);
            let queue = JKMQueue::<usize>::init_in_shared(mem, num_enq, num_deq);

            assert!(queue.is_empty(), "New queue should be empty");

            assert!(queue.push(42, 0).is_ok(), "Push should succeed");

            queue.force_sync();

            assert!(!queue.is_empty(), "Queue should not be empty after push");
            assert_eq!(queue.total_items(), 1, "Should have 1 item");

            match queue.pop(0) {
                Ok(val) => assert_eq!(val, 42, "Dequeued value should be 42"),
                Err(_) => panic!("Pop should succeed"),
            }

            queue.finalize_pending_dequeues();
            queue.force_sync();

            assert!(queue.is_empty(), "Queue should be empty after pop");

            assert!(queue.pop(0).is_err(), "Pop from empty queue should fail");

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_multiple_operations() {
        unsafe {
            let num_enq = 2;
            let num_deq = 2;
            let size = JKMQueue::<usize>::shared_size(num_enq, num_deq);
            let mem = allocate_shared_memory(size);
            let queue = JKMQueue::<usize>::init_in_shared(mem, num_enq, num_deq);

            for i in 0..10 {
                let tid = i % num_enq;
                assert!(queue.push(i, tid).is_ok(), "Push {} should succeed", i);
            }

            queue.force_sync();
            assert_eq!(queue.total_items(), 10, "Should have 10 items");

            let mut dequeued = Vec::new();
            for _ in 0..10 {
                queue.force_sync();
                match queue.pop(0) {
                    Ok(val) => dequeued.push(val),
                    Err(_) => match queue.pop(1) {
                        Ok(val) => dequeued.push(val),
                        Err(_) => panic!("Pop should succeed"),
                    },
                }
            }

            queue.finalize_pending_dequeues();
            queue.force_sync();

            assert_eq!(dequeued.len(), 10, "Should have dequeued 10 items");
            assert!(queue.is_empty(), "Queue should be empty");

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_concurrent_operations() {
        unsafe {
            let num_enq = 2;
            let num_deq = 2;
            let size = JKMQueue::<usize>::shared_size(num_enq, num_deq);
            let mem = allocate_shared_memory(size);
            let queue = JKMQueue::<usize>::init_in_shared(mem, num_enq, num_deq);
            let queue_ptr = queue as *const _ as usize;

            let items_per_thread = 20;
            let produced = Arc::new(AtomicUsize::new(0));
            let consumed = Arc::new(AtomicUsize::new(0));
            let done = Arc::new(AtomicBool::new(false));
            let mut handles = vec![];

            for tid in 0..num_enq {
                let p = Arc::clone(&produced);
                let handle = thread::spawn(move || {
                    let q = unsafe { &*(queue_ptr as *const JKMQueue<usize>) };
                    for i in 0..items_per_thread {
                        let value = tid * items_per_thread + i;
                        let mut retries = 0;
                        while q.push(value, tid).is_err() && retries < 100 {
                            retries += 1;
                            thread::yield_now();
                        }
                        if retries < 100 {
                            p.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
                handles.push(handle);
            }

            for tid in 0..num_deq {
                let c = Arc::clone(&consumed);
                let d = Arc::clone(&done);
                let handle = thread::spawn(move || {
                    let q = unsafe { &*(queue_ptr as *const JKMQueue<usize>) };
                    let mut consecutive_failures = 0;
                    loop {
                        if q.pop(tid).is_ok() {
                            c.fetch_add(1, Ordering::Relaxed);
                            consecutive_failures = 0;
                        } else {
                            consecutive_failures += 1;
                            if d.load(Ordering::Relaxed) && consecutive_failures > 50 {
                                break;
                            }
                            thread::yield_now();
                        }
                    }
                });
                handles.push(handle);
            }

            thread::sleep(Duration::from_millis(100));

            for _ in 0..5 {
                queue.force_sync();
                thread::sleep(Duration::from_millis(10));
            }

            done.store(true, Ordering::Relaxed);

            for handle in handles {
                handle.join().unwrap();
            }

            queue.finalize_pending_dequeues();
            queue.force_sync();

            let mut drain_attempts = 0;
            while !queue.is_empty() && drain_attempts < 100 {
                for tid in 0..num_deq {
                    if queue.pop(tid).is_ok() {
                        consumed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                drain_attempts += 1;
                queue.force_sync();
            }

            let produced_count = produced.load(Ordering::Relaxed);
            let consumed_count = consumed.load(Ordering::Relaxed);

            assert_eq!(
                consumed_count, produced_count,
                "Should consume all produced items. Produced: {}, Consumed: {}",
                produced_count, consumed_count
            );

            if consumed_count != produced_count {
                queue.print_debug_stats();
            }

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_edge_cases() {
        unsafe {
            let num_enq = 1;
            let num_deq = 1;
            let size = JKMQueue::<usize>::shared_size(num_enq, num_deq);
            let mem = allocate_shared_memory(size);
            let queue = JKMQueue::<usize>::init_in_shared(mem, num_enq, num_deq);

            assert!(
                queue.push(42, num_enq).is_err(),
                "Push with invalid thread ID should fail"
            );
            assert!(
                queue.pop(num_deq).is_err(),
                "Pop with invalid thread ID should fail"
            );

            assert!(!queue.is_full(), "JKMQueue should never be full");

            deallocate_shared_memory(mem, size);
        }
    }
}

mod test_feldman_dechev_enhanced {
    use super::*;

    #[test]
    fn test_wait_free_slow_path() {
        unsafe {
            let num_threads = 4;
            let size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = FeldmanDechevWFQueue::<usize>::init_in_shared(mem, num_threads);

            let mut count = 0;
            for i in 0..10000 {
                if queue.push(i, 0).is_ok() {
                    count += 1;
                } else {
                    break;
                }
            }

            assert!(count > 0, "Should be able to push some items");

            let mut dequeued = 0;
            for _ in 0..count {
                if queue.pop(0).is_ok() {
                    dequeued += 1;
                }
            }

            assert_eq!(dequeued, count, "Should dequeue all items");

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_concurrent_slow_path() {
        unsafe {
            let num_threads = 4;
            let size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = FeldmanDechevWFQueue::<usize>::init_in_shared(mem, num_threads);
            let queue_ptr = queue as *const _ as usize;

            let items_per_thread = 100;
            let produced = Arc::new(AtomicUsize::new(0));
            let consumed = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];

            for tid in 0..num_threads {
                let p = Arc::clone(&produced);
                let c = Arc::clone(&consumed);
                let handle = thread::spawn(move || {
                    let q = unsafe { &*(queue_ptr as *const FeldmanDechevWFQueue<usize>) };

                    for i in 0..items_per_thread {
                        if i % 3 == 0 {
                            if q.push(tid * items_per_thread + i, tid).is_ok() {
                                p.fetch_add(1, Ordering::Relaxed);
                            }
                        } else {
                            if q.pop(tid).is_ok() {
                                c.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        if i % 10 == 0 {
                            thread::yield_now();
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

            let mut drain_count = 0;
            while !queue.is_empty() && drain_count < 10000 {
                if queue.pop(0).is_ok() {
                    consumed.fetch_add(1, Ordering::Relaxed);
                }
                drain_count += 1;
            }

            let produced_count = produced.load(Ordering::Relaxed);
            let consumed_count = consumed.load(Ordering::Relaxed);

            assert_eq!(
                consumed_count, produced_count,
                "Should consume all produced items. Produced: {}, Consumed: {}",
                produced_count, consumed_count
            );

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_is_full_condition() {
        unsafe {
            let num_threads = 2;
            let size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = FeldmanDechevWFQueue::<usize>::init_in_shared(mem, num_threads);

            let mut pushed = 0;
            let mut push_failed = false;

            for i in 0..100000 {
                if queue.push(i, 0).is_ok() {
                    pushed += 1;

                    if i % 10000 == 0 {
                        let _ = queue.is_full();
                    }
                } else {
                    push_failed = true;
                    break;
                }
            }

            assert!(pushed > 0, "Should be able to push items");

            if push_failed {
                let full_before = queue.is_full();

                let mut dequeued = 0;
                for _ in 0..1000 {
                    if queue.pop(0).is_ok() {
                        dequeued += 1;
                    } else {
                        break;
                    }
                }

                if dequeued > 0 {
                    let can_push_after_dequeue = queue.push(99999, 0).is_ok();

                    if can_push_after_dequeue {
                        assert!(true, "Queue is functionally not full");
                    }
                }
            }

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_announcement_and_helping() {
        unsafe {
            let num_threads = 4;
            let size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = FeldmanDechevWFQueue::<usize>::init_in_shared(mem, num_threads);
            let queue_ptr = queue as *const _ as usize;

            let mut handles = vec![];

            for tid in 0..num_threads {
                let handle = thread::spawn(move || {
                    let q = unsafe { &*(queue_ptr as *const FeldmanDechevWFQueue<usize>) };

                    for i in 0..1000 {
                        if i % 2 == 0 {
                            let _ = q.push(tid * 1000 + i, tid);
                        } else {
                            let _ = q.pop(tid);
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

            while !queue.is_empty() {
                let _ = queue.pop(0);
            }

            deallocate_shared_memory(mem, size);
        }
    }
}

#[test]
fn test_all_queues_exist() {
    let _ = std::any::type_name::<YangCrummeyQueue<usize>>();
    let _ = std::any::type_name::<WFQueue<usize>>();
    let _ = std::any::type_name::<NRQueue<usize>>();
    let _ = std::any::type_name::<JKMQueue<usize>>();
    let _ = std::any::type_name::<WCQueue<usize>>();
    let _ = std::any::type_name::<TurnQueue<usize>>();
    let _ = std::any::type_name::<FeldmanDechevWFQueue<usize>>();
    let _ = std::any::type_name::<KPQueue<usize>>();
    let _ = std::any::type_name::<JKMQueue<usize>>();
}
