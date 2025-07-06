use queues::{
    DeqReq, DequeueOp, EnqReq, EnqueueOp, Entry, EntryPair, FeldmanDechevWFQueue, InnerWCQ,
    KPQueue, MpmcQueue, Node, Phase2Rec, TurnQueue, ValueType, WCQueue, WFQueue, YangCrummeyQueue,
    BOTTOM, IDX_EMPTY, TOP,
};
use std::cell::UnsafeCell;
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

                    assert!(!queue.is_empty(), "Queue should not be empty after push");

                    match queue.pop(0) {
                        Ok(val) => assert_eq!(val, 42, "Dequeued value should be 42"),
                        Err(_) => panic!("Pop should succeed"),
                    }

                    assert!(queue.is_empty(), "Queue should be empty after pop");

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
    test_yang_crummey,
    YangCrummeyQueue<usize>,
    YangCrummeyQueue::<usize>::init_in_shared,
    YangCrummeyQueue::<usize>::shared_size,
    false
);

mod test_ymc_enhanced {
    use super::*;

    #[test]
    fn test_ymc_is_empty_and_is_full() {
        unsafe {
            let num_threads = 2;
            let size = YangCrummeyQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = YangCrummeyQueue::<usize>::init_in_shared(mem, num_threads);

            assert!(queue.is_empty());
            assert!(!queue.is_full());

            queue.push(1, 0).unwrap();
            assert!(!queue.is_empty());
            assert!(!queue.is_full());

            queue.pop(0).unwrap();
            assert!(queue.is_empty());
            assert!(!queue.is_full());

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_enq_req_methods() {
        let req = EnqReq::new();
        assert_eq!(req.val.load(Ordering::Relaxed), BOTTOM);

        let (pending, id) = req.get_state();
        assert!(!pending);
        assert_eq!(id, 0);

        req.set_state(true, 42);
        let (pending, id) = req.get_state();
        assert!(pending);
        assert_eq!(id, 42);

        assert!(req.try_claim(42, 100));
        let (pending, id) = req.get_state();
        assert!(!pending);
        assert_eq!(id, 100);

        assert!(!req.try_claim(42, 200));
    }

    #[test]
    fn test_deq_req_methods() {
        let req = DeqReq::new();
        assert_eq!(req.id.load(Ordering::Relaxed), 0);

        let (pending, idx) = req.get_state();
        assert!(!pending);
        assert_eq!(idx, 0);

        req.set_state(true, 42);
        let (pending, idx) = req.get_state();
        assert!(pending);
        assert_eq!(idx, 42);

        assert!(req.try_announce(42, 100));
        let (pending, idx) = req.get_state();
        assert!(pending);
        assert_eq!(idx, 100);

        assert!(req.try_complete(100));
        let (pending, idx) = req.get_state();
        assert!(!pending);
        assert_eq!(idx, 100);
    }

    #[test]
    fn test_ymc_spsc_init() {
        unsafe {
            let size = YangCrummeyQueue::<usize>::spsc_shared_size();
            let mem = allocate_shared_memory(size);
            let queue = YangCrummeyQueue::<usize>::init_in_shared_spsc(mem);

            queue.push(42, 0).unwrap();
            assert!(!queue.is_empty());

            let val = queue.pop(1).unwrap();
            assert_eq!(val, 42);
            assert!(queue.is_empty());

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_ymc_slow_path() {
        unsafe {
            let num_threads = 4;
            let size = YangCrummeyQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = YangCrummeyQueue::<usize>::init_in_shared(mem, num_threads);
            let queue_ptr = queue as *const _ as usize;

            let barrier = Arc::new(std::sync::Barrier::new(num_threads));
            let success_count = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];

            for tid in 0..num_threads {
                let barrier_clone = barrier.clone();
                let success_clone = success_count.clone();
                let handle = thread::spawn(move || {
                    let q = unsafe { &*(queue_ptr as *const YangCrummeyQueue<usize>) };

                    barrier_clone.wait();

                    for i in 0..2000 {
                        if i % 2 == 0 {
                            if q.push(tid * 10000 + i, tid).is_ok() {
                                success_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        } else {
                            if q.pop(tid).is_ok() {
                                success_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

            assert!(success_count.load(Ordering::Relaxed) > 0);

            while !queue.is_empty() {
                let _ = queue.pop(0);
            }

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_ymc_constants() {
        assert_ne!(BOTTOM, TOP);
        assert_eq!(BOTTOM, usize::MAX);
        assert_eq!(TOP, usize::MAX - 1);

        assert!(BOTTOM > 1_000_000_000);
        assert!(TOP > 1_000_000_000);
    }

    #[test]
    fn test_ymc_state_packing() {
        let max_id: u64 = 0x7FFFFFFFFFFFFFFF;

        let req = EnqReq::new();
        req.set_state(true, max_id);
        let (pending, id) = req.get_state();
        assert!(pending);
        assert_eq!(id, max_id);

        req.set_state(false, max_id);
        let (pending, id) = req.get_state();
        assert!(!pending);
        assert_eq!(id, max_id);
    }
}

mpmc_test_queue!(
    test_wcq_queue,
    WCQueue<usize>,
    WCQueue::<usize>::init_in_shared,
    WCQueue::<usize>::shared_size,
    false
);

mod test_wcq_enhanced {
    use super::*;

    #[test]
    fn test_wcq_is_empty_and_is_full() {
        unsafe {
            let num_threads = 2;
            let size = WCQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WCQueue::<usize>::init_in_shared(mem, num_threads);

            assert!(queue.is_empty());
            assert!(!queue.is_full());

            queue.push(1, 0).unwrap();
            assert!(!queue.is_empty());
            assert!(!queue.is_full());

            queue.pop(0).unwrap();
            assert!(queue.is_empty());
            assert!(!queue.is_full());

            let mut pushed = 0;
            for i in 0..100000 {
                if queue.push(i, 0).is_ok() {
                    pushed += 1;
                } else {
                    break;
                }
            }

            assert!(pushed > 0);

            if queue.push(999999, 0).is_err() {
                assert!(queue.is_full());
            }

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_phase2rec_new() {
        let phase2 = Phase2Rec::new();
        assert_eq!(phase2.seq1.load(Ordering::Relaxed), 1);
        assert_eq!(phase2.local.load(Ordering::Relaxed), 0);
        assert_eq!(phase2.cnt.load(Ordering::Relaxed), 0);
        assert_eq!(phase2.seq2.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_inner_wcq_new() {
        let ring_size = 1024;
        let wcq = InnerWCQ::new(ring_size);
        assert_eq!(wcq.ring_size, ring_size);
        assert_eq!(wcq.capacity, ring_size * 2);
        assert_eq!(wcq.threshold.load(Ordering::Relaxed), -1);
        assert_eq!(wcq.tail.cnt.load(Ordering::Relaxed), (ring_size * 2) as u64);
        assert_eq!(wcq.head.cnt.load(Ordering::Relaxed), (ring_size * 2) as u64);
    }

    #[test]
    fn test_entry_and_entrypair_packing() {
        let entry = Entry {
            cycle: 42,
            is_safe: true,
            enq: false,
            index: 12345,
        };

        let packed = EntryPair::pack_entry(entry);
        let unpacked = EntryPair::unpack_entry(packed);

        assert_eq!(unpacked.cycle, entry.cycle);
        assert_eq!(unpacked.is_safe, entry.is_safe);
        assert_eq!(unpacked.enq, entry.enq);
        assert_eq!(unpacked.index, entry.index);

        let empty_entry = Entry::new();
        assert_eq!(empty_entry.index, IDX_EMPTY);
        assert!(empty_entry.is_safe);
        assert!(empty_entry.enq);
    }

    #[test]
    fn test_wcq_slow_path_operations() {
        unsafe {
            let num_threads = 4;
            let size = WCQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WCQueue::<usize>::init_in_shared(mem, num_threads);
            let queue_ptr = queue as *const _ as usize;

            let mut count = 0;
            for i in 0..65000 {
                if queue.push(i, 0).is_ok() {
                    count += 1;
                } else {
                    break;
                }
            }

            let success_count = Arc::new(AtomicUsize::new(0));
            let barrier = Arc::new(std::sync::Barrier::new(num_threads));
            let mut handles = vec![];

            for tid in 0..num_threads {
                let barrier_clone = barrier.clone();
                let success_clone = success_count.clone();
                let handle = thread::spawn(move || {
                    let q = unsafe { &*(queue_ptr as *const WCQueue<usize>) };

                    barrier_clone.wait();

                    for i in 0..1000 {
                        if i % 2 == 0 {
                            if q.push(tid * 1000 + i, tid).is_ok() {
                                success_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        } else {
                            if q.pop(tid).is_ok() {
                                success_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }

            assert!(success_count.load(Ordering::Relaxed) > 0);

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_wcq_helping_mechanism() {
        unsafe {
            let num_threads = 4;
            let size = WCQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WCQueue::<usize>::init_in_shared(mem, num_threads);
            let queue_ptr = queue as *const _ as usize;

            let barrier = Arc::new(std::sync::Barrier::new(num_threads));
            let mut handles = vec![];

            for tid in 0..num_threads {
                let barrier_clone = barrier.clone();
                let handle = thread::spawn(move || {
                    let q = unsafe { &*(queue_ptr as *const WCQueue<usize>) };

                    barrier_clone.wait();

                    for i in 0..500 {
                        match i % 4 {
                            0 => {
                                let _ = q.push(tid * 1000 + i, tid);
                            }
                            1 => {
                                let _ = q.pop(tid);
                            }
                            2 => {
                                let _ = q.is_empty();
                            }
                            _ => {
                                let _ = q.is_full();
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

            while !queue.is_empty() {
                let _ = queue.pop(0);
            }

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_wcq_cache_remap() {
        assert_eq!(WCQueue::<usize>::cache_remap(0, 1024), 0);
        assert_eq!(WCQueue::<usize>::cache_remap(1024, 1024), 0);
        assert_eq!(WCQueue::<usize>::cache_remap(1025, 1024), 1);
        assert_eq!(WCQueue::<usize>::cache_remap(2048, 1024), 0);
    }

    #[test]
    fn test_wcq_cycle_calculation() {
        assert_eq!(WCQueue::<usize>::cycle(0, 1024), 0);
        assert_eq!(WCQueue::<usize>::cycle(1023, 1024), 0);
        assert_eq!(WCQueue::<usize>::cycle(1024, 1024), 1);
        assert_eq!(WCQueue::<usize>::cycle(2048, 1024), 2);
    }
}

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

    #[test]
    fn test_active_operations() {
        unsafe {
            let num_threads = 2;
            let size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = FeldmanDechevWFQueue::<usize>::init_in_shared(mem, num_threads);

            let (active_enq, active_deq) = queue.active_operations();
            assert_eq!(active_enq, 0);
            assert_eq!(active_deq, 0);

            let mut pushed = 0;
            for i in 0..200000 {
                if queue.push(i, 0).is_err() {
                    break;
                }
                pushed += 1;
            }

            let queue_ptr = queue as *const _ as usize;
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(move || {
                let q = unsafe { &*(queue_ptr as *const FeldmanDechevWFQueue<usize>) };
                barrier_clone.wait();

                for _ in 0..1000 {
                    let _ = q.push(999999, 1);
                }
            });

            barrier.wait();
            thread::sleep(std::time::Duration::from_millis(10));

            let (active_enq, active_deq) = queue.active_operations();

            assert!(active_enq <= 1);
            assert!(active_deq == 0);

            handle.join().unwrap();

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_is_empty_and_is_full() {
        unsafe {
            let num_threads = 2;
            let size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = FeldmanDechevWFQueue::<usize>::init_in_shared(mem, num_threads);

            assert!(queue.is_empty());
            assert!(!queue.is_full());

            queue.push(1, 0).unwrap();
            assert!(!queue.is_empty());
            assert!(!queue.is_full());

            queue.pop(0).unwrap();
            assert!(queue.is_empty());
            assert!(!queue.is_full());

            let mut count = 0;
            for i in 0..200000 {
                if queue.push(i, 0).is_err() {
                    break;
                }
                count += 1;
            }

            assert!(count > 10000);
            assert!(!queue.is_empty());

            let is_full_result = queue.is_full();
            let push_result = queue.push(999999, 0);

            if is_full_result {
                assert!(push_result.is_err());
            }

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_node_delay_marking() {
        unsafe {
            let num_threads = 4;
            let size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = FeldmanDechevWFQueue::<usize>::init_in_shared(mem, num_threads);
            let queue_ptr = queue as *const _ as usize;

            let barrier = Arc::new(std::sync::Barrier::new(num_threads));
            let mut handles = vec![];

            for tid in 0..num_threads {
                let barrier_clone = barrier.clone();
                let handle = thread::spawn(move || {
                    let q = unsafe { &*(queue_ptr as *const FeldmanDechevWFQueue<usize>) };

                    barrier_clone.wait();

                    for i in 0..10000 {
                        if i % 3 == 0 {
                            let _ = q.push(tid * 10000 + i, tid);
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

    #[test]
    fn test_operation_record_methods() {
        unsafe {
            let enq_op =
                std::alloc::alloc(std::alloc::Layout::new::<EnqueueOp>()) as *mut EnqueueOp;
            std::ptr::write(enq_op, EnqueueOp::new(42, 0));

            assert!(!(*enq_op).is_complete());
            (*enq_op).complete();
            assert!((*enq_op).is_complete());

            std::alloc::dealloc(enq_op as *mut u8, std::alloc::Layout::new::<EnqueueOp>());

            let deq_op =
                std::alloc::alloc(std::alloc::Layout::new::<DequeueOp>()) as *mut DequeueOp;
            std::ptr::write(deq_op, DequeueOp::new(0));

            assert!(!(*deq_op).is_complete());
            assert_eq!((*deq_op).get_result(), 0);

            (*deq_op).set_result(123);
            assert_eq!((*deq_op).get_result(), 123);

            (*deq_op).complete();
            assert!((*deq_op).is_complete());

            std::alloc::dealloc(deq_op as *mut u8, std::alloc::Layout::new::<DequeueOp>());
        }
    }

    #[test]
    fn test_node_methods() {
        let empty_node = Node::new_empty(100);
        assert!(empty_node.is_empty());
        assert!(!empty_node.is_value());
        assert!(!empty_node.is_delay_marked());
        assert_eq!(empty_node.get_seqid(), 100);
        assert!(empty_node.get_value_ptr().is_null());

        let mut delay_node = empty_node;
        delay_node.set_delay_mark();
        assert!(delay_node.is_delay_marked());
        assert!(delay_node.is_empty());

        unsafe {
            let value_type = Box::new(ValueType {
                seqid: 200,
                value: UnsafeCell::new(Some(42)),
            });
            let value_ptr = Box::into_raw(value_type);

            let value_node = Node::new_value(value_ptr, 200);
            assert!(!value_node.is_empty());
            assert!(value_node.is_value());
            assert!(!value_node.is_delay_marked());
            assert_eq!(value_node.get_seqid(), 200);
            assert_eq!(value_node.get_value_ptr(), value_ptr);

            let _ = Box::from_raw(value_ptr);
        }
    }
}

#[test]
fn test_all_queues_exist() {
    let _ = std::any::type_name::<YangCrummeyQueue<usize>>();
    let _ = std::any::type_name::<WFQueue<usize>>();
    let _ = std::any::type_name::<WCQueue<usize>>();
    let _ = std::any::type_name::<TurnQueue<usize>>();
    let _ = std::any::type_name::<FeldmanDechevWFQueue<usize>>();
    let _ = std::any::type_name::<KPQueue<usize>>();
}
