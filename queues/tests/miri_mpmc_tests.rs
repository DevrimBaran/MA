use queues::{
    FeldmanDechevWFQueue, JKMQueue, KPQueue, MpmcQueue, NRQueue, TurnQueue, WCQueue, WFQueue,
    YangCrummeyQueue,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

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

macro_rules! mpmc_miri_test_queue {
    ($module_name:ident, $queue_type:ty, $init_fn:expr, $size_fn:expr) => {
        mod $module_name {
            use super::*;

            #[test]
            fn test_basic_operations() {
                unsafe {
                    let num_threads = 1;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);
                    let queue = $init_fn(mem, num_threads);

                    assert!(queue.is_empty(), "New queue should be empty");

                    assert!(queue.push(1, 0).is_ok(), "Push should succeed");

                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);
                            nr_queue.force_complete_sync();
                        }
                    }

                    match queue.pop(0) {
                        Ok(val) => assert_eq!(val, 1, "Dequeued value should be 1"),
                        Err(_) => panic!("Pop should succeed"),
                    }

                    assert!(queue.pop(0).is_err(), "Pop from empty queue should fail");

                    deallocate_shared_memory(mem, size);
                }
            }

            #[test]
            fn test_small_sequence() {
                unsafe {
                    let num_threads = 2;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);
                    let queue = $init_fn(mem, num_threads);

                    for i in 0..5 {
                        assert!(queue.push(i, 0).is_ok(), "Push {} should succeed", i);
                    }

                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);
                            nr_queue.force_complete_sync();
                        }
                    }

                    for i in 0..5 {
                        match queue.pop(0) {
                            Ok(val) => assert_eq!(val, i, "Dequeued value should be {}", i),
                            Err(_) => panic!("Pop {} should succeed", i),
                        }
                    }

                    assert!(queue.pop(0).is_err(), "Queue should be empty");

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

                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);
                            nr_queue.force_complete_sync();
                        }
                    }

                    for i in 0..10 {
                        match queue.pop(0) {
                            Ok(val) => assert_eq!(val, i, "Dequeued value should be {}", i),
                            Err(_) => panic!("Pop {} should succeed", i),
                        }
                    }

                    assert!(queue.pop(0).is_err(), "Queue should be empty");

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

                    assert!(
                        queue.push(42, 0).is_ok(),
                        "Push with valid thread ID should work"
                    );
                    assert!(
                        queue.push(43, 1).is_ok(),
                        "Push with valid thread ID should work"
                    );

                    use std::any::TypeId;
                    if TypeId::of::<$queue_type>() == TypeId::of::<KPQueue<usize>>() {
                        match queue.pop(0) {
                            Ok(val) => assert!(val == 42 || val == 43, "Should pop valid value"),
                            Err(_) => panic!("Pop with valid thread ID should work"),
                        }
                    } else {
                        let _ = queue.push(99, num_threads);
                        let _ = queue.pop(num_threads);
                    }

                    deallocate_shared_memory(mem, size);
                }
            }

            #[test]
            fn test_is_full_basic() {
                unsafe {
                    let num_threads = 1;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);
                    let queue = $init_fn(mem, num_threads);

                    assert!(!queue.is_full(), "New queue should not be full");

                    let _ = queue.push(1, 0);
                    assert!(!queue.is_full(), "Queue with one item should not be full");

                    deallocate_shared_memory(mem, size);
                }
            }

            #[test]
            fn test_simple_concurrent() {
                use std::any::TypeId;

                unsafe {
                    let num_threads = 2;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);
                    let queue = $init_fn(mem, num_threads);
                    let queue_ptr = queue as *const _ as usize;

                    let items_per_thread = 3;

                    let mut handles = vec![];

                    let handle = thread::spawn(move || {
                        let q = unsafe { &*(queue_ptr as *const $queue_type) };
                        for i in 0..items_per_thread {
                            let mut retries = 0;
                            while q.push(i, 0).is_err() && retries < 10 {
                                retries += 1;
                                thread::yield_now();
                            }
                        }
                    });
                    handles.push(handle);

                    let consumed = Arc::new(AtomicUsize::new(0));
                    let c = Arc::clone(&consumed);
                    let handle = thread::spawn(move || {
                        let q = unsafe { &*(queue_ptr as *const $queue_type) };
                        let mut attempts = 0;
                        while c.load(Ordering::Relaxed) < items_per_thread && attempts < 50 {
                            if q.pop(1).is_ok() {
                                c.fetch_add(1, Ordering::Relaxed);
                            }
                            attempts += 1;
                            thread::yield_now();
                        }
                    });
                    handles.push(handle);

                    for handle in handles {
                        handle.join().unwrap();
                    }

                    #[allow(unused_unsafe)]
                    unsafe {
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);
                            nr_queue.force_complete_sync();
                        }
                    }

                    let mut drain_count = 0;
                    while queue.pop(0).is_ok() && drain_count < items_per_thread {
                        consumed.fetch_add(1, Ordering::Relaxed);
                        drain_count += 1;
                    }

                    let consumed_count = consumed.load(Ordering::Relaxed);
                    assert!(
                        consumed_count >= items_per_thread / 2,
                        "Should consume at least half the items. Consumed: {}, Expected: {}",
                        consumed_count,
                        items_per_thread
                    );

                    deallocate_shared_memory(mem, size);
                }
            }

            #[test]
            fn test_concurrent_enqueue() {
                unsafe {
                    let num_threads = 2;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);
                    let queue = $init_fn(mem, num_threads);
                    let queue_ptr = queue as *const _ as usize;

                    let items_per_thread = 5;
                    let mut handles = vec![];

                    for tid in 0..num_threads {
                        let handle = thread::spawn(move || {
                            let q = unsafe { &*(queue_ptr as *const $queue_type) };
                            for i in 0..items_per_thread {
                                let value = tid * items_per_thread + i;
                                let mut retries = 0;
                                while q.push(value, tid).is_err() && retries < 10 {
                                    retries += 1;
                                    thread::yield_now();
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
                            nr_queue.force_complete_sync();
                        }
                    }

                    let mut count = 0;
                    let max_attempts = num_threads * items_per_thread * 2;
                    let mut attempts = 0;
                    while count < num_threads * items_per_thread && attempts < max_attempts {
                        if queue.pop(0).is_ok() {
                            count += 1;
                        }
                        attempts += 1;
                    }

                    assert_eq!(
                        count,
                        num_threads * items_per_thread,
                        "Should dequeue all items"
                    );

                    deallocate_shared_memory(mem, size);
                }
            }
        }
    };
}

mpmc_miri_test_queue!(
    miri_test_yang_crummey,
    YangCrummeyQueue<usize>,
    YangCrummeyQueue::<usize>::init_in_shared,
    YangCrummeyQueue::<usize>::shared_size
);

mpmc_miri_test_queue!(
    miri_test_turn_queue,
    TurnQueue<usize>,
    TurnQueue::<usize>::init_in_shared,
    TurnQueue::<usize>::shared_size
);

mpmc_miri_test_queue!(
    miri_test_feldman_dechev,
    FeldmanDechevWFQueue<usize>,
    FeldmanDechevWFQueue::<usize>::init_in_shared,
    FeldmanDechevWFQueue::<usize>::shared_size
);

mpmc_miri_test_queue!(
    miri_test_kogan_petrank,
    KPQueue<usize>,
    KPQueue::<usize>::init_in_shared,
    KPQueue::<usize>::shared_size
);

#[test]
fn test_type_names() {
    assert!(!std::any::type_name::<YangCrummeyQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<WFQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<NRQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<JKMQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<WCQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<TurnQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<FeldmanDechevWFQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<KPQueue<usize>>().is_empty());
}

#[test]
fn test_memory_allocation_helpers() {
    unsafe {
        let size = 4096;
        let mem = allocate_shared_memory(size);
        assert!(!mem.is_null());

        *mem = 42;
        assert_eq!(*mem, 42);

        deallocate_shared_memory(mem, size);
    }
}

mod miri_edge_cases {
    use super::*;

    #[test]
    fn test_zero_items() {
        unsafe {
            let num_threads = 1;

            {
                let size = YangCrummeyQueue::<usize>::shared_size(num_threads);
                let mem = allocate_shared_memory(size);
                let queue = YangCrummeyQueue::<usize>::init_in_shared(mem, num_threads);

                assert!(queue.is_empty());
                assert!(queue.pop(0).is_err());

                deallocate_shared_memory(mem, size);
            }

            {
                let size = TurnQueue::<usize>::shared_size(num_threads);
                let mem = allocate_shared_memory(size);
                let queue = TurnQueue::<usize>::init_in_shared(mem, num_threads);

                assert!(queue.is_empty());
                assert!(queue.pop(0).is_err());

                deallocate_shared_memory(mem, size);
            }
        }
    }

    #[test]
    fn test_alternating_operations() {
        unsafe {
            let num_threads = 2;

            let size = TurnQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = TurnQueue::<usize>::init_in_shared(mem, num_threads);

            for i in 0..3 {
                assert!(queue.push(i, 0).is_ok());
            }

            for i in 0..3 {
                match queue.pop(0) {
                    Ok(val) => assert_eq!(val, i),
                    Err(_) => panic!("Pop should succeed"),
                }
            }

            assert!(queue.is_empty());

            deallocate_shared_memory(mem, size);
        }
    }
}
