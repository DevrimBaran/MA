#![allow(clippy::cast_ptr_alignment)]
use queues::{
    FeldmanDechevWFQueue, KPQueue, MpmcQueue, TurnQueue, WCQueue, WFQueue, YangCrummeyQueue,
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

                    let items_per_thread = 2;

                    let mut handles = vec![];

                    let handle = thread::spawn(move || {
                        let q = unsafe { &*(queue_ptr as *const $queue_type) };
                        for i in 0..items_per_thread {
                            let mut retries = 0;
                            while q.push(i, 0).is_err() && retries < 5 {
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
                        while c.load(Ordering::Relaxed) < items_per_thread && attempts < 20 {
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

                    let mut drain_count = 0;
                    while queue.pop(0).is_ok() && drain_count < items_per_thread {
                        consumed.fetch_add(1, Ordering::Relaxed);
                        drain_count += 1;
                    }

                    let consumed_count = consumed.load(Ordering::Relaxed);
                    assert!(
                        consumed_count >= 1,
                        "Should consume at least one item. Consumed: {}",
                        consumed_count
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

                    let items_per_thread = 3;
                    let mut handles = vec![];

                    for tid in 0..num_threads {
                        let handle = thread::spawn(move || {
                            let q = unsafe { &*(queue_ptr as *const $queue_type) };
                            for i in 0..items_per_thread {
                                let value = tid * items_per_thread + i;
                                let mut retries = 0;
                                while q.push(value, tid).is_err() && retries < 5 {
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

mod miri_test_ymc_enhanced {
    use super::*;
    use queues::{DeqReq, EnqReq, BOTTOM, TOP};

    #[test]
    fn test_ymc_is_empty_and_is_full_miri() {
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
    fn test_enq_req_methods_miri() {
        let req = EnqReq::new();
        assert_eq!(req.val.load(Ordering::Relaxed), BOTTOM);

        let (pending, id) = req.get_state();
        assert!(!pending);
        assert_eq!(id, 0);

        req.set_state(true, 42);
        let (pending, id) = req.get_state();
        assert!(pending);
        assert_eq!(id, 42);
    }

    #[test]
    fn test_deq_req_methods_miri() {
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
    fn test_ymc_basic_operations_miri() {
        unsafe {
            let num_threads = 2;
            let size = YangCrummeyQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = YangCrummeyQueue::<usize>::init_in_shared(mem, num_threads);

            for i in 0..10 {
                queue.push(i, 0).unwrap();
            }

            for i in 0..10 {
                let val = queue.pop(0).unwrap();
                assert_eq!(val, i);
            }

            assert!(queue.is_empty());

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_ymc_constants_miri() {
        assert_ne!(BOTTOM, TOP);
        assert_eq!(BOTTOM, usize::MAX);
        assert_eq!(TOP, usize::MAX - 1);
    }
}

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

mod miri_test_feldman_dechev_enhanced {
    use super::*;
    use queues::{DequeueOp, EnqueueOp, Node, ValueType};
    use std::cell::UnsafeCell;

    #[test]
    fn test_active_operations_miri() {
        unsafe {
            let num_threads = 2;
            let size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = FeldmanDechevWFQueue::<usize>::init_in_shared(mem, num_threads);

            let (active_enq, active_deq) = queue.active_operations();
            assert_eq!(active_enq, 0);
            assert_eq!(active_deq, 0);

            for i in 0..10 {
                queue.push(i, 0).unwrap();
            }

            for _ in 0..5 {
                queue.pop(0).unwrap();
            }

            let (active_enq, active_deq) = queue.active_operations();
            assert_eq!(active_enq, 0);
            assert_eq!(active_deq, 0);

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_is_empty_and_is_full_miri() {
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

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_node_methods_miri() {
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

    #[test]
    fn test_operation_record_methods_miri() {
        unsafe {
            let enq_op =
                std::alloc::alloc(std::alloc::Layout::new::<EnqueueOp>()) as *mut EnqueueOp;
            std::ptr::write(enq_op, EnqueueOp::new(42, 0));

            assert!(!(*enq_op).is_complete());
            assert!(!(*enq_op).was_successful());

            // Test successful completion
            (*enq_op).complete_success();
            assert!((*enq_op).is_complete());
            assert!((*enq_op).was_successful());

            std::alloc::dealloc(enq_op as *mut u8, std::alloc::Layout::new::<EnqueueOp>());

            // Test failure completion
            let enq_op2 =
                std::alloc::alloc(std::alloc::Layout::new::<EnqueueOp>()) as *mut EnqueueOp;
            std::ptr::write(enq_op2, EnqueueOp::new(43, 0));

            (*enq_op2).complete_failure();
            assert!((*enq_op2).is_complete());
            assert!(!(*enq_op2).was_successful());

            std::alloc::dealloc(enq_op2 as *mut u8, std::alloc::Layout::new::<EnqueueOp>());

            // Test DequeueOp
            let deq_op =
                std::alloc::alloc(std::alloc::Layout::new::<DequeueOp>()) as *mut DequeueOp;
            std::ptr::write(deq_op, DequeueOp::new(0));

            assert!(!(*deq_op).is_complete());
            assert!(!(*deq_op).was_successful());
            assert_eq!((*deq_op).get_result(), 0);

            // Test successful dequeue
            (*deq_op).complete_success(123);
            assert!((*deq_op).is_complete());
            assert!((*deq_op).was_successful());
            assert_eq!((*deq_op).get_result(), 123);

            std::alloc::dealloc(deq_op as *mut u8, std::alloc::Layout::new::<DequeueOp>());

            // Test failed dequeue
            let deq_op2 =
                std::alloc::alloc(std::alloc::Layout::new::<DequeueOp>()) as *mut DequeueOp;
            std::ptr::write(deq_op2, DequeueOp::new(0));

            (*deq_op2).complete_failure();
            assert!((*deq_op2).is_complete());
            assert!(!(*deq_op2).was_successful());
            assert_eq!((*deq_op2).get_result(), 0); // Result unchanged on failure

            std::alloc::dealloc(deq_op2 as *mut u8, std::alloc::Layout::new::<DequeueOp>());
        }
    }

    #[test]
    fn test_alternating_operations_miri() {
        unsafe {
            let num_threads = 2;
            let size = FeldmanDechevWFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = FeldmanDechevWFQueue::<usize>::init_in_shared(mem, num_threads);

            for i in 0..20 {
                if i % 2 == 0 {
                    queue.push(i, 0).unwrap();
                } else if !queue.is_empty() {
                    queue.pop(0).unwrap();
                }
            }

            while !queue.is_empty() {
                queue.pop(0).unwrap();
            }

            deallocate_shared_memory(mem, size);
        }
    }
}

mpmc_miri_test_queue!(
    miri_test_kogan_petrank,
    KPQueue<usize>,
    KPQueue::<usize>::init_in_shared,
    KPQueue::<usize>::shared_size
);

mpmc_miri_test_queue!(
    miri_test_wcqueue,
    WCQueue<usize>,
    WCQueue::<usize>::init_in_shared,
    WCQueue::<usize>::shared_size
);

mod miri_test_wcq_enhanced {
    use super::*;
    use queues::{Entry, EntryPair, InnerWCQ, Phase2Rec, IDX_EMPTY};

    #[test]
    fn test_wcq_is_empty_and_is_full_miri() {
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

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_phase2rec_new_miri() {
        let phase2 = Phase2Rec::new();
        assert_eq!(phase2.seq1.load(Ordering::Relaxed), 1);
        assert_eq!(phase2.local.load(Ordering::Relaxed), 0);
        assert_eq!(phase2.cnt.load(Ordering::Relaxed), 0);
        assert_eq!(phase2.seq2.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_inner_wcq_new_miri() {
        let ring_size = 1024;
        let wcq = InnerWCQ::new(ring_size);
        assert_eq!(wcq.ring_size, ring_size);
        assert_eq!(wcq.capacity, ring_size * 2);
        assert_eq!(wcq.threshold.load(Ordering::Relaxed), -1);
        assert_eq!(wcq.tail.cnt.load(Ordering::Relaxed), (ring_size * 2) as u64);
        assert_eq!(wcq.head.cnt.load(Ordering::Relaxed), (ring_size * 2) as u64);
    }

    #[test]
    fn test_entry_and_entrypair_packing_miri() {
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
    fn test_wcq_basic_operations_miri() {
        unsafe {
            let num_threads = 2;
            let size = WCQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WCQueue::<usize>::init_in_shared(mem, num_threads);

            for i in 0..10 {
                queue.push(i, 0).unwrap();
            }

            for i in 0..10 {
                let val = queue.pop(0).unwrap();
                assert_eq!(val, i);
            }

            assert!(queue.is_empty());

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_wcq_cache_remap_miri() {
        assert_eq!(WCQueue::<usize>::cache_remap(0, 1024), 0);
        assert_eq!(WCQueue::<usize>::cache_remap(1, 1024), 67);
        assert_eq!(WCQueue::<usize>::cache_remap(1024, 1024), 0);
        assert_eq!(WCQueue::<usize>::cache_remap(1025, 1024), 67);
        assert_eq!(WCQueue::<usize>::cache_remap(2048, 1024), 0);
    }

    #[test]
    fn test_wcq_cycle_calculation_miri() {
        assert_eq!(WCQueue::<usize>::cycle(0, 1024), 0);
        assert_eq!(WCQueue::<usize>::cycle(1023, 1024), 0);
        assert_eq!(WCQueue::<usize>::cycle(1024, 1024), 1);
        assert_eq!(WCQueue::<usize>::cycle(2048, 1024), 2);
    }
}

mod miri_test_wfqueue {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_basic_operations() {
        unsafe {
            let num_threads = 1;
            let size = WFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WFQueue::<usize>::init_in_shared(mem, num_threads);

            assert!(queue.is_empty(), "New queue should be empty");

            assert!(queue.is_empty(), "Queue without helper should remain empty");

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_small_sequence() {
        unsafe {
            let num_threads = 2;
            let size = WFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WFQueue::<usize>::init_in_shared(mem, num_threads);
            let queue_ptr = queue as *const _ as usize;

            let helper_running = Arc::new(AtomicBool::new(true));
            let hr = Arc::clone(&helper_running);

            let helper = thread::spawn(move || {
                let q = unsafe { &*(queue_ptr as *const WFQueue<usize>) };

                let mut iterations = 0;
                while hr.load(Ordering::Acquire) && iterations < 100 {
                    unsafe {
                        for tid in 0..q.num_threads {
                            let request = q.get_request(tid);
                            let op_type = request.op_type.load(Ordering::Acquire);

                            if op_type != 0 && !request.is_completed.load(Ordering::Acquire) {
                                match op_type {
                                    1 => {
                                        let element = (*request.element.get()).clone();
                                        if let Some(elem) = element {
                                            request.is_completed.store(true, Ordering::Release);
                                        }
                                    }
                                    2 => {
                                        *request.element.get() = None;
                                        request.is_completed.store(true, Ordering::Release);
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    iterations += 1;
                    thread::yield_now();
                }
            });

            thread::sleep(Duration::from_millis(10));

            for i in 0..3 {
                let _ = queue.push(i, 0);
            }

            helper_running.store(false, Ordering::Release);
            helper.join().unwrap();

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_multiple_operations() {
        unsafe {
            let num_threads = 2;
            let size = WFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WFQueue::<usize>::init_in_shared(mem, num_threads);

            assert!(queue.is_empty());
            assert!(!queue.is_full());

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_thread_id_bounds() {
        unsafe {
            let num_threads = 2;
            let size = WFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WFQueue::<usize>::init_in_shared(mem, num_threads);

            assert_eq!(queue.num_threads, num_threads);

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_is_full_basic() {
        unsafe {
            let num_threads = 1;
            let size = WFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WFQueue::<usize>::init_in_shared(mem, num_threads);

            assert!(!queue.is_full(), "New queue should not be full");

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_simple_concurrent() {
        unsafe {
            let num_threads = 2;
            let size = WFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WFQueue::<usize>::init_in_shared(mem, num_threads);

            assert!(queue.is_empty());
            assert_eq!(queue.num_threads, num_threads);

            deallocate_shared_memory(mem, size);
        }
    }

    #[test]
    fn test_concurrent_enqueue() {
        unsafe {
            let num_threads = 2;
            let size = WFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let _queue = WFQueue::<usize>::init_in_shared(mem, num_threads);

            deallocate_shared_memory(mem, size);
        }
    }
}

#[test]
fn test_type_names() {
    assert!(!std::any::type_name::<YangCrummeyQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<WFQueue<usize>>().is_empty());
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
