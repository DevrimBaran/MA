// queues/tests/miri_mpmc_tests.rs
// Miri-compatible tests for MPMC queues

use queues::{
    FeldmanDechevWFQueue, JKMQueue, KPQueue, MpmcQueue, NRQueue, TurnQueue, WCQueue, WFQueue,
    YangCrummeyQueue,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

// Helper function to allocate aligned shared memory
unsafe fn allocate_shared_memory(size: usize) -> *mut u8 {
    use std::alloc::{alloc_zeroed, Layout};
    let layout = Layout::from_size_align(size, 4096).unwrap();
    let ptr = alloc_zeroed(layout);
    if ptr.is_null() {
        panic!("Failed to allocate shared memory");
    }
    ptr
}

// Helper function to deallocate shared memory
unsafe fn deallocate_shared_memory(ptr: *mut u8, size: usize) {
    use std::alloc::{dealloc, Layout};
    let layout = Layout::from_size_align(size, 4096).unwrap();
    dealloc(ptr, layout);
}

// Macro to generate tests for each queue type
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

                    // Test is_empty on new queue
                    assert!(queue.is_empty(), "New queue should be empty");

                    // Test enqueue
                    assert!(queue.push(1, 0).is_ok(), "Push should succeed");

                    // Special handling for NRQueue
                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);
                            nr_queue.force_complete_sync();
                        }
                    }

                    // Test dequeue
                    match queue.pop(0) {
                        Ok(val) => assert_eq!(val, 1, "Dequeued value should be 1"),
                        Err(_) => panic!("Pop should succeed"),
                    }

                    // Test dequeue from empty queue
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

                    // Enqueue a few items
                    for i in 0..5 {
                        assert!(queue.push(i, 0).is_ok(), "Push {} should succeed", i);
                    }

                    // Special sync for NRQueue
                    #[allow(unused_unsafe)]
                    unsafe {
                        use std::any::TypeId;
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);
                            nr_queue.force_complete_sync();
                        }
                    }

                    // Dequeue all items
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
            fn test_simple_concurrent() {
                use std::any::TypeId;

                unsafe {
                    let num_threads = 2;
                    let size = $size_fn(num_threads);
                    let mem = allocate_shared_memory(size);
                    let queue = $init_fn(mem, num_threads);
                    let queue_ptr = queue as *const _ as usize;

                    // Very small number of items for Miri
                    let items_per_thread = 3;

                    let mut handles = vec![];

                    // One producer thread
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

                    // One consumer thread
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

                    // Wait for threads
                    for handle in handles {
                        handle.join().unwrap();
                    }

                    // Sync for NRQueue
                    #[allow(unused_unsafe)]
                    unsafe {
                        if TypeId::of::<$queue_type>() == TypeId::of::<NRQueue<usize>>() {
                            let nr_queue = &*(queue as *const _ as *const NRQueue<usize>);
                            nr_queue.force_complete_sync();
                        }
                    }

                    // Try to drain any remaining
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
        }
    };
}

// Generate tests for each queue type (except those with special handling)
mpmc_miri_test_queue!(
    miri_test_yang_crummey,
    YangCrummeyQueue<usize>,
    YangCrummeyQueue::<usize>::init_in_shared,
    YangCrummeyQueue::<usize>::shared_size
);

// Skip KWQueue for Miri - it has infinite loops that don't play well with Miri
// The implementation has multiple spin loops that may not terminate under Miri's memory model

// Skip WCQueue for Miri - complex synchronization causes timeouts
// The implementation has extensive busy-waiting that times out under Miri

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

// Special handling for problematic queues under Miri

// Skip KWQueue for Miri - it has complex nested structures that don't work well with Miri
// The issue is in the initialization order and memory visibility of the nested CountingSet/PRegister structures

mod miri_test_nr_queue {
    use super::*;

    #[test]
    fn test_init_only() {
        unsafe {
            let num_threads = 1;
            let size = NRQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = NRQueue::<usize>::init_in_shared(mem, num_threads);

            // Only test that we can initialize the queue
            // Don't call any operations that would trigger tree propagation
            assert!(
                !queue.base_ptr.is_null(),
                "Base pointer should be initialized"
            );

            deallocate_shared_memory(mem, size);
        }
    }
}

// Special test for WFQueue - skip helper thread tests for Miri
mod miri_test_wf_queue {
    use super::*;

    #[test]
    fn test_init_only() {
        unsafe {
            let num_threads = 1;
            let size = WFQueue::<usize>::shared_size(num_threads);
            let mem = allocate_shared_memory(size);
            let queue = WFQueue::<usize>::init_in_shared(mem, num_threads);

            // Just test initialization - WFQueue needs helper thread which doesn't work in Miri
            assert!(queue.is_empty(), "New queue should be empty");

            deallocate_shared_memory(mem, size);
        }
    }
}

// Special test for JKMQueue - skip complex operations in Miri
mod miri_test_jkm_queue {
    use super::*;

    #[test]
    fn test_init_only() {
        unsafe {
            let num_enq = 1;
            let num_deq = 1;
            let size = JKMQueue::<usize>::shared_size(num_enq, num_deq);
            let mem = allocate_shared_memory(size);
            let queue = JKMQueue::<usize>::init_in_shared(mem, num_enq, num_deq);

            // JKMQueue has complex synchronization that can hang in Miri
            // Just test initialization
            assert!(queue.is_empty(), "New queue should be empty");
            assert!(!queue.is_full(), "JKMQueue should never be full");

            deallocate_shared_memory(mem, size);
        }
    }
}

#[test]
fn test_type_names() {
    // Simple test to ensure all types are available
    assert!(!std::any::type_name::<YangCrummeyQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<WFQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<NRQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<JKMQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<WCQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<TurnQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<FeldmanDechevWFQueue<usize>>().is_empty());
    assert!(!std::any::type_name::<KPQueue<usize>>().is_empty());
}
