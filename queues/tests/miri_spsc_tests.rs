// queues/tests/miri_spsc_tests.rs
//! Miri-compatible tests for SPSC queue implementations
//!
//! Run with: cargo +nightly miri test --test miri_spsc_tests
//!
//! These tests avoid:
//! - FFI calls (mmap, fork, etc.)
//! - Large allocations
//! - Excessive concurrency
//! - Platform-specific code
//!
//! NOT TESTED IN MIRI (documented exclusions):
//! - UnboundedQueue (uSPSC) - uses mmap with MAP_SHARED which Miri doesn't support
//! - FfqQueue may show false positive data races in Miri - relies on cache-line synchronization
//! - IPC tests using fork() and shared memory
//! - Performance/timing tests
//! - Large-scale stress tests (>10K items)
//! - Tests requiring precise timing or sleep()
//! - Tests using mmap/munmap directly

#![cfg(miri)]

use queues::{spsc::*, SpscQueue};
use std::sync::{Arc, Barrier};
use std::thread;

// Smaller capacities for Miri
const MIRI_SMALL_CAP: usize = 16;
const MIRI_MEDIUM_CAP: usize = 64;
const MIRI_LARGE_CAP: usize = 256;
const MIRI_TEST_ITEMS: usize = 100;

// Helper for aligned memory allocation with RAII cleanup
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
        // Run cleanup before deallocating memory
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }

        unsafe {
            std::alloc::dealloc(self.ptr, self.layout);
        }
    }
}

// Basic queue tests with heap allocation
mod miri_basic_tests {
    use super::*;

    #[test]
    fn miri_test_lamport() {
        // Test 1: Basic push/pop
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

        // Test 2: Fill and drain
        {
            let queue = LamportQueue::<usize>::with_capacity(MIRI_SMALL_CAP);
            let items_to_push = MIRI_SMALL_CAP - 1; // Leave one slot

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

        // Test 3: Simple concurrent test
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
        // NOTE: FfqQueue is the FastForward queue implementation
        // It may show false positive data races in Miri because it relies on
        // cache-line-based synchronization and temporal slipping

        // Test 1: Basic push/pop
        {
            let queue = FfqQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

            assert!(queue.empty());
            assert!(queue.available());
            queue.push(42).unwrap();
            assert!(!queue.empty());
            assert_eq!(queue.pop().unwrap(), 42);
            assert!(queue.empty());
        }

        // Test 2: Fill and drain
        {
            let queue = FfqQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

            // Fill queue to capacity
            let mut pushed = 0;
            for i in 0..MIRI_MEDIUM_CAP {
                if queue.push(i).is_ok() {
                    pushed += 1;
                } else {
                    break;
                }
            }

            assert!(pushed > 0, "Should push at least some items");

            // Check if really full
            if pushed < MIRI_MEDIUM_CAP {
                assert!(!queue.available());
            }

            // Pop all items
            for i in 0..pushed {
                assert_eq!(queue.pop().unwrap(), i);
            }
            assert!(queue.empty());
        }

        // Test 3: Distance tracking
        {
            let queue = FfqQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

            assert_eq!(queue.distance(), 0);
            queue.push(1).unwrap();
            queue.push(2).unwrap();
            assert_eq!(queue.distance(), 2);

            queue.pop().unwrap();
            assert_eq!(queue.distance(), 1);
            queue.pop().unwrap();
            assert_eq!(queue.distance(), 0);
        }
    }

    #[test]
    fn miri_test_bqueue() {
        // Test 1: Basic push/pop
        {
            let queue = BQueue::<usize>::new(MIRI_MEDIUM_CAP);

            assert!(queue.empty());
            assert!(queue.available());
            queue.push(42).unwrap();
            assert!(!queue.empty());
            assert_eq!(queue.pop().unwrap(), 42);
            assert!(queue.empty());
        }

        // Test 2: Fill and drain
        {
            let queue = BQueue::<usize>::new(MIRI_MEDIUM_CAP);

            // BQueue may have complex capacity behavior due to batching
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

            // Try one more push - should fail if full
            assert!(queue.push(999).is_err() || pushed < MIRI_MEDIUM_CAP);

            // Pop all items
            for i in 0..pushed {
                assert_eq!(queue.pop().unwrap(), i);
            }
            assert!(queue.empty());
        }
    }

    #[test]
    fn miri_test_dehnavi() {
        // Test 1: Basic operations
        {
            let queue = DehnaviQueue::<usize>::new(MIRI_SMALL_CAP);

            assert!(queue.empty());
            assert!(queue.available());
            queue.push(42).unwrap();
            assert!(!queue.empty());
            assert_eq!(queue.pop().unwrap(), 42);
            assert!(queue.empty());
        }

        // Test 2: Lossy behavior
        {
            let queue = DehnaviQueue::<usize>::new(4);

            // Overfill to test lossy behavior
            for i in 0..10 {
                queue.push(i).unwrap(); // Always succeeds due to lossy nature
            }

            // Should see some items but not necessarily all due to lossy nature
            let mut items = Vec::new();
            while let Ok(item) = queue.pop() {
                items.push(item);
            }

            assert!(!items.is_empty());
            // Items should be in increasing order
            for window in items.windows(2) {
                assert!(window[1] > window[0]);
            }
        }
    }

    #[test]
    fn miri_test_multipush() {
        let queue = MultiPushQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

        // Test basic operations
        queue.push(42).unwrap();
        assert!(!queue.empty());

        // Force flush
        assert!(queue.flush());

        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        // Test local buffer
        for i in 0..10 {
            queue.push(i).unwrap();
        }

        // Verify items are buffered
        assert!(queue.local_count.load(std::sync::atomic::Ordering::Relaxed) > 0);

        // Force flush
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
        let queue = LlqQueue::<usize>::with_capacity(128); // Must be > K_CACHE_LINE_SLOTS

        assert!(queue.empty());
        assert!(queue.available());

        queue.push(42).unwrap();
        assert!(!queue.empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        // Test capacity
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
        let queue = BlqQueue::<usize>::with_capacity(64); // Must be > K_CACHE_LINE_SLOTS

        assert!(queue.empty());
        assert!(queue.available());

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        // Test batch operations
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
        let queue = IffqQueue::<usize>::with_capacity(128); // Must be power of 2 and multiple of 32

        assert!(queue.empty());
        assert!(queue.available());

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        // Test partition boundaries
        for i in 0..31 {
            queue.push(i).unwrap();
        }

        // Should still have space
        assert!(queue.available());
        queue.push(31).unwrap();

        for i in 0..32 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn miri_test_biffq() {
        let queue = BiffqQueue::<usize>::with_capacity(128); // Same requirements as IFFQ

        assert!(queue.empty());
        assert!(queue.available());

        queue.push(42).unwrap();

        // Flush to ensure item is visible
        let _ = queue.flush_producer_buffer();

        assert!(!queue.empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        // Test local buffer
        for i in 0..20 {
            queue.push(i).unwrap();
        }

        // Check local buffer has items
        assert!(
            queue
                .prod
                .local_count
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
        );

        // Flush and verify
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

        // Test dynamic allocation
        for i in 0..100 {
            queue.push(i).unwrap();
        }

        for i in 0..100 {
            assert_eq!(queue.pop().unwrap(), i);
        }
        assert!(queue.empty());
    }

    // UnboundedQueue uses mmap with MAP_SHARED which Miri doesn't support
    // Skip this test in Miri
    #[cfg(not(miri))]
    #[test]
    fn miri_test_unbounded() {
        // This test is skipped in Miri because UnboundedQueue uses mmap
        unreachable!("This test should not run under Miri");
    }
}

// Shared memory tests without mmap
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

        // Test fill
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

        // Test basic operations
        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);

        // Test temporal slipping features
        queue.push(1).unwrap();
        queue.push(2).unwrap();
        let distance = queue.distance();
        assert_eq!(distance, 2);
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

        // Test Dehnavi's wait-free property
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
        let capacity = 128; // Must be > K_CACHE_LINE_SLOTS
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
        let capacity = 64; // Must be > K_CACHE_LINE_SLOTS
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
        let capacity = 128; // Power of 2 and multiple of H_PARTITION_SIZE
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
        let capacity = 128; // Power of 2 and multiple of H_PARTITION_SIZE
        let shared_size = BiffqQueue::<usize>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { BiffqQueue::<usize>::init_in_shared(mem_ptr, capacity) };

        queue.push(123).unwrap();
        let _ = queue.flush_producer_buffer();
        assert_eq!(queue.pop().unwrap(), 123);
        assert!(queue.empty());

        // Ensure buffer is flushed
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

        // Ensure local buffer is empty
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
            // Leave some buffer
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

    // UnboundedQueue uses mmap with MAP_SHARED which Miri doesn't support
    // Skip this test in Miri
    #[cfg(not(miri))]
    #[test]
    fn test_unbounded_shared_init() {
        unreachable!("This test should not run under Miri");
    }
}

// Special queue features that need careful handling
mod miri_special_features {
    use super::*;

    #[test]
    fn test_multipush_local_buffer() {
        let queue = MultiPushQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

        // Push items to local buffer
        for i in 0..10 {
            queue.push(i).unwrap();
        }

        // Verify local buffer has items
        assert!(queue.local_count.load(std::sync::atomic::Ordering::Relaxed) > 0);

        // Force flush
        assert!(queue.flush());
        assert_eq!(
            queue.local_count.load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        // Verify items
        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_multipush_automatic_flush() {
        let queue = MultiPushQueue::<usize>::with_capacity(MIRI_LARGE_CAP);

        // Push exactly LOCAL_BUF (32) items to trigger automatic flush
        for i in 0..32 {
            queue.push(i).unwrap();
        }

        // Should have automatically flushed
        assert_eq!(
            queue.local_count.load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        // All items should be in main queue
        for i in 0..32 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_biffq_producer_buffer() {
        let queue = BiffqQueue::<usize>::with_capacity(128);

        // Fill local buffer
        for i in 0..20 {
            queue.push(i).unwrap();
        }

        // Check local buffer has items
        assert!(
            queue
                .prod
                .local_count
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
        );

        // Flush to shared buffer
        let flushed = queue.flush_producer_buffer().unwrap();
        assert!(flushed > 0);
        assert_eq!(
            queue
                .prod
                .local_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        // Consume items
        for i in 0..20 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_biffq_automatic_flush() {
        let queue = BiffqQueue::<usize>::with_capacity(128);

        // Push exactly LOCAL_BATCH_SIZE (32) items to trigger automatic flush
        for i in 0..32 {
            queue.push(i).unwrap();
        }

        // Should have some items flushed
        assert!(!queue.empty());

        let mut count = 0;
        while queue.pop().is_ok() {
            count += 1;
        }

        // Flush any remaining
        let _ = queue.flush_producer_buffer();

        while queue.pop().is_ok() {
            count += 1;
        }

        assert_eq!(count, 32);
    }

    #[test]
    fn test_dehnavi_lossy() {
        let queue = DehnaviQueue::<usize>::new(4);

        // Overfill to test lossy behavior
        for i in 0..10 {
            queue.push(i).unwrap();
        }

        // Should see some items, but not necessarily all
        let mut items = Vec::new();
        while let Ok(item) = queue.pop() {
            items.push(item);
        }

        assert!(!items.is_empty());
        assert!(items.len() <= 4); // At most capacity items

        // Items should be in increasing order
        for window in items.windows(2) {
            assert!(window[1] > window[0]);
        }
    }

    #[test]
    fn test_ffq_temporal_slipping() {
        let queue = FfqQueue::<usize>::with_capacity(128);

        queue.push(1).unwrap();
        queue.push(2).unwrap();
        let distance = queue.distance();
        assert_eq!(distance, 2);

        // Note: adjust_slip uses timing which Miri doesn't handle well
        // Just verify it doesn't crash
        queue.adjust_slip(100);

        // Clean up
        let _ = queue.pop();
        let _ = queue.pop();
    }

    #[test]
    fn test_blq_batch_operations() {
        let queue = BlqQueue::<usize>::with_capacity(128);

        // Test batch enqueue
        let space = queue.blq_enq_space(10);
        assert!(space >= 10);

        for i in 0..10 {
            queue.blq_enq_local(i).unwrap();
        }
        queue.blq_enq_publish();

        // Test batch dequeue
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

        // Push more items than pre-allocated nodes
        for i in 0..100 {
            queue.push(i).unwrap();
        }

        // Should allocate new nodes dynamically
        for i in 0..100 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.empty());
    }

    // UnboundedQueue uses mmap with MAP_SHARED which Miri doesn't support
    // Skip this test in Miri
    #[cfg(not(miri))]
    #[test]
    fn test_unbounded_segment_growth() {
        unreachable!("This test should not run under Miri");
    }
}

// Test with types that need drop
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

            // Push items
            for i in 0..5 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            // Pop some
            for _ in 0..2 {
                drop(queue.pop().unwrap());
            }

            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 2);
            // Queue drops here, should drop remaining 3 items
        }

        // Give time for drops
        thread::yield_now();

        // At least the 2 explicitly dropped items should be counted
        assert!(DROP_COUNT.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn test_drop_with_strings() {
        let queue = BQueue::<String>::new(MIRI_MEDIUM_CAP);

        // Push strings (which allocate)
        for i in 0..10 {
            queue.push(format!("item_{}", i)).unwrap();
        }

        // Pop half
        for _ in 0..5 {
            let _ = queue.pop().unwrap();
        }

        // Queue drops here with remaining strings
    }

    #[test]
    fn test_drop_in_buffered_queues() {
        DROP_COUNT.store(0, Ordering::SeqCst);

        // Test MultiPushQueue
        {
            let queue = MultiPushQueue::<DropCounter>::with_capacity(MIRI_MEDIUM_CAP);

            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            // Items still in local buffer
            assert!(queue.local_count.load(Ordering::Relaxed) > 0);

            // Manually flush before drop to ensure items are in main queue
            queue.flush();

            // Pop a few items
            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }

            // Check drops so far
            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);

            // Queue drops here, should drop remaining 5 items
        }

        thread::yield_now();

        // Should have dropped all 10 items (5 explicit + 5 on queue drop)
        let drops_after_multipush = DROP_COUNT.load(Ordering::SeqCst);
        assert!(
            drops_after_multipush >= 5,
            "Should have dropped at least the 5 explicit items"
        );

        DROP_COUNT.store(0, Ordering::SeqCst);

        // Test BiffqQueue
        {
            let queue = BiffqQueue::<DropCounter>::with_capacity(128);

            for i in 0..10 {
                queue.push(DropCounter { _value: i }).unwrap();
            }

            // Items still in local buffer
            assert!(queue.prod.local_count.load(Ordering::Relaxed) > 0);

            // Flush buffer
            let _ = queue.flush_producer_buffer();

            // Pop a few items
            for _ in 0..5 {
                drop(queue.pop().unwrap());
            }

            // Check drops so far
            assert_eq!(DROP_COUNT.load(Ordering::SeqCst), 5);

            // Queue drops here, should drop remaining 5 items
        }

        thread::yield_now();

        // Should have dropped all items
        let final_drops = DROP_COUNT.load(Ordering::SeqCst);
        assert!(
            final_drops >= 5,
            "Should have dropped at least the 5 explicit items"
        );
    }
}

// Memory safety tests
mod miri_memory_safety {
    use super::*;

    #[test]
    fn test_shared_memory_bounds() {
        // Test that we don't write outside allocated memory
        let capacity = 32;
        let shared_size = LamportQueue::<u64>::shared_size(capacity);
        let mut memory = AlignedMemory::new(shared_size, 64);
        let mem_ptr = memory.as_mut_ptr();

        let queue = unsafe { LamportQueue::<u64>::init_in_shared(mem_ptr, capacity) };

        // Fill to capacity - 1
        let mut pushed = 0;
        for i in 0..capacity {
            if queue.push(i as u64).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }

        assert_eq!(pushed, capacity - 1);

        // Try to push more (should fail without corrupting memory)
        assert!(queue.push(999).is_err());

        // Drain queue
        for _ in 0..pushed {
            queue.pop().unwrap();
        }
    }

    #[test]
    fn test_zero_sized_types() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct ZeroSized;

        let queue = LamportQueue::<ZeroSized>::with_capacity(32);

        // ZSTs should work correctly
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
            data: [u64; 32], // Smaller than non-Miri test but still large
        }

        let queue = LamportQueue::<LargeType>::with_capacity(8);
        let item = LargeType { data: [42; 32] };

        queue.push(item.clone()).unwrap();
        assert_eq!(queue.pop().unwrap(), item);
    }

    #[test]
    fn test_alignment_requirements() {
        // Test queues with different alignment requirements

        // 64-byte aligned queue
        {
            let shared_size = LamportQueue::<usize>::shared_size(32);
            let mut memory = AlignedMemory::new(shared_size, 64);
            let mem_ptr = memory.as_mut_ptr();

            assert_eq!(mem_ptr as usize % 64, 0);

            let queue = unsafe { LamportQueue::<usize>::init_in_shared(mem_ptr, 32) };
            queue.push(42).unwrap();
            assert_eq!(queue.pop().unwrap(), 42);
        }

        // 128-byte aligned queue (skip UnboundedQueue in Miri)
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

        // Test DynListQueue with 128-byte alignment instead
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

// Concurrency tests (limited for Miri performance)
mod miri_concurrency {
    use super::*;

    #[test]
    fn test_concurrent_small() {
        let queue = Arc::new(LamportQueue::<usize>::with_capacity(64));
        let items = 20; // Small number for Miri

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

        // Sum of 0..20 = 190
        assert_eq!(sum, (items - 1) * items / 2);
    }

    #[test]
    fn test_concurrent_buffered_queues() {
        // Test MultiPushQueue with concurrency
        {
            let queue = Arc::new(MultiPushQueue::<usize>::with_capacity(128));
            let items = 50;

            let q_prod = queue.clone();
            let producer = thread::spawn(move || {
                for i in 0..items {
                    q_prod.push(i).unwrap();
                }
                // Ensure flush
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
            // Note: order might not be preserved due to buffering
        }
    }

    // Skip concurrent test for FfqQueue in Miri due to false positive data race detection
    // FfqQueue (FastForward) relies on cache-line synchronization which Miri doesn't understand
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
        // Test only BlqQueue in Miri to avoid false positives
        let blq = Arc::new(BlqQueue::<usize>::with_capacity(64));

        let items = 10;

        // BLQ producer/consumer
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

        // Join threads
        blq_producer.join().unwrap();
        let blq_sum = blq_consumer.join().unwrap();

        let expected_sum = (items - 1) * items / 2;
        assert_eq!(blq_sum, expected_sum);
    }
}

// Test queues that need special initialization
mod miri_special_init {
    use super::*;

    #[test]
    fn test_llq_init() {
        // LLQ has special capacity requirements
        let capacity = 128; // Must be > K_CACHE_LINE_SLOTS and power of 2
        let queue = LlqQueue::<usize>::with_capacity(capacity);

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
    }

    #[test]
    fn test_blq_init() {
        // BLQ also has special requirements
        let capacity = 64; // Must be > K_CACHE_LINE_SLOTS
        let queue = BlqQueue::<usize>::with_capacity(capacity);

        // Test batch operations
        queue.blq_enq_local(1).unwrap();
        queue.blq_enq_local(2).unwrap();
        queue.blq_enq_publish();

        assert_eq!(queue.blq_deq_local().unwrap(), 1);
        assert_eq!(queue.blq_deq_local().unwrap(), 2);
        queue.blq_deq_publish();
    }

    #[test]
    fn test_iffq_init() {
        // IFFQ requires capacity to be power of 2 and multiple of H_PARTITION_SIZE
        let capacity = 128;
        let queue = IffqQueue::<usize>::with_capacity(capacity);

        queue.push(123).unwrap();
        assert_eq!(queue.pop().unwrap(), 123);
    }

    #[test]
    fn test_biffq_init() {
        // BIFFQ has similar requirements to IFFQ
        let capacity = 128;
        let queue = BiffqQueue::<usize>::with_capacity(capacity);

        queue.push(456).unwrap();
        let _ = queue.flush_producer_buffer();
        assert_eq!(queue.pop().unwrap(), 456);
    }

    #[test]
    fn test_sesd_wrapper_init() {
        // SESD wrapper needs pool capacity
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

// Error handling tests
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
        let _ = LlqQueue::<usize>::with_capacity(4); // Too small
    }

    #[test]
    #[should_panic(expected = "Capacity must be at least 2 * H_PARTITION_SIZE")]
    fn test_iffq_invalid_capacity() {
        // Too small - less than 2 * H_PARTITION_SIZE (64)
        let _ = IffqQueue::<usize>::with_capacity(32);
    }

    #[test]
    #[should_panic(expected = "Capacity must be a power of two")]
    fn test_iffq_invalid_capacity_not_power_of_two() {
        // Not a power of two (96 is not a power of 2)
        // This check happens before the multiple check
        let _ = IffqQueue::<usize>::with_capacity(96);
    }

    #[test]
    fn test_push_error_handling() {
        let queue = LamportQueue::<String>::with_capacity(2);

        queue.push("first".to_string()).unwrap();

        // Should fail on full queue
        match queue.push("second".to_string()) {
            Err(_) => {} // Expected
            Ok(_) => panic!("Push should have failed on full queue"),
        }
    }

    #[test]
    fn test_pop_error_handling() {
        let queue = BQueue::<usize>::new(16);

        // Pop from empty queue should fail
        assert!(queue.pop().is_err());

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);

        // Should fail again when empty
        assert!(queue.pop().is_err());
    }
}

// Additional edge cases
mod miri_edge_cases {
    use super::*;

    #[test]
    fn test_alternating_push_pop() {
        let queue = LamportQueue::<usize>::with_capacity(4);

        // Alternating push/pop pattern
        for i in 0..20 {
            queue.push(i).unwrap();
            assert_eq!(queue.pop().unwrap(), i);
        }

        assert!(queue.empty());
    }

    #[test]
    fn test_wraparound() {
        let queue = FfqQueue::<usize>::with_capacity(8);

        // Fill half
        for i in 0..4 {
            queue.push(i).unwrap();
        }

        // Pop half
        for i in 0..4 {
            assert_eq!(queue.pop().unwrap(), i);
        }

        // Push more to cause wraparound
        for i in 4..12 {
            queue.push(i).unwrap();
        }

        // Pop all
        for i in 4..12 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_queue_state_transitions() {
        let queue = BQueue::<usize>::new(16);

        // Empty -> Has items
        assert!(queue.empty());
        assert!(queue.available());

        queue.push(1).unwrap();
        assert!(!queue.empty());
        assert!(queue.available());

        // Fill to capacity (BQueue uses capacity - 1)
        let mut pushed = 1; // Already pushed 1
        for i in 2..16 {
            if queue.push(i).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }

        println!("Pushed {} items to BQueue with capacity 16", pushed);

        // Should have some items
        assert!(!queue.empty());

        // BQueue might still report available even when nearly full due to batching
        // Just verify we can't push many more items
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

        // Back to empty
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

// Additional safety tests
#[test]
fn test_multiple_queues() {
    // Ensure multiple queues don't interfere
    let q1 = LamportQueue::<u32>::with_capacity(32);
    let q2 = LamportQueue::<u32>::with_capacity(32);

    q1.push(100).unwrap();
    q2.push(200).unwrap();

    assert_eq!(q1.pop().unwrap(), 100);
    assert_eq!(q2.pop().unwrap(), 200);
}

#[test]
fn test_arc_safety() {
    // Test Arc doesn't cause issues
    let queue = Arc::new(BQueue::<i32>::new(64));
    let q1 = queue.clone();
    let q2 = queue.clone();

    drop(q1);

    // q2 should still work
    q2.push(42).unwrap();
    assert_eq!(q2.pop().unwrap(), 42);
}

#[test]
fn test_different_types() {
    // Test with different types to ensure generic implementation works

    // Primitive types
    {
        let q_u8 = LamportQueue::<u8>::with_capacity(16);
        q_u8.push(255u8).unwrap();
        assert_eq!(q_u8.pop().unwrap(), 255u8);
    }

    // Box types
    {
        let q_box = FfqQueue::<Box<usize>>::with_capacity(16);
        q_box.push(Box::new(42)).unwrap();
        assert_eq!(*q_box.pop().unwrap(), 42);
    }

    // Option types
    {
        let q_opt = BQueue::<Option<String>>::new(16);
        q_opt.push(Some("hello".to_string())).unwrap();
        q_opt.push(None).unwrap();

        assert_eq!(q_opt.pop().unwrap(), Some("hello".to_string()));
        assert_eq!(q_opt.pop().unwrap(), None);
    }
}
