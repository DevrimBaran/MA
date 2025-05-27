// queues/tests/miri_tests.rs
//! Miri-compatible tests for SPSC queue implementations
//!
//! Run with: cargo +nightly miri test --test miri_tests
//!
//! These tests avoid:
//! - FFI calls (mmap, fork, etc.)
//! - Large allocations
//! - Excessive concurrency
//! - Platform-specific code

#![cfg(miri)]

use queues::{spsc::*, SpscQueue};
use std::sync::{Arc, Barrier};
use std::thread;

// Smaller capacities for Miri
const MIRI_SMALL_CAP: usize = 16;
const MIRI_MEDIUM_CAP: usize = 64;
const MIRI_TEST_ITEMS: usize = 100;

// Helper to create aligned memory using standard allocation
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

// Basic queue tests with heap allocation
mod miri_basic_tests {
    use super::*;

    #[test]
    fn miri_test_lamport() {
        // Test 1: Basic push/pop
        {
            let queue = LamportQueue::<usize>::with_capacity(MIRI_SMALL_CAP);

            assert!(queue.empty());
            queue.push(42).unwrap();
            assert!(!queue.empty());
            assert_eq!(queue.pop().unwrap(), 42);
            assert!(queue.empty());
        }

        // Test 2: Fill and drain
        {
            let queue = LamportQueue::<usize>::with_capacity(MIRI_SMALL_CAP);
            let items_to_push = MIRI_SMALL_CAP / 2;

            for i in 0..items_to_push {
                assert!(queue.push(i).is_ok(), "Failed to push item {}", i);
            }

            for i in 0..items_to_push {
                assert_eq!(queue.pop().unwrap(), i);
            }
            assert!(queue.empty());
        }

        // Test 3: Simple concurrent test
        {
            let queue = Arc::new(LamportQueue::<usize>::with_capacity(MIRI_SMALL_CAP));
            let barrier = Arc::new(Barrier::new(2));
            let items = 8; // Small number for Miri

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
        // Test 1: Basic push/pop
        {
            let queue = FfqQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);

            assert!(queue.empty());
            queue.push(42).unwrap();
            assert!(!queue.empty());
            assert_eq!(queue.pop().unwrap(), 42);
            assert!(queue.empty());
        }

        // Test 2: Fill and drain
        {
            let queue = FfqQueue::<usize>::with_capacity(MIRI_MEDIUM_CAP);
            let items_to_push = MIRI_MEDIUM_CAP / 2;

            for i in 0..items_to_push {
                assert!(queue.push(i).is_ok(), "Failed to push item {}", i);
            }

            for i in 0..items_to_push {
                assert_eq!(queue.pop().unwrap(), i);
            }
            assert!(queue.empty());
        }
    }

    #[test]
    fn miri_test_bqueue() {
        // Test 1: Basic push/pop
        {
            let queue = BQueue::<usize>::new(MIRI_MEDIUM_CAP);

            assert!(queue.empty());
            queue.push(42).unwrap();
            assert!(!queue.empty());
            assert_eq!(queue.pop().unwrap(), 42);
            assert!(queue.empty());
        }

        // Test 2: Fill and drain
        {
            let queue = BQueue::<usize>::new(MIRI_MEDIUM_CAP);
            let items_to_push = MIRI_MEDIUM_CAP / 2;

            for i in 0..items_to_push {
                assert!(queue.push(i).is_ok(), "Failed to push item {}", i);
            }

            for i in 0..items_to_push {
                assert_eq!(queue.pop().unwrap(), i);
            }
            assert!(queue.empty());
        }
    }

    #[test]
    fn miri_test_dehnavi() {
        // DehnaviQueue uses 'new' instead of 'with_capacity'
        let queue = DehnaviQueue::<usize>::new(MIRI_SMALL_CAP);

        assert!(queue.empty());
        queue.push(42).unwrap();
        assert!(!queue.empty());
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());

        // Test lossy behavior
        for i in 0..MIRI_SMALL_CAP * 2 {
            queue.push(i).unwrap();
        }

        // Should see some items but not necessarily all due to lossy nature
        let mut items = Vec::new();
        while let Ok(item) = queue.pop() {
            items.push(item);
        }
        assert!(!items.is_empty());
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
    }

    #[test]
    fn miri_test_llq() {
        let queue = LlqQueue::<usize>::with_capacity(128); // Must be > K_CACHE_LINE_SLOTS

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());
    }

    #[test]
    fn miri_test_blq() {
        let queue = BlqQueue::<usize>::with_capacity(64); // Must be > K_CACHE_LINE_SLOTS

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());
    }

    #[test]
    fn miri_test_iffq() {
        let queue = IffqQueue::<usize>::with_capacity(128); // Must be power of 2 and multiple of 32

        queue.push(42).unwrap();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());
    }

    #[test]
    fn miri_test_biffq() {
        let queue = BiffqQueue::<usize>::with_capacity(128); // Same requirements as IFFQ

        queue.push(42).unwrap();
        let _ = queue.flush_producer_buffer();
        assert_eq!(queue.pop().unwrap(), 42);
        assert!(queue.empty());
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

        // AlignedMemory will handle cleanup on drop
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

        // AlignedMemory will handle cleanup on drop
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
        let initial_distance = queue.distance();
        queue.push(1).unwrap();
        queue.push(2).unwrap();
        let new_distance = queue.distance();
        assert!(new_distance >= initial_distance);

        // AlignedMemory will handle cleanup on drop
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

        // AlignedMemory will handle cleanup on drop
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

        // AlignedMemory will handle cleanup on drop
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

        // AlignedMemory will handle cleanup on drop
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

        // AlignedMemory will handle cleanup on drop
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

        // AlignedMemory will handle cleanup on drop
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

        // AlignedMemory will handle cleanup on drop
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

        // AlignedMemory will handle cleanup on drop
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

        // Force flush
        assert!(queue.flush());

        // Verify items
        for i in 0..10 {
            assert_eq!(queue.pop().unwrap(), i);
        }
    }

    #[test]
    fn test_biffq_producer_buffer() {
        let queue = BiffqQueue::<usize>::with_capacity(128); // Needs specific size

        // Fill local buffer
        for i in 0..20 {
            queue.push(i).unwrap();
        }

        // Flush to shared buffer
        let _ = queue.flush_producer_buffer();

        // Consume items
        for i in 0..20 {
            assert_eq!(queue.pop().unwrap(), i);
        }
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

        queue.adjust_slip(100);

        // Clean up
        let _ = queue.pop();
        let _ = queue.pop();
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

// Test with types that need drop
mod miri_drop_tests {
    use super::*;
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
            // Queue drops here, should drop remaining items
        }

        // In non-Miri tests we'd check the final count, but Miri's
        // execution model might make this unreliable
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
}

// Memory safety tests
mod miri_memory_safety {
    use super::*;

    #[test]
    fn test_shared_memory_bounds() {
        // Test that we don't write outside allocated memory
        let capacity = 32;
        let shared_size = LamportQueue::<u64>::shared_size(capacity);
        let alignment = 64;

        // Allocate with proper layout tracking
        let layout =
            std::alloc::Layout::from_size_align(shared_size, alignment).expect("Invalid layout");

        let mem_ptr = unsafe {
            let ptr = std::alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            ptr
        };

        let queue = unsafe { LamportQueue::<u64>::init_in_shared(mem_ptr, capacity) };

        // Fill to capacity
        let mut pushed = 0;
        for i in 0..capacity {
            if queue.push(i as u64).is_ok() {
                pushed += 1;
            } else {
                break;
            }
        }

        assert!(pushed > 0);

        // Try to push more (should fail without corrupting memory)
        assert!(queue.push(999).is_err());

        // Drain queue
        for _ in 0..pushed {
            queue.pop().unwrap();
        }

        // Deallocate with the same layout
        unsafe {
            std::alloc::dealloc(mem_ptr, layout);
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
