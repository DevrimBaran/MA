// queues/tests/kw_test.rs
// Tests for the Khanchandani-Wattenhofer MPMC Queue in shared memory

use queues::mpmc::KWQueue;
use std::ptr;
use nix::libc;

// Helper to allocate shared memory
unsafe fn map_shared(bytes: usize) -> *mut u8 {
    let ptr = libc::mmap(
        ptr::null_mut(),
        bytes,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_SHARED | libc::MAP_ANONYMOUS,
        -1,
        0,
    );
    if ptr == libc::MAP_FAILED {
        panic!("mmap failed: {}", std::io::Error::last_os_error());
    }
    ptr.cast()
}

unsafe fn unmap_shared(ptr: *mut u8, len: usize) {
    if libc::munmap(ptr.cast(), len) == -1 {
        panic!("munmap failed: {}", std::io::Error::last_os_error());
    }
}

#[test]
fn test_simple_string() {
    println!("\n=== test_simple_string ===");
    
    unsafe {
        let bytes = KWQueue::<String>::shared_size(1);
        println!("Allocated {} bytes for String queue", bytes);
        
        let shm_ptr = map_shared(bytes);
        println!("Mapped shared memory at {:p}", shm_ptr);
        
        let queue = KWQueue::init_in_shared(shm_ptr, 1);
        println!("Initialized queue");
        
        // Try a single operation
        let test_str = "hello".to_string();
        println!("Attempting to enqueue: '{}'", test_str);
        
        match queue.enqueue(0, test_str.clone()) {
            Ok(_) => println!("Enqueue succeeded"),
            Err(_) => println!("Enqueue failed"),
        }
        
        println!("Attempting to dequeue");
        match queue.dequeue(0) {
            Ok(val) => println!("Dequeued: '{}'", val),
            Err(_) => println!("Dequeue failed"),
        }
        
        println!("✓ Simple string test passed");
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn test_single_thread_basic() {
    println!("\n=== test_single_thread_basic ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(1);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 1);
        
        // Test empty queue
        assert!(queue.is_empty());
        assert!(queue.dequeue(0).is_err());
        
        // Test single enqueue/dequeue
        assert!(queue.enqueue(0, 42).is_ok());
        assert!(!queue.is_empty());
        
        let val = queue.dequeue(0);
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), 42);
        assert!(queue.is_empty());
        
        println!("✓ Single thread basic operations work");
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn test_single_thread_multiple_items() {
    println!("\n=== test_single_thread_multiple_items ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(1);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 1);
        
        // Enqueue multiple items
        for i in 0..100 {
            assert!(queue.enqueue(0, i).is_ok(), "Failed to enqueue {}", i);
        }
        
        // Dequeue and verify
        for i in 0..100 {
            let val = queue.dequeue(0);
            assert!(val.is_ok(), "Failed to dequeue at position {}", i);
            assert_eq!(val.unwrap(), i, "Wrong value at position {}", i);
        }
        
        assert!(queue.is_empty());
        println!("✓ Single thread 100 items work");
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn test_alternating_enqueue_dequeue() {
    println!("\n=== test_alternating_enqueue_dequeue ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(1);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 1);
        
        for i in 0..50 {
            assert!(queue.enqueue(0, i * 2).is_ok());
            assert!(queue.enqueue(0, i * 2 + 1).is_ok());
            
            let val1 = queue.dequeue(0);
            assert!(val1.is_ok());
            assert_eq!(val1.unwrap(), i * 2);
            
            let val2 = queue.dequeue(0);
            assert!(val2.is_ok());
            assert_eq!(val2.unwrap(), i * 2 + 1);
        }
        
        assert!(queue.is_empty());
        println!("✓ Alternating enqueue/dequeue works");
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn test_single_item_trace() {
    println!("\n=== test_single_item_trace ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(2);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 2);
        
        // Test with just a few items to trace the issue
        println!("Enqueuing 5 items...");
        for i in 0..5 {
            println!("  Enqueue({})", i);
            assert!(queue.enqueue(0, i).is_ok());
        }
        
        println!("\nDequeuing 5 items...");
        for i in 0..5 {
            match queue.dequeue(1) {
                Ok(val) => {
                    println!("  Dequeue() -> {}", val);
                    assert_eq!(val, i, "Expected {}, got {}", i, val);
                }
                Err(_) => {
                    panic!("  Dequeue() failed at position {}", i);
                }
            }
        }
        
        println!("✓ Single item trace test passed");
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn test_rapid_alternating() {
    println!("\n=== test_rapid_alternating ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(2);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 2);
        
        // Rapid alternating to stress test
        for i in 0..1000 {
            assert!(queue.enqueue(0, i).is_ok(), "Failed to enqueue {}", i);
            
            let val = queue.dequeue(1);
            assert!(val.is_ok(), "Failed to dequeue after enqueue {}", i);
            assert_eq!(val.unwrap(), i);
        }
        
        assert!(queue.is_empty());
        println!("✓ Rapid alternating test passed");
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn test_batch_operations() {
    println!("\n=== test_batch_operations ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(2);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 2);
        
        // Test batches of operations
        for batch in 0..10 {
            let batch_size = 100;
            let start = batch * batch_size;
            
            // Enqueue batch
            for i in 0..batch_size {
                let val = start + i;
                assert!(queue.enqueue(0, val).is_ok(), "Failed to enqueue {}", val);
            }
            
            // Dequeue batch
            for i in 0..batch_size {
                let expected = start + i;
                let val = queue.dequeue(1);
                assert!(val.is_ok(), "Failed to dequeue at batch {}, item {}", batch, i);
                assert_eq!(val.unwrap(), expected);
            }
        }
        
        assert!(queue.is_empty());
        println!("✓ Batch operations test passed");
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn test_counting_set_basic() {
    println!("\n=== test_counting_set_basic ===");
    
    unsafe {
        let bytes = KWQueue::<String>::shared_size(1);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 1);
        
        // Test with strings to ensure Clone works correctly
        let items = vec![
            "first".to_string(),
            "second".to_string(),
            "third".to_string(),
            "fourth".to_string(),
            "fifth".to_string(),
        ];
        
        // Enqueue all
        for (i, item) in items.iter().enumerate() {
            println!("Enqueue: {} -> '{}'", i, item);
            assert!(queue.enqueue(0, item.clone()).is_ok());
        }
        
        // Dequeue all
        for (i, expected) in items.iter().enumerate() {
            match queue.dequeue(0) {
                Ok(val) => {
                    println!("Dequeue: {} -> '{}'", i, val);
                    assert_eq!(&val, expected);
                }
                Err(_) => panic!("Failed to dequeue item {}", i),
            }
        }
        
        assert!(queue.is_empty());
        println!("✓ Counting set basic test passed");
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn test_large_values() {
    println!("\n=== test_large_values ===");
    
    unsafe {
        let bytes = KWQueue::<Vec<u8>>::shared_size(1);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 1);
        
        // Test with larger values
        for i in 0..10 {
            let data = vec![i as u8; 100]; // 100 byte vectors
            assert!(queue.enqueue(0, data.clone()).is_ok());
            
            let received = queue.dequeue(0);
            assert!(received.is_ok());
            assert_eq!(received.unwrap(), data);
        }
        
        assert!(queue.is_empty());
        println!("✓ Large values test passed");
        
        unmap_shared(shm_ptr, bytes);
    }
}

// Run with: cargo test --test kw_test -- --nocapture --test-threads=1