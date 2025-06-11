// queues/tests/kw_debug_test.rs
// Debug test to find exact issue with KW queue

use queues::mpmc::KWQueue;
use std::ptr;
use nix::libc;

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
fn debug_item_5_issue() {
    println!("\n=== DEBUG: Why does it fail after item 5? ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(1);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 1);
        
        // Test items 0-10 one by one
        for i in 0..10 {
            println!("\n--- Testing item {} ---", i);
            
            println!("Before enqueue: is_empty = {}", queue.is_empty());
            
            match queue.enqueue(0, i) {
                Ok(_) => println!("Enqueue {} succeeded", i),
                Err(_) => {
                    println!("Enqueue {} FAILED!", i);
                    break;
                }
            }
            
            println!("After enqueue: is_empty = {}", queue.is_empty());
            
            match queue.dequeue(0) {
                Ok(val) => {
                    println!("Dequeue succeeded, got {}", val);
                    assert_eq!(val, i, "Wrong value!");
                }
                Err(_) => {
                    println!("Dequeue FAILED after enqueueing {}!", i);
                    break;
                }
            }
            
            println!("After dequeue: is_empty = {}", queue.is_empty());
        }
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn debug_batch_at_boundary() {
    println!("\n=== DEBUG: Batch enqueue around boundary ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(1);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 1);
        
        // Enqueue items 0-9
        println!("Enqueueing items 0-9...");
        for i in 0..10 {
            match queue.enqueue(0, i) {
                Ok(_) => print!("{} ", i),
                Err(_) => {
                    println!("\nEnqueue FAILED at item {}", i);
                    break;
                }
            }
        }
        println!("\nEnqueue phase done");
        
        // Try to dequeue them all
        println!("\nDequeueing items...");
        for i in 0..10 {
            match queue.dequeue(0) {
                Ok(val) => {
                    print!("{} ", val);
                    if val != i {
                        println!("\nERROR: Expected {}, got {}", i, val);
                    }
                }
                Err(_) => {
                    println!("\nDequeue FAILED at position {}", i);
                    break;
                }
            }
        }
        println!("\nDequeue phase done");
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn debug_counting_set_k2() {
    println!("\n=== DEBUG: Test with k=2 (recursive counting set) ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(2);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 2);
        
        // Test items 0-10
        for i in 0..10 {
            println!("\n--- Item {} ---", i);
            
            // Producer thread 0
            match queue.enqueue(0, i) {
                Ok(_) => println!("Thread 0: Enqueue {} succeeded", i),
                Err(_) => {
                    println!("Thread 0: Enqueue {} FAILED!", i);
                    break;
                }
            }
            
            // Consumer thread 1
            match queue.dequeue(1) {
                Ok(val) => {
                    println!("Thread 1: Dequeue succeeded, got {}", val);
                    assert_eq!(val, i);
                }
                Err(_) => {
                    println!("Thread 1: Dequeue FAILED!");
                    break;
                }
            }
        }
        
        unmap_shared(shm_ptr, bytes);
    }
}

#[test]
fn debug_sqrt_k_issue() {
    println!("\n=== DEBUG: Check sqrt_k calculation ===");
    
    unsafe {
        for k in 1..=10 {
            let sqrt_k = ((k as f64).sqrt().floor() as u32).max(1);
            println!("k={}, sqrt_k={}", k, sqrt_k);
        }
        
        // Test with k=4 specifically
        let k = 4;
        let bytes = KWQueue::<usize>::shared_size(k);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, k);
        
        println!("\nTesting with k={} (2 producers, 2 consumers)", k);
        
        // Enqueue from both producers
        for i in 0..10 {
            let prod_id = i % 2;
            match queue.enqueue(prod_id, i) {
                Ok(_) => println!("Producer {}: enqueued {}", prod_id, i),
                Err(_) => {
                    println!("Producer {}: FAILED to enqueue {}", prod_id, i);
                    break;
                }
            }
        }
        
        // Dequeue from both consumers
        for i in 0..10 {
            let cons_id = 2 + (i % 2);
            match queue.dequeue(cons_id) {
                Ok(val) => println!("Consumer {}: dequeued {}", cons_id - 2, val),
                Err(_) => {
                    println!("Consumer {}: FAILED to dequeue", cons_id - 2);
                    break;
                }
            }
        }
        
        unmap_shared(shm_ptr, bytes);
    }
}

// Run with: cargo test --test kw_debug_test -- --nocapture --test-threads=1