// queues/tests/kw_fork_test.rs
// Fork-based tests for the KW Queue to diagnose multi-process data loss

use queues::mpmc::KWQueue;
use std::ptr;
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use nix::libc;
use nix::unistd::{fork, ForkResult};
use nix::sys::wait::waitpid;

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
fn test_fork_spsc() {
    println!("\n=== test_fork_spsc ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(2);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 2);
        
        // Shared counters
        let counter_bytes = std::mem::size_of::<AtomicU32>() * 2;
        let counter_ptr = map_shared(counter_bytes);
        let produced = counter_ptr as *mut AtomicU32;
        let consumed = counter_ptr.add(std::mem::size_of::<AtomicU32>()) as *mut AtomicU32;
        
        ptr::write(produced, AtomicU32::new(0));
        ptr::write(consumed, AtomicU32::new(0));
        
        let items = 1000;
        
        match fork() {
            Ok(ForkResult::Parent { child }) => {
                // Parent is consumer
                let mut received = 0;
                let mut empty_count = 0;
                
                while received < items {
                    match queue.dequeue(1) {
                        Ok(val) => {
                            assert_eq!(val, received, "Out of order: expected {}, got {}", received, val);
                            received += 1;
                            (*consumed).fetch_add(1, Ordering::Release);
                            empty_count = 0;
                        }
                        Err(_) => {
                            empty_count += 1;
                            if empty_count > 1_000_000 {
                                let prod_count = (*produced).load(Ordering::Acquire);
                                println!("Consumer stuck at {}/{}, producer at {}", received, items, prod_count);
                                break;
                            }
                        }
                    }
                }
                
                waitpid(child, None).expect("waitpid failed");
                
                let final_consumed = (*consumed).load(Ordering::Acquire);
                let final_produced = (*produced).load(Ordering::Acquire);
                
                println!("Final: produced={}, consumed={}", final_produced, final_consumed);
                assert_eq!(final_consumed, items as u32, "Lost {} items", items - final_consumed as usize);
                
                unmap_shared(shm_ptr, bytes);
                unmap_shared(counter_ptr, counter_bytes);
            }
            Ok(ForkResult::Child) => {
                // Child is producer
                for i in 0..items {
                    while queue.enqueue(0, i).is_err() {
                        // Retry
                    }
                    (*produced).fetch_add(1, Ordering::Release);
                }
                libc::_exit(0);
            }
            Err(e) => panic!("Fork failed: {}", e),
        }
    }
    
    println!("✓ Fork SPSC test passed");
}

#[test]
fn test_fork_mpmc_minimal() {
    println!("\n=== test_fork_mpmc_minimal ===");
    
    unsafe {
        let bytes = KWQueue::<usize>::shared_size(4);
        let shm_ptr = map_shared(bytes);
        let queue = KWQueue::init_in_shared(shm_ptr, 4);
        
        // Shared state
        let state_bytes = std::mem::size_of::<AtomicU32>() * 3 + std::mem::size_of::<AtomicBool>();
        let state_ptr = map_shared(state_bytes);
        let produced = state_ptr as *mut AtomicU32;
        let consumed = (state_ptr as *mut AtomicU32).add(1);
        let lost_items = (state_ptr as *mut AtomicU32).add(2);
        let start_flag = state_ptr.add(std::mem::size_of::<AtomicU32>() * 3) as *mut AtomicBool;
        
        ptr::write(produced, AtomicU32::new(0));
        ptr::write(consumed, AtomicU32::new(0));
        ptr::write(lost_items, AtomicU32::new(0));
        ptr::write(start_flag, AtomicBool::new(false));
        
        let items_per_producer = 100;
        let num_producers = 2;
        let num_consumers = 2;
        let total_items = items_per_producer * num_producers;
        
        let mut child_pids = Vec::new();
        
        // Fork producers
        for prod_id in 0..num_producers {
            match fork() {
                Ok(ForkResult::Parent { child }) => {
                    child_pids.push(child);
                }
                Ok(ForkResult::Child) => {
                    // Wait for start
                    while !(*start_flag).load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    
                    // Produce items
                    for i in 0..items_per_producer {
                        let value = prod_id * 1000 + i;
                        let mut attempts = 0;
                        while queue.enqueue(prod_id, value).is_err() {
                            attempts += 1;
                            if attempts > 10000 {
                                eprintln!("Producer {} stuck on item {}", prod_id, i);
                                break;
                            }
                        }
                        (*produced).fetch_add(1, Ordering::Release);
                    }
                    
                    libc::_exit(0);
                }
                Err(e) => panic!("Fork failed: {}", e),
            }
        }
        
        // Fork consumers
        for cons_id in 0..num_consumers {
            match fork() {
                Ok(ForkResult::Parent { child }) => {
                    child_pids.push(child);
                }
                Ok(ForkResult::Child) => {
                    // Wait for start
                    while !(*start_flag).load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    
                    let consumer_id = num_producers + cons_id;
                    let mut received = 0;
                    let mut empty_count = 0;
                    let target = total_items / num_consumers;
                    
                    while received < target {
                        match queue.dequeue(consumer_id) {
                            Ok(_val) => {
                                received += 1;
                                (*consumed).fetch_add(1, Ordering::Release);
                                empty_count = 0;
                            }
                            Err(_) => {
                                empty_count += 1;
                                
                                // Check if all items produced
                                if (*produced).load(Ordering::Acquire) >= total_items as u32 {
                                    if empty_count > 10000 {
                                        // Queue seems empty but we haven't got all items
                                        let lost = target - received;
                                        (*lost_items).fetch_add(lost as u32, Ordering::Release);
                                        break;
                                    }
                                }
                                
                                if empty_count > 100 {
                                    std::thread::yield_now();
                                }
                            }
                        }
                    }
                    
                    libc::_exit(0);
                }
                Err(e) => panic!("Fork failed: {}", e),
            }
        }
        
        // Start all processes
        std::thread::sleep(std::time::Duration::from_millis(10));
        (*start_flag).store(true, Ordering::Release);
        
        // Wait for all children
        for pid in child_pids {
            waitpid(pid, None).expect("waitpid failed");
        }
        
        // Check results
        let final_produced = (*produced).load(Ordering::Acquire);
        let final_consumed = (*consumed).load(Ordering::Acquire);
        let final_lost = (*lost_items).load(Ordering::Acquire);
        
        println!("Produced: {}", final_produced);
        println!("Consumed: {}", final_consumed);
        println!("Lost: {}", final_lost);
        
        assert_eq!(final_produced, total_items as u32);
        assert_eq!(final_consumed + final_lost, total_items as u32, 
                   "Data loss: {} items unaccounted for", 
                   total_items as u32 - final_consumed - final_lost);
        
        if final_lost > 0 {
            panic!("Lost {} items in fork test", final_lost);
        }
        
        unmap_shared(shm_ptr, bytes);
        unmap_shared(state_ptr, state_bytes);
    }
    
    println!("✓ Fork MPMC minimal test passed");
}

#[test]
fn test_fork_trace_single_loss() {
    println!("\n=== test_fork_trace_single_loss ===");
    
    unsafe {
        // Run multiple rounds to catch intermittent issues
        for round in 0..5 {
            println!("\nRound {}", round);
            
            let bytes = KWQueue::<usize>::shared_size(2);
            let shm_ptr = map_shared(bytes);
            let queue = KWQueue::init_in_shared(shm_ptr, 2);
            
            let items = 20; // Small number for easier debugging
            
            match fork() {
                Ok(ForkResult::Parent { child }) => {
                    // Parent is consumer
                    std::thread::sleep(std::time::Duration::from_millis(5)); // Let producer start
                    
                    let mut received = Vec::new();
                    let mut empty_count = 0;
                    
                    while received.len() < items {
                        match queue.dequeue(1) {
                            Ok(val) => {
                                println!("Consumer: got {}", val);
                                received.push(val);
                                empty_count = 0;
                            }
                            Err(_) => {
                                empty_count += 1;
                                if empty_count > 100000 {
                                    println!("Consumer: timeout at {}/{}", received.len(), items);
                                    break;
                                }
                            }
                        }
                    }
                    
                    waitpid(child, None).expect("waitpid failed");
                    
                    if received.len() != items {
                        println!("LOST ITEMS in round {}!", round);
                        println!("Expected: {} items", items);
                        println!("Received: {} items", received.len());
                        println!("Values: {:?}", received);
                        
                        // Find missing
                        let mut missing = Vec::new();
                        for i in 0..items {
                            if !received.contains(&i) {
                                missing.push(i);
                            }
                        }
                        println!("Missing: {:?}", missing);
                        
                        panic!("Data loss detected in round {}", round);
                    }
                }
                Ok(ForkResult::Child) => {
                    // Child is producer
                    for i in 0..items {
                        println!("Producer: enqueue {}", i);
                        assert!(queue.enqueue(0, i).is_ok());
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                    println!("Producer: done");
                    libc::_exit(0);
                }
                Err(e) => panic!("Fork failed: {}", e),
            }
            
            unmap_shared(shm_ptr, bytes);
        }
    }
    
    println!("✓ Fork trace single loss test passed (5 rounds)");
}

// Run with: cargo test --test kw_fork_test -- --nocapture --test-threads=1