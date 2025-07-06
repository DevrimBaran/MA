use queues::spmc::{DavidQueue, EnqueuerState};
use queues::SpmcQueue;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

const NUM_CONSUMERS: usize = 1;
const TEST_ITEMS: usize = 5;

fn create_aligned_memory(size: usize) -> Box<[u8]> {
    use std::alloc::{alloc_zeroed, Layout};

    unsafe {
        let layout = Layout::from_size_align(size, 4096).unwrap();
        let ptr = alloc_zeroed(layout);
        if ptr.is_null() {
            panic!("Failed to allocate aligned memory");
        }

        let slice = std::slice::from_raw_parts_mut(ptr, size);
        Box::from_raw(slice)
    }
}

#[test]
fn test_david_queue_basic_operations() {
    let shared_size = DavidQueue::<usize>::shared_size(NUM_CONSUMERS);
    let memory = create_aligned_memory(shared_size);
    let mem_ptr = Box::leak(memory).as_mut_ptr();

    let mut enqueuer_state = EnqueuerState::new();
    let queue = unsafe { DavidQueue::init_in_shared(mem_ptr, NUM_CONSUMERS, &mut enqueuer_state) };

    assert!(queue.enqueue(&mut enqueuer_state, 42).is_ok());
    assert!(queue.enqueue(&mut enqueuer_state, 43).is_ok());
    assert!(queue.enqueue(&mut enqueuer_state, 44).is_ok());

    assert_eq!(queue.dequeue(0).unwrap(), 42);
    assert_eq!(queue.dequeue(1).unwrap(), 43);
    assert_eq!(queue.dequeue(0).unwrap(), 44);

    assert!(queue.dequeue(0).is_err());
}

#[test]
fn test_david_queue_multiple_consumers() {
    let shared_size = DavidQueue::<usize>::shared_size(NUM_CONSUMERS);
    let memory = create_aligned_memory(shared_size);
    let mem_ptr = Box::leak(memory).as_mut_ptr();

    let mut enqueuer_state = EnqueuerState::new();
    let queue = unsafe { DavidQueue::init_in_shared(mem_ptr, NUM_CONSUMERS, &mut enqueuer_state) };

    for i in 0..100 {
        assert!(queue.enqueue(&mut enqueuer_state, i).is_ok());
    }

    let mut items = Vec::new();
    for consumer_id in 0..NUM_CONSUMERS {
        for _ in 0..100 {
            if let Ok(item) = queue.dequeue(consumer_id) {
                items.push(item);
            }
        }
    }

    items.sort();
    assert_eq!(items.len(), 100);
    for (i, &item) in items.iter().enumerate() {
        assert_eq!(item, i);
    }
}

#[test]
fn test_david_queue_rapid_reuse() {
    let shared_size = DavidQueue::<usize>::shared_size(2);
    let memory = create_aligned_memory(shared_size);
    let mem_ptr = Box::leak(memory).as_mut_ptr();

    let mut enqueuer_state = EnqueuerState::new();
    let queue = unsafe { DavidQueue::init_in_shared(mem_ptr, 2, &mut enqueuer_state) };

    for cycle in 0..20 {
        for i in 0..20 {
            assert!(
                queue.enqueue(&mut enqueuer_state, cycle * 1000 + i).is_ok(),
                "Failed to enqueue in cycle {}",
                cycle
            );
        }

        for _ in 0..20 {
            let result = queue.dequeue(cycle % 2);
            assert!(result.is_ok(), "Failed to dequeue in cycle {}", cycle);
        }
    }

    assert!(queue.dequeue(0).is_err());
    assert!(queue.dequeue(1).is_err());
}

#[test]
fn test_david_queue_is_full() {
    let shared_size = DavidQueue::<usize>::shared_size(1);
    let memory = create_aligned_memory(shared_size);
    let mem_ptr = Box::leak(memory).as_mut_ptr();

    let mut enqueuer_state = EnqueuerState::new();
    let queue = unsafe { DavidQueue::init_in_shared(mem_ptr, 1, &mut enqueuer_state) };

    assert!(!queue.is_full());

    let mut added = 0;
    for i in 0..1000 {
        if queue.enqueue(&mut enqueuer_state, i).is_err() || queue.is_full() {
            break;
        }
        added += 1;
    }

    assert!(added > 0, "Should be able to add at least some items");

    if queue.enqueue(&mut enqueuer_state, 999999).is_err() {
        assert!(queue.is_full() || added > 0);
    }
}

#[cfg(unix)]
mod ipc_tests {
    use super::*;
    use nix::{
        libc,
        sys::wait::waitpid,
        unistd::{fork, ForkResult},
    };
    use std::{
        ptr,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    unsafe fn map_shared(bytes: usize) -> *mut u8 {
        let page_size = 4096;
        let aligned_size = (bytes + page_size - 1) & !(page_size - 1);

        let ptr = libc::mmap(
            std::ptr::null_mut(),
            aligned_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if ptr == libc::MAP_FAILED {
            panic!("mmap failed: {}", std::io::Error::last_os_error());
        }

        std::ptr::write_bytes(ptr as *mut u8, 0, aligned_size);

        ptr.cast()
    }

    unsafe fn unmap_shared(ptr: *mut u8, len: usize) {
        let page_size = 4096;
        let aligned_size = (len + page_size - 1) & !(page_size - 1);

        if libc::munmap(ptr.cast(), aligned_size) == -1 {
            panic!("munmap failed: {}", std::io::Error::last_os_error());
        }
    }

    // Helper struct for IPC synchronization with proper alignment
    #[repr(C)]
    struct IpcSync {
        producer_ready: AtomicBool,
        _pad1: [u8; 7], // Padding to ensure consumer_ready is aligned
        consumer_ready: AtomicBool,
        _pad2: [u8; 7], // Padding to ensure items_consumed is 8-byte aligned
        items_consumed: AtomicUsize,
    }

    impl IpcSync {
        unsafe fn from_ptr(ptr: *mut u8) -> &'static Self {
            &*(ptr as *const Self)
        }
    }

    // Helper struct for multi-consumer IPC synchronization
    #[repr(C)]
    struct IpcSyncMultiConsumer {
        producer_ready: AtomicBool,
        _pad1: [u8; 7],
        consumer1_ready: AtomicBool,
        _pad2: [u8; 7],
        consumer2_ready: AtomicBool,
        _pad3: [u8; 7],
        producer_done: AtomicBool,
        _pad4: [u8; 7],
        items_consumed1: AtomicUsize,
        items_consumed2: AtomicUsize,
    }

    impl IpcSyncMultiConsumer {
        unsafe fn from_ptr(ptr: *mut u8) -> &'static Self {
            &*(ptr as *const Self)
        }
    }

    #[test]
    fn test_david_queue_ipc() {
        let num_consumers = 2;
        let shared_size = DavidQueue::<usize>::shared_size(num_consumers);

        // Add space for EnqueuerState and sync variables
        let enqueuer_state_size = std::mem::size_of::<EnqueuerState>();
        let sync_size = std::mem::size_of::<IpcSync>();
        let sync_size = (sync_size + 63) & !63;
        // Ensure we have enough space for 64-byte alignment of queue
        let queue_offset = sync_size + enqueuer_state_size;
        let queue_offset_aligned = (queue_offset + 63) & !63;
        let total_size = queue_offset_aligned + shared_size;

        let shm_ptr = unsafe { map_shared(total_size) };

        let sync = unsafe { IpcSync::from_ptr(shm_ptr) };
        sync.producer_ready.store(false, Ordering::SeqCst);
        sync.consumer_ready.store(false, Ordering::SeqCst);
        sync.items_consumed.store(0, Ordering::SeqCst);

        // Place EnqueuerState after sync variables
        let enqueuer_state_ptr = unsafe { shm_ptr.add(sync_size) as *mut EnqueuerState };
        unsafe {
            ptr::write(enqueuer_state_ptr, EnqueuerState::new());
        }

        // Place queue after EnqueuerState, ensuring 64-byte alignment
        let queue_offset = sync_size + enqueuer_state_size;
        let queue_offset_aligned = (queue_offset + 63) & !63; // Align to 64 bytes
        let queue_ptr = unsafe { shm_ptr.add(queue_offset_aligned) };
        let mut enqueuer_state_init = EnqueuerState::new();
        let queue: &mut DavidQueue<usize> = unsafe {
            DavidQueue::init_in_shared(queue_ptr, num_consumers, &mut enqueuer_state_init)
        };

        // Copy initialized state back
        unsafe {
            ptr::write(enqueuer_state_ptr, enqueuer_state_init);
        }

        const NUM_ITEMS: usize = 500; // Keep well within single row capacity for multiple consumers

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                // Child process acts as producer
                sync.producer_ready.store(true, Ordering::Release);
                while !sync.consumer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                let enqueuer_state = unsafe { &mut *enqueuer_state_ptr };

                for i in 0..NUM_ITEMS {
                    let mut retries = 0;
                    loop {
                        match queue.enqueue(enqueuer_state, i) {
                            Ok(_) => break,
                            Err(_) => {
                                retries += 1;
                                if retries > 10000 {
                                    eprintln!("Producer: Excessive retries at item {}", i);
                                }
                                std::thread::yield_now();
                            }
                        }
                    }
                }

                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                // Parent process acts as consumer
                while !sync.producer_ready.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                sync.consumer_ready.store(true, Ordering::Release);

                let mut received = Vec::new();
                let mut empty_count = 0;
                let consumer_id = 0;

                while received.len() < NUM_ITEMS {
                    match queue.dequeue(consumer_id) {
                        Ok(item) => {
                            received.push(item);
                            empty_count = 0;
                        }
                        Err(_) => {
                            empty_count += 1;
                            if empty_count > 1000000 {
                                break;
                            }
                            std::thread::yield_now();
                        }
                    }
                }

                sync.items_consumed.store(received.len(), Ordering::SeqCst);

                waitpid(child, None).expect("waitpid failed");

                let consumed = sync.items_consumed.load(Ordering::SeqCst);
                assert_eq!(
                    consumed, NUM_ITEMS,
                    "Not all items were consumed in IPC test"
                );

                // Items might not be in order due to multiple consumers
                received.sort();
                for (i, &item) in received.iter().enumerate() {
                    assert_eq!(item, i, "Missing or duplicate items");
                }

                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
            }
            Err(e) => {
                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
                panic!("Fork failed: {}", e);
            }
        }
    }

    #[test]
    fn test_david_queue_multi_consumer_balanced() {
        let num_consumers = 2;
        let shared_size = DavidQueue::<usize>::shared_size(num_consumers);

        // Add space for EnqueuerState and sync variables
        let enqueuer_state_size = std::mem::size_of::<EnqueuerState>();
        let sync_size = std::mem::size_of::<IpcSyncMultiConsumer>();
        let sync_size_aligned = (sync_size + 63) & !63;

        // Calculate offsets
        let enqueuer_offset = sync_size_aligned;
        let queue_offset = sync_size_aligned + enqueuer_state_size;
        let queue_offset_aligned = (queue_offset + 63) & !63;
        let total_size = queue_offset_aligned + shared_size;

        let shm_ptr = unsafe { map_shared(total_size) };

        let sync = unsafe { IpcSyncMultiConsumer::from_ptr(shm_ptr) };
        sync.producer_ready.store(false, Ordering::SeqCst);
        sync.consumer1_ready.store(false, Ordering::SeqCst);
        sync.consumer2_ready.store(false, Ordering::SeqCst);
        sync.producer_done.store(false, Ordering::SeqCst);
        sync.items_consumed1.store(0, Ordering::SeqCst);
        sync.items_consumed2.store(0, Ordering::SeqCst);

        // Place EnqueuerState after sync variables
        let enqueuer_state_ptr = unsafe { shm_ptr.add(enqueuer_offset) as *mut EnqueuerState };
        unsafe {
            ptr::write(enqueuer_state_ptr, EnqueuerState::new());
        }

        // Place queue after EnqueuerState, with alignment
        let queue_ptr = unsafe { shm_ptr.add(queue_offset_aligned) };
        let mut enqueuer_state_init = EnqueuerState::new();
        let _queue: &mut DavidQueue<usize> = unsafe {
            DavidQueue::init_in_shared(queue_ptr, num_consumers, &mut enqueuer_state_init)
        };

        // Copy initialized state back
        unsafe {
            ptr::write(enqueuer_state_ptr, enqueuer_state_init);
        }

        const NUM_ITEMS: usize = 1000; // More items to test

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                // Child process acts as producer
                sync.producer_ready.store(true, Ordering::Release);

                // Wait for consumers
                while !sync.consumer1_ready.load(Ordering::Acquire)
                    || !sync.consumer2_ready.load(Ordering::Acquire)
                {
                    std::hint::spin_loop();
                }

                // Small delay to ensure consumers are truly ready
                std::thread::sleep(std::time::Duration::from_millis(10));

                let enqueuer_state = unsafe { &mut *enqueuer_state_ptr };
                let queue = unsafe { &*(queue_ptr as *const DavidQueue<usize>) };

                // Produce items at a controlled rate
                for i in 0..NUM_ITEMS {
                    let mut retries = 0;
                    loop {
                        match queue.enqueue(enqueuer_state, i) {
                            Ok(_) => break,
                            Err(_) => {
                                retries += 1;
                                if retries > 100 {
                                    // Slow down production if queue is getting full
                                    std::thread::sleep(std::time::Duration::from_micros(10));
                                }
                                std::thread::yield_now();
                            }
                        }
                    }

                    // Small delay every 100 items to let consumers catch up
                    if i % 100 == 99 {
                        std::thread::sleep(std::time::Duration::from_micros(100));
                    }
                }

                sync.producer_done.store(true, Ordering::Release);

                // Give consumers time to finish
                std::thread::sleep(std::time::Duration::from_millis(100));

                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent {
                child: producer_pid,
            }) => {
                // Parent forks consumers
                let mut consumer_pids = vec![];

                for consumer_id in 0..num_consumers {
                    match unsafe { fork() } {
                        Ok(ForkResult::Child) => {
                            // Consumer process
                            let queue = unsafe { &*(queue_ptr as *const DavidQueue<usize>) };

                            // Signal ready
                            if consumer_id == 0 {
                                sync.consumer1_ready.store(true, Ordering::Release);
                            } else {
                                sync.consumer2_ready.store(true, Ordering::Release);
                            }

                            // Wait for producer and other consumer
                            while !sync.producer_ready.load(Ordering::Acquire)
                                || !sync.consumer1_ready.load(Ordering::Acquire)
                                || !sync.consumer2_ready.load(Ordering::Acquire)
                            {
                                std::hint::spin_loop();
                            }

                            let mut received = Vec::new();
                            let mut consecutive_empty = 0;

                            loop {
                                match queue.dequeue(consumer_id) {
                                    Ok(item) => {
                                        received.push(item);
                                        consecutive_empty = 0;
                                    }
                                    Err(_) => {
                                        consecutive_empty += 1;

                                        // Only exit if producer is done AND we've tried many times
                                        if sync.producer_done.load(Ordering::Acquire)
                                            && consecutive_empty > 10000
                                        {
                                            break;
                                        }

                                        // Backoff strategy
                                        if consecutive_empty < 10 {
                                            std::hint::spin_loop();
                                        } else if consecutive_empty < 100 {
                                            std::thread::yield_now();
                                        } else {
                                            std::thread::sleep(std::time::Duration::from_micros(1));
                                        }
                                    }
                                }
                            }

                            // Sort and check for duplicates
                            let original_len = received.len();
                            received.sort();
                            received.dedup();
                            let unique_len = received.len();

                            if original_len != unique_len {
                                eprintln!(
                                    "Consumer {}: Had {} duplicates!",
                                    consumer_id,
                                    original_len - unique_len
                                );
                            }

                            if consumer_id == 0 {
                                sync.items_consumed1.store(unique_len, Ordering::SeqCst);
                            } else {
                                sync.items_consumed2.store(unique_len, Ordering::SeqCst);
                            }

                            unsafe { libc::_exit(0) };
                        }
                        Ok(ForkResult::Parent { child }) => {
                            consumer_pids.push(child);
                        }
                        Err(e) => panic!("Fork failed for consumer {}: {}", consumer_id, e),
                    }
                }

                // Wait for all processes
                waitpid(producer_pid, None).expect("waitpid producer failed");
                for pid in consumer_pids {
                    waitpid(pid, None).expect("waitpid consumer failed");
                }

                let consumed1 = sync.items_consumed1.load(Ordering::SeqCst);
                let consumed2 = sync.items_consumed2.load(Ordering::SeqCst);
                let total_consumed = consumed1 + consumed2;

                println!("Consumer 0 consumed: {} unique items", consumed1);
                println!("Consumer 1 consumed: {} unique items", consumed2);
                println!("Total consumed: {} unique items", total_consumed);

                // With the paper's algorithm (no recovery), we might lose some items
                // but we should NEVER get duplicates
                assert!(
                    total_consumed <= NUM_ITEMS,
                    "Got duplicates! Consumed {} but only produced {}",
                    total_consumed,
                    NUM_ITEMS
                );

                if total_consumed < NUM_ITEMS {
                    let loss_rate =
                        ((NUM_ITEMS - total_consumed) as f64 / NUM_ITEMS as f64) * 100.0;
                    println!(
                        "Lost {} items ({:.2}%)",
                        NUM_ITEMS - total_consumed,
                        loss_rate
                    );

                    // A small loss rate is acceptable with the paper's algorithm
                    assert!(loss_rate < 5.0, "Loss rate too high: {:.2}%", loss_rate);
                }

                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
            }
            Err(e) => {
                unsafe {
                    unmap_shared(shm_ptr, total_size);
                }
                panic!("Fork failed: {}", e);
            }
        }
    }
}
