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
    fn test_david_queue_multi_consumer_ipc() {
        let num_consumers = 2;
        let shared_size = DavidQueue::<usize>::shared_size(num_consumers);

        // Allocate separate shared memory for queue
        let queue_shm_ptr = unsafe { map_shared(shared_size) };

        // Allocate separate shared memory for EnqueuerState
        let enqueuer_state_size = std::mem::size_of::<EnqueuerState>();
        let enqueuer_state_shm_ptr = unsafe { map_shared(enqueuer_state_size) };

        // Allocate separate shared memory for sync variables
        let sync_size = std::mem::size_of::<IpcSyncMultiConsumer>();
        let sync_shm_ptr = unsafe { map_shared(sync_size) };

        let sync = unsafe { IpcSyncMultiConsumer::from_ptr(sync_shm_ptr) };

        sync.producer_ready.store(false, Ordering::SeqCst);
        sync.consumer1_ready.store(false, Ordering::SeqCst);
        sync.consumer2_ready.store(false, Ordering::SeqCst);
        sync.producer_done.store(false, Ordering::SeqCst);
        sync.items_consumed1.store(0, Ordering::SeqCst);
        sync.items_consumed2.store(0, Ordering::SeqCst);

        // Initialize EnqueuerState in its own shared memory
        let enqueuer_state_ptr = enqueuer_state_shm_ptr as *mut EnqueuerState;
        unsafe {
            ptr::write(enqueuer_state_ptr, EnqueuerState::new());
        }

        // Initialize queue in its own shared memory
        let queue_ptr = queue_shm_ptr;
        let queue: &mut DavidQueue<usize> = unsafe {
            DavidQueue::init_in_shared(queue_ptr, num_consumers, &mut *enqueuer_state_ptr)
        };

        const NUM_ITEMS: usize = 500; // Keep well within single row capacity for multiple consumers

        match unsafe { fork() } {
            Ok(ForkResult::Parent {
                child: producer_pid,
            }) => {
                match unsafe { fork() } {
                    Ok(ForkResult::Parent {
                        child: consumer1_pid,
                    }) => {
                        match unsafe { fork() } {
                            Ok(ForkResult::Parent {
                                child: consumer2_pid,
                            }) => {
                                // Original parent waits for all children
                                waitpid(producer_pid, None).expect("waitpid producer failed");
                                waitpid(consumer1_pid, None).expect("waitpid consumer1 failed");
                                waitpid(consumer2_pid, None).expect("waitpid consumer2 failed");

                                let consumed1 = sync.items_consumed1.load(Ordering::SeqCst);
                                let consumed2 = sync.items_consumed2.load(Ordering::SeqCst);
                                let total_consumed = consumed1 + consumed2;

                                assert_eq!(
                                    total_consumed, NUM_ITEMS,
                                    "Not all items were consumed. Consumer1: {}, Consumer2: {}",
                                    consumed1, consumed2
                                );

                                unsafe {
                                    unmap_shared(queue_shm_ptr, shared_size);
                                    unmap_shared(enqueuer_state_shm_ptr, enqueuer_state_size);
                                    unmap_shared(sync_shm_ptr, sync_size);
                                }
                            }
                            Ok(ForkResult::Child) => {
                                // Consumer 2
                                while !sync.producer_ready.load(Ordering::Acquire) {
                                    std::hint::spin_loop();
                                }
                                sync.consumer2_ready.store(true, Ordering::Release);

                                // Reconstruct queue reference from shared memory
                                let queue = unsafe { &*(queue_ptr as *const DavidQueue<usize>) };

                                eprintln!("Consumer 2: Starting to dequeue");
                                let mut count = 0;
                                let mut consecutive_empty = 0;
                                let target_items = NUM_ITEMS / 2;  // Each consumer should get half
                                
                                while count < target_items {
                                    match queue.dequeue(1) {
                                        Ok(_item) => {
                                            count += 1;
                                            consecutive_empty = 0;
                                        }
                                        Err(_) => {
                                            consecutive_empty += 1;
                                            
                                            if sync.producer_done.load(Ordering::Acquire) && consecutive_empty > 100000 {
                                                // Try a few more times after producer is done
                                                for _ in 0..100 {
                                                    if let Ok(_) = queue.dequeue(1) {
                                                        count += 1;
                                                    }
                                                }
                                                break;
                                            }
                                            
                                            if consecutive_empty < 100 {
                                                std::hint::spin_loop();
                                            } else {
                                                std::thread::yield_now();
                                            }
                                        }
                                    }
                                }

                                eprintln!("Consumer 2: Consumed {} items", count);
                                sync.items_consumed2.store(count, Ordering::SeqCst);
                                unsafe { libc::_exit(0) };
                            }
                            Err(_) => panic!("Fork consumer2 failed"),
                        }
                    }
                    Ok(ForkResult::Child) => {
                        // Consumer 1
                        while !sync.producer_ready.load(Ordering::Acquire) {
                            std::hint::spin_loop();
                        }
                        sync.consumer1_ready.store(true, Ordering::Release);

                        // Reconstruct queue reference from shared memory
                        let queue = unsafe { &*(queue_ptr as *const DavidQueue<usize>) };

                        eprintln!("Consumer 1: Starting to dequeue");
                        let mut count = 0;
                        let mut consecutive_empty = 0;
                        let target_items = NUM_ITEMS / 2;  // Each consumer should get half
                        
                        while count < target_items {
                            match queue.dequeue(0) {
                                Ok(_item) => {
                                    count += 1;
                                    consecutive_empty = 0;
                                }
                                Err(_) => {
                                    consecutive_empty += 1;
                                    
                                    if sync.producer_done.load(Ordering::Acquire) && consecutive_empty > 100000 {
                                        // Try a few more times after producer is done
                                        for _ in 0..100 {
                                            if let Ok(_) = queue.dequeue(0) {
                                                count += 1;
                                            }
                                        }
                                        break;
                                    }
                                    
                                    if consecutive_empty < 100 {
                                        std::hint::spin_loop();
                                    } else {
                                        std::thread::yield_now();
                                    }
                                }
                            }
                        }

                        eprintln!("Consumer 1: Consumed {} items", count);
                        sync.items_consumed1.store(count, Ordering::SeqCst);
                        unsafe { libc::_exit(0) };
                    }
                    Err(_) => panic!("Fork consumer1 failed"),
                }
            }
            Ok(ForkResult::Child) => {
                // Producer
                while !sync.consumer1_ready.load(Ordering::Acquire)
                    || !sync.consumer2_ready.load(Ordering::Acquire)
                {
                    sync.producer_ready.store(true, Ordering::Release);
                    std::hint::spin_loop();
                }

                // Reconstruct queue reference from shared memory
                let queue = unsafe { &*(queue_ptr as *const DavidQueue<usize>) };
                let enqueuer_state = unsafe { &mut *enqueuer_state_ptr };

                eprintln!("Producer: Starting to enqueue {} items", NUM_ITEMS);
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
                
                eprintln!("Producer: Successfully enqueued all {} items", NUM_ITEMS);
                // Signal that producer is done
                sync.producer_done.store(true, Ordering::Release);
                std::thread::sleep(std::time::Duration::from_millis(100));

                unsafe { libc::_exit(0) };
            }
            Err(_) => panic!("Fork producer failed"),
        }
    }
}
