// queues/src/mpmc/polylog_queue_test.rs

#[cfg(test)]
mod tests {
    use ::queues::mpmc::polylog_queue::NRQueue;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_single_thread_basic() {
        unsafe {
            let mem_size = NRQueue::<usize>::shared_size(1);
            let mem = libc::mmap(
                std::ptr::null_mut(),
                mem_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            ) as *mut u8;

            let queue = NRQueue::<usize>::init_in_shared(mem, 1);

            // Test basic enqueue/dequeue
            assert!(queue.is_empty());
            queue.enqueue(0, 42).unwrap();
            assert!(!queue.is_empty());
            assert_eq!(queue.dequeue(0).unwrap(), 42);
            assert!(queue.is_empty());

            // Test multiple items
            for i in 0..10 {
                queue.enqueue(0, i).unwrap();
            }
            for i in 0..10 {
                assert_eq!(queue.dequeue(0).unwrap(), i);
            }

            // Test empty dequeue
            assert!(queue.dequeue(0).is_err());

            libc::munmap(mem as *mut libc::c_void, mem_size);
        }
    }

    #[test]
    fn test_single_producer_consumer() {
        unsafe {
            let mem_size = NRQueue::<usize>::shared_size(2);
            let mem = libc::mmap(
                std::ptr::null_mut(),
                mem_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            ) as *mut u8;

            let queue = NRQueue::<usize>::init_in_shared(mem, 2);
            let queue_ptr = Arc::new(queue as *const NRQueue<usize>);

            let produced = Arc::new(AtomicUsize::new(0));
            let consumed = Arc::new(AtomicUsize::new(0));
            let items = 1000;

            let produced_clone = produced.clone();
            let queue_clone = queue_ptr.clone();
            let producer = thread::spawn(move || {
                let q = &**queue_clone;
                for i in 0..items {
                    q.enqueue(0, i).unwrap();
                    produced_clone.fetch_add(1, Ordering::Relaxed);
                }
            });

            let consumed_clone = consumed.clone();
            let queue_clone = queue_ptr.clone();
            let consumer = thread::spawn(move || {
                let q = &**queue_clone;
                let mut count = 0;
                let mut last_value = None;

                while count < items {
                    match q.dequeue(1) {
                        Ok(val) => {
                            // Check ordering
                            if let Some(last) = last_value {
                                assert_eq!(val, last + 1, "Out of order: {} after {}", val, last);
                            }
                            last_value = Some(val);
                            count += 1;
                            consumed_clone.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            // Queue empty, spin
                            std::hint::spin_loop();
                        }
                    }
                }
                count
            });

            producer.join().unwrap();
            let total_consumed = consumer.join().unwrap();

            assert_eq!(total_consumed, items);
            assert_eq!(produced.load(Ordering::Relaxed), items);
            assert_eq!(consumed.load(Ordering::Relaxed), items);

            libc::munmap(mem as *mut libc::c_void, mem_size);
        }
    }

    #[test]
    fn test_propagation_tracking() {
        unsafe {
            let mem_size = NRQueue::<usize>::shared_size(2);
            let mem = libc::mmap(
                std::ptr::null_mut(),
                mem_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            ) as *mut u8;

            let queue = NRQueue::<usize>::init_in_shared(mem, 2);

            // Enqueue items
            for i in 0..100 {
                queue.enqueue(0, i).unwrap();
            }

            // Now dequeue and check
            let mut dequeued = 0;
            for expected in 0..100 {
                match queue.dequeue(1) {
                    Ok(val) => {
                        assert_eq!(val, expected, "Wrong value at position {}", dequeued);
                        dequeued += 1;
                    }
                    Err(_) => {
                        println!(
                            "Failed to dequeue at position {}, expected {}",
                            dequeued, expected
                        );
                        break;
                    }
                }
            }

            println!("Successfully dequeued: {}/100", dequeued);
            assert_eq!(dequeued, 100);

            libc::munmap(mem as *mut libc::c_void, mem_size);
        }
    }

    #[test]
    fn test_interleaved_operations() {
        unsafe {
            let mem_size = NRQueue::<usize>::shared_size(1);
            let mem = libc::mmap(
                std::ptr::null_mut(),
                mem_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            ) as *mut u8;

            let queue = NRQueue::<usize>::init_in_shared(mem, 1);

            // Interleave enqueues and dequeues
            queue.enqueue(0, 1).unwrap();
            queue.enqueue(0, 2).unwrap();
            assert_eq!(queue.dequeue(0).unwrap(), 1);
            queue.enqueue(0, 3).unwrap();
            assert_eq!(queue.dequeue(0).unwrap(), 2);
            assert_eq!(queue.dequeue(0).unwrap(), 3);
            assert!(queue.dequeue(0).is_err());

            libc::munmap(mem as *mut libc::c_void, mem_size);
        }
    }

    #[test]
    fn test_exact_250k_items() {
        unsafe {
            let mem_size = NRQueue::<usize>::shared_size(2);
            let mem = libc::mmap(
                std::ptr::null_mut(),
                mem_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            ) as *mut u8;

            let queue = NRQueue::<usize>::init_in_shared(mem, 2);
            let queue_ptr = Arc::new(queue as *const NRQueue<usize>);

            let items = 250_000;
            let produced = Arc::new(AtomicUsize::new(0));
            let consumed = Arc::new(AtomicUsize::new(0));

            // Producer
            let produced_clone = produced.clone();
            let queue_clone = queue_ptr.clone();
            let producer = thread::spawn(move || {
                let q = &**queue_clone;
                for i in 0..items {
                    q.enqueue(0, i).unwrap();
                    produced_clone.fetch_add(1, Ordering::Relaxed);

                    if i % 10000 == 0 {
                        println!("Produced {}", i);
                    }
                }
                println!("Producer done: {} items", items);
            });

            // Consumer
            let consumed_clone = consumed.clone();
            let queue_clone = queue_ptr.clone();
            let consumer = thread::spawn(move || {
                let q = &**queue_clone;
                let mut count = 0;
                let mut consecutive_empty = 0;
                let max_empty_retries = 1_000_000;

                while count < items && consecutive_empty < max_empty_retries {
                    match q.dequeue(1) {
                        Ok(_val) => {
                            count += 1;
                            consecutive_empty = 0;
                            consumed_clone.fetch_add(1, Ordering::Relaxed);

                            if count % 10000 == 0 {
                                println!("Consumed {}", count);
                            }
                        }
                        Err(_) => {
                            consecutive_empty += 1;
                            if consecutive_empty % 100000 == 0 {
                                println!(
                                    "Empty checks: {}, consumed so far: {}",
                                    consecutive_empty, count
                                );
                            }
                            std::hint::spin_loop();
                        }
                    }
                }

                println!(
                    "Consumer done: {} items, {} empty checks",
                    count, consecutive_empty
                );
                count
            });

            producer.join().unwrap();
            let total_consumed = consumer.join().unwrap();

            println!(
                "Final: Produced={}, Consumed={}",
                produced.load(Ordering::Relaxed),
                consumed.load(Ordering::Relaxed)
            );

            assert_eq!(
                total_consumed,
                items,
                "Lost {} items",
                items - total_consumed
            );

            libc::munmap(mem as *mut libc::c_void, mem_size);
        }
    }
}
