#[cfg(test)]
mod tests {
    use queues::NRQueue;

    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::thread;

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

            // Test 1: Simple enqueue/dequeue
            println!("Test 1: Simple operations");
            queue.enqueue(0, 42).unwrap();
            queue.verify_tree_state();

            let val = queue.dequeue(1).unwrap();
            assert_eq!(val, 42);
            queue.verify_tree_state();

            // Test 2: Multiple operations
            println!("\nTest 2: Multiple operations");
            for i in 0..10 {
                queue.enqueue(0, i).unwrap();
                queue.verify_tree_state();
            }

            for i in 0..10 {
                let val = queue.dequeue(1).unwrap();
                assert_eq!(val, i);
                println!("Dequeued: {}", val);
            }

            // Test 3: Check propagation
            println!("\nTest 3: Check propagation after many ops");
            for i in 0..100 {
                queue.enqueue(0, i).unwrap();
            }
            queue.verify_tree_state();

            let mut dequeued = 0;
            for _ in 0..100 {
                match queue.dequeue(1) {
                    Ok(_) => dequeued += 1,
                    Err(_) => break,
                }
            }
            println!("Dequeued {}/100 items", dequeued);

            libc::munmap(mem as *mut _, mem_size);
        }
    }

    #[test]
    fn test_propagation_path() {
        unsafe {
            let mem_size = NRQueue::<usize>::shared_size(4);
            let mem = libc::mmap(
                std::ptr::null_mut(),
                mem_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            ) as *mut u8;

            let queue = NRQueue::<usize>::init_in_shared(mem, 4);

            // Enqueue from different processes
            for p in 0..4 {
                for i in 0..5 {
                    queue.enqueue(p, p * 100 + i).unwrap();
                }
            }

            queue.verify_tree_state();

            // Try to dequeue all
            let mut total_dequeued = 0;
            for _ in 0..20 {
                for p in 0..4 {
                    match queue.dequeue(p) {
                        Ok(val) => {
                            println!("Dequeued: {}", val);
                            total_dequeued += 1;
                        }
                        Err(_) => {}
                    }
                }
            }

            println!("Total dequeued: {}/20", total_dequeued);

            libc::munmap(mem as *mut _, mem_size);
        }
    }
}
