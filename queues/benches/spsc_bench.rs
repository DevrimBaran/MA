// benchmarking process-based SPSC queues using criterion
#![allow(clippy::cast_possible_truncation)] 

use criterion::{criterion_group, criterion_main, Criterion}; 
use std::time::Duration; 
use std::ptr; 
use nix::{ 
   libc, 
   sys::wait::waitpid, 
   unistd::{fork, ForkResult}, 
}; 

// Import all necessary SPSC queue types and the main SpscQueue trait 
use queues::{ 
   BQueue, LamportQueue, MultiPushQueue, UnboundedQueue, SpscQueue, DynListQueue, DehnaviQueue,
   IffqQueue, BiffqQueue, FfqQueue, BlqQueue
}; 

use std::sync::atomic::{AtomicU32, Ordering};

use queues::spsc::llq::{LlqQueue, K_CACHE_LINE_SLOTS};
use queues::spsc::blq::K_CACHE_LINE_SLOTS as BLQ_K_SLOTS;

const PERFORMANCE_TEST: bool = false; // Set to true for actual perf runs, false for quicker debug runs

const RING_CAP: usize = 65_536;
const ITERS:     usize = 40_000_000; 


// Helper trait for benchmarking SPSC-like queues
trait BenchSpscQueue<T: Send>: Send + Sync + 'static { 
   fn bench_push(&self, item: T) -> Result<(), ()>; 
   fn bench_pop(&self) -> Result<T, ()>; 
   fn bench_is_empty(&self) -> bool; 
   fn bench_is_full(&self) -> bool;
} 

// mmap / munmap helpers 
unsafe fn map_shared(bytes: usize) -> *mut u8 { 
   let ptr = libc::mmap( 
      std::ptr::null_mut(), 
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

// --- BenchSpscQueue Implementations ---
impl<T: Send + 'static> BenchSpscQueue<T> for DehnaviQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item).map_err(|_| ()) } 
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self).map_err(|_| ()) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { !SpscQueue::available(self) }
} 
impl<T: Send + 'static> BenchSpscQueue<T> for LamportQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item) } 
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { !SpscQueue::available(self) }
} 
impl<T: Send + 'static> BenchSpscQueue<T> for BQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item).map_err(|_| ()) } 
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { !SpscQueue::available(self) }
} 
impl<T: Send + 'static> BenchSpscQueue<T> for MultiPushQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item) } 
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self).map_err(|_| ()) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { !SpscQueue::available(self) }
} 
impl<T: Send + 'static> BenchSpscQueue<T> for UnboundedQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item) } 
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { !SpscQueue::available(self) } 
} 
impl<T: Send + 'static> BenchSpscQueue<T> for DynListQueue<T> { 
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item) } 
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { !SpscQueue::available(self) } 
} 
impl<T: Send + 'static> BenchSpscQueue<T> for IffqQueue<T> {
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item).map_err(|_e| ()) }
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self).map_err(|_e| ()) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { !SpscQueue::available(self) }
}
impl<T: Send + 'static> BenchSpscQueue<T> for BiffqQueue<T> {
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item).map_err(|_e| ()) }
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self).map_err(|_e| ()) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { !SpscQueue::available(self) }
}
impl<T: Send + 'static> BenchSpscQueue<T> for FfqQueue<T> {
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item).map_err(|_e| ()) }
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self).map_err(|_e| ()) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { !SpscQueue::available(self) }
}
impl<T: Send + 'static> BenchSpscQueue<T> for LlqQueue<T> {
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item).map_err(|_| ()) }
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self).map_err(|_| ()) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { !SpscQueue::available(self) }
}
impl<T: Send + 'static> BenchSpscQueue<T> for BlqQueue<T> {
   fn bench_push(&self, item: T) -> Result<(), ()> { SpscQueue::push(self, item).map_err(|_| ()) }
   fn bench_pop(&self) -> Result<T, ()> { SpscQueue::pop(self).map_err(|_| ()) }
   fn bench_is_empty(&self) -> bool { SpscQueue::empty(self) }
   fn bench_is_full(&self) -> bool { SpscQueue::available(self) }
}



// --- Benchmark Functions ---
fn bench_dehnavi(c: &mut Criterion) { 
   c.bench_function("Dehnavi", |b| { 
      b.iter_custom(|_iters| { 
         let current_ring_cap = if RING_CAP <= 1 { 2 } else { RING_CAP };  
         let bytes = DehnaviQueue::<usize>::shared_size(current_ring_cap); 
         let shm_ptr = unsafe { map_shared(bytes) }; 
         let q = unsafe { DehnaviQueue::init_in_shared(shm_ptr, current_ring_cap) }; 
         
         let dur = fork_and_run(q); // Uses global ITERS
         unsafe { 
            unmap_shared(shm_ptr, bytes); 
         } 
         dur 
      }) 
   }); 
} 

fn bench_lamport(c: &mut Criterion) { 
   c.bench_function("Lamport", |b| { 
      b.iter_custom(|_iters| { 
         let bytes   = LamportQueue::<usize>::shared_size(RING_CAP); 
         let shm_ptr = unsafe { map_shared(bytes) }; 
         let q       = unsafe { LamportQueue::init_in_shared(shm_ptr, RING_CAP) }; 
         let dur     = fork_and_run(q); 
         unsafe { unmap_shared(shm_ptr, bytes) }; 
         dur 
      }) 
   }); 
} 

fn bench_bqueue(c: &mut Criterion) { 
   c.bench_function("B-Queue", |b| { 
      b.iter_custom(|_iters| { 
         let bytes   = BQueue::<usize>::shared_size(RING_CAP); 
         let shm_ptr = unsafe { map_shared(bytes) }; 
         let q = unsafe { BQueue::init_in_shared(shm_ptr, RING_CAP) }; 
         let dur = fork_and_run(q); 
         unsafe { unmap_shared(shm_ptr, bytes) }; 
         dur 
      }) 
   }); 
} 

fn bench_mp(c: &mut Criterion) { 
   c.bench_function("mSPSC", |b| { 
      b.iter_custom(|_iters| { 
         let bytes   = MultiPushQueue::<usize>::shared_size(RING_CAP); 
         let shm_ptr = unsafe { map_shared(bytes) }; 
         let q       = unsafe { MultiPushQueue::init_in_shared(shm_ptr, RING_CAP) }; 
         let dur = fork_and_run(q); 
         unsafe { 
            unmap_shared(shm_ptr, bytes); 
         } 
         dur 
      }) 
   }); 
} 

fn bench_dspsc(c: &mut Criterion) { 
   c.bench_function("dSPSC", |b| { 
      b.iter_custom(|_iters| { 
         let bytes = DynListQueue::<usize>::shared_size(); 
         let shm_ptr = unsafe { map_shared(bytes) }; 
         let q = unsafe { DynListQueue::init_in_shared(shm_ptr) }; 
         let dur = fork_and_run(q); 
         unsafe { 
            unmap_shared(shm_ptr, bytes); 
         } 
         dur 
      }) 
   }); 
} 

fn bench_unbounded(c: &mut Criterion) { 
   c.bench_function("uSPSC", |b| { 
      b.iter_custom(|_iters| { 
         let size = UnboundedQueue::<usize>::shared_size(); 
         let shm_ptr = unsafe { map_shared(size) }; 
         let q = unsafe { UnboundedQueue::init_in_shared(shm_ptr) }; 
         let dur = fork_and_run(q); 
         unsafe { unmap_shared(shm_ptr, size); } 
         dur 
      }) 
   }); 
} 

fn bench_iffq(c: &mut Criterion) {
   c.bench_function("Iffq", |b| { 
      b.iter_custom(|_iters| {
         assert!(RING_CAP.is_power_of_two());
         assert_eq!(RING_CAP % 32, 0);
         assert!(RING_CAP >= 2 * 32);
         let bytes = IffqQueue::<usize>::shared_size(RING_CAP);
         let shm_ptr = unsafe { map_shared(bytes) };
         let q = unsafe { IffqQueue::init_in_shared(shm_ptr, RING_CAP) };
         let dur = fork_and_run(q);
         unsafe { unmap_shared(shm_ptr, bytes); }
         dur
      })
   });
}

fn bench_biffq(c: &mut Criterion) {
   c.bench_function("Biffq", |b| { 
      b.iter_custom(|_iters| {
         assert!(RING_CAP.is_power_of_two());
         assert_eq!(RING_CAP % 32, 0);
         assert!(RING_CAP >= 2 * 32);
         let bytes = BiffqQueue::<usize>::shared_size(RING_CAP);
         let shm_ptr = unsafe { map_shared(bytes) };
         let q = unsafe { BiffqQueue::init_in_shared(shm_ptr, RING_CAP) };
         let dur = fork_and_run(q);
         unsafe { unmap_shared(shm_ptr, bytes); }
         dur
      })
   });
}

fn bench_ffq(c: &mut Criterion) {
   c.bench_function("FFq", |b| { 
      b.iter_custom(|_iters| {
         assert!(RING_CAP.is_power_of_two() && RING_CAP > 0);
         let bytes = FfqQueue::<usize>::shared_size(RING_CAP);
         let shm_ptr = unsafe { map_shared(bytes) };
         let q = unsafe { FfqQueue::init_in_shared(shm_ptr, RING_CAP) };
         let dur = fork_and_run(q);
         unsafe { unmap_shared(shm_ptr, bytes); }
         dur
      })
   });
}

fn bench_llq(c: &mut Criterion) {
   c.bench_function("Llq", |b| {
      b.iter_custom(|_iters| {
         // Ensure current_ring_cap meets LLQ's requirements
         let current_ring_cap = if RING_CAP <= K_CACHE_LINE_SLOTS {
            let min_valid_cap = (K_CACHE_LINE_SLOTS + 1).next_power_of_two();
            if min_valid_cap < 16 { 16 } else {min_valid_cap} 
         } else {
            RING_CAP.next_power_of_two() 
         };
         
         assert!(current_ring_cap.is_power_of_two());
         assert!(current_ring_cap > K_CACHE_LINE_SLOTS);

         let bytes = LlqQueue::<usize>::llq_shared_size(current_ring_cap);
         let shm_ptr = unsafe { map_shared(bytes) };
         let q_static = unsafe { LlqQueue::<usize>::init_in_shared(shm_ptr, current_ring_cap) };
         
         let dur = fork_and_run(q_static);
         
         unsafe { unmap_shared(shm_ptr, bytes); }
         dur
      })
   });
}

fn bench_blq(c: &mut Criterion) {
   c.bench_function("Blq", |b| {
      b.iter_custom(|_iters| {
         // Ensure current_ring_cap meets BlqQueue's requirements
         // BLQ, like LLQ, requires capacity > K_CACHE_LINE_SLOTS.
         let current_ring_cap = if RING_CAP <= BLQ_K_SLOTS {
            // If RING_CAP is too small, find the next power of two that is > BLQ_K_SLOTS
            let mut min_valid_cap = (BLQ_K_SLOTS + 1).next_power_of_two();
            if min_valid_cap <= BLQ_K_SLOTS { // Ensure it's strictly greater
               min_valid_cap = (BLQ_K_SLOTS + 1).next_power_of_two();
               if min_valid_cap == 0 { // next_power_of_two can return 0 for large inputs
                  min_valid_cap = 1 << (BLQ_K_SLOTS.leading_zeros() as usize +1); // A sufficiently large power of 2
               }
            }
             // As a fallback for very small K_SLOTS or edge cases with next_power_of_two:
            if min_valid_cap < 16 { 16 } else { min_valid_cap }
         } else {
            RING_CAP.next_power_of_two()
         };
         
         assert!(current_ring_cap.is_power_of_two());
         assert!(current_ring_cap > BLQ_K_SLOTS);

         let bytes = BlqQueue::<usize>::shared_size(current_ring_cap);
         let shm_ptr = unsafe { map_shared(bytes) };
         let q_static = unsafe { BlqQueue::<usize>::init_in_shared(shm_ptr, current_ring_cap) };
         
         let dur = fork_and_run(q_static); // Uses global ITERS

         unsafe { unmap_shared(shm_ptr, bytes); }
         dur
      })
   });
}

// Generic fork-and-run helper - Reverted to the older, simpler style
fn fork_and_run<Q>(q: &'static Q) -> std::time::Duration
where
    Q: BenchSpscQueue<usize> + Sync + 'static, // Added 'static bound, common for shared queue refs
{
    let page_size = 4096; // Or use a crate like `page_size` to get it dynamically
    let sync_shm = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            page_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };

    if sync_shm == libc::MAP_FAILED {
        panic!("mmap for sync_shm failed: {}", std::io::Error::last_os_error());
    }

    let sync_atomic_flag = unsafe { &*(sync_shm as *const AtomicU32) };
    sync_atomic_flag.store(0, Ordering::Relaxed); // Initialize: 0 = initial, 1 = P ready, 2 = C ready/Go, 3 = P done

    match unsafe { fork() } {
        Ok(ForkResult::Child) => { // Producer
            sync_atomic_flag.store(1, Ordering::Release); // 1. Producer signals it's ready
            while sync_atomic_flag.load(Ordering::Acquire) < 2 { // Wait for consumer to signal ready (state 2)
                std::hint::spin_loop();
            }

            // Producer's main work loop
            for i in 0..ITERS {
                while q.bench_push(i).is_err() {
                    std::hint::spin_loop(); // Spin if push fails (queue temporarily full)
                }
            }

            // After producing all items, explicitly flush any buffered items for relevant queue types
            if let Some(mp_queue) = (q as &dyn std::any::Any).downcast_ref::<MultiPushQueue<usize>>() {
                let mut attempts = 0;
                // Loop to ensure flush succeeds, especially if the consumer is slow to make space
                while mp_queue.local_count.load(Ordering::Relaxed) > 0 && attempts < 10000 { // Limit attempts
                    if !mp_queue.flush() { // Assuming flush() returns true on success
                        std::hint::spin_loop();
                        attempts += 1;
                    } else {
                        // Flush was successful or buffer became empty through other means
                        if mp_queue.local_count.load(Ordering::Relaxed) == 0 {
                             break;
                        }
                        // If flush reported success but buffer not empty, could be a partial flush logic. Spin.
                        std::hint::spin_loop();
                        attempts +=1;
                    }
                }
                 if mp_queue.local_count.load(Ordering::Relaxed) > 0 && PERFORMANCE_TEST == false {
                     eprintln!(
                        "Warning (SPSC Producer): MultiPushQueue failed to flush all local items after {} attempts. {} items remaining in local_buf.",
                        attempts,
                        mp_queue.local_count.load(Ordering::Relaxed)
                    );
                }
            } else if let Some(biffq_queue) = (q as &dyn std::any::Any).downcast_ref::<BiffqQueue<usize>>() {
                // Similar robust flush for BiffqQueue if it has a comparable local buffer and flush mechanism
                let mut attempts = 0;
                // Assuming BiffqQueue::flush_producer_buffer returns Result<usize, ()>
                // where usize is items published or similar indication of progress.
                while biffq_queue.prod.local_count.load(Ordering::Relaxed) > 0 && attempts < 10000 {
                    match biffq_queue.flush_producer_buffer() {
                        Ok(_published_count) => {
                            // If flush_producer_buffer doesn't guarantee full flush on Ok, recheck local_count
                            if biffq_queue.prod.local_count.load(Ordering::Relaxed) == 0 {
                                break;
                            }
                            // If Ok but not empty, might need to spin or means partial success
                            std::hint::spin_loop(); // Allow consumer to process
                            attempts +=1; // Count this as an attempt if still not empty
                        }
                        Err(_) => { // Flush indicated an error (e.g., underlying queue full)
                            std::hint::spin_loop();
                            attempts += 1;
                        }
                    }
                }
                if biffq_queue.prod.local_count.load(Ordering::Relaxed) > 0 && PERFORMANCE_TEST == false {
                     eprintln!(
                        "Warning (SPSC Producer): BiffqQueue failed to flush all local items after {} attempts. {} items remaining in local_buf.",
                        attempts,
                        biffq_queue.prod.local_count.load(Ordering::Relaxed)
                    );
                }
            }
            // Add other queue types here if they also have internal producer buffers that need explicit flushing.

            sync_atomic_flag.store(3, Ordering::Release); // 3. Producer signals it's done (after flush)
            unsafe { libc::_exit(0) };
        }
        Ok(ForkResult::Parent { child }) => { // Consumer
            while sync_atomic_flag.load(Ordering::Acquire) < 1 { // Wait for producer to signal ready (state 1)
                std::hint::spin_loop();
            }

            sync_atomic_flag.store(2, Ordering::Release); // 2. Consumer signals it's ready, producer can start
            let start_time = std::time::Instant::now();
            let mut consumed_count = 0;

            if ITERS > 0 {
                loop {
                    if consumed_count >= ITERS {
                        break; // All expected items have been consumed
                    }

                    match q.bench_pop() {
                        Ok(_item) => {
                            consumed_count += 1;
                        }
                        Err(_) => { // Pop failed
                            if sync_atomic_flag.load(Ordering::Acquire) == 3 {
                                // Producer is done. If pop fails now, check if queue is TRULY empty.
                                // The SpscQueue::empty() method for each queue type should be accurate.
                                if q.bench_is_empty() {
                                    break; // Producer done and queue is definitively empty
                                }
                                // If not empty, but pop failed, producer is done. Spin briefly.
                                // This allows for visibility delays or transient empty states for complex queues.
                                std::hint::spin_loop();
                            } else {
                                // Producer not done, but queue is temporarily empty. Spin.
                                std::hint::spin_loop();
                            }
                        }
                    }
                }
            }

            let duration = start_time.elapsed();

            // Ensure producer has actually signaled done before waitpid,
            // especially if consumer loop exited early due to consumed_count == ITERS.
            while sync_atomic_flag.load(Ordering::Acquire) != 3 {
                std::hint::spin_loop();
            }
            waitpid(child, None).expect("SPSC waitpid failed");

            unsafe {
                unmap_shared(sync_shm as *mut u8, page_size);
            }

            if !PERFORMANCE_TEST && consumed_count < ITERS {
                eprintln!(
                    "Warning (SPSC Consumer): Consumed {}/{} items. Q: {}. Potential items missed.",
                    consumed_count,
                    ITERS,
                    std::any::type_name::<Q>()
                );
            } else if !PERFORMANCE_TEST && consumed_count > ITERS {
                 eprintln!( // Should not happen if producer sends exactly ITERS
                    "Warning (SPSC Consumer): Consumed more items {}/{} than expected. Q: {}",
                    consumed_count,
                    ITERS,
                    std::any::type_name::<Q>()
                );
            }
            duration
        }
        Err(e) => {
            unsafe { unmap_shared(sync_shm as *mut u8, page_size); }
            panic!("SPSC fork failed: {}", e);
        }
    }
}

// Criterion setup 
fn custom_criterion() -> Criterion { 
   Criterion::default() 
      .warm_up_time(Duration::from_secs(2)) 
      .measurement_time(Duration::from_secs(15)) 
      .sample_size(10) 
} 

criterion_group!{ 
   name = benches; 
   config = custom_criterion(); 
   targets = 
      //bench_lamport, 
      //bench_bqueue, 
      //bench_mp, 
      bench_unbounded, 
      bench_dspsc, 
      bench_dehnavi,
      bench_iffq,  
      bench_biffq,
      bench_ffq,
      bench_llq,
      bench_blq,
} 
criterion_main!(benches);

