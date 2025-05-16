// benches/mpsc_bench.rs

#![allow(clippy::cast_possible_truncation)]

use criterion::{criterion_group, criterion_main, Criterion, Bencher};
// Import the queue types from your crate
use queues::mpsc::{DrescherQueue, JayantiPetrovicMpscQueue, JiffyQueue}; // Added JiffyQueue
use queues::MpscQueue; // Use the main MpscQueue trait from your crate

use core::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use std::ptr;
use nix::{
   libc,
   sys::wait::waitpid,
   unistd::{fork, ForkResult},
};

// Using your original constants for items
const PERFORMANCE_TEST: bool = false; // Set to false for testing, true for actual perf runs

const NUM_PRODUCERS: usize = 1;
const ITEMS_PER_PRODUCER: usize = 1_000_000; // Kept as per your original file
const TOTAL_ITEMS: usize = NUM_PRODUCERS * ITEMS_PER_PRODUCER; // Derived, will be 1,000,000

// Capacity for DrescherQueue, kept as per your original file
const NODE_CAPACITY: usize = TOTAL_ITEMS + NUM_PRODUCERS;

// Parameters for JiffyQueue for benchmarking
// These need to be sensible for the scale of TOTAL_ITEMS
const JIFFY_NODES_PER_BUFFER_BENCH: usize = 4860; // Default from Jiffy implementation
const JIFFY_MAX_BUFFERS_BENCH: usize = if TOTAL_ITEMS > 0 && JIFFY_NODES_PER_BUFFER_BENCH > 0 {
    (TOTAL_ITEMS / JIFFY_NODES_PER_BUFFER_BENCH) + NUM_PRODUCERS + 10 // Ensure enough buffers + safety margin
} else {
    NUM_PRODUCERS + 10 // Default if no items or zero capacity per buffer
};


// --- Local Trait for benchmarking within this file ---
// This trait definition is local to mpsc_bench.rs
trait BenchMpscQueue<T: Send>: Send + Sync + 'static {
   fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()>;
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
   let ret = libc::munmap(ptr.cast(), len);
   assert_eq!(ret, 0, "munmap failed: {}", std::io::Error::last_os_error());
}

// --- Impls for the local BenchMpscQueue trait ---

impl<T: Send + 'static> BenchMpscQueue<T> for DrescherQueue<T> {
   fn bench_push(&self, item: T, _producer_id: usize) -> Result<(), ()> {
       MpscQueue::push(self, item).map_err(|_| ())
   }
   fn bench_pop(&self) -> Result<T, ()> { MpscQueue::pop(self).map_err(|_| ()) }
   fn bench_is_empty(&self) -> bool { MpscQueue::is_empty(self) }
   fn bench_is_full(&self) -> bool { MpscQueue::is_full(self) }
}

impl<T: Send + Clone + 'static> BenchMpscQueue<T> for JayantiPetrovicMpscQueue<T> {
   fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
       self.enqueue(producer_id, item).map_err(|_| ())
   }
   fn bench_pop(&self) -> Result<T, ()> { self.dequeue().ok_or(()) }
   fn bench_is_empty(&self) -> bool { MpscQueue::is_empty(self) }
   fn bench_is_full(&self) -> bool { MpscQueue::is_full(self) }
}

// --- Impl for JiffyQueue using the local BenchMpscQueue trait ---
impl<T: Send + 'static + Clone + fmt::Debug> BenchMpscQueue<T> for JiffyQueue<T> {
    fn bench_push(&self, item: T, _producer_id: usize) -> Result<(), ()> {
        // Jiffy's MpscQueue impl of push doesn't use producer_id from its signature
        MpscQueue::push(self, item).map_err(|_| ())
    }

    fn bench_pop(&self) -> Result<T, ()> {
        MpscQueue::pop(self).map_err(|_| ())
    }

    fn bench_is_empty(&self) -> bool {
        MpscQueue::is_empty(self)
    }

    fn bench_is_full(&self) -> bool {
        MpscQueue::is_full(self) 
    }
}

// --- Benchmark Setup ---
struct ProducerSync {
   producers_done_count: AtomicU32,
}

impl ProducerSync {
   fn new_in_shm(mem_ptr: *mut u8) -> &'static Self {
      let sync_ptr = mem_ptr as *mut Self;
      unsafe {
         ptr::write(sync_ptr, Self { producers_done_count: AtomicU32::new(0) });
         &*sync_ptr
      }
   }
   fn shared_size() -> usize { std::mem::size_of::<Self>() }
}

// Generic MPSC fork-and-run helper
fn fork_and_run_mpsc<Q, F>(
   queue_init_fn: F,
   num_producers: usize,
   items_per_prod_arg: usize, // Changed name to avoid conflict with const
) -> std::time::Duration
where
   Q: BenchMpscQueue<usize> + 'static, 
   F: FnOnce() -> (&'static Q, *mut u8, usize),
{
   let total_items_to_produce = num_producers * items_per_prod_arg;
    // Handle case where no items are to be produced to prevent issues later
    if total_items_to_produce == 0 {
        if num_producers > 0 { // Still fork if producers > 0, they just do nothing
             // eprintln!("Warning: No items per producer, benchmark run will be trivial for producers.");
        } else { // No producers, no items
            return Duration::from_nanos(1); // Trivial duration
        }
    }

   let producer_sync_size = ProducerSync::shared_size();
   let producer_sync_shm_ptr = unsafe { map_shared(producer_sync_size) };
   let producer_sync = ProducerSync::new_in_shm(producer_sync_shm_ptr);

   let (q, q_shm_ptr, q_shm_size) = queue_init_fn();
   let mut producer_pids = Vec::with_capacity(num_producers);

   for producer_id in 0..num_producers {
       match unsafe { fork() } {
           Ok(ForkResult::Child) => {
               for i in 0..items_per_prod_arg { // Use the argument here
                   let item_value = producer_id * items_per_prod_arg + i; 
                   while q.bench_push(item_value, producer_id).is_err() {
                       std::hint::spin_loop();
                   }
               }
               producer_sync.producers_done_count.fetch_add(1, Ordering::AcqRel);
               unsafe { libc::_exit(0) };
           }
           Ok(ForkResult::Parent { child }) => { producer_pids.push(child); }
           Err(e) => panic!("Fork failed for producer {}: {}", producer_id, e),
       }
   }

   let start_time = std::time::Instant::now();
   let mut consumed_count = 0;

    if total_items_to_produce > 0 {
        while consumed_count < total_items_to_produce {
            if let Ok(_item) = q.bench_pop() {
                consumed_count += 1;
            } else { // Pop failed (queue might be temporarily empty)
                if producer_sync.producers_done_count.load(Ordering::Acquire) == num_producers as u32 {
                    // Producers are done. Attempt to drain any remaining items.
                    // Loop to pop until it's truly empty and pop fails.
                    let mut drained_after_prods_done = 0;
                    while let Ok(_item_after_done) = q.bench_pop() {
                        consumed_count += 1;
                        drained_after_prods_done += 1;
                        if consumed_count >= total_items_to_produce { break; }
                    }
                    // After this inner drain loop, if we still haven't consumed all,
                    // and the queue claims to be empty, we can break.
                    if consumed_count >= total_items_to_produce || q.bench_is_empty() {
                        break;
                    }
                }
                // If producers not done, or done but queue wasn't empty on first check after done,
                // or drain didn't get all items, spin and retry.
                std::hint::spin_loop(); 
            }
        }
    }
   let duration = start_time.elapsed();

   for pid in producer_pids {
       waitpid(pid, None).expect("waitpid for producer failed");
   }

   if !PERFORMANCE_TEST && consumed_count < total_items_to_produce {
       eprintln!(
           "Warning: MPSC Consumer consumed {}/{} items. Queue type: {}",
           consumed_count, total_items_to_produce, std::any::type_name::<Q>()
       );
   }
   
   unsafe {
       unmap_shared(q_shm_ptr, q_shm_size);
       unmap_shared(producer_sync_shm_ptr, producer_sync_size);
   }
   duration
}

// --- Benchmark Functions ---

fn bench_drescher_mpsc(c: &mut Criterion) {
   let mut group = c.benchmark_group("DrescherMPSC");
   // Using constants from above
   group.bench_function(
      format!("{}Prod_{}ItemsPerProd", NUM_PRODUCERS, ITEMS_PER_PRODUCER),
      |b: &mut Bencher| {
         b.iter_custom(|_iters| {
            fork_and_run_mpsc::<DrescherQueue<usize>, _>(
               || {
                  let bytes = DrescherQueue::<usize>::shared_size(NODE_CAPACITY);
                  let shm_ptr = unsafe { map_shared(bytes) };
                  let q_mut_ref = unsafe { DrescherQueue::init_in_shared(shm_ptr, NODE_CAPACITY) };
                  (q_mut_ref, shm_ptr, bytes)
               },
               NUM_PRODUCERS,
               ITEMS_PER_PRODUCER,
            )
         })
      },
   );
   group.finish();
}

fn bench_jayanti_petrovic_mpsc(c: &mut Criterion) {
   let mut group = c.benchmark_group("JayantiPetrovicMPSC");
   let node_pool_capacity = TOTAL_ITEMS + NUM_PRODUCERS * 2;

   group.bench_function(
       format!("{}Prod_{}ItemsPerProd", NUM_PRODUCERS, ITEMS_PER_PRODUCER),
       |b: &mut Bencher| {
           b.iter_custom(|_iters| {
               fork_and_run_mpsc::<JayantiPetrovicMpscQueue<usize>, _>(
                   || {
                       let bytes = JayantiPetrovicMpscQueue::<usize>::shared_size(NUM_PRODUCERS, node_pool_capacity);
                       let shm_ptr = unsafe { map_shared(bytes) };
                       let q_mut_ref = unsafe { 
                           JayantiPetrovicMpscQueue::init_in_shared(
                               shm_ptr, NUM_PRODUCERS, node_pool_capacity
                           ) 
                       };
                       (q_mut_ref, shm_ptr, bytes)
                   },
                   NUM_PRODUCERS,
                   ITEMS_PER_PRODUCER,
               )
           })
       },
   );
   group.finish();
}

// --- JiffyQueue MPSC benchmark function ---
fn bench_jiffy_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("JiffyMPSC");
    
    // Scale based on number of producers
    for num_producers in [1, 2, 4, 8].iter() {
        group.bench_function(
            format!("{}Prod_{}ItemsPerProd", num_producers, ITEMS_PER_PRODUCER),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpsc::<JiffyQueue<usize>, _>(
                        || {
                            let bytes = JiffyQueue::<usize>::shared_size(JIFFY_NODES_PER_BUFFER_BENCH, JIFFY_MAX_BUFFERS_BENCH);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q_mut_ref = unsafe { 
                                JiffyQueue::init_in_shared(shm_ptr, JIFFY_NODES_PER_BUFFER_BENCH, JIFFY_MAX_BUFFERS_BENCH) 
                            };
                            (q_mut_ref, shm_ptr, bytes)
                        },
                        *num_producers,
                        ITEMS_PER_PRODUCER / num_producers, // Scale items to keep total constant
                    )
                })
            },
        );
    }
    group.finish();
}

// --- Criterion Setup ---
fn custom_criterion() -> Criterion {
   Criterion::default()
      .warm_up_time(Duration::from_secs(2))
      .measurement_time(Duration::from_secs(10))
      .sample_size(10)
}

criterion_group! {
   name = mpsc_benches;
   config = custom_criterion();
   targets =
      bench_drescher_mpsc,
      bench_jayanti_petrovic_mpsc,
      bench_jiffy_mpsc,
}
criterion_main!(mpsc_benches);