// benches/bench_mpsc.rs

#![allow(clippy::cast_possible_truncation)]

use criterion::{criterion_group, criterion_main, Criterion, Bencher};
use queues::mpsc::{DrescherQueue, JayantiPetrovicMpscQueue};
use std::sync::atomic::{AtomicU32, Ordering, AtomicBool};
use std::sync::Arc;
use queues::MpscQueue;
use std::time::Duration;
use std::ptr;
use nix::{
   libc,
   sys::wait::waitpid,
   unistd::{fork, ForkResult},
};

const PERFORMANCE_TEST: bool = false; // Set to false for testing

// Constants for MPSC benchmark
const NUM_PRODUCERS: usize = 5;
const ITEMS_PER_PRODUCER: usize = 1_000_000;
const TOTAL_ITEMS: usize = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
const NODE_CAPACITY: usize = TOTAL_ITEMS + NUM_PRODUCERS;

trait BenchMpscQueue<T: Send>: Send + Sync + 'static {
   fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()>;
   fn bench_pop(&self) -> Result<T, ()>;
   fn bench_is_empty(&self) -> bool;
   fn bench_is_full(&self) -> bool;
}


// mmap / munmap helpers (can be shared or duplicated from bench_spsc.rs)
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

// Trait for DrescherQueue
impl<T: Send + 'static> BenchMpscQueue<T> for DrescherQueue<T> {
   fn bench_push(&self, item: T, _producer_id: usize) -> Result<(), ()> {
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

// Trait for JayantiPetrovicMpscQueue
impl<T: Send + Clone + 'static> BenchMpscQueue<T> for JayantiPetrovicMpscQueue<T> {
   fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
       // Call the specialized enqueue method with producer_id
       self.enqueue(producer_id, item).map_err(|_| ())
   }
   
   fn bench_pop(&self) -> Result<T, ()> {
       // Use the dequeue method, which returns Option<T>
       self.dequeue().ok_or(())
   }
   
   fn bench_is_empty(&self) -> bool {
       MpscQueue::is_empty(self)
   }
   
   fn bench_is_full(&self) -> bool {
       MpscQueue::is_full(self)
   }
}

// MPSC Benchmark Setup

// We need a way for producers to signal completion to the main (consumer) process/thread
// This can be done via another shared AtomicU32 or multiple AtomicBools.
struct ProducerSync {
   producers_done_count: AtomicU32,
   // Can add more sync primitives if needed, like a start signal
}

impl ProducerSync {
   fn new_in_shm(mem_ptr: *mut u8) -> &'static Self {
      let sync_ptr = mem_ptr as *mut Self;
      unsafe {
         ptr::write(sync_ptr, Self {
               producers_done_count: AtomicU32::new(0),
         });
         &*sync_ptr
      }
   }
   fn shared_size() -> usize {
      std::mem::size_of::<Self>()
   }
}


// Generic MPSC fork-and-run helper
// Q will be &'static DrescherQueue<usize>
// For MPSC, we'll have multiple producer child processes and one consumer parent process.
fn fork_and_run_mpsc<Q, F>(
   queue_init_fn: F,
   num_producers: usize,
   items_per_producer: usize,
) -> std::time::Duration
where
   Q: BenchMpscQueue<usize> + 'static,
   F: FnOnce() -> (&'static Q, *mut u8, usize),
{
   let total_items_to_produce = num_producers * items_per_producer;

   let producer_sync_size = ProducerSync::shared_size();
   let producer_sync_shm_ptr = unsafe { map_shared(producer_sync_size) };
   let producer_sync = ProducerSync::new_in_shm(producer_sync_shm_ptr);

   let (q, shm_ptr, shm_size) = queue_init_fn();

   let mut producer_pids = Vec::new();

   for producer_id in 0..num_producers {
       match unsafe { fork() } {
           Ok(ForkResult::Child) => {
               for i in 0..items_per_producer {
                   let item_value = producer_id * items_per_producer + i;
                   while q.bench_push(item_value, producer_id).is_err() { // Pass producer_id to bench_push
                       std::hint::spin_loop();
                   }
               }
               producer_sync.producers_done_count.fetch_add(1, Ordering::AcqRel);
               unsafe { libc::_exit(0) };
           }
           Ok(ForkResult::Parent { child }) => {
               producer_pids.push(child);
           }
           Err(_) => panic!("Fork failed"),
       }
   }

   // Rest of the function remains the same...
   let start_time = std::time::Instant::now();
   let mut consumed_count = 0;

   while consumed_count < total_items_to_produce {
       if let Ok(_item) = q.bench_pop() {
           consumed_count += 1;
       } else {
           if producer_sync.producers_done_count.load(Ordering::Acquire) == num_producers as u32 {
               if let Ok(_item) = q.bench_pop() {
                   consumed_count += 1;
                   continue;
               }
               if q.bench_is_empty() {
                   break;
               }
           }
           std::hint::spin_loop();
       }
   }
   let duration = start_time.elapsed();

   // Wait for all producer children
   for pid in producer_pids {
       waitpid(pid, None).expect("waitpid for producer failed");
   }

   if (consumed_count < total_items_to_produce) && (PERFORMANCE_TEST == false) {
       eprintln!(
           "Warning: MPSC Consumer consumed {}/{} items. Queue type: JayantiPetrovicMpscQueue",
           consumed_count, total_items_to_produce
       );
   }

   unsafe {
       unmap_shared(shm_ptr, shm_size);
       unmap_shared(producer_sync_shm_ptr, producer_sync_size);
   }
   duration
}

// DrescherQueue MPSC benchmark function
fn bench_drescher_mpsc(c: &mut Criterion) {
   let mut group = c.benchmark_group("DrescherMPSC");

   let current_num_producers = NUM_PRODUCERS;
   let items_per_producer = TOTAL_ITEMS / current_num_producers;

   group.bench_function(
      format!("{}Producers_{}ItemsPerProd", current_num_producers, items_per_producer),
      |b: &mut Bencher| {
         b.iter_custom(|_iters| {
            fork_and_run_mpsc::<DrescherQueue<usize>, _>( // Q is DrescherQueue<usize>
               || {
                  let bytes = DrescherQueue::<usize>::shared_size(NODE_CAPACITY);
                  let shm_ptr = unsafe { map_shared(bytes) };
                  let q_mut_ref = unsafe { DrescherQueue::init_in_shared(shm_ptr, NODE_CAPACITY) };
                  let q_imm_ref: &'static DrescherQueue<usize> = q_mut_ref; // Coercion
                  (q_imm_ref, shm_ptr, bytes)
               },
               current_num_producers,
               items_per_producer,
            )
         })
      },
   );
   group.finish();
}

// JayantiPetrovicMpscQueue MPSC benchmark function
fn bench_jayanti_petrovic_mpsc(c: &mut Criterion) {
   let mut group = c.benchmark_group("JayantiPetrovicMPSC");

   let current_num_producers = NUM_PRODUCERS;
   let items_per_producer = TOTAL_ITEMS / current_num_producers;
   // Node pool size needs to accommodate all items plus some overhead for dummies
   let node_pool_capacity = TOTAL_ITEMS + NUM_PRODUCERS * 2;

   group.bench_function(
       format!("{}Producers_{}ItemsPerProd", current_num_producers, items_per_producer),
       |b: &mut Bencher| {
           b.iter_custom(|_iters| {
               fork_and_run_mpsc::<JayantiPetrovicMpscQueue<usize>, _>(
                   || {
                       let bytes = JayantiPetrovicMpscQueue::<usize>::shared_size(current_num_producers, node_pool_capacity);
                       let shm_ptr = unsafe { map_shared(bytes) };
                       let q_mut_ref = unsafe { 
                           JayantiPetrovicMpscQueue::init_in_shared(
                               shm_ptr, 
                               current_num_producers,
                               node_pool_capacity
                           ) 
                       };
                       let q_imm_ref: &'static JayantiPetrovicMpscQueue<usize> = q_mut_ref;
                       (q_imm_ref, shm_ptr, bytes)
                   },
                   current_num_producers,
                   items_per_producer,
               )
           })
       },
   );

   group.finish();
}

fn custom_criterion() -> Criterion {
   Criterion::default()
      .warm_up_time(Duration::from_secs(5))
      .measurement_time(Duration::from_secs(55))
      .sample_size(10)
}

criterion_group! {
   name = mpsc_benches;
   config = custom_criterion();
   targets =
      //bench_drescher_mpsc,
      bench_jayanti_petrovic_mpsc,
}
criterion_main!(mpsc_benches);