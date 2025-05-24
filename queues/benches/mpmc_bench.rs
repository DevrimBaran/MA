use criterion::{criterion_group, criterion_main, Criterion, Bencher};
use queues::mpmc::YangCrummeyQueue;
use queues::MpmcQueue;
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use std::time::Duration;
use std::ptr;
use nix::{
   libc,
   sys::wait::waitpid,
   unistd::{fork, ForkResult},
};

const PERFORMANCE_TEST: bool = true;
const ITEMS_PER_THREAD_TARGET: usize = 200_000;
const THREAD_COUNTS_TO_TEST: &[(usize, usize)] = &[
   (1, 1),    
   (2, 2),    
   (4, 4),    
   (7, 7),    
];


trait BenchMpmcQueue<T: Send + Clone>: Send + Sync + 'static {
   fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()>;
   fn bench_pop(&self, thread_id: usize) -> Result<T, ()>;
   fn bench_is_empty(&self) -> bool;
   fn bench_is_full(&self) -> bool;
}


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


impl<T: Send + Clone + 'static> BenchMpmcQueue<T> for YangCrummeyQueue<T> {
   fn bench_push(&self, item: T, thread_id: usize) -> Result<(), ()> {
      self.enqueue(thread_id, item)
   }
   
   fn bench_pop(&self, thread_id: usize) -> Result<T, ()> {
      self.dequeue(thread_id)
   }
   
   fn bench_is_empty(&self) -> bool {
      self.is_empty()
   }
   
   fn bench_is_full(&self) -> bool {
      false 
   }
}


#[repr(C)]
struct MpmcStartupSync {
   producers_ready: AtomicU32,
   consumers_ready: AtomicU32,
   go_signal: AtomicBool,
}

impl MpmcStartupSync {
   fn new_in_shm(mem_ptr: *mut u8) -> &'static Self {
      let sync_ptr = mem_ptr as *mut Self;
      unsafe {
         ptr::write(sync_ptr, Self {
               producers_ready: AtomicU32::new(0),
               consumers_ready: AtomicU32::new(0),
               go_signal: AtomicBool::new(false),
         });
         &*sync_ptr
      }
   }
   
   fn shared_size() -> usize {
      std::mem::size_of::<Self>()
   }
}

#[repr(C)]
struct MpmcDoneSync {
   producers_done: AtomicU32,
   consumers_done: AtomicU32,
}

impl MpmcDoneSync {
   fn new_in_shm(mem_ptr: *mut u8) -> &'static Self {
      let sync_ptr = mem_ptr as *mut Self;
      unsafe {
         ptr::write(sync_ptr, Self {
               producers_done: AtomicU32::new(0),
               consumers_done: AtomicU32::new(0),
         });
         &*sync_ptr
      }
   }
   
   fn shared_size() -> usize {
      std::mem::size_of::<Self>()
   }
}


fn fork_and_run_mpmc<Q, F>(
   queue_init_fn: F,
   num_producers: usize,
   num_consumers: usize,
   items_per_thread: usize,
) -> Duration
where
   Q: BenchMpmcQueue<usize> + 'static,
   F: FnOnce() -> (&'static Q, *mut u8, usize),
{
   if num_producers == 0 || num_consumers == 0 {
      return Duration::from_nanos(1);
   }
   
   let total_items = num_producers * items_per_thread;
   
   
   let (q, q_shm_ptr, q_shm_size) = queue_init_fn();
   
   
   let startup_sync_size = MpmcStartupSync::shared_size();
   let startup_sync_shm_ptr = unsafe { map_shared(startup_sync_size) };
   let startup_sync = MpmcStartupSync::new_in_shm(startup_sync_shm_ptr);
   
   let done_sync_size = MpmcDoneSync::shared_size();
   let done_sync_shm_ptr = unsafe { map_shared(done_sync_size) };
   let done_sync = MpmcDoneSync::new_in_shm(done_sync_shm_ptr);
   
   let mut producer_pids = Vec::with_capacity(num_producers);
   let mut consumer_pids = Vec::with_capacity(num_consumers);
   
   
   for producer_id in 0..num_producers {
      match unsafe { fork() } {
         Ok(ForkResult::Child) => {
               
               startup_sync.producers_ready.fetch_add(1, Ordering::AcqRel);
               
               
               while !startup_sync.go_signal.load(Ordering::Acquire) {
                  std::hint::spin_loop();
               }
               
               
               for i in 0..items_per_thread {
                  let item_value = producer_id * items_per_thread + i;
                  while q.bench_push(item_value, producer_id).is_err() {
                     std::hint::spin_loop();
                  }
               }
               
               done_sync.producers_done.fetch_add(1, Ordering::AcqRel);
               unsafe { libc::_exit(0) };
         }
         Ok(ForkResult::Parent { child }) => {
               producer_pids.push(child);
         }
         Err(e) => {
               
               unsafe {
                  if !q_shm_ptr.is_null() { unmap_shared(q_shm_ptr, q_shm_size); }
                  unmap_shared(startup_sync_shm_ptr, startup_sync_size);
                  unmap_shared(done_sync_shm_ptr, done_sync_size);
               }
               panic!("Fork failed for producer {}: {}", producer_id, e);
         }
      }
   }
   
   
   for consumer_id in 0..num_consumers {
      match unsafe { fork() } {
         Ok(ForkResult::Child) => {
               
               startup_sync.consumers_ready.fetch_add(1, Ordering::AcqRel);
               
               
               while !startup_sync.go_signal.load(Ordering::Acquire) {
                  std::hint::spin_loop();
               }
               
               let mut consumed_count = 0;
               let target_items = total_items / num_consumers;
               let extra_items = if consumer_id < (total_items % num_consumers) { 1 } else { 0 };
               let my_target = target_items + extra_items;
               
               
               while consumed_count < my_target {
                  match q.bench_pop(num_producers + consumer_id) {
                     Ok(_item) => {
                           consumed_count += 1;
                     }
                     Err(_) => {
                           
                           if done_sync.producers_done.load(Ordering::Acquire) == num_producers as u32 {
                              
                              let mut retries = 0;
                              while retries < 1000 && consumed_count < my_target {
                                 if q.bench_pop(num_producers + consumer_id).is_ok() {
                                       consumed_count += 1;
                                 } else {
                                       retries += 1;
                                       std::thread::yield_now();
                                 }
                              }
                              
                              if consumed_count < my_target && q.bench_is_empty() {
                                 break;
                              }
                           } else {
                              std::thread::yield_now();
                           }
                     }
                  }
               }
               
               done_sync.consumers_done.fetch_add(1, Ordering::AcqRel);
               
               if !PERFORMANCE_TEST && consumed_count != my_target {
                  eprintln!(
                     "Warning: Consumer {} consumed {}/{} items",
                     consumer_id, consumed_count, my_target
                  );
               }
               
               unsafe { libc::_exit(0) };
         }
         Ok(ForkResult::Parent { child }) => {
               consumer_pids.push(child);
         }
         Err(e) => {
               
               unsafe {
                  if !q_shm_ptr.is_null() { unmap_shared(q_shm_ptr, q_shm_size); }
                  unmap_shared(startup_sync_shm_ptr, startup_sync_size);
                  unmap_shared(done_sync_shm_ptr, done_sync_size);
               }
               panic!("Fork failed for consumer {}: {}", consumer_id, e);
         }
      }
   }
   
   
   while startup_sync.producers_ready.load(Ordering::Acquire) < num_producers as u32 ||
         startup_sync.consumers_ready.load(Ordering::Acquire) < num_consumers as u32 {
      std::hint::spin_loop();
   }
   
   
   let start_time = std::time::Instant::now();
   startup_sync.go_signal.store(true, Ordering::Release);
   
   
   for pid in producer_pids {
      waitpid(pid, None).expect("waitpid for producer failed");
   }
   
   for pid in consumer_pids {
      waitpid(pid, None).expect("waitpid for consumer failed");
   }
   
   let duration = start_time.elapsed();
   
   
   unsafe {
      if !q_shm_ptr.is_null() { unmap_shared(q_shm_ptr, q_shm_size); }
      unmap_shared(startup_sync_shm_ptr, startup_sync_size);
      unmap_shared(done_sync_shm_ptr, done_sync_size);
   }
   
   duration
}


fn bench_yang_crummey(c: &mut Criterion) {
   let mut group = c.benchmark_group("YangCrummeyMPMC");
   
   for &(num_prods, num_cons) in THREAD_COUNTS_TO_TEST {
      let items_per_thread = ITEMS_PER_THREAD_TARGET;
      let total_threads = num_prods + num_cons;
      
      group.bench_function(
         format!("{}P_{}C", num_prods, num_cons),
         |b: &mut Bencher| {
               b.iter_custom(|_iters| {
                  fork_and_run_mpmc::<YangCrummeyQueue<usize>, _>(
                     || {
                           let bytes = YangCrummeyQueue::<usize>::shared_size(total_threads);
                           let shm_ptr = unsafe { map_shared(bytes) };
                           let q = unsafe { 
                              YangCrummeyQueue::init_in_shared(shm_ptr, total_threads) 
                           };
                           (q, shm_ptr, bytes)
                     },
                     num_prods,
                     num_cons,
                     items_per_thread,
                  )
               })
         },
      );
   }
   
   group.finish();
}

fn custom_criterion() -> Criterion {
   Criterion::default()
      .warm_up_time(Duration::from_secs(2))
      .measurement_time(Duration::from_secs(10))
      .sample_size(10)
}

criterion_group! {
   name = benches;
   config = custom_criterion();
   targets = bench_yang_crummey
}

criterion_main!(benches);