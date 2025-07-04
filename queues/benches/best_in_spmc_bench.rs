#![allow(clippy::cast_possible_truncation)]

use criterion::{criterion_group, criterion_main, Criterion};
use nix::{
    libc,
    sys::wait::waitpid,
    unistd::{fork, ForkResult},
};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

// Import the best-performing queue types for SPMC scenario
use queues::{
    spmc::{DavidQueue, EnqueuerState},
    SpmcQueue, YangCrummeyQueue,
};

const PERFORMANCE_TEST: bool = false;
const ITEMS_PER_PRODUCER_TARGET: usize = 5_000;
const CONSUMER_COUNTS_TO_TEST: &[usize] = &[1, 2, 4, 8, 14];
const MAX_BENCH_SPIN_RETRY_ATTEMPTS: usize = 100_000_000;

// Helper trait for benchmarking different queue types in SPMC scenario
trait BenchSpmcQueue<T: Send + Clone>: Send + Sync + 'static {
    fn bench_push(&self, item: T) -> Result<(), ()>;
    fn bench_pop(&self, consumer_id: usize) -> Result<T, ()>;
    fn bench_is_empty(&self) -> bool;
    fn bench_is_full(&self) -> bool;
}

// mmap / munmap helpers
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

// DavidQueue (SPMC) - native SPMC implementation wrapper
struct DavidQueueWrapper<T: Send + Clone + 'static> {
    queue: &'static DavidQueue<T>,
    enqueuer_state: *mut EnqueuerState,
}

unsafe impl<T: Send + Clone> Send for DavidQueueWrapper<T> {}
unsafe impl<T: Send + Clone> Sync for DavidQueueWrapper<T> {}

impl<T: Send + Clone + 'static> BenchSpmcQueue<T> for DavidQueueWrapper<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        unsafe { self.queue.enqueue(&mut *self.enqueuer_state, item) }
    }

    fn bench_pop(&self, consumer_id: usize) -> Result<T, ()> {
        self.queue.dequeue(consumer_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.queue.is_full()
    }
}

// YangCrummeyQueue (MPMC) - testing in SPMC scenario
impl<T: Send + Clone + 'static> BenchSpmcQueue<T> for YangCrummeyQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        // Use process_id 0 for single producer
        self.enqueue(0, item)
    }

    fn bench_pop(&self, consumer_id: usize) -> Result<T, ()> {
        // Use process_id starting from 1 for consumers
        self.dequeue(consumer_id + 1)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        false // YMC doesn't have a full check
    }
}

#[repr(C)]
struct SpmcStartupSync {
    producer_ready: AtomicBool,
    consumers_ready: AtomicU32,
    go_signal: AtomicBool,
}

impl SpmcStartupSync {
    fn new_in_shm(mem_ptr: *mut u8) -> &'static Self {
        let sync_ptr = mem_ptr as *mut Self;
        unsafe {
            ptr::write(
                sync_ptr,
                Self {
                    producer_ready: AtomicBool::new(false),
                    consumers_ready: AtomicU32::new(0),
                    go_signal: AtomicBool::new(false),
                },
            );
            &*sync_ptr
        }
    }

    fn shared_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

#[repr(C)]
struct ConsumerDoneSync {
    consumers_done: AtomicU32,
}

impl ConsumerDoneSync {
    fn new_in_shm(mem_ptr: *mut u8) -> &'static Self {
        let sync_ptr = mem_ptr as *mut Self;
        unsafe {
            ptr::write(
                sync_ptr,
                Self {
                    consumers_done: AtomicU32::new(0),
                },
            );
            &*sync_ptr
        }
    }

    fn shared_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

fn fork_and_run_spmc<Q, F>(
    queue_init_fn: F,
    num_consumers: usize,
    items_per_producer: usize,
) -> std::time::Duration
where
    Q: BenchSpmcQueue<usize> + ?Sized + 'static,
    F: FnOnce(usize) -> (&'static Q, *mut u8, usize),
{
    if num_consumers == 0 {
        return Duration::from_nanos(1);
    }
    let (q, q_shm_ptr, q_shm_size) = queue_init_fn(num_consumers);
    let total_items_to_produce = items_per_producer;

    let startup_sync_size = SpmcStartupSync::shared_size();
    let startup_sync_shm_ptr = unsafe { map_shared(startup_sync_size) };
    let startup_sync = SpmcStartupSync::new_in_shm(startup_sync_shm_ptr);

    let done_sync_size = ConsumerDoneSync::shared_size();
    let done_sync_shm_ptr = unsafe { map_shared(done_sync_size) };
    let done_sync = ConsumerDoneSync::new_in_shm(done_sync_shm_ptr);

    let mut consumer_pids = Vec::with_capacity(num_consumers);

    // Fork consumers
    for consumer_id in 0..num_consumers {
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                // Consumer process
                startup_sync.consumers_ready.fetch_add(1, Ordering::AcqRel);

                // Wait for go signal
                while !startup_sync.go_signal.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                let mut pop_attempts = 0;
                let mut consumed_count = 0;

                loop {
                    match q.bench_pop(consumer_id) {
                        Ok(_item) => {
                            consumed_count += 1;
                            pop_attempts = 0;
                        }
                        Err(_) => {
                            pop_attempts += 1;

                            // Check if producer is done
                            if startup_sync.producer_ready.load(Ordering::Acquire) == false {
                                // Producer has finished
                                if pop_attempts > 1000 {
                                    // Give up after many attempts when producer is done
                                    break;
                                }
                            } else if pop_attempts > MAX_BENCH_SPIN_RETRY_ATTEMPTS {
                                panic!("Consumer {} exceeded max spin retry attempts", consumer_id);
                            }

                            std::hint::spin_loop();
                        }
                    }
                }

                done_sync.consumers_done.fetch_add(1, Ordering::AcqRel);

                if !PERFORMANCE_TEST && consumed_count == 0 {
                    eprintln!("Warning: Consumer {} consumed 0 items", consumer_id);
                }

                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                consumer_pids.push(child);
            }
            Err(e) => panic!("fork failed: {}", e),
        }
    }

    // Producer process (parent)

    // Wait for all consumers to be ready
    while startup_sync.consumers_ready.load(Ordering::Acquire) < num_consumers as u32 {
        std::hint::spin_loop();
    }

    // Signal producer ready
    startup_sync.producer_ready.store(true, Ordering::Release);

    // Signal go
    startup_sync.go_signal.store(true, Ordering::Release);

    let start_time = std::time::Instant::now();

    // Producer work
    let mut push_attempts = 0;
    for i in 0..total_items_to_produce {
        while q.bench_push(i).is_err() {
            push_attempts += 1;
            if push_attempts > MAX_BENCH_SPIN_RETRY_ATTEMPTS {
                panic!("Producer exceeded max spin retry attempts for push");
            }
            std::hint::spin_loop();
        }
    }

    // Signal producer done
    startup_sync.producer_ready.store(false, Ordering::Release);

    // Wait for all consumers to finish
    while done_sync.consumers_done.load(Ordering::Acquire) < num_consumers as u32 {
        std::hint::spin_loop();
    }

    let duration = start_time.elapsed();

    // Wait for all consumer processes
    for pid in consumer_pids {
        let _ = waitpid(pid, None).expect("waitpid failed");
    }

    // Cleanup
    unsafe {
        unmap_shared(q_shm_ptr, q_shm_size);
        unmap_shared(startup_sync_shm_ptr, startup_sync_size);
        unmap_shared(done_sync_shm_ptr, done_sync_size);
    }

    duration
}

fn bench_david_native(c: &mut Criterion) {
    for &num_consumers in CONSUMER_COUNTS_TO_TEST {
        let bench_name = format!("David (Native SPMC) - 1P{}C", num_consumers);
        c.bench_function(&bench_name, |b| {
            b.iter_custom(|_iters| {
                let queue_init = |num_cons: usize| {
                    let bytes = DavidQueue::<usize>::shared_size(num_cons);
                    let shm_ptr = unsafe { map_shared(bytes) };

                    // Create enqueuer state
                    let enqueuer_state_bytes = std::mem::size_of::<EnqueuerState>();
                    let enqueuer_state_ptr = unsafe { map_shared(enqueuer_state_bytes) };
                    let enqueuer_state = enqueuer_state_ptr as *mut EnqueuerState;
                    unsafe {
                        ptr::write(enqueuer_state, EnqueuerState::new());
                    }

                    let q = unsafe {
                        DavidQueue::init_in_shared(shm_ptr, num_cons, &mut *enqueuer_state)
                    };

                    let wrapper = Box::leak(Box::new(DavidQueueWrapper {
                        queue: q,
                        enqueuer_state,
                    }));

                    (
                        wrapper as &'static dyn BenchSpmcQueue<usize>,
                        shm_ptr,
                        bytes,
                    )
                };

                fork_and_run_spmc(queue_init, num_consumers, ITEMS_PER_PRODUCER_TARGET)
            })
        });
    }
}

fn bench_ymc_as_spmc(c: &mut Criterion) {
    for &num_consumers in CONSUMER_COUNTS_TO_TEST {
        let bench_name = format!("YMC (MPMC as SPMC) - 1P{}C", num_consumers);
        c.bench_function(&bench_name, |b| {
            b.iter_custom(|_iters| {
                let queue_init = |num_cons: usize| {
                    // YMC needs to know total number of threads
                    // 1 producer + num_consumers
                    let num_threads = 1 + num_cons;
                    let bytes = YangCrummeyQueue::<usize>::shared_size(num_threads);
                    let shm_ptr = unsafe { map_shared(bytes) };
                    let q = unsafe { YangCrummeyQueue::init_in_shared(shm_ptr, num_threads) };

                    (q as &'static dyn BenchSpmcQueue<usize>, shm_ptr, bytes)
                };

                fork_and_run_spmc(queue_init, num_consumers, ITEMS_PER_PRODUCER_TARGET)
            })
        });
    }
}

// Criterion setup
fn custom_criterion() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(10))
        .sample_size(10)
}

criterion_group! {
    name = benches;
    config = custom_criterion();
    targets =
        bench_david_native,
        bench_ymc_as_spmc
}
criterion_main!(benches);
