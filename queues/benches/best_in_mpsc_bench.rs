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

// Import the best-performing queue types for MPSC scenario
use queues::{DQueue, MpscQueue, YangCrummeyQueue};

const PERFORMANCE_TEST: bool = false;
const ITEMS_PER_PRODUCER_TARGET: usize = 10_000;
const PRODUCER_COUNTS_TO_TEST: &[usize] = &[1, 2, 4, 8, 14];
const MAX_BENCH_SPIN_RETRY_ATTEMPTS: usize = 100_000_000;

// Wrapper for YangCrummeyQueue to track the number of producers
struct YmcMpscWrapper<T: Send + Clone + 'static> {
    queue: &'static YangCrummeyQueue<T>,
    num_producers: usize,
}

unsafe impl<T: Send + Clone> Send for YmcMpscWrapper<T> {}
unsafe impl<T: Send + Clone> Sync for YmcMpscWrapper<T> {}

// Helper trait for benchmarking different queue types in MPSC scenario
trait BenchMpscQueue<T: Send>: Send + Sync + 'static {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()>;
    fn bench_pop(&self) -> Result<T, ()>;
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

// DQueue (MPSC) - native MPSC implementation
impl<T: Send + Clone + 'static> BenchMpscQueue<T> for DQueue<T> {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
        self.enqueue(producer_id, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        self.dequeue().ok_or(())
    }
    fn bench_is_empty(&self) -> bool {
        MpscQueue::is_empty(self)
    }
    fn bench_is_full(&self) -> bool {
        MpscQueue::is_full(self)
    }
}

// YmcMpscWrapper - YangCrummeyQueue (MPMC) testing in MPSC scenario
impl<T: Send + Clone + 'static> BenchMpscQueue<T> for YmcMpscWrapper<T> {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
        self.queue.enqueue(producer_id, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        // Consumer uses the thread ID after all producers
        self.queue.dequeue(self.num_producers)
    }
    fn bench_is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    fn bench_is_full(&self) -> bool {
        false // YMC doesn't have a full check
    }
}

#[repr(C)]
struct MultiProducerStartupSync {
    producers_ready_count: AtomicU32,
    go_signal: AtomicBool,
}

impl MultiProducerStartupSync {
    fn new_in_shm(mem_ptr: *mut u8) -> &'static Self {
        let sync_ptr = mem_ptr as *mut Self;
        unsafe {
            ptr::write(
                sync_ptr,
                Self {
                    producers_ready_count: AtomicU32::new(0),
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
struct ProducerDoneSync {
    producers_done_count: AtomicU32,
}

impl ProducerDoneSync {
    fn new_in_shm(mem_ptr: *mut u8) -> &'static Self {
        let sync_ptr = mem_ptr as *mut Self;
        unsafe {
            ptr::write(
                sync_ptr,
                Self {
                    producers_done_count: AtomicU32::new(0),
                },
            );
            &*sync_ptr
        }
    }
    fn shared_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

fn fork_and_run_mpsc<Q, F>(
    queue_init_fn: F,
    num_producers: usize,
    items_per_producer: usize,
) -> std::time::Duration
where
    Q: BenchMpscQueue<usize> + ?Sized + 'static,
    F: FnOnce(usize) -> (&'static Q, *mut u8, usize),
{
    if num_producers == 0 {
        return Duration::from_nanos(1);
    }
    let (q, q_shm_ptr, q_shm_size) = queue_init_fn(num_producers);
    let total_items_to_produce = num_producers * items_per_producer;

    let startup_sync_size = MultiProducerStartupSync::shared_size();
    let startup_sync_shm_ptr = unsafe { map_shared(startup_sync_size) };
    let startup_sync = MultiProducerStartupSync::new_in_shm(startup_sync_shm_ptr);

    let done_sync_size = ProducerDoneSync::shared_size();
    let done_sync_shm_ptr = unsafe { map_shared(done_sync_size) };
    let done_sync = ProducerDoneSync::new_in_shm(done_sync_shm_ptr);

    let mut producer_pids = Vec::with_capacity(num_producers);

    for producer_id in 0..num_producers {
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                startup_sync
                    .producers_ready_count
                    .fetch_add(1, Ordering::AcqRel);
                while !startup_sync.go_signal.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }
                let mut push_attempts = 0;
                for i in 0..items_per_producer {
                    let item = producer_id * items_per_producer + i;
                    while q.bench_push(item, producer_id).is_err() {
                        push_attempts += 1;
                        if push_attempts > MAX_BENCH_SPIN_RETRY_ATTEMPTS {
                            panic!(
                                "Producer {} exceeded max spin retry attempts for push",
                                producer_id
                            );
                        }
                        std::hint::spin_loop();
                    }
                }
                done_sync
                    .producers_done_count
                    .fetch_add(1, Ordering::AcqRel);
                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                producer_pids.push(child);
            }
            Err(e) => panic!("fork failed: {}", e),
        }
    }

    // Consumer process (parent)
    while startup_sync.producers_ready_count.load(Ordering::Acquire) < num_producers as u32 {
        std::hint::spin_loop();
    }

    startup_sync.go_signal.store(true, Ordering::Release);

    let start_time = std::time::Instant::now();
    let mut pop_spin_attempts = 0;
    let mut consumed_count = 0;

    while consumed_count < total_items_to_produce {
        match q.bench_pop() {
            Ok(_item) => {
                consumed_count += 1;
                pop_spin_attempts = 0;
            }
            Err(_) => {
                pop_spin_attempts += 1;
                let producers_done =
                    done_sync.producers_done_count.load(Ordering::Acquire) == num_producers as u32;
                if producers_done && pop_spin_attempts > 10_000 {
                    break;
                }
                if pop_spin_attempts > MAX_BENCH_SPIN_RETRY_ATTEMPTS && !producers_done {
                    panic!("Consumer exceeded max spin retry attempts for pop");
                }
                std::hint::spin_loop();
            }
        }
    }

    let duration = start_time.elapsed();

    for pid in producer_pids {
        let _ = waitpid(pid, None).expect("waitpid failed");
    }

    unsafe {
        unmap_shared(q_shm_ptr, q_shm_size);
        unmap_shared(startup_sync_shm_ptr, startup_sync_size);
        unmap_shared(done_sync_shm_ptr, done_sync_size);
    }

    if !PERFORMANCE_TEST && consumed_count != total_items_to_produce {
        eprintln!(
            "Warning: Consumer consumed {}/{} items. Queue type: {}",
            consumed_count,
            total_items_to_produce,
            std::any::type_name::<Q>()
        );
    }

    duration
}

fn bench_dqueue_native(c: &mut Criterion) {
    for &num_producers in PRODUCER_COUNTS_TO_TEST {
        let bench_name = format!("DQueue (Native MPSC) - {}P1C", num_producers);
        c.bench_function(&bench_name, |b| {
            b.iter_custom(|_iters| {
                let queue_init = |num_prods: usize| {
                    // Calculate segment pool capacity based on expected number of items
                    let total_items = num_prods * ITEMS_PER_PRODUCER_TARGET;
                    let segment_pool_capacity = (total_items / 262144) + 10; // N_SEGMENT_CAPACITY is 262144
                    let bytes = DQueue::<usize>::shared_size(num_prods, segment_pool_capacity);
                    let shm_ptr = unsafe { map_shared(bytes) };
                    let q = unsafe {
                        DQueue::init_in_shared(shm_ptr, num_prods, segment_pool_capacity)
                    };

                    (q as &'static dyn BenchMpscQueue<usize>, shm_ptr, bytes)
                };

                fork_and_run_mpsc(queue_init, num_producers, ITEMS_PER_PRODUCER_TARGET)
            })
        });
    }
}

fn bench_ymc_as_mpsc(c: &mut Criterion) {
    for &num_producers in PRODUCER_COUNTS_TO_TEST {
        let bench_name = format!("YMC (MPMC as MPSC) - {}P1C", num_producers);
        c.bench_function(&bench_name, |b| {
            b.iter_custom(|_iters| {
                let queue_init = |num_prods: usize| {
                    // YMC needs to know total number of threads
                    // num_producers + 1 consumer
                    let num_threads = num_prods + 1;
                    let bytes = YangCrummeyQueue::<usize>::shared_size(num_threads);
                    let shm_ptr = unsafe { map_shared(bytes) };
                    let q = unsafe { YangCrummeyQueue::init_in_shared(shm_ptr, num_threads) };

                    let wrapper = Box::leak(Box::new(YmcMpscWrapper {
                        queue: q,
                        num_producers: num_prods,
                    }));

                    (
                        wrapper as &'static dyn BenchMpscQueue<usize>,
                        shm_ptr,
                        bytes,
                    )
                };

                fork_and_run_mpsc(queue_init, num_producers, ITEMS_PER_PRODUCER_TARGET)
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
        bench_dqueue_native,
        bench_ymc_as_mpsc
}
criterion_main!(benches);
