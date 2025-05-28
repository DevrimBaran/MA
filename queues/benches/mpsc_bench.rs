// spinloops are just there so that producer and consumer can start at the same time and handling temporary empty/full queues
// Since the algorithms are wait-free, the spinloops will not affect the wait-free synchronization between producer and consumer
#![allow(clippy::cast_possible_truncation)]

use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use queues::mpsc::{DQueue, DrescherQueue, JayantiPetrovicMpscQueue, JiffyQueue};
use queues::MpscQueue;

use core::fmt;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use nix::{
    libc,
    sys::wait::waitpid,
    unistd::{fork, ForkResult},
};
use queues::mpsc::dqueue::N_SEGMENT_CAPACITY;

const PERFORMANCE_TEST: bool = true;
const ITEMS_PER_PRODUCER_TARGET: usize = 3_000_000;
const JIFFY_NODES_PER_BUFFER_BENCH: usize = 8192;
const PRODUCER_COUNTS_TO_TEST: &[usize] = &[1, 2, 4, 8, 14];
const MAX_BENCH_SPIN_RETRY_ATTEMPTS: usize = 100_000_000;

trait BenchMpscQueue<T: Send>: Send + Sync + 'static {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()>;
    fn bench_pop(&self) -> Result<T, ()>;
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

// Implementations for existing queues remain here
impl<T: Send + 'static + std::fmt::Debug> BenchMpscQueue<T> for DrescherQueue<T> {
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

impl<T: Send + Clone + 'static> BenchMpscQueue<T> for JayantiPetrovicMpscQueue<T> {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
        self.enqueue(producer_id, item).map_err(|_| ())
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

impl<T: Send + 'static + Clone + fmt::Debug> BenchMpscQueue<T> for JiffyQueue<T> {
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

impl<T: Send + Clone + 'static> BenchMpscQueue<T> for DQueue<T> {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
        self.enqueue(producer_id, item)
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
    items_per_producer_arg: usize,
) -> std::time::Duration
where
    Q: BenchMpscQueue<usize> + 'static,
    F: FnOnce() -> (&'static Q, *mut u8, usize),
{
    if num_producers == 0 {
        return Duration::from_nanos(1);
    }
    let (q, q_shm_ptr, q_shm_size) = queue_init_fn();
    let total_items_to_produce = num_producers * items_per_producer_arg;

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
                for i in 0..items_per_producer_arg {
                    let item_value = producer_id * items_per_producer_arg + i;
                    while q.bench_push(item_value, producer_id).is_err() {
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
            Err(e) => {
                // Clean up shared memory before panicking
                unsafe {
                    if !q_shm_ptr.is_null() {
                        unmap_shared(q_shm_ptr, q_shm_size);
                    }
                    unmap_shared(startup_sync_shm_ptr, startup_sync_size);
                    unmap_shared(done_sync_shm_ptr, done_sync_size);
                }
                panic!("Fork failed for MPSC producer {}: {}", producer_id, e);
            }
        }
    }

    while startup_sync.producers_ready_count.load(Ordering::Acquire) < num_producers as u32 {
        std::hint::spin_loop();
    }
    startup_sync.go_signal.store(true, Ordering::Release);
    let start_time = std::time::Instant::now();
    let mut consumed_count = 0;

    if total_items_to_produce > 0 {
        let mut pop_attempts_outer = 0;
        loop {
            // Main consumption loop
            if q.bench_pop().is_ok() {
                consumed_count += 1;
                pop_attempts_outer = 0; // Reset attempts on successful pop
            } else {
                pop_attempts_outer += 1;
                if pop_attempts_outer > MAX_BENCH_SPIN_RETRY_ATTEMPTS
                    && !(done_sync.producers_done_count.load(Ordering::Acquire)
                        == num_producers as u32)
                {
                    panic!(
                        "Consumer exceeded max spin retry attempts for pop (producers not done)"
                    );
                }
                // Pop returned None or Error
                let producers_done =
                    done_sync.producers_done_count.load(Ordering::Acquire) == num_producers as u32;
                if producers_done {
                    // Producers are done, try a final drain aggressively
                    let mut final_drain_attempts = 0;
                    const MAX_FINAL_DRAIN_ATTEMPTS: usize = 1_000; // Can be tuned

                    while consumed_count < total_items_to_produce
                        && final_drain_attempts < MAX_FINAL_DRAIN_ATTEMPTS
                    {
                        if q.bench_pop().is_ok() {
                            consumed_count += 1;
                            if consumed_count >= total_items_to_produce {
                                break;
                            }
                        } else {
                            final_drain_attempts += 1;
                            std::thread::yield_now(); // Give queue time if it was a near-miss
                        }
                    }
                    // After aggressive drain attempts, if not all items consumed,
                    // try one last pop. If it fails and queue is empty, then break.
                    if consumed_count < total_items_to_produce {
                        if q.bench_pop().is_err() && q.bench_is_empty() {
                            break;
                        } else if q.bench_pop().is_ok() {
                            consumed_count += 1;
                        }
                    }
                }
                std::thread::yield_now();
            }
            if consumed_count >= total_items_to_produce {
                break;
            }
        }
    }
    let duration = start_time.elapsed();

    for pid in producer_pids {
        waitpid(pid, None).expect("waitpid for MPSC producer failed");
    }

    if (!PERFORMANCE_TEST && consumed_count != total_items_to_produce) {
        eprintln!(
            "Warning (MPSC): Consumed {}/{} items. Q: {}, Prods: {}",
            consumed_count,
            total_items_to_produce,
            std::any::type_name::<Q>(),
            num_producers
        );
    }

    unsafe {
        if !q_shm_ptr.is_null() {
            unmap_shared(q_shm_ptr, q_shm_size);
        }
        unmap_shared(startup_sync_shm_ptr, startup_sync_size);
        unmap_shared(done_sync_shm_ptr, done_sync_size);
    }
    duration
}

// Benchmark Functions
fn bench_drescher_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("DrescherMPSC");
    for &num_prods_current_run in PRODUCER_COUNTS_TO_TEST.iter().filter(|&&p| p > 0) {
        let items_per_prod = ITEMS_PER_PRODUCER_TARGET;
        let total_items_run = num_prods_current_run * items_per_prod;
        group.bench_function(
            format!("{}Prod_{}ItemsPer", num_prods_current_run, items_per_prod),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpsc::<DrescherQueue<usize>, _>(
                        || {
                            let node_cap = total_items_run + num_prods_current_run; // Extra nodes for producers
                            let bytes = DrescherQueue::<usize>::shared_size(node_cap);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe { DrescherQueue::init_in_shared(shm_ptr, node_cap) };
                            (q, shm_ptr, bytes)
                        },
                        num_prods_current_run,
                        items_per_prod,
                    )
                })
            },
        );
    }
    group.finish();
}

fn bench_jayanti_petrovic_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("JayantiPetrovicMPSC");
    for &num_prods_current_run in PRODUCER_COUNTS_TO_TEST.iter().filter(|&&p| p > 0) {
        let items_per_prod = ITEMS_PER_PRODUCER_TARGET;
        let total_items_run = num_prods_current_run * items_per_prod;
        let node_pool_capacity = total_items_run + num_prods_current_run * 2; // Adjusted pool capacity
        group.bench_function(
            format!("{}Prod_{}ItemsPer", num_prods_current_run, items_per_prod),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpsc::<JayantiPetrovicMpscQueue<usize>, _>(
                        || {
                            let bytes = JayantiPetrovicMpscQueue::<usize>::shared_size(
                                num_prods_current_run,
                                node_pool_capacity,
                            );
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe {
                                JayantiPetrovicMpscQueue::init_in_shared(
                                    shm_ptr,
                                    num_prods_current_run,
                                    node_pool_capacity,
                                )
                            };
                            (q, shm_ptr, bytes)
                        },
                        num_prods_current_run,
                        items_per_prod,
                    )
                })
            },
        );
    }
    group.finish();
}

fn bench_jiffy_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("JiffyMPSC");
    for &num_prods_current_run in PRODUCER_COUNTS_TO_TEST.iter().filter(|&&p| p > 0) {
        let items_per_prod = ITEMS_PER_PRODUCER_TARGET;
        let total_items_run = num_prods_current_run * items_per_prod;
        let jiffy_max_buffers = if total_items_run > 0 && JIFFY_NODES_PER_BUFFER_BENCH > 0 {
            (total_items_run / JIFFY_NODES_PER_BUFFER_BENCH) + num_prods_current_run + 20
        } else {
            num_prods_current_run + 20
        }
        .max(1);
        group.bench_function(
            format!("{}Prod_{}ItemsPer", num_prods_current_run, items_per_prod),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpsc::<JiffyQueue<usize>, _>(
                        || {
                            let bytes = JiffyQueue::<usize>::shared_size(
                                JIFFY_NODES_PER_BUFFER_BENCH,
                                jiffy_max_buffers,
                            );
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe {
                                JiffyQueue::init_in_shared(
                                    shm_ptr,
                                    JIFFY_NODES_PER_BUFFER_BENCH,
                                    jiffy_max_buffers,
                                )
                            };
                            (q, shm_ptr, bytes)
                        },
                        num_prods_current_run,
                        items_per_prod,
                    )
                })
            },
        );
    }
    group.finish();
}

fn bench_d_queue_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("DQueueMPSC");
    for &num_prods_current_run in PRODUCER_COUNTS_TO_TEST.iter().filter(|&&p| p > 0) {
        let items_per_prod = ITEMS_PER_PRODUCER_TARGET;
        let total_items_run = num_prods_current_run * items_per_prod;

        let n_segment_capacity = N_SEGMENT_CAPACITY;

        let dqueue_segment_pool_cap = if total_items_run > 0 && n_segment_capacity > 0 {
            (total_items_run / n_segment_capacity) + num_prods_current_run + 50
        } else {
            num_prods_current_run + 50
        }
        .max(1);

        group.bench_function(
            format!("{}Prod_{}ItemsPer", num_prods_current_run, items_per_prod),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpsc::<DQueue<usize>, _>(
                        || {
                            let bytes = DQueue::<usize>::shared_size(
                                num_prods_current_run,
                                dqueue_segment_pool_cap,
                            );
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe {
                                DQueue::init_in_shared(
                                    shm_ptr,
                                    num_prods_current_run,
                                    dqueue_segment_pool_cap,
                                )
                            };
                            (q, shm_ptr, bytes)
                        },
                        num_prods_current_run,
                        items_per_prod,
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
        .measurement_time(Duration::from_secs(60))
        .sample_size(10)
}

criterion_group! {
    name = mpsc_benches;
    config = custom_criterion();
    targets =
        bench_drescher_mpsc,
        bench_jayanti_petrovic_mpsc,
        bench_jiffy_mpsc,
        bench_d_queue_mpsc,
}
criterion_main!(mpsc_benches);
