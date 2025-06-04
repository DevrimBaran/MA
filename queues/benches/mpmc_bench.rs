use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use nix::{
    libc,
    sys::wait::waitpid,
    unistd::{fork, ForkResult},
};
use queues::mpmc::{self, polylog_queue, ymc_queue};
use queues::mpmc::{BurdenWFQueue, KWQueue, NRQueue, WFQueue, YangCrummeyQueue};
use queues::MpmcQueue;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

const PERFORMANCE_TEST: bool = false;
const ITEMS_PER_PROCESS_TARGET: usize = 250_000;
const PROCESS_COUNTS_TO_TEST: &[(usize, usize)] = &[(1, 1), (2, 2)];
const MAX_BENCH_SPIN_RETRY_ATTEMPTS: usize = 100_000_000;

trait BenchMpmcQueue<T: Send + Clone>: Send + Sync + 'static {
    fn bench_push(&self, item: T, process_id: usize) -> Result<(), ()>;
    fn bench_pop(&self, process_id: usize) -> Result<T, ()>;
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
    fn bench_push(&self, item: T, process_id: usize) -> Result<(), ()> {
        self.enqueue(process_id, item)
    }

    fn bench_pop(&self, process_id: usize) -> Result<T, ()> {
        self.dequeue(process_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        false
    }
}

impl<T: Send + Clone + 'static> BenchMpmcQueue<T> for KWQueue<T> {
    fn bench_push(&self, item: T, process_id: usize) -> Result<(), ()> {
        self.enqueue(process_id, item)
    }

    fn bench_pop(&self, process_id: usize) -> Result<T, ()> {
        self.dequeue(process_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        false
    }
}

impl<T: Send + Clone + 'static> BenchMpmcQueue<T> for WFQueue<T> {
    fn bench_push(&self, item: T, process_id: usize) -> Result<(), ()> {
        self.enqueue(process_id, item)
    }

    fn bench_pop(&self, process_id: usize) -> Result<T, ()> {
        self.dequeue(process_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + Clone + 'static> BenchMpmcQueue<T> for BurdenWFQueue<T> {
    fn bench_push(&self, item: T, process_id: usize) -> Result<(), ()> {
        self.enqueue(process_id, item)
    }

    fn bench_pop(&self, process_id: usize) -> Result<T, ()> {
        self.dequeue(process_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        false
    }
}

impl<T: Send + Clone + 'static> BenchMpmcQueue<T> for NRQueue<T> {
    fn bench_push(&self, item: T, process_id: usize) -> Result<(), ()> {
        self.enqueue(process_id, item)
    }

    fn bench_pop(&self, process_id: usize) -> Result<T, ()> {
        self.dequeue(process_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
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
            ptr::write(
                sync_ptr,
                Self {
                    producers_ready: AtomicU32::new(0),
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
struct MpmcDoneSync {
    producers_done: AtomicU32,
    consumers_done: AtomicU32,
    total_consumed: AtomicUsize,
    total_produced: AtomicUsize, // Add this to track actual production
    consumers_active: AtomicU32, // Track active consumers
}

impl MpmcDoneSync {
    fn new_in_shm(mem_ptr: *mut u8) -> &'static Self {
        let sync_ptr = mem_ptr as *mut Self;
        unsafe {
            ptr::write(
                sync_ptr,
                Self {
                    producers_done: AtomicU32::new(0),
                    consumers_done: AtomicU32::new(0),
                    total_consumed: AtomicUsize::new(0),
                    total_produced: AtomicUsize::new(0),
                    consumers_active: AtomicU32::new(0),
                },
            );
            &*sync_ptr
        }
    }

    fn shared_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

// Update fork_and_run_mpmc_with_helper function
fn fork_and_run_mpmc_with_helper<Q, F>(
    queue_init_fn: F,
    num_producers: usize,
    num_consumers: usize,
    items_per_process: usize,
    needs_helper: bool,
) -> Duration
where
    Q: BenchMpmcQueue<usize> + 'static,
    F: FnOnce() -> (&'static Q, *mut u8, usize),
{
    if num_producers == 0 || num_consumers == 0 {
        return Duration::from_nanos(1);
    }

    let total_items = num_producers * items_per_process;
    let total_processes = num_producers + num_consumers;

    let (q, q_shm_ptr, q_shm_size) = queue_init_fn();

    let startup_sync_size = MpmcStartupSync::shared_size();
    let startup_sync_shm_ptr = unsafe { map_shared(startup_sync_size) };
    let startup_sync = MpmcStartupSync::new_in_shm(startup_sync_shm_ptr);

    let done_sync_size = MpmcDoneSync::shared_size();
    let done_sync_shm_ptr = unsafe { map_shared(done_sync_size) };
    let done_sync = MpmcDoneSync::new_in_shm(done_sync_shm_ptr); // Pass total_items

    let mut producer_pids = Vec::with_capacity(num_producers);
    let mut consumer_pids = Vec::with_capacity(num_consumers);
    let mut helper_pid = None;

    // Fork helper process if needed - counts as a participant in startup sync
    if needs_helper {
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                // Helper process - pin to last core
                #[cfg(target_os = "linux")]
                unsafe {
                    use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
                    let mut set = std::mem::zeroed::<cpu_set_t>();
                    CPU_ZERO(&mut set);
                    CPU_SET(total_processes, &mut set); // Use core after all workers
                    sched_setaffinity(0, std::mem::size_of::<cpu_set_t>(), &set);
                }

                // Signal helper is ready
                startup_sync.producers_ready.fetch_add(1, Ordering::AcqRel);

                // Wait for go signal like other processes
                while !startup_sync.go_signal.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                // Run helper loop
                unsafe {
                    let wf_queue = &*(q as *const _ as *const WFQueue<usize>);
                    wf_queue.run_helper();
                }

                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                helper_pid = Some(child);
            }
            Err(e) => {
                unsafe {
                    if !q_shm_ptr.is_null() {
                        unmap_shared(q_shm_ptr, q_shm_size);
                    }
                    unmap_shared(startup_sync_shm_ptr, startup_sync_size);
                    unmap_shared(done_sync_shm_ptr, done_sync_size);
                }
                panic!("Fork failed for helper: {}", e);
            }
        }
    }

    // Fork producers (unchanged)
    for producer_id in 0..num_producers {
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                startup_sync.producers_ready.fetch_add(1, Ordering::AcqRel);

                while !startup_sync.go_signal.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                let mut push_attempts = 0;
                let mut produced = 0;
                for i in 0..items_per_process {
                    let item_value = producer_id * items_per_process + i;
                    let mut item_push_attempts = 0;
                    while q.bench_push(item_value, producer_id).is_err() {
                        item_push_attempts += 1;
                        push_attempts += 1;
                        if item_push_attempts > MAX_BENCH_SPIN_RETRY_ATTEMPTS {
                            eprintln!(
                                "Producer {} failed to push item {} after {} attempts",
                                producer_id, i, item_push_attempts
                            );
                            break;
                        }
                        std::hint::spin_loop();
                    }
                    if item_push_attempts <= MAX_BENCH_SPIN_RETRY_ATTEMPTS {
                        produced += 1;
                    }
                }

                // Report how many we actually produced
                done_sync
                    .total_produced
                    .fetch_add(produced, Ordering::AcqRel);
                done_sync.producers_done.fetch_add(1, Ordering::AcqRel);
                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                producer_pids.push(child);
            }
            Err(e) => {
                unsafe {
                    if !q_shm_ptr.is_null() {
                        unmap_shared(q_shm_ptr, q_shm_size);
                    }
                    unmap_shared(startup_sync_shm_ptr, startup_sync_size);
                    unmap_shared(done_sync_shm_ptr, done_sync_size);
                }
                panic!("Fork failed for producer {}: {}", producer_id, e);
            }
        }
    }

    // Fork consumers - UPDATED LOGIC
    for consumer_id in 0..num_consumers {
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                startup_sync.consumers_ready.fetch_add(1, Ordering::AcqRel);

                while !startup_sync.go_signal.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                // Mark this consumer as active
                done_sync.consumers_active.fetch_add(1, Ordering::AcqRel);

                let mut consumed_count = 0;
                let mut consecutive_empty_checks = 0;
                let mut total_empty_checks = 0;
                const MAX_CONSECUTIVE_EMPTY_CHECKS: usize = 50_000;
                const SPIN_BEFORE_YIELD: usize = 1000;

                loop {
                    match q.bench_pop(num_producers + consumer_id) {
                        Ok(_item) => {
                            consumed_count += 1;
                            done_sync.total_consumed.fetch_add(1, Ordering::AcqRel);
                            consecutive_empty_checks = 0;
                            total_empty_checks = 0;
                        }
                        Err(_) => {
                            consecutive_empty_checks += 1;
                            total_empty_checks += 1;

                            // Check current state
                            let current_consumed = done_sync.total_consumed.load(Ordering::Acquire);
                            let current_produced = done_sync.total_produced.load(Ordering::Acquire);
                            let producers_done = done_sync.producers_done.load(Ordering::Acquire)
                                == num_producers as u32;

                            // If all items that were produced have been consumed, we can exit
                            if producers_done
                                && current_produced > 0
                                && current_consumed >= current_produced
                            {
                                break;
                            }

                            // Give up if we've checked many times and producers are done
                            if producers_done
                                && consecutive_empty_checks > MAX_CONSECUTIVE_EMPTY_CHECKS
                            {
                                // Mark ourselves as inactive
                                let active =
                                    done_sync.consumers_active.fetch_sub(1, Ordering::AcqRel) - 1;

                                // Wait a bit to see if we're the last consumer
                                for _ in 0..100 {
                                    let final_consumed =
                                        done_sync.total_consumed.load(Ordering::Acquire);
                                    let final_produced =
                                        done_sync.total_produced.load(Ordering::Acquire);

                                    if final_produced > 0 && final_consumed >= final_produced {
                                        break;
                                    }

                                    if active == 0 {
                                        // We're the last consumer, do a final check
                                        break;
                                    }

                                    std::thread::yield_now();
                                }

                                // Final attempt
                                match q.bench_pop(num_producers + consumer_id) {
                                    Ok(_item) => {
                                        consumed_count += 1;
                                        done_sync.total_consumed.fetch_add(1, Ordering::AcqRel);
                                        // Reactivate ourselves
                                        done_sync.consumers_active.fetch_add(1, Ordering::AcqRel);
                                        consecutive_empty_checks = 0;
                                        continue;
                                    }
                                    Err(_) => break,
                                }
                            }

                            // Yield periodically
                            if consecutive_empty_checks % SPIN_BEFORE_YIELD == 0 {
                                std::thread::yield_now();
                            } else {
                                std::hint::spin_loop();
                            }
                        }
                    }
                }

                done_sync.consumers_done.fetch_add(1, Ordering::AcqRel);
                unsafe { libc::_exit(0) };
            }
            Ok(ForkResult::Parent { child }) => {
                consumer_pids.push(child);
            }
            Err(e) => {
                unsafe {
                    if !q_shm_ptr.is_null() {
                        unmap_shared(q_shm_ptr, q_shm_size);
                    }
                    unmap_shared(startup_sync_shm_ptr, startup_sync_size);
                    unmap_shared(done_sync_shm_ptr, done_sync_size);
                }
                panic!("Fork failed for consumer {}: {}", consumer_id, e);
            }
        }
    }

    // Wait for all workers AND helper to be ready
    let expected_producers = if needs_helper {
        num_producers as u32 + 1 // +1 for helper counted as producer
    } else {
        num_producers as u32
    };

    while startup_sync.producers_ready.load(Ordering::Acquire) < expected_producers
        || startup_sync.consumers_ready.load(Ordering::Acquire) < num_consumers as u32
    {
        std::hint::spin_loop();
    }

    // START TIMING HERE - all processes are ready
    let start_time = std::time::Instant::now();
    startup_sync.go_signal.store(true, Ordering::Release);

    // Wait for producers and consumers
    for pid in producer_pids {
        waitpid(pid, None).expect("waitpid for producer failed");
    }

    for pid in consumer_pids {
        waitpid(pid, None).expect("waitpid for consumer failed");
    }

    let duration = start_time.elapsed();

    // Wait for helper if it exists
    if let Some(helper_pid) = helper_pid {
        unsafe {
            let wf_queue = &*(q as *const _ as *const WFQueue<usize>);
            wf_queue.stop_helper();
        }
        waitpid(helper_pid, None).expect("waitpid for helper failed");
    }

    // Get the total consumed count
    let total_consumed = done_sync.total_consumed.load(Ordering::Acquire);
    let total_produced = done_sync.total_produced.load(Ordering::Acquire);

    if total_consumed != total_produced || total_produced != total_items {
        eprintln!(
            "Warning (MPMC): Produced {}/{} items, consumed {}/{} items. Q: {}, Prods: {}, Cons: {}",
            total_produced,
            total_items,
            total_consumed,
            total_produced,
            std::any::type_name::<Q>(),
            num_producers,
            num_consumers
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

// Add benchmark function
fn bench_wf_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("VermaMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;
        let total_processes = num_prods + num_cons;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<WFQueue<usize>, _>(
                        || {
                            let bytes = WFQueue::<usize>::shared_size(total_processes);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe { WFQueue::init_in_shared(shm_ptr, total_processes) };
                            (q, shm_ptr, bytes)
                        },
                        num_prods,
                        num_cons,
                        items_per_process,
                        true, // needs_helper = true
                    )
                })
            },
        );
    }

    group.finish();
}

// Update the other benchmarks to use the new function
fn bench_yang_crummey(c: &mut Criterion) {
    let mut group = c.benchmark_group("YangCrummeyMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;
        let total_processes = num_prods + num_cons;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<YangCrummeyQueue<usize>, _>(
                        || {
                            let bytes = YangCrummeyQueue::<usize>::shared_size(total_processes);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe {
                                YangCrummeyQueue::init_in_shared(shm_ptr, total_processes)
                            };
                            (q, shm_ptr, bytes)
                        },
                        num_prods,
                        num_cons,
                        items_per_process,
                        false, // needs_helper = false
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_kw_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("KhanchandaniWattenhoferMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;
        let total_processes = num_prods + num_cons;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<KWQueue<usize>, _>(
                        || {
                            let bytes = KWQueue::<usize>::shared_size(total_processes);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe { KWQueue::init_in_shared(shm_ptr, total_processes) };
                            (q, shm_ptr, bytes)
                        },
                        num_prods,
                        num_cons,
                        items_per_process,
                        false, // needs_helper = false
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_burden_wf_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("BurdenMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;
        let total_processes = num_prods + num_cons;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<BurdenWFQueue<usize>, _>(
                        || {
                            let bytes = BurdenWFQueue::<usize>::shared_size(total_processes);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q =
                                unsafe { BurdenWFQueue::init_in_shared(shm_ptr, total_processes) };
                            (q, shm_ptr, bytes)
                        },
                        num_prods,
                        num_cons,
                        items_per_process,
                        false, // needs_helper = false (no helper process needed)
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_nr_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("NaderibeniRuppertMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;
        let total_processes = num_prods + num_cons;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<NRQueue<usize>, _>(
                        || {
                            let bytes = NRQueue::<usize>::shared_size(total_processes * 2);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q =
                                unsafe { NRQueue::init_in_shared(shm_ptr, total_processes * 2) };
                            (q, shm_ptr, bytes)
                        },
                        num_prods,
                        num_cons,
                        items_per_process,
                        false, // needs_helper = false
                    )
                })
            },
        );
    }

    group.finish();
}

fn custom_criterion() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(10))
        .sample_size(10)
}

criterion_group! {
    name = benches;
    config = custom_criterion();
    targets =
        bench_yang_crummey,
        bench_kw_queue,
        bench_burden_wf_queue,
        bench_nr_queue,
}

criterion_main!(benches);
