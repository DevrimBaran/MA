use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use nix::{
    libc,
    sys::wait::waitpid,
    unistd::{fork, ForkResult},
};
use queues::spmc::{DavidQueue, EnqueuerState};
use queues::SpmcQueue;
use std::ptr;
use std::sync::atomic::{fence, AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

const PERFORMANCE_TEST: bool = false;
const ITEMS_PER_PRODUCER_TARGET: usize = 5_000;
const CONSUMER_COUNTS_TO_TEST: &[usize] = &[1, 2, 4, 8, 14];
const MAX_BENCH_SPIN_RETRY_ATTEMPTS: usize = 100_000_000;

trait BenchSpmcQueue<T: Send + Clone>: Send + Sync + 'static {
    fn bench_push(&self, item: T) -> Result<(), ()>;
    fn bench_pop(&self, consumer_id: usize) -> Result<T, ()>;
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
struct SpmcDoneSync {
    producer_done: AtomicBool,
    consumers_done: AtomicU32,
    total_consumed: AtomicUsize,
    total_produced: AtomicUsize,
}

impl SpmcDoneSync {
    fn new_in_shm(mem_ptr: *mut u8) -> &'static Self {
        let sync_ptr = mem_ptr as *mut Self;
        unsafe {
            ptr::write(
                sync_ptr,
                Self {
                    producer_done: AtomicBool::new(false),
                    consumers_done: AtomicU32::new(0),
                    total_consumed: AtomicUsize::new(0),
                    total_produced: AtomicUsize::new(0),
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
    items_to_produce: usize,
) -> Duration
where
    Q: BenchSpmcQueue<usize> + 'static,
    F: FnOnce() -> (&'static Q, *mut u8, usize),
{
    if num_consumers == 0 {
        return Duration::from_nanos(1);
    }

    let (q, q_shm_ptr, q_shm_size) = queue_init_fn();

    let startup_sync_size = SpmcStartupSync::shared_size();
    let startup_sync_shm_ptr = unsafe { map_shared(startup_sync_size) };
    let startup_sync = SpmcStartupSync::new_in_shm(startup_sync_shm_ptr);

    let done_sync_size = SpmcDoneSync::shared_size();
    let done_sync_shm_ptr = unsafe { map_shared(done_sync_size) };
    let done_sync = SpmcDoneSync::new_in_shm(done_sync_shm_ptr);

    let mut consumer_pids = Vec::with_capacity(num_consumers);

    let producer_pid = match unsafe { fork() } {
        Ok(ForkResult::Child) => {
            #[cfg(target_os = "linux")]
            unsafe {
                use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
                let mut set = std::mem::zeroed::<cpu_set_t>();
                CPU_ZERO(&mut set);
                CPU_SET(0, &mut set);
                sched_setaffinity(0, std::mem::size_of::<cpu_set_t>(), &set);
            }

            startup_sync.producer_ready.store(true, Ordering::Release);

            while !startup_sync.go_signal.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            let mut push_attempts = 0;
            let mut push_failures = 0;
            for i in 0..items_to_produce {
                let mut retry_count = 0;
                loop {
                    match q.bench_push(i) {
                        Ok(_) => break,
                        Err(_) => {
                            retry_count += 1;
                            push_attempts += 1;
                            if retry_count > 100 {
                                push_failures += 1;
                                eprintln!("Producer: Failed to push item {} after 100 retries", i);
                                break;
                            }
                            if push_attempts > MAX_BENCH_SPIN_RETRY_ATTEMPTS {
                                panic!("Producer exceeded max spin retry attempts");
                            }
                            std::hint::spin_loop();
                        }
                    }
                }
            }

            if push_failures > 0 {
                eprintln!("Producer: Failed to push {} items", push_failures);
            }

            done_sync
                .total_produced
                .store(items_to_produce, Ordering::Release);
            done_sync.producer_done.store(true, Ordering::Release);
            unsafe { libc::_exit(0) };
        }
        Ok(ForkResult::Parent { child }) => child,
        Err(e) => {
            unsafe {
                if !q_shm_ptr.is_null() {
                    unmap_shared(q_shm_ptr, q_shm_size);
                }
                unmap_shared(startup_sync_shm_ptr, startup_sync_size);
                unmap_shared(done_sync_shm_ptr, done_sync_size);
            }
            panic!("Fork failed for producer: {}", e);
        }
    };

    for consumer_id in 0..num_consumers {
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                #[cfg(target_os = "linux")]
                unsafe {
                    use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
                    let mut set = std::mem::zeroed::<cpu_set_t>();
                    CPU_ZERO(&mut set);
                    CPU_SET(consumer_id + 1, &mut set);
                    sched_setaffinity(0, std::mem::size_of::<cpu_set_t>(), &set);
                }

                startup_sync.consumers_ready.fetch_add(1, Ordering::AcqRel);

                while !startup_sync.go_signal.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                let mut consumed_count = 0;
                let target_items = items_to_produce / num_consumers;
                let extra_items = if consumer_id < (items_to_produce % num_consumers) {
                    1
                } else {
                    0
                };
                let my_target = target_items + extra_items;

                let mut consecutive_empty = 0;
                const MAX_CONSECUTIVE_EMPTY: usize = 100_000;

                while consumed_count < my_target {
                    match q.bench_pop(consumer_id) {
                        Ok(_item) => {
                            consumed_count += 1;
                            consecutive_empty = 0;
                        }
                        Err(_) => {
                            consecutive_empty += 1;

                            if done_sync.producer_done.load(Ordering::Acquire) {
                                if consecutive_empty > MAX_CONSECUTIVE_EMPTY {
                                    fence(Ordering::SeqCst);
                                    std::thread::sleep(std::time::Duration::from_millis(1));

                                    let mut final_found = 0;
                                    for _ in 0..100 {
                                        match q.bench_pop(consumer_id) {
                                            Ok(_) => {
                                                consumed_count += 1;
                                                final_found += 1;
                                            }
                                            Err(_) => {
                                                if final_found > 0 {
                                                    final_found = 0;
                                                } else {
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                    if final_found == 0 {
                                        break;
                                    }
                                    consecutive_empty = 0;
                                }
                            }

                            if consecutive_empty < 100 {
                                std::hint::spin_loop();
                            } else if consecutive_empty < 1000 {
                                std::thread::yield_now();
                            } else {
                                std::thread::sleep(std::time::Duration::from_micros(1));
                            }
                        }
                    }
                }

                done_sync
                    .total_consumed
                    .fetch_add(consumed_count, Ordering::AcqRel);
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

    while !startup_sync.producer_ready.load(Ordering::Acquire)
        || startup_sync.consumers_ready.load(Ordering::Acquire) < num_consumers as u32
    {
        std::hint::spin_loop();
    }

    let start_time = std::time::Instant::now();
    startup_sync.go_signal.store(true, Ordering::Release);

    waitpid(producer_pid, None).expect("waitpid for producer failed");

    for pid in consumer_pids {
        waitpid(pid, None).expect("waitpid for consumer failed");
    }

    let duration = start_time.elapsed();

    let total_consumed = done_sync.total_consumed.load(Ordering::Acquire);

    if total_consumed != items_to_produce {
        std::thread::sleep(Duration::from_millis(10));
        let final_consumed = done_sync.total_consumed.load(Ordering::Acquire);

        if final_consumed != items_to_produce {
            let loss_rate =
                ((items_to_produce - final_consumed) as f64 / items_to_produce as f64) * 100.0;
            eprintln!(
                "Warning (SPMC): Consumed {}/{} items. Loss rate: {:.2}%. Consumers: {}",
                final_consumed, items_to_produce, loss_rate, num_consumers
            );
        }
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

fn bench_david_spmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("DavidSPMC");

    for &num_consumers in CONSUMER_COUNTS_TO_TEST {
        let items_to_produce = ITEMS_PER_PRODUCER_TARGET * num_consumers;

        group.bench_function(format!("1P_{}C", num_consumers), |b: &mut Bencher| {
            b.iter_custom(|_iters| {
                fork_and_run_spmc::<DavidQueueWrapper<usize>, _>(
                    || {
                        let bytes = DavidQueue::<usize>::shared_size(num_consumers);
                        let shm_ptr = unsafe { map_shared(bytes) };

                        let state_size = std::mem::size_of::<EnqueuerState>();
                        let state_ptr = unsafe { map_shared(state_size) };
                        let enqueuer_state = state_ptr as *mut EnqueuerState;

                        unsafe {
                            ptr::write(enqueuer_state, EnqueuerState::new());
                            let q = DavidQueue::init_in_shared(
                                shm_ptr,
                                num_consumers,
                                &mut *enqueuer_state,
                            );

                            let wrapper = Box::leak(Box::new(DavidQueueWrapper {
                                queue: q,
                                enqueuer_state,
                            }));

                            (wrapper, shm_ptr, bytes)
                        }
                    },
                    num_consumers,
                    items_to_produce,
                )
            })
        });
    }

    group.finish();
}

fn custom_criterion() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(1000))
        .sample_size(500)
}

criterion_group! {
    name = benches;
    config = custom_criterion();
    targets = bench_david_spmc
}

criterion_main!(benches);
