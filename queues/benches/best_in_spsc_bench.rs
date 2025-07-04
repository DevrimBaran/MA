#![allow(clippy::cast_possible_truncation)]

use criterion::{criterion_group, criterion_main, Criterion};
use nix::{
    libc,
    sys::wait::waitpid,
    unistd::{fork, ForkResult},
};
use std::ptr;
use std::time::Duration;

// Import all the best-performing queue types
use queues::{
    spmc::{DavidQueue, EnqueuerState},
    DQueue, IffqQueue, SpscQueue, YangCrummeyQueue,
};
use std::sync::atomic::{AtomicU32, Ordering};

const PERFORMANCE_TEST: bool = false;
const RING_CAP: usize = 1024;
const ITERS: usize = 10_000;  // Reduced to match other benchmarks
const MAX_BENCH_SPIN_RETRY_ATTEMPTS: usize = 1_000_000_000;

// Helper trait for benchmarking different queue types in SPSC scenario
trait BenchSpscQueue<T: Send>: Send + Sync + 'static {
    fn bench_push(&self, item: T) -> Result<(), ()>;
    fn bench_pop(&self) -> Result<T, ()>;
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

// IffqQueue (SPSC) - native SPSC implementation
impl<T: Send + 'static> BenchSpscQueue<T> for IffqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_e| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_e| ())
    }
}

// DQueue (MPSC) - testing in SPSC scenario with single producer
impl<T: Send + Clone + 'static> BenchSpscQueue<T> for DQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        // Use producer_id 0 for single producer scenario
        self.enqueue(0, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        self.dequeue().ok_or(())
    }
}

// YangCrummeyQueue (MPMC) - testing in SPSC scenario
impl<T: Send + Clone + 'static> BenchSpscQueue<T> for YangCrummeyQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        // Use process_id 0 for single producer
        self.enqueue(0, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        // Use process_id 1 for single consumer (different from producer)
        self.dequeue(1)
    }
}

// DavidQueue (SPMC) wrapper for SPSC scenario
struct DavidQueueWrapper<T: Send + Clone + 'static> {
    queue: &'static DavidQueue<T>,
    enqueuer_state: *mut EnqueuerState,
}

unsafe impl<T: Send + Clone> Send for DavidQueueWrapper<T> {}
unsafe impl<T: Send + Clone> Sync for DavidQueueWrapper<T> {}

impl<T: Send + Clone + 'static> BenchSpscQueue<T> for DavidQueueWrapper<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        unsafe { self.queue.enqueue(&mut *self.enqueuer_state, item) }
    }
    fn bench_pop(&self) -> Result<T, ()> {
        // Use consumer_id 0 for single consumer
        self.queue.dequeue(0)
    }
}

fn bench_iffq_native(c: &mut Criterion) {
    c.bench_function("IffQ (Native SPSC)", |b| {
        b.iter_custom(|_iters| {
            assert!(RING_CAP.is_power_of_two());
            assert_eq!(
                RING_CAP % 32,
                0,
                "RING_CAP must be a multiple of IFFQ H_PARTITION_SIZE (32)"
            );
            assert!(
                RING_CAP >= 2 * 32,
                "RING_CAP must be >= 2 * IFFQ H_PARTITION_SIZE (64)"
            );

            let bytes = IffqQueue::<usize>::shared_size(RING_CAP);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { IffqQueue::init_in_shared(shm_ptr, RING_CAP) };

            let dur = fork_and_run(q);

            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

fn bench_dqueue_as_spsc(c: &mut Criterion) {
    c.bench_function("DQueue (MPSC as SPSC)", |b| {
        b.iter_custom(|_iters| {
            // DQueue needs space for segments
            let num_producers = 1; // Single producer for SPSC
            let segment_pool_capacity = 10; // Should be enough for ITERS items
            let bytes = DQueue::<usize>::shared_size(num_producers, segment_pool_capacity);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { DQueue::init_in_shared(shm_ptr, num_producers, segment_pool_capacity) };

            let dur = fork_and_run(q);

            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

fn bench_ymc_as_spsc(c: &mut Criterion) {
    c.bench_function("YMC (MPMC as SPSC)", |b| {
        b.iter_custom(|_iters| {
            // YangCrummeyQueue needs to know number of threads
            // In SPSC scenario: 1 producer + 1 consumer = 2 threads
            let num_threads = 2;
            let bytes = YangCrummeyQueue::<usize>::shared_size(num_threads);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { YangCrummeyQueue::init_in_shared(shm_ptr, num_threads) };

            let dur = fork_and_run(q);

            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

fn bench_david_as_spsc(c: &mut Criterion) {
    c.bench_function("David (SPMC as SPSC)", |b| {
        b.iter_custom(|_iters| {
            // DavidQueue setup with single consumer
            let num_consumers = 1;
            let bytes = DavidQueue::<usize>::shared_size(num_consumers);
            let shm_ptr = unsafe { map_shared(bytes) };

            // Create enqueuer state
            let enqueuer_state_bytes = std::mem::size_of::<EnqueuerState>();
            let enqueuer_state_ptr = unsafe { map_shared(enqueuer_state_bytes) };
            let enqueuer_state = enqueuer_state_ptr as *mut EnqueuerState;
            unsafe {
                ptr::write(enqueuer_state, EnqueuerState::new());
            }

            let q =
                unsafe { DavidQueue::init_in_shared(shm_ptr, num_consumers, &mut *enqueuer_state) };

            let wrapper = Box::leak(Box::new(DavidQueueWrapper {
                queue: q,
                enqueuer_state,
            }));

            let dur = fork_and_run(wrapper);

            unsafe {
                unmap_shared(shm_ptr, bytes);
                unmap_shared(enqueuer_state_ptr, enqueuer_state_bytes);
            }
            dur
        })
    });
}

fn fork_and_run<Q>(q: &'static Q) -> std::time::Duration
where
    Q: BenchSpscQueue<usize> + Sync,
{
    let page_size = 4096;
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
        panic!(
            "mmap for sync_shm failed: {}",
            std::io::Error::last_os_error()
        );
    }

    let sync_atomic_flag = unsafe { &*(sync_shm as *const AtomicU32) };
    sync_atomic_flag.store(0, Ordering::Relaxed);

    match unsafe { fork() }.expect("fork failed") {
        ForkResult::Child => {
            // Producer
            sync_atomic_flag.store(1, Ordering::Release);
            while sync_atomic_flag.load(Ordering::Acquire) < 2 {
                std::hint::spin_loop();
            }

            let mut push_attempts = 0;
            for i in 0..ITERS {
                while q.bench_push(i).is_err() {
                    push_attempts += 1;
                    if push_attempts > MAX_BENCH_SPIN_RETRY_ATTEMPTS {
                        panic!("Producer exceeded max spin retry attempts for push");
                    }
                    std::hint::spin_loop();
                }
            }

            sync_atomic_flag.store(3, Ordering::Release);
            unsafe { libc::_exit(0) };
        }
        ForkResult::Parent { child } => {
            // Consumer
            while sync_atomic_flag.load(Ordering::Acquire) < 1 {
                std::hint::spin_loop();
            }

            sync_atomic_flag.store(2, Ordering::Release);

            let start_time = std::time::Instant::now();
            let mut consumed_count = 0;
            let mut pop_spin_attempts = 0;

            while consumed_count < ITERS {
                let producer_done = sync_atomic_flag.load(Ordering::Acquire) == 3;

                if let Ok(_item) = q.bench_pop() {
                    consumed_count += 1;
                    pop_spin_attempts = 0;
                } else {
                    pop_spin_attempts += 1;

                    if pop_spin_attempts > MAX_BENCH_SPIN_RETRY_ATTEMPTS && !producer_done {
                        panic!(
                            "Consumer exceeded max spin retry attempts for pop (producer not done)"
                        );
                    }

                    std::hint::spin_loop();
                }
            }

            let duration = start_time.elapsed();

            // Wait for child to properly exit
            while sync_atomic_flag.load(Ordering::Acquire) != 3 {
                std::hint::spin_loop();
            }
            let _ = waitpid(child, None).expect("waitpid failed");

            unsafe {
                libc::munmap(sync_shm as *mut libc::c_void, page_size);
            }

            if !PERFORMANCE_TEST && consumed_count != ITERS {
                eprintln!(
                    "Warning: Parent consumed {}/{} items. Queue type: {}",
                    consumed_count,
                    ITERS,
                    std::any::type_name::<Q>()
                );
            }
            duration
        }
    }
}

// Criterion setup with same parameters as other benchmarks
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
        bench_dqueue_as_spsc,
        bench_iffq_native,
        bench_ymc_as_spsc,
        bench_david_as_spsc
}
criterion_main!(benches);
