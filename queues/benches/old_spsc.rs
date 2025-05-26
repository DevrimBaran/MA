// benchmarking process-based SPSC queues using criterion
// spinloops are just there so that producer and consumer can start at the same time and handling temporary empty/full queues
// Since the algorithms are wait-free, the spinloops will not affect the wait-free synchronization between producer and consumer
#![allow(clippy::cast_possible_truncation)]

use criterion::{criterion_group, criterion_main, Criterion};
use nix::{
    libc,
    sys::wait::waitpid,
    unistd::{fork, ForkResult},
};
use std::ptr;
use std::time::Duration;

// Import all necessary SPSC queue types and the main SpscQueue trait
use queues::{
    BQueue, BiffqQueue, BlqQueue, DehnaviQueue, DynListQueue, FfqQueue, IffqQueue, LamportQueue,
    MultiPushQueue, SesdJpSpscBenchWrapper, SpscQueue, UnboundedQueue,
};

use std::sync::atomic::{AtomicU32, Ordering};

use queues::spsc::blq::K_CACHE_LINE_SLOTS as BLQ_K_SLOTS;
use queues::spsc::llq::{LlqQueue, K_CACHE_LINE_SLOTS};

const PERFORMANCE_TEST: bool = false;
const RING_CAP_GENERAL: usize = 65536;
const ITERS_GENERAL: usize = 40_000_000;

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

// BenchSpscQueue Implementations
impl<T: Send + 'static> BenchSpscQueue<T> for DehnaviQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_| ())
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + 'static> BenchSpscQueue<T> for LamportQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self)
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + 'static> BenchSpscQueue<T> for BQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self)
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + 'static> BenchSpscQueue<T> for MultiPushQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_| ())
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + 'static> BenchSpscQueue<T> for UnboundedQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self)
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + 'static> BenchSpscQueue<T> for DynListQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self)
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + 'static> BenchSpscQueue<T> for IffqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_e| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_e| ())
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + 'static> BenchSpscQueue<T> for BiffqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_e| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_e| ())
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + 'static> BenchSpscQueue<T> for FfqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_e| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_e| ())
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + 'static> BenchSpscQueue<T> for LlqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_| ())
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + 'static> BenchSpscQueue<T> for BlqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_| ())
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}
impl<T: Send + Clone + 'static> BenchSpscQueue<T> for SesdJpSpscBenchWrapper<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_| ())
    }
    fn bench_is_empty(&self) -> bool {
        SpscQueue::empty(self)
    }
    fn bench_is_full(&self) -> bool {
        !SpscQueue::available(self)
    }
}

// Benchmark Functions
fn bench_dehnavi(c: &mut Criterion) {
    c.bench_function("Dehnavi", |b| {
        b.iter_custom(|_iters_arg_ignored| {
            // Dehnavi is a lossy queue, it will overwrite the beginning of the queue if consumer not fast enough.
            let dehnavi_k_param = if ITERS_GENERAL == 0 {
                2
            } else {
                ITERS_GENERAL + 1
            };
            let current_ring_cap_param = dehnavi_k_param.max(2);
            let bytes = DehnaviQueue::<usize>::shared_size(current_ring_cap_param);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { DehnaviQueue::init_in_shared(shm_ptr, current_ring_cap_param) };
            let dur = fork_and_run(q, ITERS_GENERAL);
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
            let bytes = LamportQueue::<usize>::shared_size(RING_CAP_GENERAL);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { LamportQueue::init_in_shared(shm_ptr, RING_CAP_GENERAL) };
            let dur = fork_and_run(q, ITERS_GENERAL);
            unsafe { unmap_shared(shm_ptr, bytes) };
            dur
        })
    });
}

fn bench_bqueue(c: &mut Criterion) {
    c.bench_function("B-Queue", |b| {
        b.iter_custom(|_iters| {
            let bytes = BQueue::<usize>::shared_size(RING_CAP_GENERAL);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { BQueue::init_in_shared(shm_ptr, RING_CAP_GENERAL) };
            let dur = fork_and_run(q, ITERS_GENERAL);
            unsafe { unmap_shared(shm_ptr, bytes) };
            dur
        })
    });
}

fn bench_mp(c: &mut Criterion) {
    c.bench_function("mSPSC", |b| {
        b.iter_custom(|_iters| {
            let bytes = MultiPushQueue::<usize>::shared_size(RING_CAP_GENERAL);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { MultiPushQueue::init_in_shared(shm_ptr, RING_CAP_GENERAL) };
            let dur = fork_and_run(q, ITERS_GENERAL);
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
            let bytes = DynListQueue::<usize>::shared_size(RING_CAP_GENERAL);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { DynListQueue::init_in_shared(shm_ptr, RING_CAP_GENERAL) };
            let dur = fork_and_run(q, ITERS_GENERAL);
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
            let size = UnboundedQueue::<usize>::shared_size(RING_CAP_GENERAL);
            let shm_ptr = unsafe { map_shared(size) };
            let q = unsafe { UnboundedQueue::init_in_shared(shm_ptr, RING_CAP_GENERAL) };
            let dur = fork_and_run(q, ITERS_GENERAL);
            unsafe {
                unmap_shared(shm_ptr, size);
            }
            dur
        })
    });
}

fn bench_iffq(c: &mut Criterion) {
    c.bench_function("Iffq", |b| {
        b.iter_custom(|_iters| {
            assert!(RING_CAP_GENERAL.is_power_of_two());
            assert_eq!(RING_CAP_GENERAL % 32, 0);
            assert!(RING_CAP_GENERAL >= 2 * 32);
            let bytes = IffqQueue::<usize>::shared_size(RING_CAP_GENERAL);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { IffqQueue::init_in_shared(shm_ptr, RING_CAP_GENERAL) };
            let dur = fork_and_run(q, ITERS_GENERAL);
            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

fn bench_biffq(c: &mut Criterion) {
    c.bench_function("Biffq", |b| {
        b.iter_custom(|_iters| {
            assert!(RING_CAP_GENERAL.is_power_of_two());
            assert_eq!(RING_CAP_GENERAL % 32, 0);
            assert!(RING_CAP_GENERAL >= 2 * 32);
            let bytes = BiffqQueue::<usize>::shared_size(RING_CAP_GENERAL);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { BiffqQueue::init_in_shared(shm_ptr, RING_CAP_GENERAL) };
            let dur = fork_and_run(q, ITERS_GENERAL);
            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

fn bench_ffq(c: &mut Criterion) {
    c.bench_function("FFq", |b| {
        b.iter_custom(|_iters| {
            assert!(RING_CAP_GENERAL.is_power_of_two() && RING_CAP_GENERAL > 0);
            let bytes = FfqQueue::<usize>::shared_size(RING_CAP_GENERAL);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { FfqQueue::init_in_shared(shm_ptr, RING_CAP_GENERAL) };
            let dur = fork_and_run(q, ITERS_GENERAL);
            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

fn bench_llq(c: &mut Criterion) {
    c.bench_function("Llq", |b| {
        b.iter_custom(|_iters| {
            let current_ring_cap = if RING_CAP_GENERAL <= K_CACHE_LINE_SLOTS {
                let min_valid_cap = (K_CACHE_LINE_SLOTS + 1).next_power_of_two();
                if min_valid_cap < 16 {
                    16
                } else {
                    min_valid_cap
                }
            } else {
                RING_CAP_GENERAL.next_power_of_two()
            };

            assert!(current_ring_cap.is_power_of_two());
            assert!(current_ring_cap > K_CACHE_LINE_SLOTS);

            let bytes = LlqQueue::<usize>::llq_shared_size(current_ring_cap);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q_static = unsafe { LlqQueue::<usize>::init_in_shared(shm_ptr, current_ring_cap) };

            let dur = fork_and_run(q_static, ITERS_GENERAL);

            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

fn bench_blq(c: &mut Criterion) {
    c.bench_function("Blq", |b| {
        b.iter_custom(|_iters| {
            let current_ring_cap = if RING_CAP_GENERAL <= BLQ_K_SLOTS {
                let mut min_valid_cap = (BLQ_K_SLOTS + 1).next_power_of_two();
                if min_valid_cap <= BLQ_K_SLOTS {
                    min_valid_cap = (BLQ_K_SLOTS + 1).next_power_of_two();
                    if min_valid_cap == 0 {
                        min_valid_cap = 1 << (BLQ_K_SLOTS.leading_zeros() as usize + 1);
                    }
                }
                if min_valid_cap < 16 {
                    16
                } else {
                    min_valid_cap
                }
            } else {
                RING_CAP_GENERAL.next_power_of_two()
            };

            assert!(current_ring_cap.is_power_of_two());
            assert!(current_ring_cap > BLQ_K_SLOTS);

            let bytes = BlqQueue::<usize>::shared_size(current_ring_cap);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q_static = unsafe { BlqQueue::<usize>::init_in_shared(shm_ptr, current_ring_cap) };

            let dur = fork_and_run(q_static, ITERS_GENERAL);

            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

fn bench_sesd_jp(c: &mut Criterion) {
    c.bench_function("SesdJpSPSC", |b| {
        b.iter_custom(|_iters_arg_ignored| {
            let pool_capacity = ITERS_GENERAL + 1000; // Extra buffer for safety

            let bytes = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q_shared: &'static SesdJpSpscBenchWrapper<usize> =
                unsafe { SesdJpSpscBenchWrapper::init_in_shared(shm_ptr, pool_capacity) };

            let dur = fork_and_run(q_shared, ITERS_GENERAL);

            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

// Generic fork-and-run helper
fn fork_and_run<Q>(q: &'static Q, iterations: usize) -> std::time::Duration
where
    Q: BenchSpscQueue<usize> + Sync + 'static,
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

    match unsafe { fork() } {
        Ok(ForkResult::Child) => {
            // Producer
            sync_atomic_flag.store(1, Ordering::Release);
            while sync_atomic_flag.load(Ordering::Acquire) < 2 {
                std::hint::spin_loop();
            }

            for i in 0..iterations {
                while q.bench_push(i).is_err() {
                    // If queue is full, yield to allow consumer to progress.
                    std::thread::yield_now();
                }
            }

            if let Some(mp_queue) =
                (q as &dyn std::any::Any).downcast_ref::<MultiPushQueue<usize>>()
            {
                let mut attempts = 0;
                while mp_queue.local_count.load(Ordering::Relaxed) > 0 && attempts < 10000 {
                    if !mp_queue.flush() {
                        std::thread::yield_now(); // Yield if flush fails
                        attempts += 1;
                    } else {
                        if mp_queue.local_count.load(Ordering::Relaxed) == 0 {
                            break;
                        }
                        std::thread::yield_now();
                        attempts += 1;
                    }
                }
                if mp_queue.local_count.load(Ordering::Relaxed) > 0 && !PERFORMANCE_TEST {
                    eprintln!(
                     "Warning (SPSC Producer): MultiPushQueue failed to flush all local items after {} attempts. {} items remaining in local_buf.",
                     attempts,
                     mp_queue.local_count.load(Ordering::Relaxed)
                  );
                }
            } else if let Some(biffq_queue) =
                (q as &dyn std::any::Any).downcast_ref::<BiffqQueue<usize>>()
            {
                let mut attempts = 0;
                while biffq_queue.prod.local_count.load(Ordering::Relaxed) > 0 && attempts < 10000 {
                    match biffq_queue.flush_producer_buffer() {
                        Ok(_published_count) => {
                            if biffq_queue.prod.local_count.load(Ordering::Relaxed) == 0 {
                                break;
                            }
                            std::thread::yield_now();
                            attempts += 1;
                        }
                        Err(_) => {
                            std::thread::yield_now();
                            attempts += 1;
                        }
                    }
                }
                if biffq_queue.prod.local_count.load(Ordering::Relaxed) > 0 && !PERFORMANCE_TEST {
                    eprintln!(
                     "Warning (SPSC Producer): BiffqQueue failed to flush all local items after {} attempts. {} items remaining in local_buf.",
                     attempts,
                     biffq_queue.prod.local_count.load(Ordering::Relaxed)
                  );
                }
            }
            sync_atomic_flag.store(3, Ordering::Release);
            unsafe { libc::_exit(0) };
        }
        Ok(ForkResult::Parent { child }) => {
            // Consumer
            while sync_atomic_flag.load(Ordering::Acquire) < 1 {
                std::hint::spin_loop();
            }

            sync_atomic_flag.store(2, Ordering::Release);
            let start_time = std::time::Instant::now();
            let mut consumed_count = 0;

            if iterations > 0 {
                loop {
                    if consumed_count >= iterations {
                        break;
                    }

                    match q.bench_pop() {
                        Ok(_item) => {
                            consumed_count += 1;
                        }
                        Err(_) => {
                            if sync_atomic_flag.load(Ordering::Acquire) == 3 {
                                if q.bench_is_empty() {
                                    break;
                                }
                                std::thread::yield_now(); // Yield if producer done but queue not empty
                            } else {
                                std::thread::yield_now(); // Yield if queue temporarily empty
                            }
                        }
                    }
                }
            }

            let duration = start_time.elapsed();
            while sync_atomic_flag.load(Ordering::Acquire) != 3 {
                std::hint::spin_loop();
            }
            waitpid(child, None).expect("SPSC waitpid failed");

            unsafe {
                unmap_shared(sync_shm as *mut u8, page_size);
            }

            if !PERFORMANCE_TEST && consumed_count != iterations {
                eprintln!(
                    "Warning (SPSC Consumer): Consumed {}/{} items. Q: {}. Potential items missed.",
                    consumed_count,
                    iterations,
                    std::any::type_name::<Q>()
                );
            }
            duration
        }
        Err(e) => {
            unsafe {
                unmap_shared(sync_shm as *mut u8, page_size);
            }
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

criterion_group! {
    name = benches;
    config = custom_criterion();
    targets =
        bench_sesd_jp,
        bench_lamport,
        bench_bqueue,
        bench_mp,
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
