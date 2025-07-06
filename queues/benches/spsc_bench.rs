#![allow(clippy::cast_possible_truncation)]

use criterion::{criterion_group, criterion_main, Criterion};
use nix::{
    libc,
    sys::wait::waitpid,
    unistd::{fork, ForkResult},
};
use std::ptr;
use std::time::Duration;

use queues::{
    BQueue, BiffqQueue, BlqQueue, DynListQueue, FfqQueue, IffqQueue, LamportQueue, LlqQueue,
    MultiPushQueue, SesdJpSpscBenchWrapper, SpscQueue, UnboundedQueue,
};
use std::sync::atomic::{AtomicU32, Ordering};

use queues::spsc::blq::K_CACHE_LINE_SLOTS as BLQ_K_SLOTS;
use queues::spsc::llq::K_CACHE_LINE_SLOTS as LLQ_K_SLOTS;

const PERFORMANCE_TEST: bool = false;
const RING_CAP: usize = 524_288;
const ITERS: usize = 35_000_000;
const MAX_BENCH_SPIN_RETRY_ATTEMPTS: usize = 1_000_000_000;

trait BenchSpscQueue<T: Send>: Send + Sync + 'static {
    fn bench_push(&self, item: T) -> Result<(), ()>;
    fn bench_pop(&self) -> Result<T, ()>;
}

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

impl<T: Send + 'static> BenchSpscQueue<T> for LamportQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self)
    }
}

impl<T: Send + 'static> BenchSpscQueue<T> for BQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self)
    }
}

impl<T: Send + 'static> BenchSpscQueue<T> for MultiPushQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_| ())
    }
}

impl<T: Send + 'static> BenchSpscQueue<T> for UnboundedQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self)
    }
}

impl<T: Send + 'static> BenchSpscQueue<T> for DynListQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item)
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self)
    }
}

impl<T: Send + 'static> BenchSpscQueue<T> for IffqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_e| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_e| ())
    }
}

impl<T: Copy + Send + Default + 'static> BenchSpscQueue<T> for BiffqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_e| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_e| ())
    }
}

impl<T: Send + 'static> BenchSpscQueue<T> for FfqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_e| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_e| ())
    }
}

impl<T: Send + 'static> BenchSpscQueue<T> for LlqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_| ())
    }
}

impl<T: Send + 'static> BenchSpscQueue<T> for BlqQueue<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_| ())
    }
}

impl<T: Send + Clone + 'static> BenchSpscQueue<T> for SesdJpSpscBenchWrapper<T> {
    fn bench_push(&self, item: T) -> Result<(), ()> {
        SpscQueue::push(self, item).map_err(|_| ())
    }
    fn bench_pop(&self) -> Result<T, ()> {
        SpscQueue::pop(self).map_err(|_| ())
    }
}

fn bench_lamport(c: &mut Criterion) {
    c.bench_function("Lamport", |b| {
        b.iter_custom(|_iters| {
            let bytes = LamportQueue::<usize>::shared_size(RING_CAP);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { LamportQueue::init_in_shared(shm_ptr, RING_CAP) };
            let dur = fork_and_run(q);
            unsafe { unmap_shared(shm_ptr, bytes) };
            dur
        })
    });
}

fn bench_bqueue(c: &mut Criterion) {
    c.bench_function("B-Queue", |b| {
        b.iter_custom(|_iters| {
            let bytes = BQueue::<usize>::shared_size(RING_CAP);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { BQueue::init_in_shared(shm_ptr, RING_CAP) };
            let dur = fork_and_run(q);
            unsafe { unmap_shared(shm_ptr, bytes) };
            dur
        })
    });
}

fn bench_mp(c: &mut Criterion) {
    c.bench_function("mSPSC", |b| {
        b.iter_custom(|_iters| {
            let bytes = MultiPushQueue::<usize>::shared_size(RING_CAP);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { MultiPushQueue::init_in_shared(shm_ptr, RING_CAP) };
            let q_ptr: *mut MultiPushQueue<usize> = q;

            let dur = fork_and_run(q);

            unsafe {
                ptr::drop_in_place(q_ptr);
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

fn bench_dspsc(c: &mut Criterion) {
    c.bench_function("dSPSC", |b| {
        b.iter_custom(|_iters| {
            let nodes_needed = ITERS + 1;
            let bytes = DynListQueue::<usize>::shared_size(RING_CAP, nodes_needed);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { DynListQueue::init_in_shared(shm_ptr, RING_CAP, nodes_needed) };

            let dur = fork_and_run(q);

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
            let segments_needed = (ITERS + RING_CAP - 1) / RING_CAP + 2;

            let size = UnboundedQueue::<usize>::shared_size(RING_CAP, segments_needed);
            let shm_ptr = unsafe { map_shared(size) };
            let q = unsafe { UnboundedQueue::init_in_shared(shm_ptr, RING_CAP, segments_needed) };

            let dur = fork_and_run(q);

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

fn bench_biffq(c: &mut Criterion) {
    c.bench_function("Biffq", |b| {
        b.iter_custom(|_iters| {
            assert!(RING_CAP.is_power_of_two());

            assert_eq!(
                RING_CAP % 32,
                0,
                "RING_CAP must be a multiple of BIFFQ H_PARTITION_SIZE (32)"
            );
            assert!(
                RING_CAP >= 2 * 32,
                "RING_CAP must be >= 2 * BIFFQ H_PARTITION_SIZE (64)"
            );

            let bytes = BiffqQueue::<usize>::shared_size(RING_CAP);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { BiffqQueue::init_in_shared(shm_ptr, RING_CAP) };

            let dur = fork_and_run(q);

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
            assert!(RING_CAP.is_power_of_two() && RING_CAP > 0);

            let bytes = FfqQueue::<usize>::shared_size(RING_CAP);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { FfqQueue::init_in_shared(shm_ptr, RING_CAP) };

            let dur = fork_and_run(q);

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
            let current_ring_cap = if RING_CAP <= LLQ_K_SLOTS {
                let min_valid_cap = (LLQ_K_SLOTS + 1).next_power_of_two();
                if min_valid_cap < 16 {
                    16
                } else {
                    min_valid_cap
                }
            } else {
                RING_CAP.next_power_of_two()
            };

            assert!(current_ring_cap.is_power_of_two());
            assert!(current_ring_cap > LLQ_K_SLOTS);

            let bytes = LlqQueue::<usize>::llq_shared_size(current_ring_cap);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { LlqQueue::init_in_shared(shm_ptr, current_ring_cap) };

            let dur = fork_and_run(q);

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
            let current_ring_cap = if RING_CAP <= BLQ_K_SLOTS {
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
                RING_CAP.next_power_of_two()
            };

            assert!(current_ring_cap.is_power_of_two());
            assert!(current_ring_cap > BLQ_K_SLOTS);

            let bytes = BlqQueue::<usize>::shared_size(current_ring_cap);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { BlqQueue::init_in_shared(shm_ptr, current_ring_cap) };

            let dur = fork_and_run(q);

            unsafe {
                unmap_shared(shm_ptr, bytes);
            }
            dur
        })
    });
}

fn bench_sesd_jp(c: &mut Criterion) {
    c.bench_function("SesdJpSPSC", |b| {
        b.iter_custom(|_iters| {
            let pool_capacity = ITERS + 1000;

            let bytes = SesdJpSpscBenchWrapper::<usize>::shared_size(pool_capacity);
            let shm_ptr = unsafe { map_shared(bytes) };
            let q = unsafe { SesdJpSpscBenchWrapper::init_in_shared(shm_ptr, pool_capacity) };

            let dur = fork_and_run(q);

            unsafe {
                unmap_shared(shm_ptr, bytes);
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

    let queue_type_name = std::any::type_name::<Q>();
    let needs_special_sync =
        queue_type_name.contains("DynListQueue") || queue_type_name.contains("UnboundedQueue");

    match unsafe { fork() }.expect("fork failed") {
        ForkResult::Child => {
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

                if needs_special_sync && i > 0 && i % 1000 == 0 {
                    std::sync::atomic::fence(Ordering::SeqCst);

                    for _ in 0..10 {
                        std::hint::spin_loop();
                    }
                }
            }

            if let Some(mp_queue) =
                (q as &dyn std::any::Any).downcast_ref::<MultiPushQueue<usize>>()
            {
                for _attempt in 0..1000 {
                    if mp_queue.local_count.load(Ordering::Relaxed) == 0 {
                        break;
                    }
                    if mp_queue.flush() {
                        if mp_queue.local_count.load(Ordering::Relaxed) == 0 {
                            break;
                        }
                    }
                    std::hint::spin_loop();
                }
                if !PERFORMANCE_TEST && mp_queue.local_count.load(Ordering::Relaxed) > 0 {
                    eprintln!(
                        "Warning: MultiPushQueue failed to flush all items. {} items remaining",
                        mp_queue.local_count.load(Ordering::Relaxed)
                    );
                }
            } else if let Some(biffq_queue) =
                (q as &dyn std::any::Any).downcast_ref::<BiffqQueue<usize>>()
            {
                for _attempt in 0..1000 {
                    if biffq_queue.flush_producer_buffer().is_ok() {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }

            if needs_special_sync {
                std::sync::atomic::fence(Ordering::SeqCst);

                std::thread::sleep(std::time::Duration::from_millis(10));
            }

            sync_atomic_flag.store(3, Ordering::Release);
            unsafe { libc::_exit(0) };
        }
        ForkResult::Parent { child } => {
            while sync_atomic_flag.load(Ordering::Acquire) < 1 {
                std::hint::spin_loop();
            }

            sync_atomic_flag.store(2, Ordering::Release);

            while sync_atomic_flag.load(Ordering::Acquire) < 2 {
                std::hint::spin_loop();
            }

            let start_time = std::time::Instant::now();
            let mut consumed_count = 0;
            let mut pop_spin_attempts = 0;
            let mut consecutive_failures = 0;

            while consumed_count < ITERS {
                let producer_done = sync_atomic_flag.load(Ordering::Acquire) == 3;

                if let Ok(_item) = q.bench_pop() {
                    consumed_count += 1;
                    pop_spin_attempts = 0;
                    consecutive_failures = 0;
                } else {
                    pop_spin_attempts += 1;
                    consecutive_failures += 1;

                    if needs_special_sync && consecutive_failures > 100 {
                        std::sync::atomic::fence(Ordering::SeqCst);
                        std::thread::yield_now();
                        consecutive_failures = 0;
                    }

                    if producer_done && consecutive_failures > 1000 {
                        for _ in 0..100 {
                            std::sync::atomic::fence(Ordering::SeqCst);
                            std::thread::sleep(std::time::Duration::from_micros(10));

                            if let Ok(_) = q.bench_pop() {
                                consumed_count += 1;
                                break;
                            }
                        }

                        if consumed_count < ITERS {
                            break;
                        }
                    }

                    if pop_spin_attempts > MAX_BENCH_SPIN_RETRY_ATTEMPTS && !producer_done {
                        panic!(
                            "Consumer exceeded max spin retry attempts for pop (producer not done)"
                        );
                    }

                    std::hint::spin_loop();
                }
            }

            let duration = start_time.elapsed();

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

fn custom_criterion() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(2500))
        .sample_size(500)
}

criterion_group! {
    name = benches;
    config = custom_criterion();
    targets =
        bench_lamport,
        bench_bqueue,
        bench_mp,
        bench_dspsc,
        bench_unbounded,
        bench_iffq,
        bench_biffq,
        bench_ffq,
        bench_llq,
        bench_blq,
        bench_sesd_jp
}
criterion_main!(benches);
