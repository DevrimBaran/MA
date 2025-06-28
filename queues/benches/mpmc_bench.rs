use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use nix::{
    libc,
    sys::wait::waitpid,
    unistd::{fork, ForkResult},
};
use queues::mpmc::{self, polylog_queue, ymc_queue};
use queues::mpmc::{
    BurdenWFQueue, FeldmanDechevWFQueue, JKMQueue, KPQueue, KWQueue, NRQueue, SDPWFQueue,
    TurnQueue, WCQueue, WFQueue, YangCrummeyQueue,
};
use queues::MpmcQueue;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

const PERFORMANCE_TEST: bool = false;
const ITEMS_PER_PROCESS_TARGET: usize = 5_000;
const PROCESS_COUNTS_TO_TEST: &[(usize, usize)] = &[(1, 1), (2, 2), (4, 4), (6, 6)];
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
        self.simple_dequeue(process_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + Clone + 'static> BenchMpmcQueue<T> for JKMQueue<T> {
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

impl<T: Send + Clone + 'static> crate::BenchMpmcQueue<T> for WCQueue<T> {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()> {
        self.push(item, producer_id)
    }

    fn bench_pop(&self, consumer_id: usize) -> Result<T, ()> {
        self.pop(consumer_id)
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + Clone + 'static> BenchMpmcQueue<T> for TurnQueue<T> {
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

impl<T: Send + Clone + 'static> BenchMpmcQueue<T> for FeldmanDechevWFQueue<T> {
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

impl<T: Send + Clone + 'static> BenchMpmcQueue<T> for SDPWFQueue<T> {
    fn bench_push(&self, item: T, process_id: usize) -> Result<(), ()> {
        match self.enqueue(process_id, item) {
            Ok(()) => Ok(()),
            Err(_) => Err(()), // Convert any error to unit type
        }
    }

    fn bench_pop(&self, process_id: usize) -> Result<T, ()> {
        match self.dequeue(process_id) {
            Ok(item) => Ok(item),
            Err(_) => Err(()), // Convert any error to unit type
        }
    }

    fn bench_is_empty(&self) -> bool {
        self.is_empty()
    }

    fn bench_is_full(&self) -> bool {
        self.is_full()
    }
}

impl<T: Send + Clone + 'static> BenchMpmcQueue<T> for KPQueue<T> {
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
    total_consumed: AtomicUsize, // Add this to track total consumed items
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
                },
            );
            &*sync_ptr
        }
    }

    fn shared_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

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

    let (q, q_shm_ptr, q_shm_size) = queue_init_fn();

    let startup_sync_size = MpmcStartupSync::shared_size();
    let startup_sync_shm_ptr = unsafe { map_shared(startup_sync_size) };
    let startup_sync = MpmcStartupSync::new_in_shm(startup_sync_shm_ptr);

    let done_sync_size = MpmcDoneSync::shared_size();
    let done_sync_shm_ptr = unsafe { map_shared(done_sync_size) };
    let done_sync = MpmcDoneSync::new_in_shm(done_sync_shm_ptr);

    let mut producer_pids = Vec::with_capacity(num_producers);
    let mut consumer_pids = Vec::with_capacity(num_consumers);
    let mut helper_pid = None;

    // Detect queue types
    let queue_type_name = std::any::type_name::<Q>();
    let is_jkm = queue_type_name.contains("JKMQueue");
    let is_nr = queue_type_name.contains("NRQueue");
    let is_wcq = queue_type_name.contains("WCQueue");

    // Fork helper process if needed - counts as a participant in startup sync
    if needs_helper && !is_wcq {
        // WCQ doesn't need external helper
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                // Helper process - pin to last core
                #[cfg(target_os = "linux")]
                unsafe {
                    use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};
                    let mut set = std::mem::zeroed::<cpu_set_t>();
                    CPU_ZERO(&mut set);
                    CPU_SET(num_producers + num_consumers, &mut set); // Use core after all workers
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

    // Fork producers
    for producer_id in 0..num_producers {
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                startup_sync.producers_ready.fetch_add(1, Ordering::AcqRel);

                while !startup_sync.go_signal.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                let mut push_attempts = 0;
                for i in 0..items_per_process {
                    let item_value = producer_id * items_per_process + i;

                    // WCQ uses producer_id directly, others might need adjustment
                    let thread_id = producer_id;

                    while q.bench_push(item_value, thread_id).is_err() {
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

    // Fork consumers - with queue-specific handling
    for consumer_id in 0..num_consumers {
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                startup_sync.consumers_ready.fetch_add(1, Ordering::AcqRel);

                while !startup_sync.go_signal.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                }

                let mut consumed_count = 0;
                let target_items = total_items / num_consumers;
                let extra_items = if consumer_id < (total_items % num_consumers) {
                    1
                } else {
                    0
                };
                let my_target = target_items + extra_items;

                // WCQ-specific consumer handling
                if is_wcq {
                    let mut consecutive_empty = 0;
                    let mut total_attempts = 0;
                    let start_time = std::time::Instant::now();
                    let timeout_duration = std::time::Duration::from_secs(30);

                    while consumed_count < my_target {
                        if start_time.elapsed() > timeout_duration {
                            break;
                        }

                        total_attempts += 1;

                        // WCQ expects consumer thread_id from num_producers to num_producers + num_consumers - 1
                        let thread_id = num_producers + consumer_id;

                        match q.bench_pop(thread_id) {
                            Ok(_) => {
                                consumed_count += 1;
                                consecutive_empty = 0;
                            }
                            Err(_) => {
                                consecutive_empty += 1;

                                if consecutive_empty > 10_000 {
                                    let producers_done =
                                        done_sync.producers_done.load(Ordering::Acquire)
                                            == num_producers as u32;

                                    if producers_done && consecutive_empty > 50_000 {
                                        let mut final_attempts = 0;
                                        while final_attempts < 10_000 && consumed_count < my_target
                                        {
                                            if let Ok(_) = q.bench_pop(thread_id) {
                                                consumed_count += 1;
                                                final_attempts = 0;
                                            } else {
                                                final_attempts += 1;
                                                if final_attempts % 100 == 0 {
                                                    std::thread::yield_now();
                                                }
                                            }
                                        }

                                        if consumed_count < my_target {
                                            break;
                                        }
                                    }
                                }

                                // Adaptive backoff
                                if consecutive_empty < 100 {
                                    std::hint::spin_loop();
                                } else if consecutive_empty < 1000 {
                                    std::thread::yield_now();
                                } else if consecutive_empty < 10_000 {
                                    std::thread::sleep(std::time::Duration::from_micros(1));
                                } else {
                                    std::thread::sleep(std::time::Duration::from_micros(10));

                                    if consecutive_empty > 1_000_000 {
                                        consecutive_empty = 10_000;
                                    }
                                }
                            }
                        }
                    }
                } else if is_jkm {
                    // JKM-specific consumer logic (existing code)
                    let mut pop_attempts_outer = 0;

                    while consumed_count < my_target {
                        match q.bench_pop(consumer_id) {
                            Ok(_item) => {
                                consumed_count += 1;
                                pop_attempts_outer = 0;
                            }
                            Err(_) => {
                                pop_attempts_outer += 1;

                                // Check if we should panic (producers not done but we're spinning too long)
                                if pop_attempts_outer > MAX_BENCH_SPIN_RETRY_ATTEMPTS
                                    && !(done_sync.producers_done.load(Ordering::Acquire)
                                        == num_producers as u32)
                                {
                                    panic!(
                                        "Consumer {} exceeded max spin retry attempts for pop (producers not done)",
                                        consumer_id
                                    );
                                }

                                // Pop returned error - check if producers are done
                                let producers_done =
                                    done_sync.producers_done.load(Ordering::Acquire)
                                        == num_producers as u32;

                                if producers_done {
                                    // Producers are done, try a final drain aggressively
                                    let mut final_drain_attempts = 0;
                                    const MAX_FINAL_DRAIN_ATTEMPTS: usize = 100_000;

                                    // Force sync before aggressive drain
                                    if let Some(jkm_queue) = unsafe {
                                        (q as *const _ as *const JKMQueue<usize>).as_ref()
                                    } {
                                        // Multiple rounds of synchronization
                                        for _ in 0..3 {
                                            jkm_queue.force_sync();
                                            std::thread::sleep(std::time::Duration::from_micros(
                                                10,
                                            ));
                                        }
                                        jkm_queue.finalize_pending_dequeues();
                                    }

                                    while consumed_count < my_target
                                        && final_drain_attempts < MAX_FINAL_DRAIN_ATTEMPTS
                                    {
                                        // More aggressive periodic sync during drain
                                        if final_drain_attempts % 500 == 0 {
                                            if let Some(jkm_queue) = unsafe {
                                                (q as *const _ as *const JKMQueue<usize>).as_ref()
                                            } {
                                                jkm_queue.force_sync();
                                                // Give time for propagation
                                                if final_drain_attempts % 2000 == 0 {
                                                    std::thread::sleep(
                                                        std::time::Duration::from_micros(100),
                                                    );
                                                }
                                            }
                                        }

                                        if q.bench_pop(consumer_id).is_ok() {
                                            consumed_count += 1;
                                            final_drain_attempts = 0;
                                            if consumed_count >= my_target {
                                                break;
                                            }
                                        } else {
                                            final_drain_attempts += 1;
                                            if final_drain_attempts < 100 {
                                                std::hint::spin_loop();
                                            } else {
                                                std::thread::yield_now();
                                            }
                                        }
                                    }

                                    // After aggressive drain attempts, do more thorough checks
                                    if consumed_count < my_target {
                                        // Multiple final sync rounds
                                        if let Some(jkm_queue) = unsafe {
                                            (q as *const _ as *const JKMQueue<usize>).as_ref()
                                        } {
                                            for sync_round in 0..5 {
                                                jkm_queue.force_sync();
                                                jkm_queue.finalize_pending_dequeues();

                                                // Give time between syncs
                                                std::thread::sleep(
                                                    std::time::Duration::from_millis(1),
                                                );

                                                // Try to drain after each sync
                                                for attempt in 0..1000 {
                                                    if q.bench_pop(consumer_id).is_ok() {
                                                        consumed_count += 1;
                                                        if consumed_count >= my_target {
                                                            break;
                                                        }
                                                    } else if attempt % 100 == 0 {
                                                        std::thread::yield_now();
                                                    } else {
                                                        std::hint::spin_loop();
                                                    }
                                                }

                                                if consumed_count >= my_target {
                                                    break;
                                                }
                                            }

                                            // Final check if queue truly has no items
                                            let items_left = jkm_queue.total_items();
                                            if items_left == 0 && q.bench_is_empty() {
                                                break;
                                            } else if items_left > 0 {
                                                // Queue still has items, keep trying
                                                eprintln!("JKM Queue still has {} items, consumer {} only got {}/{}", 
                                                    items_left, consumer_id, consumed_count, my_target);
                                            }
                                        }
                                    }
                                } else {
                                    // Producers not done - adaptive backoff
                                    if pop_attempts_outer < 100 {
                                        std::hint::spin_loop();
                                    } else if pop_attempts_outer < 1000 {
                                        std::thread::yield_now();
                                    } else {
                                        std::thread::sleep(std::time::Duration::from_micros(1));
                                    }
                                }
                            }
                        }

                        if consumed_count >= my_target {
                            break;
                        }
                    }
                } else if is_nr {
                    // NRQueue-specific consumer logic
                    let mut consecutive_empty_checks = 0;
                    const MAX_CONSECUTIVE_EMPTY_CHECKS: usize = 1000;
                    let mut total_empty_checks = 0;
                    const MAX_TOTAL_EMPTY_CHECKS: usize = 100000;

                    while consumed_count < my_target && total_empty_checks < MAX_TOTAL_EMPTY_CHECKS
                    {
                        match q.bench_pop(num_producers + consumer_id) {
                            Ok(_item) => {
                                consumed_count += 1;
                                consecutive_empty_checks = 0;
                                total_empty_checks = 0;
                            }
                            Err(_) => {
                                total_empty_checks += 1;

                                // Only increment consecutive counter if producers are done
                                if done_sync.producers_done.load(Ordering::Acquire)
                                    == num_producers as u32
                                {
                                    consecutive_empty_checks += 1;

                                    if consecutive_empty_checks > MAX_CONSECUTIVE_EMPTY_CHECKS {
                                        // Do a final aggressive sync attempt
                                        if let Some(nr_queue) = unsafe {
                                            (q as *const _ as *const NRQueue<usize>).as_ref()
                                        } {
                                            // Try multiple syncs to ensure propagation
                                            for sync_round in 0..10 {
                                                nr_queue.sync();

                                                // Small delay between syncs
                                                if sync_round > 5 {
                                                    std::thread::sleep(
                                                        std::time::Duration::from_micros(10),
                                                    );
                                                }

                                                // Try to pop after each sync
                                                match q.bench_pop(num_producers + consumer_id) {
                                                    Ok(_) => {
                                                        consumed_count += 1;
                                                        consecutive_empty_checks = 0;
                                                        total_empty_checks = 0;
                                                        break;
                                                    }
                                                    Err(_) => continue,
                                                }
                                            }

                                            // If still failing after aggressive sync, check one more time
                                            if consecutive_empty_checks
                                                > MAX_CONSECUTIVE_EMPTY_CHECKS
                                            {
                                                // Force a complete sync
                                                nr_queue.force_complete_sync();

                                                // One final attempt
                                                match q.bench_pop(num_producers + consumer_id) {
                                                    Ok(_) => {
                                                        consumed_count += 1;
                                                        consecutive_empty_checks = 0;
                                                        total_empty_checks = 0;
                                                    }
                                                    Err(_) => {
                                                        // We're likely done
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    consecutive_empty_checks = 0;
                                }

                                // Adaptive backoff
                                if consecutive_empty_checks < 10 {
                                    for _ in 0..10 {
                                        std::hint::spin_loop();
                                    }
                                } else if consecutive_empty_checks < 100 {
                                    std::thread::yield_now();
                                } else {
                                    std::thread::sleep(std::time::Duration::from_micros(1));
                                }
                            }
                        }
                    }
                } else {
                    // Standard consumer logic for other queues
                    let mut consecutive_empty_checks = 0;
                    const MAX_CONSECUTIVE_EMPTY_CHECKS: usize = 40000;

                    while consumed_count < my_target {
                        match q.bench_pop(num_producers + consumer_id) {
                            Ok(_item) => {
                                consumed_count += 1;
                                consecutive_empty_checks = 0;
                            }
                            Err(_) => {
                                // Only count it as truly empty if all producers are done
                                // AND we've waited a reasonable time for propagation
                                if done_sync.producers_done.load(Ordering::Acquire)
                                    == num_producers as u32
                                {
                                    consecutive_empty_checks += 1;

                                    // Give more time for propagation
                                    if consecutive_empty_checks < 100 {
                                        std::thread::sleep(std::time::Duration::from_micros(1));
                                    } else if consecutive_empty_checks
                                        > MAX_CONSECUTIVE_EMPTY_CHECKS
                                    {
                                        break;
                                    }
                                }
                                // Don't yield immediately - spin a bit first
                                for _ in 0..100 {
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    }
                }

                // Add this consumer's count to the total
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

    // Wait for all workers AND helper to be ready
    let expected_producers = if needs_helper && !is_wcq {
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

    // Wait for producers
    for pid in producer_pids {
        waitpid(pid, None).expect("waitpid for producer failed");
    }

    // Queue-specific synchronization after producers finish
    if is_jkm {
        if let Some(jkm_queue) = unsafe { (q as *const _ as *const JKMQueue<usize>).as_ref() } {
            // Multiple rounds of synchronization
            for round in 0..3 {
                // Ensure all enqueued items are visible
                jkm_queue.force_sync();

                // Help any pending dequeue operations
                jkm_queue.finalize_pending_dequeues();

                // Give time for propagation between rounds
                std::thread::sleep(std::time::Duration::from_millis(2));
            }

            // Final longer wait for propagation
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    // Wait for consumers
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

    if total_consumed != total_items {
        eprintln!(
            "Warning (MPMC): Total consumed {}/{} items. Q: {}, Prods: {}, Cons: {}",
            total_consumed,
            total_items,
            std::any::type_name::<Q>(),
            num_producers,
            num_consumers
        );
        if is_jkm {
            unsafe {
                let jkm_queue = &*(q as *const _ as *const JKMQueue<usize>);
                jkm_queue.print_debug_stats();
            }
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
                            let bytes = NRQueue::<usize>::shared_size(total_processes);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe { NRQueue::init_in_shared(shm_ptr, total_processes) };
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

fn bench_jkm_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("JohnenKhattabiMilaniMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<JKMQueue<usize>, _>(
                        || {
                            let bytes = JKMQueue::<usize>::shared_size(num_prods, num_cons);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q =
                                unsafe { JKMQueue::init_in_shared(shm_ptr, num_prods, num_cons) };
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

fn bench_wcq_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("WCQueueMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;
        let total_processes = num_prods + num_cons;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<WCQueue<usize>, _>(
                        || {
                            let bytes = WCQueue::<usize>::shared_size(total_processes);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe { WCQueue::init_in_shared(shm_ptr, total_processes) };
                            (q, shm_ptr, bytes)
                        },
                        num_prods,
                        num_cons,
                        items_per_process,
                        false,
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_turn_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("TurnQueueMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;
        let total_processes = num_prods + num_cons;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<TurnQueue<usize>, _>(
                        || {
                            let bytes = TurnQueue::<usize>::shared_size(total_processes);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe { TurnQueue::init_in_shared(shm_ptr, total_processes) };
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

fn bench_feldman_dechev_wf_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("FeldmanDechevWFMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;
        let total_processes = num_prods + num_cons;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<FeldmanDechevWFQueue<usize>, _>(
                        || {
                            let bytes = FeldmanDechevWFQueue::<usize>::shared_size(total_processes);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe {
                                FeldmanDechevWFQueue::init_in_shared(shm_ptr, total_processes)
                            };
                            (q, shm_ptr, bytes)
                        },
                        num_prods,
                        num_cons,
                        items_per_process,
                        false, // needs_helper = false (progress assurance is internal)
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_sdp_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("StellwagDitterPreikschatMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;
        let total_processes = num_prods + num_cons;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<SDPWFQueue<usize>, _>(
                        || {
                            // Always enable helping queue as it's a core optimization in the paper
                            let bytes = SDPWFQueue::<usize>::shared_size(total_processes, true);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe {
                                SDPWFQueue::init_in_shared(shm_ptr, total_processes, true)
                            };
                            (q, shm_ptr, bytes)
                        },
                        num_prods,
                        num_cons,
                        items_per_process,
                        false,
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_kogan_petrank_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("KoganPetrankMPMC");

    for &(num_prods, num_cons) in PROCESS_COUNTS_TO_TEST {
        let items_per_process = ITEMS_PER_PROCESS_TARGET;
        let total_processes = num_prods + num_cons;

        group.bench_function(
            format!("{}P_{}C", num_prods, num_cons),
            |b: &mut Bencher| {
                b.iter_custom(|_iters| {
                    fork_and_run_mpmc_with_helper::<KPQueue<usize>, _>(
                        || {
                            let bytes = KPQueue::<usize>::shared_size(total_processes);
                            let shm_ptr = unsafe { map_shared(bytes) };
                            let q = unsafe { KPQueue::init_in_shared(shm_ptr, total_processes) };
                            (q, shm_ptr, bytes)
                        },
                        num_prods,
                        num_cons,
                        items_per_process,
                        false, // needs_helper = false (internal helping mechanism)
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
        bench_nr_queue,
        bench_kogan_petrank_queue
}

criterion_main!(benches);
