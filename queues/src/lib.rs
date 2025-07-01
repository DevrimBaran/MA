pub mod mpmc;
pub mod mpsc;
pub mod spmc;
pub mod spsc;

pub use spsc::BQueue;
pub use spsc::BiffqQueue;
pub use spsc::BlqQueue;
pub use spsc::DehnaviQueue;
pub use spsc::DynListQueue;
pub use spsc::FfqQueue;
pub use spsc::IffqQueue;
pub use spsc::LamportQueue;
pub use spsc::LlqQueue;
pub use spsc::MultiPushQueue;
pub use spsc::PopError;
pub use spsc::SesdJpSpscBenchWrapper;
pub use spsc::UnboundedQueue;

pub use mpsc::DQueue;
pub use mpsc::DrescherQueue;
pub use mpsc::JayantiPetrovicMpscQueue;
pub use mpsc::JiffyQueue;

pub use mpmc::FeldmanDechevWFQueue;
pub use mpmc::KPQueue;
pub use mpmc::TurnQueue;
pub use mpmc::WCQueue;
pub use mpmc::WFQueue;
pub use mpmc::YangCrummeyQueue;

pub use spmc::DavidQueue;
pub use spmc::EnqueuerState;
// Common interface for all spsc queues.
pub trait SpscQueue<T: Send>: Send + 'static {
    type PushError;
    type PopError;

    fn push(&self, item: T) -> Result<(), Self::PushError>;
    fn pop(&self) -> Result<T, Self::PopError>;
    fn available(&self) -> bool;
    fn empty(&self) -> bool;
}

// Common interface for all MPSC queues.
pub trait MpscQueue<T: Send>: Send + Sync + 'static {
    type PushError;
    type PopError;
    fn push(&self, item: T) -> Result<(), Self::PushError>;
    fn pop(&self) -> Result<T, Self::PopError>;
    fn is_empty(&self) -> bool;
    fn is_full(&self) -> bool;
}

pub trait BenchMpscQueue<T: Send>: Send + Sync + 'static {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()>;
    fn bench_pop(&self) -> Result<T, ()>;
    fn bench_is_empty(&self) -> bool;
    fn bench_is_full(&self) -> bool;
}

pub trait MpmcQueue<T: Send>: Send + Sync + 'static {
    type PushError;
    type PopError;
    fn push(&self, item: T, thread_id: usize) -> Result<(), Self::PushError>;
    fn pop(&self, thread_id: usize) -> Result<T, Self::PopError>;
    fn is_empty(&self) -> bool;
    fn is_full(&self) -> bool;
}

pub trait SpmcQueue<T: Send>: Send + Sync + 'static {
    type PushError;
    type PopError;

    fn push(&self, item: T, producer_id: usize) -> Result<(), Self::PushError>;
    fn pop(&self, consumer_id: usize) -> Result<T, Self::PopError>;
    fn is_empty(&self) -> bool;
    fn is_full(&self) -> bool;
}
