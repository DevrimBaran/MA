pub mod spsc;
pub mod mpsc;

pub use spsc::LamportQueue;
pub use spsc::DynListQueue;
pub use spsc::UnboundedQueue;
pub use spsc::MultiPushQueue;
pub use spsc::BQueue;
pub use spsc::DehnaviQueue;
pub use spsc::PopError;
pub use spsc::IffqQueue;
pub use spsc::BiffqQueue;
pub use spsc::FfqQueue;

pub use mpsc::DrescherQueue;
pub use mpsc::JayantiPetrovicMpscQueue;
pub use mpsc::JiffyQueue;
pub use mpsc::DQueue;

// Common interface for all spsc queues.
pub trait SpscQueue<T: Send>: Send + 'static {
    // Error on push when the queue is full.
    type PushError;
    // Error on pop when the queue is empty.
    type PopError;

    fn push(&self, item: T) -> Result<(), Self::PushError>;
    fn pop(&self) -> Result<T, Self::PopError>;

    /// True when a subsequent `push` *may* succeed without blocking.
    fn available(&self) -> bool;
    /// True when a subsequent `pop` will fail.
    fn empty(&self) -> bool;
}

// Common interface for all MPSC queues.
pub trait MpscQueue<T: Send>: Send + Sync + 'static { // Added Sync since producers access it concurrently
    // Error on push, e.g., when the queue is full or allocation fails.
    // Using T allows the producer to retrieve the item if push fails.
    type PushError;
    // Error on pop, e.g., when the queue is empty.
    type PopError;

    // Attempts to push an item into the queue.
    // Called by producers.
    fn push(&self, item: T) -> Result<(), Self::PushError>;

    // Attempts to pop an item from the queue.
    // Called by the single consumer.
    fn pop(&self) -> Result<T, Self::PopError>;

    // Returns `true` if the queue is empty.
    // Typically called by the consumer.
    fn is_empty(&self) -> bool;

    // Returns `true` if the queue is full or cannot accept more items at the moment.
    // Typically called by producers.
    // For DrescherQueue, this would relate to the node pool capacity.
    fn is_full(&self) -> bool; // Or consider `has_capacity()` or `can_push()`
}

pub trait BenchMpscQueue<T: Send>: Send + Sync + 'static {
    fn bench_push(&self, item: T, producer_id: usize) -> Result<(), ()>;
    fn bench_pop(&self) -> Result<T, ()>;
    fn bench_is_empty(&self) -> bool;
    fn bench_is_full(&self) -> bool;
}