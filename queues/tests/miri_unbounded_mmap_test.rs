// Run with: cargo miri test --test miri_unbounded_mmap_error

use queues::spsc::UnboundedQueue;
use queues::SpscQueue;

#[test]
fn test_unbounded_queue_mmap_fails() {
    let shared_size = UnboundedQueue::<usize>::shared_size(8192);

    // Allocate aligned memory
    let layout = std::alloc::Layout::from_size_align(shared_size, 128).unwrap();
    let mem_ptr = unsafe {
        let ptr = std::alloc::alloc(layout);
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        ptr
    };

    // This will fail under Miri with mmap error
    let queue = unsafe { UnboundedQueue::<usize>::init_in_shared(mem_ptr, 8192) };

    queue.push(42).unwrap();
    assert_eq!(queue.pop().unwrap(), 42);

    unsafe {
        std::alloc::dealloc(mem_ptr, layout);
    }
}
