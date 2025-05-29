#[test]
#[cfg(unix)]
fn test_fork_fails() {
    let _ = unsafe { libc::fork() };
}
