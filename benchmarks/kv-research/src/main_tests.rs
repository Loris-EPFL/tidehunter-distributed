use super::*;
use std::os::unix::process::ExitStatusExt;

#[test]
fn replay_panic_hook_aborts_on_a_background_panic() {
    const CHILD: &str = "KV_RESEARCH_REPLAY_PANIC_TEST";
    if let Some(output) = std::env::var_os(CHILD) {
        install_replay_panic_hook(Path::new(&output));
        let thread = std::thread::spawn(|| panic!("replay background health regression"));
        let _ = thread.join();
        panic!("panic hook did not abort");
    }
    let guard = tempfile::TempDir::with_prefix("mainnet-panic-hook-").expect("test tree");
    let output = guard.path().to_path_buf();
    // Avoid producing a core dump for this deliberately aborting child.
    let child = std::process::Command::new("bash")
        .args(["-c", "ulimit -c 0; exec \"$@\"", "--"])
        .arg(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "tests::replay_panic_hook_aborts_on_a_background_panic",
            "--nocapture",
        ])
        .env(CHILD, &output)
        .output()
        .unwrap();
    assert_eq!(child.status.signal(), Some(libc::SIGABRT));
    assert!(String::from_utf8_lossy(&child.stderr).contains("replay background health regression"));
    let error = std::fs::read_to_string(output.join("panic.txt")).unwrap();
    assert!(error.contains("replay background health regression"));
    assert!(error.contains("src/main.rs:"));
}
