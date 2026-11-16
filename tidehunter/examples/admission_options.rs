//! Build with `cargo build --release -p tidehunter --example admission_options`.
//! The resulting ELF runs without Cargo; use `--help` for matrix and JSON options.
#![recursion_limit = "256"]

#[path = "../benches/admission_options/core.rs"]
mod core;

fn main() {
    core::run();
}
