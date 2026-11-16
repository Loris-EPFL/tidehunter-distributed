// The flat auditable JSON record exceeds serde_json's default macro depth.
#![recursion_limit = "256"]

#[path = "admission_options/core.rs"]
mod core;

fn main() {
    core::run();
}
