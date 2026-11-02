use std::{env, path::PathBuf, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=VALKEY_SOURCE_DIR");
    println!("cargo:rerun-if-changed=src/backends/valkey_rdma_shim.c");
    if env::var_os("CARGO_FEATURE_VALKEY_RDMA").is_none() {
        return;
    }
    assert_eq!(
        env::var("CARGO_CFG_TARGET_OS").as_deref(),
        Ok("linux"),
        "valkey-rdma requires Linux"
    );
    let source = PathBuf::from(
        env::var_os("VALKEY_SOURCE_DIR")
            .expect("valkey-rdma requires VALKEY_SOURCE_DIR pointing to pinned Valkey checkout"),
    );
    let rev = Command::new("git")
        .args(["-C"])
        .arg(&source)
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("git required to verify Valkey source");
    assert!(
        rev.status.success()
            && String::from_utf8_lossy(&rev.stdout).trim()
                == "fe8d406c0b5a64ec897f6455e4de684629883764",
        "VALKEY_SOURCE_DIR must be the pinned Valkey 9.0.7 checkout"
    );
    let clean = Command::new("git")
        .arg("-C")
        .arg(&source)
        .args(["diff", "--quiet", "HEAD", "--"])
        .status()
        .expect("git required to verify clean Valkey source");
    assert!(
        clean.success(),
        "VALKEY_SOURCE_DIR contains modified tracked files; use a clean pinned checkout"
    );
    println!(
        "cargo:rerun-if-changed={}",
        source.join("deps/libvalkey/include").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        source.join("deps/libvalkey/src").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        source.join(".git/index").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        source.join(".git/HEAD").display()
    );
    let lib = source.join("deps/libvalkey/lib");
    for name in ["libvalkey.a", "libvalkey_rdma.a"] {
        assert!(
            lib.join(name).is_file(),
            "missing {}; build standalone libvalkey with USE_RDMA=1 static",
            lib.join(name).display()
        );
    }
    println!(
        "cargo:rerun-if-changed={}",
        lib.join("libvalkey.a").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        lib.join("libvalkey_rdma.a").display()
    );
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let obj = out.join("valkey_rdma_shim.o");
    let status = Command::new(env::var_os("CC").unwrap_or_else(|| "cc".into()))
        .args(["-std=c11", "-O2", "-fPIC", "-c"])
        .arg("src/backends/valkey_rdma_shim.c")
        .arg("-I")
        .arg(source.join("deps/libvalkey/include"))
        .arg("-o")
        .arg(&obj)
        .status()
        .expect("C compiler required for valkey-rdma");
    assert!(
        status.success(),
        "failed to compile Valkey RDMA shim against pinned headers"
    );
    let status = Command::new(env::var_os("AR").unwrap_or_else(|| "ar".into()))
        .arg("crs")
        .arg(out.join("libkv_valkey_rdma_shim.a"))
        .arg(obj)
        .status()
        .expect("ar required for valkey-rdma");
    assert!(status.success(), "failed to archive Valkey RDMA shim");
    println!("cargo:rustc-link-search=native={}", out.display());
    println!("cargo:rustc-link-search=native={}", lib.display());
    println!("cargo:rustc-link-lib=static=kv_valkey_rdma_shim");
    println!("cargo:rustc-link-lib=static=valkey_rdma");
    println!("cargo:rustc-link-lib=static=valkey");
    println!("cargo:rustc-link-lib=rdmacm");
    println!("cargo:rustc-link-lib=ibverbs");
}
