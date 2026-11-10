use std::{env, path::PathBuf, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=VALKEY_SOURCE_DIR");
    println!("cargo:rerun-if-changed=src/backends/valkey_rdma_shim.c");
    println!("cargo:rustc-check-cfg=cfg(valkey_cq_hook)");
    println!("cargo:rerun-if-changed=../../experiments/kv-comparison/prepare_valkey_rdma.py");
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
    if !clean.success() {
        let otherwise_clean = Command::new("git")
            .arg("-C")
            .arg(&source)
            .args([
                "diff",
                "--quiet",
                "HEAD",
                "--",
                ".",
                ":(exclude)deps/libvalkey/src/rdma.c",
            ])
            .status()
            .expect("git required to verify Valkey source");
        assert!(
            otherwise_clean.success(),
            "Valkey checkout has unrelated modifications"
        );
        let verified = Command::new("python3")
            .arg("../../experiments/kv-comparison/prepare_valkey_rdma.py")
            .arg("--verify-hook")
            .arg(&source)
            .status()
            .expect("python3 required to verify Valkey CQ diagnostic hook");
        assert!(verified.success(), "invalid Valkey CQ diagnostic hook");
        println!("cargo:rustc-cfg=valkey_cq_hook");
    }
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
    let symbols = Command::new("nm")
        .arg("-u")
        .arg(lib.join("libvalkey_rdma.a"))
        .output()
        .expect("nm required to verify Valkey RDMA diagnostic archive");
    assert!(
        symbols.status.success(),
        "cannot inspect Valkey RDMA archive"
    );
    let symbols = String::from_utf8_lossy(&symbols.stdout);
    for symbol in [
        "kv_diag_poll_begin",
        "kv_diag_poll_end",
        "kv_diag_cq_poll_begin",
        "kv_diag_cq_poll_end",
    ] {
        assert_eq!(
            symbols
                .lines()
                .any(|line| line.split_whitespace().last() == Some(symbol)),
            !clean.success(),
            "Valkey diagnostic source and archive do not match"
        );
    }
    for (name, path) in [
        (
            "VALKEY_RDMA_SOURCE_SHA256",
            source.join("deps/libvalkey/src/rdma.c"),
        ),
        ("VALKEY_RDMA_ARCHIVE_SHA256", lib.join("libvalkey_rdma.a")),
        ("VALKEY_CLIENT_ARCHIVE_SHA256", lib.join("libvalkey.a")),
    ] {
        let digest = Command::new("sha256sum")
            .arg(path)
            .output()
            .expect("sha256sum required for Valkey artifact provenance");
        assert!(digest.status.success(), "cannot hash Valkey artifact");
        let text = String::from_utf8(digest.stdout).expect("invalid sha256sum output");
        let hash = text
            .split_whitespace()
            .next()
            .expect("missing sha256sum digest");
        println!("cargo:rustc-env={name}={hash}");
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
