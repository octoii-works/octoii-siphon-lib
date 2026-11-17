use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=SIPHON_LIB_DIR");
    let directory = PathBuf::from(env::var_os("SIPHON_LIB_DIR")
        .expect("Siphon binary not installed: SIPHON_LIB_DIR must name a verified artifact directory; no public release is available yet"));
    let manifest = directory.join("manifest.json");
    println!("cargo:rerun-if-changed={}", manifest.display());
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest).expect("read binary manifest"))
            .expect("parse binary manifest");
    let target = env::var("TARGET").expect("Cargo target");
    assert_eq!(
        manifest["target"].as_str(),
        Some(target.as_str()),
        "Siphon target mismatch"
    );
    assert_eq!(manifest["abi"].as_u64(), Some(1), "Siphon ABI mismatch");
    let filename = if target.contains("windows-msvc") {
        "siphon_native.lib"
    } else {
        "libsiphon_native.a"
    };
    let library = directory.join(filename);
    println!("cargo:rerun-if-changed={}", library.display());
    let hash: String = Sha256::digest(fs::read(&library).expect("read native library"))
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(
        manifest["sha256"].as_str(),
        Some(hash.as_str()),
        "Siphon checksum mismatch"
    );
    println!("cargo:rustc-link-search=native={}", directory.display());
    println!("cargo:rustc-link-lib=static=siphon_native");
    if target.contains("apple-darwin") {
        for lib in ["System", "c", "m", "iconv"] {
            println!("cargo:rustc-link-lib={lib}");
        }
    } else {
        panic!("this development slice has not validated native linking for {target}");
    }
}
