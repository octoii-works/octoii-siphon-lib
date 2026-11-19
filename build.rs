use sha2::{Digest, Sha256};
use std::{
    env, fs,
    path::{Path, PathBuf},
};

fn verify_file(directory: &Path, filename: &str, expected: &serde_json::Value) -> PathBuf {
    let binary = directory.join(filename);
    println!("cargo:rerun-if-changed={}", binary.display());
    let digest: String = Sha256::digest(
        fs::read(&binary).unwrap_or_else(|error| panic!("read {filename}: {error}")),
    )
    .iter()
    .map(|byte| format!("{byte:02x}"))
    .collect();
    assert_eq!(
        expected.as_str(),
        Some(digest.as_str()),
        "Siphon checksum mismatch for {filename}"
    );
    binary
}

fn main() {
    println!("cargo:rerun-if-env-changed=SIPHON_LIB_DIR");
    let directory = PathBuf::from(
        env::var_os("SIPHON_LIB_DIR")
            .expect("SIPHON_LIB_DIR must name the verified binary distribution"),
    );
    let manifest_path = directory.join("manifest.json");
    println!("cargo:rerun-if-changed={}", manifest_path.display());
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read binary manifest"))
            .expect("parse binary manifest");
    let target = env::var("TARGET").expect("Cargo target");
    assert_eq!(
        manifest["target"].as_str(),
        Some(target.as_str()),
        "Siphon target mismatch"
    );
    assert_eq!(manifest["abi"].as_u64(), Some(6), "Siphon ABI mismatch");
    let pins: serde_json::Value = serde_json::from_str(include_str!("artifacts.json"))
        .expect("parse the published artifact checksums");
    assert_eq!(
        pins.get(target.as_str()),
        Some(&manifest),
        "the native distribution does not match octoii-siphon-lib {}",
        env!("CARGO_PKG_VERSION"),
    );

    if target.ends_with("windows-msvc") {
        let library = verify_file(&directory, "siphon_native.lib", &manifest["sha256"]);
        let runtime = verify_file(&directory, "siphon_ffi.dll", &manifest["dll_sha256"]);
        let out = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo build output"));
        fs::copy(library, out.join("siphon_native.lib")).expect("stage Windows import library");
        fs::copy(runtime, out.join("siphon_ffi.dll")).expect("stage Windows runtime");
        println!("cargo:rustc-link-search=native={}", out.display());
        println!("cargo:rustc-link-lib=dylib=siphon_native");
    } else {
        let libraries: &[&str] = if target.ends_with("apple-darwin") {
            &["System", "c", "m", "iconv"]
        } else if target.ends_with("linux-gnu") {
            &["gcc_s", "util", "rt", "pthread", "m", "dl", "c"]
        } else {
            panic!("unsupported Siphon target {target}");
        };
        verify_file(&directory, "libsiphon_native.a", &manifest["sha256"]);
        println!("cargo:rustc-link-search=native={}", directory.display());
        println!("cargo:rustc-link-lib=static=siphon_native");
        for lib in libraries {
            println!("cargo:rustc-link-lib={lib}");
        }
    }
}
