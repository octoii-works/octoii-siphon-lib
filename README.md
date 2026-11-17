# octoii-siphon-lib

Public Rust interface and binary distribution for Siphon.

## Status

Development preview, not a published Cargo package or binary release. The current slice implements
atomic storage changes, retained snapshots, shutdown and persistent reopening. Program execution,
host request handlers, machine operations and processing libraries are still being adapted; this
package cannot yet replace all of Octoii's private source dependencies.

The Cargo package is `octoii-siphon-lib`, imported as `octoii_siphon_lib`. It links a separately built
static library through the C ABI in `include/siphon.h`. No private source dependency, bindgen, or
consumer-side C compiler is needed. No Rust ABI, runtime object or allocator ownership crosses the boundary.

## Development builds

Set `SIPHON_LIB_DIR` to a prebuilt artifact directory containing `libsiphon_native.a` and `manifest.json`:

```json
{"abi": 1, "target": "aarch64-apple-darwin", "sha256": "<SHA-256 of the archive>"}
```

```sh
SIPHON_LIB_DIR=/path/to/artifact cargo test --locked
```

The build checks the target, ABI version and archive hash. This local development manifest is not a
release signature or a trusted download mechanism. Immutable release metadata and automatic artifact
installation remain release work. Missing artifacts fail explicitly; there is no source-build fallback.
Only macOS native linking is implemented in this slice and currently verified on ARM64. Required final
release platforms are macOS, Windows and Linux; each release will name its verified targets and OS minima.

```rust,no_run
use octoii_siphon_lib::{Changes, Runtime};
let runtime = Runtime::open(std::path::Path::new("./state"))?;
let mut changes = Changes::new()?;
changes.write(b"message", b"hello")?;
runtime.commit(&changes)?;
assert_eq!(runtime.snapshot()?.read(b"message")?, Some(b"hello".to_vec()));
# Ok::<(), octoii_siphon_lib::Error>(())
```

Calls are synchronous. Async applications should use a blocking-task facility for long operations.
Calling from a Tokio task is supported; the binary owns its executor on a separate thread. Closing
stops execution and waits for shutdown. Snapshots remain readable after close, but can retain storage:
drop them before reopening the same directory. Drop closes an unclosed runtime; explicit `close` can
report shutdown errors. Paths inside the volume are bytes; the storage directory currently requires UTF-8.

The private implementation lives in `devxiyang/octoii-siphon`. This repository contains only the public
wrapper, interface declarations, consumer tests and distribution metadata. The wrapper source is MIT;
artifact licensing and dependency notices must accompany the eventual binary release.
