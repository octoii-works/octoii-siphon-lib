# octoii-siphon-lib

Rust interface and native binary distribution for the Siphon runtime.

Install the archive for your Rust target from the [matching GitHub release](https://github.com/octoii-works/octoii-siphon-lib/releases).
Extract it and set `SIPHON_LIB_DIR` to the directory containing `manifest.json`. Add
`octoii-siphon-lib = "=0.0.3"` to your Cargo dependencies. Cargo verifies the target, ABI and
SHA-256 of the extracted files against the release metadata in the crate.

The distributions target macOS ARM64 and x64, Linux x64 GNU, and Windows x64 MSVC. The macOS and
Linux builds link a static archive. On Windows, place the shipped `siphon_ffi.dll` beside the
application or on its DLL search path. Restricted commands, pseudo-terminals and the descriptor
channel currently require macOS or Linux.

## Volume and runtime

```rust,no_run
use octoii_siphon_lib::{Builder, Changes};

let runtime = Builder::new()?.open(std::path::Path::new("./state"))?;
let mut batch = Changes::new();
batch.write(b"message", b"hello");
runtime.commit_blocking(&batch)?;
let snapshot = runtime.snapshot()?;
assert_eq!(snapshot.read(b"message")?.as_deref(), Some(b"hello".as_slice()));
# Ok::<(), octoii_siphon_lib::Error>(())
```

Use `RuntimeHandle::commit` on a Tokio task. The synchronous method is available for ordinary caller
threads. The runtime owns its execution thread. `Runtime::close` stops it and waits for shutdown.
A retained handle refers to the runtime without taking over shutdown. Retained snapshots and mounted
volume views can keep the directory locked until they are released.

Use `RuntimeHandle::committed` to watch commit progress. `CommitWatch::next` yields the current
sequence first and any later progress. Use the snapshot journal to read each commit since your
cursor because notifications can coalesce.

## Durable execution

Register a `Fiber` on a `Builder` before opening the volume. Its phase receives typed input, its last
checkpoint, a snapshot and a batch. The runtime commits the batch with the phase's ending. A returned
error or a caught panic discards the batch and follows the Fiber's retry policy.

Register request handlers with the host's Tokio executor. A delivery carries its exact identity,
retained runtime access, and a cancellation token. A handler can publish output while its reply is
open. Person requests use a payload-dependent answer schema and commit validation against the exact
request identity. Carry a `RequestId` across a client connection as an opaque string using
`to_token` and `from_token`. Read its Fiber path and mailbox key through `path` and `key`.

`Run::mail` and `Run::answer` read the phase snapshot. `Run::discard_mail` stages removal in
the phase's commit.

The [C header](include/siphon.h) specifies native ownership, borrowed views, and asynchronous callbacks.
