# octoii-siphon-lib

Rust bindings for Siphon's native execution library. Siphon owns the behavior of the
operations below. This crate maps Rust types, errors, callbacks and ownership to the native ABI.

The 0.0.4 candidate targets development ABI 6. Set `SIPHON_LIB_DIR` to the directory containing
the matching binary and its `manifest.json`. The build checks the target, ABI and SHA-256 against
`artifacts.json`. A paired development build supplies its verified manifest during testing.
Release pins are populated from the platform release builds.

The distribution targets macOS ARM64 and x64, Linux x64 GNU, and Windows x64 MSVC. macOS and Linux
link a static archive. On Windows, place the shipped `siphon_ffi.dll` beside the application or on its
DLL search path. Restricted commands, pseudo-terminals and the descriptor channel currently require
macOS or Linux.

## Volume and runtime

```rust,no_run
use octoii_siphon_lib::{Changes, Runtime};

let runtime = Runtime::open(Some(std::path::Path::new("./state")), None)?;
let observed = runtime.snapshot()?;
let mut batch = Changes::new()?;
batch.expect(&observed)?.write(b"message", b"hello")?;
let revision = runtime.commit_blocking(&batch)?;
runtime.flush(&revision)?;
let snapshot = runtime.snapshot()?;
assert_eq!(snapshot.read(b"message")?.as_deref(), Some(b"hello".as_slice()));
# Ok::<(), octoii_siphon_lib::Error>(())
```

`RuntimeHandle::commit` runs the synchronous binding on a Tokio blocking task.
`Runtime::close` waits for shutdown. A retained handle can call operations while the owner has the
runtime open. Snapshots and mounted volume views can retain storage until they are released.

Use `RuntimeHandle::commits` to observe the latest visible head and the terminal event. Read the
snapshot journal from your cursor for change details. Notifications can coalesce.

## Durable execution

Open the volume, create a `Builder` bound to that opening, register Fibers and request handlers, then
call `Builder::start`. A phase receives its input, checkpoint, snapshot and batch. Siphon commits
the phase's changes and ending together. A returned error or caught panic follows the native retry policy.

A delivery carries its exact identity, retained runtime access, and a cancellation token. A configured
local machine lets a handler start commands through the delivery's request capability. Command input
reports `WouldBlock` when its queue is full. Pipe and terminal output can report a skipped byte range
that callers can reconcile against the retained output.

`Run::mail` reads a delivery. `Run::take_mail` stages its consumption once and returns the staged
delivery. Carry a `RequestId` as an opaque string using `to_token` and `from_token`.

The [C header](include/siphon.h) specifies native ownership, borrowed views, and asynchronous callbacks.
