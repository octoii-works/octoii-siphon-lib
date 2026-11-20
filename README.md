# octoii-siphon-lib

Rust bindings for Siphon's native execution library. Siphon owns the behavior of the
operations below. This crate maps Rust types, errors, callbacks and ownership to the native ABI.

Version 0.0.4 uses ABI 6. Download the ZIP for your Rust target from the
[0.0.4 release](https://github.com/octoii-works/octoii-siphon-lib/releases/tag/v0.0.4),
extract it, and set `SIPHON_LIB_DIR` to the absolute path of the directory containing
`manifest.json`. The build checks the target, ABI and SHA-256 against `artifacts.json`.

| Target | Native library |
| --- | --- |
| `aarch64-apple-darwin`, `x86_64-apple-darwin` | macOS static archive |
| `aarch64-unknown-linux-gnu`, `x86_64-unknown-linux-gnu` | Linux GNU static archive |
| `x86_64-pc-windows-msvc` | Windows DLL and import library |

On Windows, deploy `siphon_ffi.dll` beside the application or on its DLL search path and install
the [x64 Visual C++ Redistributable](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist/).
Restricted commands, pseudo-terminals and the descriptor channel require macOS or Linux.

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
`Runtime::close` stops admitting new work, waits for the native worker and its commit callbacks,
and reports the first terminal fault, if any. Snapshots and mounted volume views can retain storage
until they are released.

Use `RuntimeHandle::commits` for passive observation of the latest visible head. Notifications can
coalesce. Read the snapshot journal from your cursor for change details. Active subscriptions and
subscriptions created on retained handles after close receive the same terminal event with the final
visible sequence, either `CommitEvent::Closed(seq)` or `CommitEvent::Fault { head, error }`. The next
read returns `None`. Registration during shutdown can wait for the terminal event.

## Durable execution

Open the volume, create a `Builder` bound to that opening, register Fibers and request handlers, then
call `Builder::start`. A phase receives its input, checkpoint, snapshot and batch. Siphon commits
the phase's changes and ending together. A returned error or caught panic follows the native retry policy.

A delivery carries its exact identity and a read-only stop signal. Handlers can check
`asked.stop.is_requested()` or wait with `asked.stop.requested().await`. Capture a handle from
`Runtime::runtime_handle` when registering a handler that needs volume state. A configured local machine
lets a handler start commands through the delivery's request capability. Command input reports
`WouldBlock` when its queue is full. Pipe and terminal output can report a skipped byte range that
callers can reconcile against the retained output.

`Run::mail` reads a delivery from the phase snapshot. `Run::take_mail` takes the observed mail, and
`Run::take_answer` takes and decodes a request answer. Each delivery can be taken once per phase
and is consumed when the phase commits. Carry a `RequestId` as an opaque string using `to_token`
and `from_token`.

The [C header](include/siphon.h) specifies native ownership, borrowed views, and asynchronous callbacks.
