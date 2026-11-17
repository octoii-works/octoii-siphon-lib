# octoii-siphon-lib

Public Rust interface and binary distribution for Siphon.

## Status

Development preview. The current slice implements atomic storage changes, retained snapshots, program
execution, asynchronous host request handlers, shutdown and persistent reopening. Streaming, machine
operations and processing libraries are still being adapted.

The Cargo package is `octoii-siphon-lib`, imported as `octoii_siphon_lib`. It links a separately built
static library through the C ABI in `include/siphon.h`. No private source dependency, bindgen, or
consumer-side C compiler is needed. No Rust ABI, runtime object or allocator ownership crosses the boundary.

## Development builds

Set `SIPHON_LIB_DIR` to a prebuilt artifact directory containing `libsiphon_native.a` and `manifest.json`:

```json
{"abi": 3, "target": "aarch64-apple-darwin", "sha256": "<SHA-256 of the archive>"}
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

## Running a program

```rust,no_run
use octoii_siphon_lib::{Builder, Changes, Ending, Program, Result, Run, process_path};

struct WriteMessage;

impl Program for WriteMessage {
    const KIND: &'static str = "example.write-message";
    type Input = String;
    type Checkpoint = ();
    type Output = usize;

    fn run(&self, run: &mut Run<'_>, text: String, _: Option<()>) -> Result<Ending<(), usize>> {
        run.write(b"message", text.as_bytes())?;
        Ok(Ending::Done(text.len()))
    }
}

let mut builder = Builder::new()?;
builder.program(WriteMessage)?;
let runtime = builder.memory()?;
let mut changes = Changes::new()?;
changes.start::<WriteMessage>(b"", b"example", &"hello".to_owned())?;
runtime.commit(&changes)?;
let process = process_path(b"", b"example")?;
let outcome = runtime.snapshot()?.outcome::<WriteMessage>(&process)?;
# Ok::<(), octoii_siphon_lib::Error>(())
```

The Rust adapter stores typed input, checkpoints and output as JSON. A phase reads its snapshot
and stages changes through `Run`. The runtime commits its ending and changes together. `Ending::Fail`
records a failure with those changes. A returned error or caught panic discards the staged changes
and follows the program's retry policy. An existing process awaiting an outcome is `Outcome::Pending`.

Initialize the program before registering it. A persisted process can resume during `Builder::open`,
before the caller receives the runtime. Blocking runtime calls return status 6 from phase callbacks
and program destructors. Dropping the last runtime owner there starts shutdown, which finishes once
the callback returns.

## Handling a request

Register each request type on the builder with a handle to the host's Tokio executor. The handler
receives the typed payload, the requesting process path, its mailbox key, and a cancellation token.
Keep the executor running for as long as it handles requests. A process can resume and deliver a
request before `Builder::open` returns.

```rust,no_run
use octoii_siphon_lib::{
    Answer, Asked, Builder, Ending, Program, Request, Result, Run, Runtime, Wait,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

#[derive(Deserialize, Serialize)]
struct Measure(String);

impl Request for Measure {
    const NAME: &'static str = "example.measure";
    type Answer = usize;
}

struct MeasureMessage;

impl Program for MeasureMessage {
    const KIND: &'static str = "example.measure-message";
    type Input = String;
    type Checkpoint = String;
    type Output = usize;

    fn run(
        &self,
        run: &mut Run<'_>,
        text: String,
        checkpoint: Option<String>,
    ) -> Result<Ending<String, usize>> {
        let Some(key) = checkpoint else {
            let key = run.request(&Measure(text))?;
            return Ok(Ending::Wait(key.clone(), Wait::new().mail(key)));
        };
        let Some(answer) = run.answer::<Measure>(&key)? else {
            return Ok(Ending::Wait(key.clone(), Wait::new().mail(key)));
        };
        run.take_mail(&key)?;
        Ok(match answer {
            Answer::Given(length) => Ending::Done(length),
            Answer::Failed(reason) => Ending::Fail(reason),
            Answer::Interrupted => Ending::Fail("measurement interrupted".into()),
        })
    }
}

fn open(executor: &Handle) -> Result<Runtime> {
    let mut builder = Builder::new()?;
    builder.program(MeasureMessage)?;
    builder.handle(executor, |asked: Asked<Measure>| async move {
        Ok(asked.payload.0.len())
    })?;
    builder.memory()
}
```

The request and its mailbox wait commit with the phase. Reading an answer uses the phase's snapshot,
and removing mail stages a change in the same batch as the phase's writes and ending.

A stop signals `asked.cancelled`. The handler can return a partial result or
`Unanswered::Interrupted`. Closing the runtime asks the host executor to abort pending handler
tasks and returns without waiting for them. When persistent storage reopens with the same request
name registered, pending requests are answered with `Interrupted` without another handler call.
Panics while constructing or polling the handler become failed answers.

The private implementation lives in `devxiyang/octoii-siphon`. This repository contains only the public
wrapper, interface declarations, consumer tests and distribution metadata. The wrapper source is MIT;
artifact licensing and dependency notices must accompany the eventual binary release.
