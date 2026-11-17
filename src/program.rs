use std::{
    any::Any,
    ffi::c_void,
    mem::{ManuallyDrop, forget},
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    ptr::NonNull,
};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{from_slice, to_vec};

use crate::{
    ChangeHandle, Error, Result, Runtime, SnapshotHandle, check, compatible, handle, slice, sys,
};

/// Programs to register before opening a runtime.
#[derive(Debug)]
pub struct Builder(NonNull<sys::Builder>);

impl Builder {
    pub fn new() -> Result<Self> {
        compatible()?;
        let mut out = std::ptr::null_mut();
        // SAFETY: out points to writable storage.
        check(unsafe { sys::siphon_builder_new(&mut out) })?;
        Ok(Self(handle(out)?))
    }

    /// Registers `program`. A rejected registration drops its program before returning.
    pub fn program<P: Program>(&mut self, program: P) -> Result<&mut Self> {
        let context = Box::into_raw(Box::new(program)).cast();
        let Options {
            stop_handler,
            retries,
        } = P::OPTIONS;
        // SAFETY: builder is exclusively borrowed, kind is static, and the native library takes
        // ownership of context on all paths and keeps it alive until release_program is called.
        check(unsafe {
            sys::siphon_builder_program(
                self.0.as_ptr(),
                slice(P::KIND.as_bytes()),
                P::VERSION,
                u8::from(stop_handler),
                retries.count,
                retries.delay_min_ms,
                retries.delay_max_ms,
                context,
                Some(run_program::<P>),
                Some(release_program::<P>),
            )
        })?;
        Ok(self)
    }

    /// Opens persistent storage, consuming the builder and its registrations. Programs can run
    /// resumed processes before this call returns.
    pub fn open(self, directory: &Path) -> Result<Runtime> {
        let directory = directory
            .to_str()
            .filter(|path| !path.is_empty())
            .ok_or_else(|| Error {
                code: 2,
                message: "directory must be nonempty UTF-8".into(),
            })?;
        self.open_bytes(directory.as_bytes())
    }

    /// Opens memory storage, consuming the builder and its registrations.
    pub fn memory(self) -> Result<Runtime> {
        self.open_bytes(b"")
    }

    fn open_bytes(self, directory: &[u8]) -> Result<Runtime> {
        let builder = ManuallyDrop::new(self);
        let mut out = std::ptr::null_mut();
        // SAFETY: the native library consumes the builder on all paths. The directory is readable
        // for this call and out is writable.
        check(unsafe { sys::siphon_builder_open(builder.0.as_ptr(), slice(directory), &mut out) })?;
        Ok(Runtime(handle(out)?))
    }
}

impl Drop for Builder {
    fn drop(&mut self) {
        // SAFETY: this builder was never passed to open and is released once.
        unsafe { sys::siphon_builder_free(self.0.as_ptr()) };
    }
}

/// How the runtime retries a thrown phase, doubling its delay up to `delay_max_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retries {
    pub count: u32,
    pub delay_min_ms: i64,
    pub delay_max_ms: i64,
}

impl Retries {
    pub const NONE: Self = Self {
        count: 0,
        delay_min_ms: 0,
        delay_max_ms: 0,
    };
}

/// Runtime options for processes of a registered program.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Options {
    pub stop_handler: bool,
    pub retries: Retries,
}

impl Options {
    pub const DEFAULT: Self = Self {
        stop_handler: false,
        retries: Retries::NONE,
    };
}

impl Default for Options {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The code processes of one kind run, one synchronous phase at a time.
pub trait Program: Send + Sync + 'static {
    const KIND: &'static str;
    const VERSION: u32 = 1;
    const OPTIONS: Options = Options::DEFAULT;
    type Input: Serialize + DeserializeOwned;
    type Checkpoint: Serialize + DeserializeOwned;
    type Output: Serialize + DeserializeOwned;

    /// Runs a phase from its input and the last committed checkpoint, if any. Returning an error
    /// or panicking discards the phase's staged changes and follows the retry policy.
    fn run(
        &self,
        run: &mut Run<'_>,
        input: Self::Input,
        checkpoint: Option<Self::Checkpoint>,
    ) -> Result<Ending<Self::Checkpoint, Self::Output>>;
}

/// The ending committed together with the phase's writes.
#[derive(Debug, Clone)]
pub enum Ending<C, O> {
    Next(C),
    Wait(C, Wait),
    Done(O),
    Fail(String),
    Stopped(Option<O>),
}

/// Wakes a waiting process when all named children have ended, any watched directory changes,
/// or the deadline arrives.
#[derive(Debug, Clone, Default)]
pub struct Wait {
    children: Vec<Vec<u8>>,
    changes: Vec<Vec<u8>>,
    deadline: Option<i64>,
}

impl Wait {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn child(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.children.push(key.into());
        self
    }

    #[must_use]
    pub fn change(mut self, path: impl Into<Vec<u8>>) -> Self {
        self.changes.push(path.into());
        self
    }

    #[must_use]
    pub fn deadline(mut self, at: i64) -> Self {
        self.deadline = Some(at);
        self
    }
}

/// A process's pending or recorded result at the time of the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<O> {
    Pending,
    Completed(Option<O>),
    Failed {
        error: Option<Vec<u8>>,
        reason: Option<Vec<u8>>,
    },
    Stopped {
        result: Option<O>,
        reason: Option<Vec<u8>>,
    },
}

/// A phase's view of the last commit and the changes it is staging, valid during the callback.
#[derive(Debug)]
pub struct Run<'a> {
    path: &'a [u8],
    time_ms: i64,
    stop_handler: bool,
    changes: ChangeHandle,
    snapshot: SnapshotHandle,
}

impl<'a> Run<'a> {
    /// Safety: `run` is a live, exclusive native callback handle for this borrow.
    unsafe fn new(run: &'a mut NonNull<sys::Run>) -> Result<Self> {
        let run = run.as_ptr();
        let mut path = sys::Slice::default();
        let mut time_ms = 0;
        let mut stop_handler = 0;
        check(unsafe { sys::siphon_run_info(run, &mut path, &mut time_ms, &mut stop_handler) })?;
        Ok(Self {
            path: unsafe { borrow_slice(path) },
            time_ms,
            stop_handler: stop_handler != 0,
            changes: ChangeHandle(handle(unsafe { sys::siphon_run_changes(run) })?),
            snapshot: SnapshotHandle(handle(unsafe { sys::siphon_run_snapshot(run) }.cast_mut())?),
        })
    }

    pub fn path(&self) -> &[u8] {
        self.path
    }

    /// The dispatch time in milliseconds since the Unix epoch.
    pub fn time_ms(&self) -> i64 {
        self.time_ms
    }

    pub fn is_stop_handler(&self) -> bool {
        self.stop_handler
    }

    pub fn sequence(&self) -> Result<u64> {
        self.snapshot.sequence()
    }

    pub fn read(&self, path: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read_range(path, 0, u64::MAX)
    }

    pub fn read_range(&self, path: &[u8], offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.snapshot.read_range(path, offset, length)
    }

    pub fn outcome<P: Program>(&self, process: &[u8]) -> Result<Option<Outcome<P::Output>>> {
        self.snapshot.outcome::<P>(process)
    }

    pub fn mkdir(&mut self, path: &[u8]) -> Result<&mut Self> {
        self.changes.mkdir(path)?;
        Ok(self)
    }

    pub fn write(&mut self, path: &[u8], data: &[u8]) -> Result<&mut Self> {
        self.changes.write(path, data)?;
        Ok(self)
    }

    pub fn append(&mut self, path: &[u8], data: &[u8]) -> Result<&mut Self> {
        self.changes.append(path, data)?;
        Ok(self)
    }

    pub fn delete(&mut self, path: &[u8]) -> Result<&mut Self> {
        self.changes.delete(path)?;
        Ok(self)
    }

    pub fn start<P: Program>(
        &mut self,
        parent: &[u8],
        key: &[u8],
        input: &P::Input,
    ) -> Result<&mut Self> {
        self.changes.start::<P>(parent, key, input)?;
        Ok(self)
    }

    pub fn terminate(&mut self, process: &[u8]) -> Result<&mut Self> {
        self.changes.terminate(process)?;
        Ok(self)
    }
}

unsafe extern "C" fn run_program<P: Program>(
    context: *mut c_void,
    run: *mut sys::Run,
    input: sys::Slice,
    checkpoint: sys::Slice,
) {
    let executed = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: registration keeps context and callback data alive until this call returns.
        let program = unsafe { &*context.cast::<P>() };
        let (input, checkpoint) = unsafe { (borrow_slice(input), borrow_slice(checkpoint)) };
        let input = from_slice(input)?;
        let checkpoint = (!checkpoint.is_empty())
            .then(|| from_slice(checkpoint))
            .transpose()?;
        let mut phase = handle(run)?;
        // SAFETY: this is the unique live callback handle for this invocation.
        let mut scoped = unsafe { Run::new(&mut phase)? };
        let ending = program.run(&mut scoped, input, checkpoint)?;
        finish(run, ending)
    }));
    match executed {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = end(run, sys::END_THROW, error.message.as_bytes());
        }
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("no message");
            let _ = end(
                run,
                sys::END_THROW,
                format!("the phase panicked: {message}").as_bytes(),
            );
            discard_panic(panic);
        }
    }
}

unsafe extern "C" fn release_program<P: Program>(context: *mut c_void) {
    // SAFETY: release is called once for the box created by Builder::program, after its final use.
    let program = unsafe { Box::from_raw(context.cast::<P>()) };
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(program))) {
        discard_panic(panic);
    }
}

fn discard_panic(panic: Box<dyn Any + Send>) {
    // A custom panic payload can itself panic on drop. Never let that unwind into C.
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(panic))) {
        forget(panic);
    }
}

/// Safety: the slice must remain readable for the returned borrow.
unsafe fn borrow_slice<'a>(value: sys::Slice) -> &'a [u8] {
    if value.len == 0 {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(value.data, value.len) }
}

fn finish<C: Serialize, O: Serialize>(run: *mut sys::Run, ending: Ending<C, O>) -> Result<()> {
    let (kind, payload) = match ending {
        Ending::Next(checkpoint) => (sys::END_NEXT, to_vec(&checkpoint)?),
        Ending::Wait(checkpoint, wait) => {
            let checkpoint = to_vec(&checkpoint)?;
            for child in &wait.children {
                // SAFETY: run and the borrowed bytes are live for this call.
                check(unsafe { sys::siphon_run_wait_for(run, sys::WAIT_CHILD, slice(child)) })?;
            }
            for path in &wait.changes {
                // SAFETY: as above, run and path are live for this call.
                check(unsafe { sys::siphon_run_wait_for(run, sys::WAIT_CHANGE, slice(path)) })?;
            }
            if let Some(deadline) = wait.deadline {
                // SAFETY: run is live throughout the callback.
                check(unsafe { sys::siphon_run_wait_until(run, deadline) })?;
            }
            (sys::END_WAIT, checkpoint)
        }
        Ending::Done(output) => (sys::END_DONE, to_vec(&output)?),
        Ending::Fail(message) => (sys::END_FAIL, message.into_bytes()),
        Ending::Stopped(output) => (
            sys::END_STOPPED,
            output.as_ref().map(to_vec).transpose()?.unwrap_or_default(),
        ),
    };
    // Serialize and drop program-owned values before publishing the ending, as either can panic.
    end(run, kind, &payload)
}

fn end(run: *mut sys::Run, kind: u32, payload: &[u8]) -> Result<()> {
    // SAFETY: run is live throughout the callback, and the payload is readable for this call.
    check(unsafe { sys::siphon_run_end(run, kind, slice(payload)) })
}
