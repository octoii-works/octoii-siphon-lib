use std::{
    ffi::c_void,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    ptr::NonNull,
};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{from_slice, to_vec};

use crate::Mail;

use crate::{
    Answer, ChangeHandle, Entry, Error, FiberRecord, Found, NameChange, Query, Range, Request,
    Result, Runtime, RuntimeHandle, SnapshotHandle, borrow_slice, check, compatible, discard_panic,
    handle, panic_message, present, release_context, slice, sys, take,
};

/// Fibers and request handlers to register before opening a runtime.
#[derive(Debug)]
pub struct Builder(pub(crate) NonNull<sys::Builder>);

impl Builder {
    pub fn new() -> Result<Self> {
        compatible()?;
        let mut out = std::ptr::null_mut();
        // SAFETY: out points to writable storage.
        check(unsafe { sys::siphon_builder_new(&mut out) })?;
        Ok(Self(handle(out)?))
    }

    /// Retains access for services assembled before the runtime opens.
    pub fn runtime_handle(&self) -> Result<RuntimeHandle> {
        let mut result = std::ptr::null();
        // SAFETY: the builder is live and result is writable.
        check(unsafe { sys::siphon_builder_runtime(self.0.as_ptr(), &mut result) })?;
        RuntimeHandle::from_retained(result)
    }

    /// Registers `fiber`. A rejected registration drops its fiber before returning.
    pub fn fiber<P: Fiber>(&mut self, fiber: P) -> Result<&mut Self> {
        let context = Box::into_raw(Box::new(fiber)).cast();
        let Options {
            stop_handler,
            retries,
        } = P::OPTIONS;
        // SAFETY: builder is exclusively borrowed, kind is static, and the native library takes
        // ownership of context on all paths and keeps it alive until release_context is called.
        check(unsafe {
            sys::siphon_builder_fiber(
                self.0.as_ptr(),
                slice(P::KIND.as_bytes()),
                P::VERSION,
                u8::from(stop_handler),
                retries.count,
                retries.delay_min_ms,
                retries.delay_max_ms,
                context,
                Some(run_fiber::<P>),
                Some(release_context::<P>),
            )
        })?;
        Ok(self)
    }

    /// Opens persistent storage, consuming the builder and its registrations. Fibers can run
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
        Runtime::from_open(out)
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

/// Runtime options for processes of a registered fiber.
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
pub trait Fiber: Send + Sync + 'static {
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
/// any named mail arrives, or the deadline arrives.
#[derive(Debug, Clone, Default)]
pub struct Wait {
    children: Vec<Vec<u8>>,
    changes: Vec<Vec<u8>>,
    mail: Vec<Vec<u8>>,
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

    /// Waits for mail of this key, waking immediately if it is already present.
    #[must_use]
    pub fn mail(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.mail.push(key.into());
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
    phase: &'a mut NonNull<sys::Run>,
    path: &'a [u8],
    time_ms: i64,
    stop_handler: bool,
    changes: ChangeHandle,
    snapshot: SnapshotHandle,
}

impl<'a> Run<'a> {
    /// Safety: `run` is a live, exclusive native callback handle for this borrow.
    unsafe fn new(phase: &'a mut NonNull<sys::Run>) -> Result<Self> {
        let run = phase.as_ptr();
        let mut path = sys::Slice::default();
        let mut time_ms = 0;
        let mut stop_handler = 0;
        check(unsafe { sys::siphon_run_info(run, &mut path, &mut time_ms, &mut stop_handler) })?;
        Ok(Self {
            phase,
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

    /// Mints a UUIDv7 from the same volume as the runtime.
    pub fn new_id(&self) -> Result<String> {
        let mut out = sys::Buffer::default();
        // SAFETY: this phase is live and out is writable.
        let status = unsafe { sys::siphon_run_new_id(self.phase.as_ptr(), &mut out) };
        crate::decode_id(status, out)
    }

    pub fn is_stop_handler(&self) -> bool {
        self.stop_handler
    }

    /// Reads one entry from the immutable snapshot for this phase.
    pub fn stat(&self, path: &[u8]) -> Result<Option<Entry>> {
        self.snapshot.stat(path)
    }

    /// Reads a directory page from the immutable snapshot for this phase.
    pub fn list(&self, path: &[u8], range: &Range<'_>) -> Result<Option<Vec<Entry>>> {
        self.snapshot.list(path, range)
    }

    /// Finds indexed directories in the immutable snapshot for this phase.
    pub fn find(&self, query: &Query<'_>, limit: usize) -> Result<Vec<Found>> {
        self.snapshot.find(query, limit)
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

    /// Reads a Fiber record from the immutable snapshot for this phase.
    pub fn fiber(&self, process: &[u8]) -> Result<Option<FiberRecord>> {
        self.snapshot.fiber(process)
    }

    /// Decodes the outcome for the stated Fiber kind and version.
    pub fn outcome<P: Fiber>(&self, process: &[u8]) -> Result<Option<Outcome<P::Output>>> {
        self.snapshot.outcome::<P>(process)
    }

    /// Stages a request and returns its mailbox key. Store the key in the checkpoint to read the
    /// answer in a later phase.
    pub fn request<R: Request>(&mut self, request: &R) -> Result<String> {
        let mut key = sys::Buffer::default();
        let payload = to_vec(request)?;
        // SAFETY: the phase is live and exclusive, the slices are readable, and key is writable.
        let status = unsafe {
            sys::siphon_run_request(
                self.phase.as_ptr(),
                slice(R::NAME.as_bytes()),
                slice(&payload),
                &mut key,
            )
        };
        let key = take(key);
        check(status)?;
        String::from_utf8(key).map_err(|error| Error {
            code: 4,
            message: format!("library returned an invalid request key: {error}"),
        })
    }

    /// Reads the request's answer from this phase's snapshot. Returns `None` when it is absent.
    pub fn answer<R: Request>(&self, key: &str) -> Result<Option<Answer<R::Answer>>> {
        let mut kind = 0;
        let mut payload = sys::Buffer::default();
        // SAFETY: the phase and key are live for this call and both outputs are writable.
        let status = unsafe {
            sys::siphon_run_answer(
                self.phase.as_ptr(),
                slice(key.as_bytes()),
                &mut kind,
                &mut payload,
            )
        };
        let payload = take(payload);
        if !present(status)? {
            return Ok(None);
        }
        let answer = match kind {
            sys::ANSWER_GIVEN => from_slice(&payload).map_or_else(
                |error| Answer::Failed(format!("the answer could not be read: {error}")),
                Answer::Given,
            ),
            sys::ANSWER_FAILED => Answer::Failed(String::from_utf8_lossy(&payload).into_owned()),
            sys::ANSWER_INTERRUPTED => Answer::Interrupted,
            _ => {
                return Err(Error {
                    code: 4,
                    message: format!("library returned an unknown answer kind: {kind}"),
                });
            }
        };
        Ok(Some(answer))
    }

    /// Stages removal of mail at `key` with the phase's other changes.
    /// Reads a mailbox entry from the immutable phase snapshot.
    pub fn mail(&self, key: &[u8]) -> Result<Option<Mail>> {
        let mut mail: Option<Mail> = None;
        // SAFETY: the phase and key are live. The visitor writes into mail exclusively.
        let status = unsafe {
            sys::siphon_run_mail(
                self.phase.as_ptr(),
                slice(key),
                std::ptr::from_mut(&mut mail).cast(),
                Some(collect_mail),
            )
        };
        if !present(status)? {
            return Ok(None);
        }
        mail.map(Some).ok_or_else(|| Error {
            code: 4,
            message: "Siphon returned no mail for a successful mailbox read".into(),
        })
    }

    /// Stages consumption in the phase's commit.
    pub fn take_mail(&mut self, key: impl AsRef<[u8]>) -> Result<&mut Self> {
        // SAFETY: the phase is live and exclusive, and key is readable for this call.
        check(unsafe { sys::siphon_run_take_mail(self.phase.as_ptr(), slice(key.as_ref())) })?;
        Ok(self)
    }

    pub fn mkdir(&mut self, path: &[u8]) -> Result<&mut Self> {
        self.changes.mkdir(path)?;
        Ok(self)
    }

    /// Stages label and annotation changes for the phase's commit.
    pub fn meta(
        &mut self,
        path: &[u8],
        labels: &[NameChange<'_>],
        annotations: &[NameChange<'_>],
    ) -> Result<&mut Self> {
        self.changes.meta(path, labels, annotations)?;
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

    /// Stages sealing a file in this phase.
    pub fn seal(&mut self, path: &[u8]) -> Result<&mut Self> {
        self.changes.seal(path)?;
        Ok(self)
    }
    /// Stages expiration in milliseconds since the Unix epoch. None clears it.
    pub fn expire(&mut self, path: &[u8], deadline: Option<i64>) -> Result<&mut Self> {
        self.changes.expire(path, deadline)?;
        Ok(self)
    }
    pub fn delete(&mut self, path: &[u8]) -> Result<&mut Self> {
        self.changes.delete(path)?;
        Ok(self)
    }

    pub fn start<P: Fiber>(
        &mut self,
        parent: &[u8],
        key: &[u8],
        input: &P::Input,
    ) -> Result<&mut Self> {
        self.changes.start::<P>(parent, key, input)?;
        Ok(self)
    }

    /// Stages mail. A started sequence fences delivery to that Fiber incarnation.
    pub fn send(&mut self, process: &[u8], mail: &Mail, started: Option<u64>) -> Result<&mut Self> {
        self.changes.send(process, mail, started)?;
        Ok(self)
    }

    /// Stages a signal to the Fiber and its descendants.
    pub fn signal(
        &mut self,
        process: &[u8],
        signal: crate::Signal,
        reason: Option<&[u8]>,
    ) -> Result<&mut Self> {
        self.changes.signal(process, signal, reason)?;
        Ok(self)
    }

    /// Stages removal after the Fibers under path have had a chance to stop.
    pub fn remove(&mut self, path: &[u8], reason: Option<&[u8]>) -> Result<&mut Self> {
        self.changes.remove(path, reason)?;
        Ok(self)
    }

    /// Stages removal by killing live Fibers under path.
    pub fn force_remove(&mut self, path: &[u8], reason: Option<&[u8]>) -> Result<&mut Self> {
        self.changes.force_remove(path, reason)?;
        Ok(self)
    }

    /// Stages reaping an ended Fiber record.
    pub fn reap(&mut self, process: &[u8]) -> Result<&mut Self> {
        self.changes.reap(process)?;
        Ok(self)
    }

    /// Stages usage charges to the Fiber and its ancestors.
    pub fn charge(&mut self, process: &[u8], counters: &[(&[u8], u64)]) -> Result<&mut Self> {
        self.changes.charge(process, counters)?;
        Ok(self)
    }
}

unsafe extern "C" fn collect_mail(context: *mut c_void, view: *const sys::MailView) {
    // SAFETY: the wrapper supplies an exclusive slot and the library lends a live mail view.
    let (mail, view) = unsafe { (&mut *context.cast::<Option<Mail>>(), &*view) };
    // SAFETY: all three slices are readable during this visit.
    *mail = Some(unsafe {
        Mail {
            key: borrow_slice(view.key).to_vec(),
            name: borrow_slice(view.name).to_vec(),
            payload: borrow_slice(view.payload).to_vec(),
        }
    });
}

unsafe extern "C" fn run_fiber<P: Fiber>(
    context: *mut c_void,
    run: *mut sys::Run,
    input: sys::Slice,
    checkpoint: sys::Slice,
) {
    let executed = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: registration keeps context and callback data alive until this call returns.
        let fiber = unsafe { &*context.cast::<P>() };
        let (input, checkpoint) = unsafe { (borrow_slice(input), borrow_slice(checkpoint)) };
        let input = from_slice(input)?;
        let checkpoint = (!checkpoint.is_empty())
            .then(|| from_slice(checkpoint))
            .transpose()?;
        let mut phase = handle(run)?;
        // SAFETY: this is the unique live callback handle for this invocation.
        let mut scoped = unsafe { Run::new(&mut phase)? };
        let ending = fiber.run(&mut scoped, input, checkpoint)?;
        finish(run, ending)
    }));
    match executed {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = end(run, sys::END_THROW, error.message.as_bytes());
        }
        Err(panic) => {
            let message = panic_message(panic.as_ref());
            let _ = end(
                run,
                sys::END_THROW,
                format!("the phase panicked: {message}").as_bytes(),
            );
            discard_panic(panic);
        }
    }
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
            for key in &wait.mail {
                // SAFETY: as above, run and key are live for this call.
                check(unsafe { sys::siphon_run_wait_for(run, sys::WAIT_MAIL, slice(key)) })?;
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
    // Serialize and drop fiber-owned values before publishing the ending, as either can panic.
    end(run, kind, &payload)
}

fn end(run: *mut sys::Run, kind: u32, payload: &[u8]) -> Result<()> {
    // SAFETY: run is live throughout the callback, and the payload is readable for this call.
    check(unsafe { sys::siphon_run_end(run, kind, slice(payload)) })
}
