use std::{
    ffi::c_void,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr::{self, NonNull},
};

use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{from_slice, to_vec};

use crate::{
    Answer, Batch, Error, FiberId, Mail, Request, Result, RuntimeHandle, Snapshot, borrow_slice,
    check, discard_panic, handle, panic_message, present, release_context, slice, sys, take,
};

/// Fibers, request handlers, and adapters to register before starting execution.
#[derive(Debug)]
pub struct Builder(pub(crate) NonNull<sys::Builder>);

impl Builder {
    /// Binds the builder to an open runtime.
    pub fn new(runtime: &RuntimeHandle) -> Result<Self> {
        let mut out = std::ptr::null_mut();
        // SAFETY: the runtime is live and out points to writable storage.
        check(unsafe { sys::siphon_builder_new(runtime.0.as_ptr(), &mut out) })?;
        Ok(Self(handle(out)?))
    }

    /// Enables local command execution for the runtime when it starts.
    pub fn local_machine(&mut self) -> Result<&mut Self> {
        // SAFETY: the builder is live and exclusively borrowed.
        check(unsafe { sys::siphon_builder_local_machine(self.0.as_ptr()) })?;
        Ok(self)
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

    /// Starts execution, consuming the builder and its registrations. Restored phases may
    /// run before this call returns.
    pub fn start(self) -> Result<()> {
        let builder = ManuallyDrop::new(self);
        // SAFETY: the native library consumes the builder on all paths.
        check(unsafe { sys::siphon_builder_start(builder.0.as_ptr()) })
    }
}

impl Drop for Builder {
    fn drop(&mut self) {
        // SAFETY: this builder was never passed to start and is released once.
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
    ) -> anyhow::Result<Ending<Self::Checkpoint, Self::Output>>;
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

/// A Fiber's recorded result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome<O = Bytes> {
    Completed(Option<O>),
    Failed {
        error: Option<Bytes>,
        reason: Option<Bytes>,
    },
    Stopped {
        result: Option<O>,
        reason: Option<Bytes>,
    },
}

/// A phase's view of the last commit and the changes it is staging, valid during the callback.
#[derive(Debug)]
pub struct Run<'a> {
    phase: &'a mut NonNull<sys::Run>,
    id: FiberId,
    time_ms: i64,
    stop_handler: bool,
    pub changes: Batch<'a>,
    snapshot: &'a Snapshot,
}

impl<'a> Run<'a> {
    /// Safety: the exclusive phase and the values in its view remain live for this borrow.
    unsafe fn new(phase: &'a mut NonNull<sys::Run>, view: &'a sys::PhaseView) -> Result<Self> {
        Ok(Self {
            phase,
            // SAFETY: the view lends the ID bytes for the callback.
            id: unsafe { FiberId::from_view(&view.id) },
            time_ms: view.time_ms,
            stop_handler: view.stop_handler != 0,
            changes: Batch::new(handle(view.changes)?),
            // SAFETY: the view lends its live, immutable snapshot for this borrow.
            snapshot: unsafe {
                handle(view.snapshot.cast_mut())?
                    .cast::<Snapshot>()
                    .as_ref()
            },
        })
    }

    pub fn id(&self) -> &FiberId {
        &self.id
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

    /// Reads the immutable snapshot of this phase.
    pub fn snapshot(&self) -> &'a Snapshot {
        self.snapshot
    }

    /// Stages the request at the given mailbox key.
    pub fn request<R: Request>(&mut self, key: &str, request: &R) -> Result<()> {
        let payload = to_vec(request)?;
        // SAFETY: the phase is live and exclusive and all input slices stay readable.
        check(unsafe {
            sys::siphon_run_request(
                self.phase.as_ptr(),
                slice(key.as_bytes()),
                slice(R::NAME.as_bytes()),
                slice(&payload),
            )
        })
    }

    /// Stages cancellation of a pending request in this Fiber's mailbox.
    pub fn cancel_request(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
        // SAFETY: the phase is live and exclusive and the key is readable.
        check(unsafe { sys::siphon_run_cancel_request(self.phase.as_ptr(), slice(key.as_ref())) })
    }

    /// Takes the observed request answer once per phase and consumes it when the phase commits.
    /// An absent or already taken delivery returns `None`.
    pub fn take_answer<R: Request>(&mut self, key: &str) -> Result<Option<Answer<R::Answer>>> {
        let mut kind = 0;
        let mut payload = sys::Buffer::default();
        // SAFETY: the phase is live and exclusive, the key is readable, and both outputs are writable.
        let status = unsafe {
            sys::siphon_run_take_answer(
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
        mailbox_result(status, mail)
    }

    /// Takes the observed mail once per phase, staging its consumption with the phase's changes.
    /// An absent or already taken entry returns `None`.
    pub fn take_mail(&mut self, key: &[u8]) -> Result<Option<Mail>> {
        let mut mail: Option<Mail> = None;
        // SAFETY: the phase is live and exclusive. The key is readable and the visitor has
        // exclusive access to mail during this call.
        let status = unsafe {
            sys::siphon_run_take_mail(
                self.phase.as_ptr(),
                slice(key),
                std::ptr::from_mut(&mut mail).cast(),
                Some(collect_mail),
            )
        };
        mailbox_result(status, mail)
    }

    /// Starts a direct child of this Fiber in the same commit.
    pub fn start<P: Fiber>(&mut self, key: impl AsRef<[u8]>, input: &P::Input) -> Result<()> {
        self.changes.start::<P>(Some(&self.id), key, input)?;
        Ok(())
    }
}

fn mailbox_result(status: sys::Status, mail: Option<Mail>) -> Result<Option<Mail>> {
    if !present(status)? {
        return Ok(None);
    }
    mail.map(Some).ok_or_else(|| Error {
        code: 4,
        message: "Siphon returned no mail for a successful mailbox read".into(),
    })
}

unsafe extern "C" fn collect_mail(context: *mut c_void, view: *const sys::MailView) {
    // SAFETY: the wrapper supplies an exclusive slot and the library lends a live mail view.
    let (mail, view) = unsafe { (&mut *context.cast::<Option<Mail>>(), &*view) };
    // SAFETY: all three slices are readable during this visit.
    *mail = Some(unsafe {
        Mail {
            key: Bytes::copy_from_slice(borrow_slice(view.key)),
            name: Bytes::copy_from_slice(borrow_slice(view.name)),
            payload: Bytes::copy_from_slice(borrow_slice(view.payload)),
        }
    });
}

unsafe extern "C" fn run_fiber<P: Fiber>(
    context: *mut c_void,
    run: *mut sys::Run,
    view: *const sys::PhaseView,
) {
    let executed = catch_unwind(AssertUnwindSafe(|| -> anyhow::Result<()> {
        // SAFETY: registration keeps context and callback data alive until this call returns.
        let (fiber, view) = unsafe { (&*context.cast::<P>(), &*view) };
        // SAFETY: the view lends both slices until this callback returns.
        let (input, checkpoint) =
            unsafe { (borrow_slice(view.input), borrow_slice(view.checkpoint)) };
        let input = from_slice(input)?;
        let checkpoint = (!checkpoint.is_empty())
            .then(|| from_slice(checkpoint))
            .transpose()?;
        let mut phase = handle(run)?;
        // SAFETY: the library lends the exclusive callback handle and its view in this invocation.
        let mut scoped = unsafe { Run::new(&mut phase, view)? };
        let ending = fiber.run(&mut scoped, input, checkpoint)?;
        finish(run, ending)?;
        Ok(())
    }));
    match executed {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let _ = end(run, sys::END_THROW, format!("{error:#}").as_bytes(), None);
        }
        Err(panic) => {
            let message = panic_message(panic.as_ref());
            let _ = end(
                run,
                sys::END_THROW,
                format!("the phase panicked: {message}").as_bytes(),
                None,
            );
            discard_panic(panic);
        }
    }
}

fn finish<C: Serialize, O: Serialize>(run: *mut sys::Run, ending: Ending<C, O>) -> Result<()> {
    // Serialize and drop fiber-owned values before publishing the ending, as either can panic.
    let (kind, payload) = match ending {
        Ending::Next(checkpoint) => (sys::END_NEXT, to_vec(&checkpoint)?),
        Ending::Wait(checkpoint, wait) => {
            let payload = to_vec(&checkpoint)?;
            drop(checkpoint);
            return end_wait(run, &payload, &wait);
        }
        Ending::Done(output) => (sys::END_DONE, to_vec(&output)?),
        Ending::Fail(message) => (sys::END_FAIL, message.into_bytes()),
        Ending::Stopped(output) => (
            sys::END_STOPPED,
            output.as_ref().map(to_vec).transpose()?.unwrap_or_default(),
        ),
    };
    end(run, kind, &payload, None)
}

fn end_wait(run: *mut sys::Run, payload: &[u8], wait: &Wait) -> Result<()> {
    let slices = |values: &[Vec<u8>]| values.iter().map(|value| slice(value)).collect::<Vec<_>>();
    let children = slices(&wait.children);
    let changes = slices(&wait.changes);
    let mail = slices(&wait.mail);
    let view = sys::WaitView {
        children: children.as_slice().into(),
        changes: changes.as_slice().into(),
        mail: mail.as_slice().into(),
        deadline: wait.deadline.as_ref().map_or(ptr::null(), ptr::from_ref),
    };
    end(run, sys::END_WAIT, payload, Some(&view))
}

fn end(run: *mut sys::Run, kind: u32, payload: &[u8], wait: Option<&sys::WaitView>) -> Result<()> {
    let ending = sys::EndingView {
        kind,
        payload: slice(payload),
        wait: wait.map_or(ptr::null(), ptr::from_ref),
    };
    // SAFETY: run is live within the callback. The ending and all borrowed values are live for
    // this call, and the library copies the values it retains.
    check(unsafe { sys::siphon_run_end(run, &ending) })
}
