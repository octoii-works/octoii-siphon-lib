//! Safe Rust interface to a separately built Siphon binary.
//! Register fibers on a [`Builder`] and use a [`Run`] to stage the writes of each phase.
//! Register handlers on a host executor to answer typed [`Request`]s from fibers.
//! Calls are synchronous; async applications should use their blocking-task facility for long operations.
mod changes;
mod command;
mod events;
mod fiber;
mod namespace;
mod person;
mod request;
mod snapshot;
mod stream;
mod sys;

use std::{
    any::Any,
    ffi::c_void,
    mem::{ManuallyDrop, forget},
    ops::Deref,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    ptr::NonNull,
    sync::Arc,
};

use tokio::sync::watch;

pub use changes::{Batch, Changes, Mail, Named, Signal};
pub use command::{Chunk, Command, CommandEvent, CommandEvents, Exec, Exit, Screen, Spec, Stdio};
pub use fiber::{Builder, Ending, Fiber, Options, Outcome, Retries, Run, Wait};
pub use namespace::{Dirent, FileKind, Namespace, Stat, machine_confines};
pub use person::{AskedOfPerson, PersonRequest};
pub use request::{Answer, Asked, Request, RequestId, Unanswered};
pub use snapshot::{
    ChangeSet, Directory, Entry, FiberRecord, Found, Journal, OwnedSnapshot, Query, Range, Snapshot,
};

pub use stream::{Following, Published, StreamEvent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub code: u32,
    pub message: String,
}
/// The status returned by the native operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Status {
    NotFound = 1,
    Invalid = 2,
    Closed = 3,
    Failed = 4,
    Panicked = 5,
    CallbackReentry = 6,
    Denied = 7,
    ReadOnly = 8,
    Exists = 9,
    Unsupported = 10,
    MissingProgram = 11,
    Refused = 12,
    WriteAborted = 13,
    ReopenRequired = 14,
    Unknown = u32::MAX,
}

impl Error {
    pub fn new(status: Status, message: impl Into<String>) -> Self {
        Self {
            code: status as u32,
            message: message.into(),
        }
    }

    pub fn status(&self) -> Status {
        match self.code {
            1 => Status::NotFound,
            2 => Status::Invalid,
            3 => Status::Closed,
            4 => Status::Failed,
            5 => Status::Panicked,
            6 => Status::CallbackReentry,
            7 => Status::Denied,
            8 => Status::ReadOnly,
            9 => Status::Exists,
            10 => Status::Unsupported,
            11 => Status::MissingProgram,
            12 => Status::Refused,
            13 => Status::WriteAborted,
            14 => Status::ReopenRequired,
            _ => Status::Unknown,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for Error {}
impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self {
            code: 4,
            message: error.to_string(),
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;

fn slice(bytes: &[u8]) -> sys::Slice {
    sys::Slice {
        data: bytes.as_ptr(),
        len: bytes.len(),
    }
}

fn optional_slice(bytes: Option<&[u8]>) -> sys::Slice {
    bytes.map_or_else(sys::Slice::default, slice)
}

/// Safety: the slice must remain readable for the returned borrow.
unsafe fn borrow_slice<'a>(value: sys::Slice) -> &'a [u8] {
    if value.len == 0 {
        return &[];
    }
    // SAFETY: the caller guarantees the bytes are readable for this borrow.
    unsafe { std::slice::from_raw_parts(value.data, value.len) }
}

unsafe extern "C" fn release_context<T>(context: *mut c_void) {
    // SAFETY: release is called once for this boxed context, after its final use.
    let context = unsafe { Box::from_raw(context.cast::<T>()) };
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(context))) {
        discard_panic(panic);
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("no message")
}

fn discard_panic(panic: Box<dyn Any + Send>) {
    // A custom panic payload can itself panic on drop. Never let that unwind into C.
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(panic))) {
        forget(panic);
    }
}

fn take(buffer: sys::Buffer) -> Vec<u8> {
    // SAFETY: only library-owned result buffers reach this function; bytes are copied before release.
    unsafe {
        let bytes = if buffer.len == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts(buffer.data, buffer.len).to_vec()
        };
        sys::siphon_buffer_free(buffer);
        bytes
    }
}
fn check(status: sys::Status) -> Result<()> {
    let error = take(status.error);
    if status.code == 0 {
        Ok(())
    } else {
        Err(Error {
            code: status.code,
            message: String::from_utf8_lossy(&error).into_owned(),
        })
    }
}
fn decode_id(status: sys::Status, buffer: sys::Buffer) -> Result<String> {
    let id = take(buffer);
    check(status)?;
    String::from_utf8(id).map_err(|error| Error {
        code: 4,
        message: format!("Siphon returned an invalid ID: {error}"),
    })
}
fn compatible() -> Result<()> {
    // SAFETY: this function has no arguments or preconditions.
    if unsafe { sys::siphon_abi_version() } == 4 {
        Ok(())
    } else {
        Err(Error {
            code: 2,
            message: "Siphon ABI mismatch".into(),
        })
    }
}
fn handle<T>(ptr: *mut T) -> Result<NonNull<T>> {
    NonNull::new(ptr).ok_or_else(|| Error {
        code: 4,
        message: "library returned a null handle".into(),
    })
}

/// The address of the process keyed by `key` under `parent`. An empty parent denotes the root.
pub fn process_path(parent: &[u8], key: &[u8]) -> bytes::Bytes {
    let mut out = sys::Buffer::default();
    // SAFETY: slices are readable for the call and out is writable.
    let status = unsafe { sys::siphon_process_path(slice(parent), slice(key), &mut out) };
    let path = take(out);
    check(status).expect("readable slices produce a fiber path");
    path.into()
}

/// Owns the running volume and its executor. The last owner starts shutdown.
#[derive(Debug, Clone)]
pub struct Runtime(Arc<Owner>);

#[derive(Debug)]
struct Owner {
    handle: ManuallyDrop<RuntimeHandle>,
}

impl Runtime {
    pub fn open(directory: &Path) -> Result<Self> {
        Builder::new()?.open(directory)
    }
    pub fn memory() -> Result<Self> {
        Builder::new()?.memory()
    }

    fn from_open(ptr: *mut sys::Runtime) -> Result<Self> {
        Ok(Self(Arc::new(Owner {
            handle: ManuallyDrop::new(RuntimeHandle(handle(ptr)?)),
        })))
    }

    /// Retains access without taking ownership of runtime shutdown.
    pub fn runtime_handle(&self) -> RuntimeHandle {
        (**self).clone()
    }

    /// Stops execution and waits for the worker. Retained snapshots can still hold the volume lock.
    pub fn close(&self) -> Result<()> {
        // SAFETY: the owner's reference stays live during the call.
        check(unsafe { sys::siphon_runtime_close(self.0.handle.0.as_ptr()) })
    }
}

impl Deref for Runtime {
    type Target = RuntimeHandle;

    fn deref(&self) -> &RuntimeHandle {
        &self.0.handle
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        // SAFETY: native free consumes the runtime owner's reference exactly once.
        let _ = check(unsafe { sys::siphon_runtime_free(self.handle.0.as_ptr()) });
    }
}

/// Access to a runtime owned elsewhere. Commands reject use after the owner closes it.
#[derive(Debug)]
pub struct RuntimeHandle(NonNull<sys::Runtime>);

// SAFETY: every handle owns a native reference and native serializes command admission.
unsafe impl Send for RuntimeHandle {}
// SAFETY: native commands accept concurrent callers.
unsafe impl Sync for RuntimeHandle {}

impl Clone for RuntimeHandle {
    fn clone(&self) -> Self {
        // SAFETY: this handle owns a live reference.
        unsafe { sys::siphon_runtime_retain(self.0.as_ptr()) };
        Self(self.0)
    }
}

impl Drop for RuntimeHandle {
    fn drop(&mut self) {
        // SAFETY: this is the only release of this retained reference.
        unsafe { sys::siphon_runtime_release(self.0.as_ptr()) };
    }
}

impl RuntimeHandle {
    fn from_retained(out: *const sys::Runtime) -> Result<Self> {
        Ok(Self(handle(out.cast_mut())?))
    }

    /// Commits a batch without blocking the caller's asynchronous executor.
    pub async fn commit(&self, changes: Changes) -> Result<u64> {
        let runtime = self.clone();
        tokio::task::spawn_blocking(move || runtime.commit_blocking(&changes))
            .await
            .map_err(|error| Error::new(Status::Failed, error.to_string()))?
    }

    /// Commits a batch on a synchronous caller thread.
    pub fn commit_blocking(&self, changes: &Changes) -> Result<u64> {
        changes.ready()?;
        let mut seq = 0;
        // SAFETY: handles remain live and changes are immutably borrowed during the call.
        check(unsafe { sys::siphon_runtime_commit(self.0.as_ptr(), changes.as_ptr(), &mut seq) })?;
        Ok(seq)
    }
    pub fn snapshot(&self) -> Result<OwnedSnapshot> {
        let mut out = std::ptr::null_mut();
        // SAFETY: runtime is live; out is writable.
        check(unsafe { sys::siphon_runtime_snapshot(self.0.as_ptr(), &mut out) })?;
        Ok(OwnedSnapshot(handle(out)?.cast()))
    }
    /// Mints a UUIDv7 from this volume.
    pub fn new_id(&self) -> Result<String> {
        let mut out = sys::Buffer::default();
        // SAFETY: the runtime is live and out is writable.
        let status = unsafe { sys::siphon_runtime_new_id(self.0.as_ptr(), &mut out) };
        decode_id(status, out)
    }

    /// Reads the runtime clock in milliseconds since the Unix epoch.
    pub fn now_ms(&self) -> Result<i64> {
        let mut now = 0;
        // SAFETY: the runtime is live and now is writable.
        check(unsafe { sys::siphon_runtime_now_ms(self.0.as_ptr(), &mut now) })?;
        Ok(now)
    }

    /// Watches the latest committed sequence. Read the snapshot journal for change details.
    pub fn committed(&self) -> Result<CommitWatch> {
        let (sender, mut receiver) = watch::channel(0);
        let context = Box::into_raw(Box::new(sender)).cast();
        let mut out = std::ptr::null_mut();
        // SAFETY: the runtime and output are live. The native call takes ownership of context.
        check(unsafe {
            sys::siphon_runtime_watch_commits(
                self.0.as_ptr(),
                context,
                Some(received_commit),
                Some(release_context::<watch::Sender<u64>>),
                &mut out,
            )
        })?;
        let handle = handle(out)?;
        receiver.mark_unchanged();
        Ok(CommitWatch { handle, receiver })
    }
}

/// Observes the latest committed sequence. Notifications can coalesce commits.
#[derive(Debug)]
pub struct CommitWatch {
    handle: NonNull<sys::CommitWatch>,
    receiver: watch::Receiver<u64>,
}

// SAFETY: the native watch is owned. Its only operation here is cancellation on drop.
unsafe impl Send for CommitWatch {}

impl CommitWatch {
    /// The latest sequence delivered to this watch.
    pub fn sequence(&self) -> u64 {
        *self.receiver.borrow()
    }

    /// Waits for progress and returns the latest sequence. Closing the runtime ends the watch.
    pub async fn changed(&mut self) -> std::result::Result<u64, watch::error::RecvError> {
        self.receiver.changed().await?;
        Ok(*self.receiver.borrow_and_update())
    }
}

impl Drop for CommitWatch {
    fn drop(&mut self) {
        // SAFETY: the handle is uniquely owned and freed once.
        unsafe { sys::siphon_commit_watch_free(self.handle.as_ptr()) };
    }
}

unsafe extern "C" fn received_commit(context: *mut c_void, seq: u64) {
    // SAFETY: the native registration keeps the boxed sender alive through its last notification.
    let sender = unsafe { &*context.cast::<watch::Sender<u64>>() };
    sender.send_replace(seq);
}

fn present(status: sys::Status) -> Result<bool> {
    match check(status) {
        Ok(()) => Ok(true),
        Err(error) if error.code == 1 => Ok(false),
        Err(error) => Err(error),
    }
}
