//! Safe Rust interface to a separately built Siphon binary.
//! Register fibers on a [`Builder`] and use a [`Run`] to stage the writes of each phase.
//! Register handlers on a host executor to answer typed [`Request`]s from fibers.
//! Calls are synchronous; async applications should use their blocking-task facility for long operations.
mod fiber;
mod request;
mod snapshot;
mod sys;

use std::{
    any::Any,
    ffi::c_void,
    mem::forget,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    ptr::NonNull,
};

pub use fiber::{Builder, Ending, Fiber, Options, Outcome, Retries, Run, Wait};
pub use request::{Answer, Asked, Request, Unanswered};
use snapshot::SnapshotHandle;
pub use snapshot::{
    ChangeSet, Directory, Entry, FiberRecord, Found, Journal, Query, Range, Snapshot,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub code: u32,
    pub message: String,
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
pub fn process_path(parent: &[u8], key: &[u8]) -> Result<Vec<u8>> {
    compatible()?;
    let mut out = sys::Buffer::default();
    // SAFETY: slices are readable for the call and out is writable.
    let status = unsafe { sys::siphon_process_path(slice(parent), slice(key), &mut out) };
    let path = take(out);
    check(status)?;
    Ok(path)
}

#[derive(Debug)]
pub struct Runtime(NonNull<sys::Runtime>);
// SAFETY: the native runtime serializes calls. Rust ownership prevents concurrent free.
unsafe impl Send for Runtime {}
// SAFETY: as above; closing requires an exclusive Rust borrow.
unsafe impl Sync for Runtime {}
impl Runtime {
    pub fn open(directory: &Path) -> Result<Self> {
        Builder::new()?.open(directory)
    }
    pub fn memory() -> Result<Self> {
        Builder::new()?.memory()
    }
    pub fn commit(&self, changes: &Changes) -> Result<u64> {
        let mut seq = 0;
        // SAFETY: handles remain live and changes are immutably borrowed during the call.
        check(unsafe {
            sys::siphon_runtime_commit(self.0.as_ptr(), changes.0.0.as_ptr(), &mut seq)
        })?;
        Ok(seq)
    }
    pub fn snapshot(&self) -> Result<Snapshot> {
        let mut out = std::ptr::null_mut();
        // SAFETY: runtime is live; out is writable.
        check(unsafe { sys::siphon_runtime_snapshot(self.0.as_ptr(), &mut out) })?;
        Ok(Snapshot(SnapshotHandle(handle(out)?)))
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

    /// Stops execution. Retained snapshots remain readable and may retain the storage lock.
    pub fn close(&mut self) -> Result<()> {
        // SAFETY: runtime is exclusively borrowed and remains allocated until Drop.
        check(unsafe { sys::siphon_runtime_close(self.0.as_ptr()) })
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        // SAFETY: this handle is uniquely owned and released once.
        let _ = check(unsafe { sys::siphon_runtime_free(self.0.as_ptr()) });
    }
}

/// Sets or removes one directory label or annotation.
#[derive(Debug, Clone, Copy)]
pub struct NameChange<'a> {
    pub name: &'a [u8],
    /// None removes the name. An empty slice sets an empty value.
    pub value: Option<&'a [u8]>,
}

/// A signal for a Fiber and its descendants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Signal {
    Hangup = 1,
    Interrupt = 2,
    Terminate = 3,
    Kill = 4,
    Stop = 5,
    Continue = 6,
}

#[derive(Debug)]
pub struct Changes(ChangeHandle);
// SAFETY: the batch can move between threads; mutation still requires exclusive access.
unsafe impl Send for Changes {}
impl Changes {
    pub fn new() -> Result<Self> {
        compatible()?;
        let mut out = std::ptr::null_mut();
        // SAFETY: out is writable.
        check(unsafe { sys::siphon_changes_new(&mut out) })?;
        Ok(Self(ChangeHandle(handle(out)?)))
    }
    pub fn mkdir(&mut self, path: &[u8]) -> Result<&mut Self> {
        self.0.mkdir(path)?;
        Ok(self)
    }
    /// Stages label and annotation changes for a directory in this batch.
    pub fn meta(
        &mut self,
        path: &[u8],
        labels: &[NameChange<'_>],
        annotations: &[NameChange<'_>],
    ) -> Result<&mut Self> {
        self.0.meta(path, labels, annotations)?;
        Ok(self)
    }
    pub fn write(&mut self, path: &[u8], data: &[u8]) -> Result<&mut Self> {
        self.0.write(path, data)?;
        Ok(self)
    }
    pub fn append(&mut self, path: &[u8], data: &[u8]) -> Result<&mut Self> {
        self.0.append(path, data)?;
        Ok(self)
    }
    /// Stages sealing a file in the batch.
    pub fn seal(&mut self, path: &[u8]) -> Result<&mut Self> {
        self.0.seal(path)?;
        Ok(self)
    }
    /// Stages expiration in milliseconds since the Unix epoch. None clears it.
    pub fn expire(&mut self, path: &[u8], deadline: Option<i64>) -> Result<&mut Self> {
        self.0.expire(path, deadline)?;
        Ok(self)
    }
    pub fn delete(&mut self, path: &[u8]) -> Result<&mut Self> {
        self.0.delete(path)?;
        Ok(self)
    }
    /// Stages a process start in this batch. An empty parent denotes the root.
    pub fn start<P: Fiber>(
        &mut self,
        parent: &[u8],
        key: &[u8],
        input: &P::Input,
    ) -> Result<&mut Self> {
        self.0.start::<P>(parent, key, input)?;
        Ok(self)
    }
    /// Stages a signal to the Fiber and its descendants.
    pub fn signal(
        &mut self,
        process: &[u8],
        signal: Signal,
        reason: Option<&[u8]>,
    ) -> Result<&mut Self> {
        self.0.signal(process, signal, reason)?;
        Ok(self)
    }
    /// Stages removal after the Fibers under path have had a chance to stop.
    pub fn remove(&mut self, path: &[u8], reason: Option<&[u8]>) -> Result<&mut Self> {
        self.0.remove(path, reason)?;
        Ok(self)
    }
    /// Stages removal by killing live Fibers under path.
    pub fn force_remove(&mut self, path: &[u8], reason: Option<&[u8]>) -> Result<&mut Self> {
        self.0.force_remove(path, reason)?;
        Ok(self)
    }
    /// Stages reaping an ended Fiber record.
    pub fn reap(&mut self, process: &[u8]) -> Result<&mut Self> {
        self.0.reap(process)?;
        Ok(self)
    }
    /// Stages usage charges to the Fiber and its ancestors.
    pub fn charge(&mut self, process: &[u8], counters: &[(&[u8], u64)]) -> Result<&mut Self> {
        self.0.charge(process, counters)?;
        Ok(self)
    }
}
impl Drop for Changes {
    fn drop(&mut self) {
        // SAFETY: batch is uniquely owned and released once.
        unsafe { sys::siphon_changes_free(self.0.0.as_ptr()) };
    }
}

// A live handle backed by either an owned batch or the current phase.
#[derive(Debug)]
struct ChangeHandle(NonNull<sys::Changes>);

impl ChangeHandle {
    fn mkdir(&mut self, path: &[u8]) -> Result<()> {
        // SAFETY: batch is exclusively borrowed, path lives for the call.
        check(unsafe { sys::siphon_changes_mkdir(self.0.as_ptr(), slice(path)) })
    }
    fn meta(
        &mut self,
        path: &[u8],
        labels: &[NameChange<'_>],
        annotations: &[NameChange<'_>],
    ) -> Result<()> {
        let encode = |updates: &[NameChange<'_>]| {
            updates
                .iter()
                .map(|update| sys::NamedBytes {
                    name: slice(update.name),
                    value: optional_slice(update.value),
                })
                .collect::<Vec<_>>()
        };
        let (labels, annotations) = (encode(labels), encode(annotations));
        // SAFETY: the batch is exclusively borrowed. The library copies the live arrays and slices.
        check(unsafe {
            sys::siphon_changes_meta(
                self.0.as_ptr(),
                slice(path),
                labels.as_slice().into(),
                annotations.as_slice().into(),
            )
        })
    }
    fn write(&mut self, path: &[u8], data: &[u8]) -> Result<()> {
        // SAFETY: batch is exclusively borrowed, slices live for the call.
        check(unsafe { sys::siphon_changes_write(self.0.as_ptr(), slice(path), slice(data)) })
    }
    fn append(&mut self, path: &[u8], data: &[u8]) -> Result<()> {
        // SAFETY: batch is exclusively borrowed, slices live for the call.
        check(unsafe { sys::siphon_changes_append(self.0.as_ptr(), slice(path), slice(data)) })
    }
    fn seal(&mut self, path: &[u8]) -> Result<()> {
        // SAFETY: the batch is exclusively borrowed and path is readable.
        check(unsafe { sys::siphon_changes_seal(self.0.as_ptr(), slice(path)) })
    }
    fn expire(&mut self, path: &[u8], deadline: Option<i64>) -> Result<()> {
        let at = deadline
            .as_ref()
            .map_or(std::ptr::null(), std::ptr::from_ref);
        // SAFETY: the batch is exclusively borrowed. Path and the optional deadline are live.
        check(unsafe { sys::siphon_changes_expire(self.0.as_ptr(), slice(path), at) })
    }
    fn delete(&mut self, path: &[u8]) -> Result<()> {
        // SAFETY: batch is exclusively borrowed, path lives for the call.
        check(unsafe { sys::siphon_changes_delete(self.0.as_ptr(), slice(path)) })
    }
    fn start<P: Fiber>(&mut self, parent: &[u8], key: &[u8], input: &P::Input) -> Result<()> {
        let input = serde_json::to_vec(input)?;
        // SAFETY: batch is exclusively borrowed, slices live for the call.
        check(unsafe {
            sys::siphon_changes_start(
                self.0.as_ptr(),
                slice(parent),
                slice(key),
                slice(P::KIND.as_bytes()),
                P::VERSION,
                slice(&input),
            )
        })
    }
    fn signal(&mut self, process: &[u8], signal: Signal, reason: Option<&[u8]>) -> Result<()> {
        // SAFETY: the batch is exclusively borrowed and both input slices are live.
        check(unsafe {
            sys::siphon_changes_signal(
                self.0.as_ptr(),
                slice(process),
                signal as u32,
                optional_slice(reason),
            )
        })
    }
    fn remove(&mut self, path: &[u8], reason: Option<&[u8]>) -> Result<()> {
        // SAFETY: the batch is exclusively borrowed and both input slices are live.
        check(unsafe {
            sys::siphon_changes_remove(self.0.as_ptr(), slice(path), optional_slice(reason))
        })
    }
    fn force_remove(&mut self, path: &[u8], reason: Option<&[u8]>) -> Result<()> {
        // SAFETY: the batch is exclusively borrowed and both input slices are live.
        check(unsafe {
            sys::siphon_changes_force_remove(self.0.as_ptr(), slice(path), optional_slice(reason))
        })
    }
    fn reap(&mut self, process: &[u8]) -> Result<()> {
        // SAFETY: the batch is exclusively borrowed and process is live.
        check(unsafe { sys::siphon_changes_reap(self.0.as_ptr(), slice(process)) })
    }
    fn charge(&mut self, process: &[u8], counters: &[(&[u8], u64)]) -> Result<()> {
        let counters: Vec<_> = counters
            .iter()
            .map(|&(name, amount)| sys::CounterView {
                name: slice(name),
                amount,
            })
            .collect();
        // SAFETY: the batch is exclusively borrowed. The library copies the live array and names.
        check(unsafe {
            sys::siphon_changes_charge(
                self.0.as_ptr(),
                slice(process),
                counters.as_ptr(),
                counters.len(),
            )
        })
    }
}

fn present(status: sys::Status) -> Result<bool> {
    match check(status) {
        Ok(()) => Ok(true),
        Err(error) if error.code == 1 => Ok(false),
        Err(error) => Err(error),
    }
}
