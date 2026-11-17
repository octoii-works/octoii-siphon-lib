//! Safe Rust interface to a separately built Siphon binary.
//! Register programs on a [`Builder`] and use a [`Run`] to stage the writes of each phase.
//! Register handlers on a host executor to answer typed [`Request`]s from programs.
//! Calls are synchronous; async applications should use their blocking-task facility for long operations.
mod program;
mod request;
mod sys;

use std::{
    any::Any,
    ffi::c_void,
    mem::forget,
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    ptr::NonNull,
};

pub use program::{Builder, Ending, Options, Outcome, Program, Retries, Run, Wait};
pub use request::{Answer, Asked, Request, Unanswered};

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
fn compatible() -> Result<()> {
    // SAFETY: this function has no arguments or preconditions.
    if unsafe { sys::siphon_abi_version() } == 3 {
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
    pub fn write(&mut self, path: &[u8], data: &[u8]) -> Result<&mut Self> {
        self.0.write(path, data)?;
        Ok(self)
    }
    pub fn append(&mut self, path: &[u8], data: &[u8]) -> Result<&mut Self> {
        self.0.append(path, data)?;
        Ok(self)
    }
    pub fn delete(&mut self, path: &[u8]) -> Result<&mut Self> {
        self.0.delete(path)?;
        Ok(self)
    }
    /// Stages a process start in this batch. An empty parent denotes the root.
    pub fn start<P: Program>(
        &mut self,
        parent: &[u8],
        key: &[u8],
        input: &P::Input,
    ) -> Result<&mut Self> {
        self.0.start::<P>(parent, key, input)?;
        Ok(self)
    }
    /// Stages a request for the process and its descendants to stop.
    pub fn terminate(&mut self, process: &[u8]) -> Result<&mut Self> {
        self.0.terminate(process)?;
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
    fn write(&mut self, path: &[u8], data: &[u8]) -> Result<()> {
        // SAFETY: batch is exclusively borrowed, slices live for the call.
        check(unsafe { sys::siphon_changes_write(self.0.as_ptr(), slice(path), slice(data)) })
    }
    fn append(&mut self, path: &[u8], data: &[u8]) -> Result<()> {
        // SAFETY: batch is exclusively borrowed, slices live for the call.
        check(unsafe { sys::siphon_changes_append(self.0.as_ptr(), slice(path), slice(data)) })
    }
    fn delete(&mut self, path: &[u8]) -> Result<()> {
        // SAFETY: batch is exclusively borrowed, path lives for the call.
        check(unsafe { sys::siphon_changes_delete(self.0.as_ptr(), slice(path)) })
    }
    fn start<P: Program>(&mut self, parent: &[u8], key: &[u8], input: &P::Input) -> Result<()> {
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
    fn terminate(&mut self, process: &[u8]) -> Result<()> {
        // SAFETY: batch is exclusively borrowed, process lives for the call.
        check(unsafe { sys::siphon_changes_terminate(self.0.as_ptr(), slice(process)) })
    }
}

#[derive(Debug)]
pub struct Snapshot(SnapshotHandle);
// SAFETY: native snapshots are immutable and support concurrent reads.
unsafe impl Send for Snapshot {}
// SAFETY: as above; Rust ownership prevents concurrent free.
unsafe impl Sync for Snapshot {}
impl Snapshot {
    pub fn sequence(&self) -> Result<u64> {
        self.0.sequence()
    }
    pub fn read(&self, path: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read_range(path, 0, u64::MAX)
    }
    pub fn read_range(&self, path: &[u8], offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        self.0.read_range(path, offset, length)
    }
    /// Reads the outcome at `process`, returning `None` when the path has no process record.
    pub fn outcome<P: Program>(&self, process: &[u8]) -> Result<Option<Outcome<P::Output>>> {
        self.0.outcome::<P>(process)
    }
}
impl Drop for Snapshot {
    fn drop(&mut self) {
        // SAFETY: snapshot is uniquely owned and released once.
        unsafe { sys::siphon_snapshot_free(self.0.0.as_ptr()) };
    }
}

// A live handle backed by either a retained snapshot or the current phase.
#[derive(Debug)]
struct SnapshotHandle(NonNull<sys::Snapshot>);

impl SnapshotHandle {
    fn sequence(&self) -> Result<u64> {
        let mut seq = 0;
        // SAFETY: live snapshot and writable output.
        check(unsafe { sys::siphon_snapshot_seq(self.0.as_ptr(), &mut seq) })?;
        Ok(seq)
    }
    fn read_range(&self, path: &[u8], offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let mut out = sys::Buffer::default();
        // SAFETY: snapshot is live, path borrowed for the call, output is writable.
        let status = unsafe {
            sys::siphon_snapshot_read(self.0.as_ptr(), slice(path), offset, length, &mut out)
        };
        let data = take(out);
        Ok(present(status)?.then_some(data))
    }
    fn outcome<P: Program>(&self, process: &[u8]) -> Result<Option<Outcome<P::Output>>> {
        let mut kind = sys::OUTCOME_PENDING;
        let mut payload = sys::Buffer::default();
        let mut reason = sys::Buffer::default();
        // SAFETY: snapshot is live, process is readable for the call, outputs are writable.
        let status = unsafe {
            sys::siphon_snapshot_outcome(
                self.0.as_ptr(),
                slice(process),
                &mut kind,
                &mut payload,
                &mut reason,
            )
        };
        let payload = take(payload);
        let reason = take(reason);
        if !present(status)? {
            return Ok(None);
        }
        let payload = (!payload.is_empty()).then_some(payload);
        let reason = (!reason.is_empty()).then_some(reason);
        let outcome = match kind {
            sys::OUTCOME_PENDING => Outcome::Pending,
            sys::OUTCOME_COMPLETED => Outcome::Completed(decode(payload.as_deref())?),
            sys::OUTCOME_FAILED => Outcome::Failed {
                error: payload,
                reason,
            },
            sys::OUTCOME_STOPPED => Outcome::Stopped {
                result: decode(payload.as_deref())?,
                reason,
            },
            _ => {
                return Err(Error {
                    code: 4,
                    message: format!("library returned an unknown outcome kind: {kind}"),
                });
            }
        };
        Ok(Some(outcome))
    }
}

fn present(status: sys::Status) -> Result<bool> {
    match check(status) {
        Ok(()) => Ok(true),
        Err(error) if error.code == 1 => Ok(false),
        Err(error) => Err(error),
    }
}

fn decode<T: serde::de::DeserializeOwned>(bytes: Option<&[u8]>) -> Result<Option<T>> {
    Ok(bytes.map(serde_json::from_slice).transpose()?)
}
