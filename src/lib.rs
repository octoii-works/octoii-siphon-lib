//! Safe Rust interface to a separately built Siphon binary.
//! This development slice covers atomic changes, snapshots and persistent reopening.
//! Calls are synchronous; async applications should use their blocking-task facility for long operations.
mod sys;

use std::{path::Path, ptr::NonNull};

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
pub type Result<T> = std::result::Result<T, Error>;

fn slice(bytes: &[u8]) -> sys::Slice {
    sys::Slice {
        data: bytes.as_ptr(),
        len: bytes.len(),
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
    if unsafe { sys::siphon_abi_version() } == 1 {
        Ok(())
    } else {
        Err(Error {
            code: 2,
            message: "Siphon ABI mismatch".into(),
        })
    }
}
fn owned<T>(ptr: *mut T) -> Result<NonNull<T>> {
    NonNull::new(ptr).ok_or_else(|| Error {
        code: 4,
        message: "library returned a null handle".into(),
    })
}

#[derive(Debug)]
pub struct Runtime(NonNull<sys::Runtime>);
// SAFETY: the native runtime serializes calls. Rust ownership prevents concurrent free.
unsafe impl Send for Runtime {}
// SAFETY: as above; closing requires an exclusive Rust borrow.
unsafe impl Sync for Runtime {}
impl Runtime {
    pub fn open(directory: &Path) -> Result<Self> {
        let directory = directory
            .to_str()
            .filter(|path| !path.is_empty())
            .ok_or_else(|| Error {
                code: 2,
                message: "directory must be nonempty UTF-8".into(),
            })?;
        Self::open_bytes(directory.as_bytes())
    }
    pub fn memory() -> Result<Self> {
        Self::open_bytes(b"")
    }
    fn open_bytes(directory: &[u8]) -> Result<Self> {
        compatible()?;
        let mut out = std::ptr::null_mut();
        // SAFETY: slice lives through the call; out is writable.
        check(unsafe { sys::siphon_runtime_open(slice(directory), &mut out) })?;
        Ok(Self(owned(out)?))
    }
    pub fn commit(&self, changes: &Changes) -> Result<u64> {
        let mut seq = 0;
        // SAFETY: handles remain live and changes are immutably borrowed during the call.
        check(unsafe {
            sys::siphon_runtime_commit(self.0.as_ptr(), changes.0.as_ptr(), &mut seq)
        })?;
        Ok(seq)
    }
    pub fn snapshot(&self) -> Result<Snapshot> {
        let mut out = std::ptr::null_mut();
        // SAFETY: runtime is live; out is writable.
        check(unsafe { sys::siphon_runtime_snapshot(self.0.as_ptr(), &mut out) })?;
        Ok(Snapshot(owned(out)?))
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
pub struct Changes(NonNull<sys::Changes>);
// SAFETY: the batch can move between threads; mutation still requires exclusive access.
unsafe impl Send for Changes {}
impl Changes {
    pub fn new() -> Result<Self> {
        compatible()?;
        let mut out = std::ptr::null_mut();
        // SAFETY: out is writable.
        check(unsafe { sys::siphon_changes_new(&mut out) })?;
        Ok(Self(owned(out)?))
    }
    pub fn mkdir(&mut self, path: &[u8]) -> Result<&mut Self> {
        // SAFETY: batch is exclusively borrowed, path lives for the call.
        check(unsafe { sys::siphon_changes_mkdir(self.0.as_ptr(), slice(path)) })?;
        Ok(self)
    }
    pub fn write(&mut self, path: &[u8], data: &[u8]) -> Result<&mut Self> {
        // SAFETY: batch is exclusively borrowed, slices live for the call.
        check(unsafe { sys::siphon_changes_write(self.0.as_ptr(), slice(path), slice(data)) })?;
        Ok(self)
    }
    pub fn append(&mut self, path: &[u8], data: &[u8]) -> Result<&mut Self> {
        // SAFETY: batch is exclusively borrowed, slices live for the call.
        check(unsafe { sys::siphon_changes_append(self.0.as_ptr(), slice(path), slice(data)) })?;
        Ok(self)
    }
    pub fn delete(&mut self, path: &[u8]) -> Result<&mut Self> {
        // SAFETY: batch is exclusively borrowed, path lives for the call.
        check(unsafe { sys::siphon_changes_delete(self.0.as_ptr(), slice(path)) })?;
        Ok(self)
    }
}
impl Drop for Changes {
    fn drop(&mut self) {
        // SAFETY: batch is uniquely owned and released once.
        unsafe { sys::siphon_changes_free(self.0.as_ptr()) };
    }
}

#[derive(Debug)]
pub struct Snapshot(NonNull<sys::Snapshot>);
// SAFETY: native snapshots are immutable and support concurrent reads.
unsafe impl Send for Snapshot {}
// SAFETY: as above; Rust ownership prevents concurrent free.
unsafe impl Sync for Snapshot {}
impl Snapshot {
    pub fn sequence(&self) -> Result<u64> {
        let mut seq = 0;
        // SAFETY: live snapshot and writable output.
        check(unsafe { sys::siphon_snapshot_seq(self.0.as_ptr(), &mut seq) })?;
        Ok(seq)
    }
    pub fn read(&self, path: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read_range(path, 0, u64::MAX)
    }
    pub fn read_range(&self, path: &[u8], offset: u64, length: u64) -> Result<Option<Vec<u8>>> {
        let mut out = sys::Buffer {
            data: std::ptr::null_mut(),
            len: 0,
        };
        // SAFETY: snapshot is live, path borrowed for the call, output is writable.
        let status = unsafe {
            sys::siphon_snapshot_read(self.0.as_ptr(), slice(path), offset, length, &mut out)
        };
        let data = take(out);
        match check(status) {
            Ok(()) => Ok(Some(data)),
            Err(error) if error.code == 1 => Ok(None),
            Err(error) => Err(error),
        }
    }
}
impl Drop for Snapshot {
    fn drop(&mut self) {
        // SAFETY: snapshot is uniquely owned and released once.
        unsafe { sys::siphon_snapshot_free(self.0.as_ptr()) };
    }
}
