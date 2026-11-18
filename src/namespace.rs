use std::{ffi::c_void, ptr, ptr::NonNull};

use bytes::Bytes;

use crate::{
    Error, Range, Result, RuntimeHandle, Status, borrow_slice, check, handle, optional_slice,
    present, slice, sys, take,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Directory,
    File,
    Link,
}

impl FileKind {
    fn decode(kind: u32) -> Result<Self> {
        match kind {
            1 => Ok(Self::Directory),
            2 => Ok(Self::File),
            3 => Ok(Self::Link),
            _ => Err(Error::new(Status::Failed, "unknown file kind")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stat {
    pub kind: FileKind,
    pub executable: bool,
    pub size: u64,
    pub modified_ms: i64,
    pub file_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dirent {
    pub name: Bytes,
    pub kind: FileKind,
}

/// Filesystem mounts shared by file operations and by commands.
#[derive(Debug)]
pub struct Namespace(pub(crate) NonNull<sys::Namespace>);

// SAFETY: native owns the mounts and synchronizes concurrent operations.
unsafe impl Send for Namespace {}
// SAFETY: as above. Rust requires unique ownership before freeing the namespace.
unsafe impl Sync for Namespace {}

impl Namespace {
    pub fn new() -> Result<Self> {
        let mut out = ptr::null_mut();
        // SAFETY: out is writable for the call.
        check(unsafe { sys::siphon_namespace_new(&mut out) })?;
        Ok(Self(handle(out)?))
    }

    /// Mounts a host directory. An empty mount point denotes the root.
    pub fn mount_host(&self, at: &str, host: &str, writable: bool) -> Result<()> {
        // SAFETY: the namespace and both paths are live for the call.
        check(unsafe {
            sys::siphon_namespace_mount_host(
                self.0.as_ptr(),
                slice(at.as_bytes()),
                slice(host.as_bytes()),
                u8::from(writable),
            )
        })
    }

    /// Hides a namespace path from all operations.
    pub fn hide(&self, at: &str) -> Result<()> {
        // SAFETY: the namespace and path are live.
        check(unsafe { sys::siphon_namespace_hide(self.0.as_ptr(), slice(at.as_bytes())) })
    }

    /// Mounts a read-only view of this volume's directories carrying the label.
    /// The view retains the volume until the namespace is dropped.
    pub fn mount_labels(
        &self,
        runtime: &RuntimeHandle,
        at: &str,
        kind: &str,
        label: &str,
        under: &[u8],
    ) -> Result<()> {
        // SAFETY: all inputs are live for the call.
        check(unsafe {
            sys::siphon_namespace_mount_labels(
                self.0.as_ptr(),
                runtime.0.as_ptr(),
                slice(at.as_bytes()),
                slice(kind.as_bytes()),
                slice(label.as_bytes()),
                slice(under),
            )
        })
    }

    pub fn stat(&self, path: &str) -> Result<Option<Stat>> {
        let mut stat: Result<Option<Stat>> = Ok(None);
        // SAFETY: the synchronous visitor has exclusive access to stat.
        let status = unsafe {
            sys::siphon_namespace_stat(
                self.0.as_ptr(),
                slice(path.as_bytes()),
                ptr::from_mut(&mut stat).cast(),
                Some(visit_file),
            )
        };
        if present(status)? { stat } else { Ok(None) }
    }

    pub fn list(&self, path: &str, range: &Range<'_>) -> Result<Vec<Dirent>> {
        let mut entries: Result<Vec<Dirent>> = Ok(Vec::new());
        let range = sys::SnapshotRange {
            prefix: slice(range.prefix),
            after: optional_slice(range.after),
            limit: range.limit,
            reverse: u8::from(range.reverse),
        };
        // SAFETY: the synchronous visitor has exclusive access to entries.
        check(unsafe {
            sys::siphon_namespace_list(
                self.0.as_ptr(),
                slice(path.as_bytes()),
                range,
                ptr::from_mut(&mut entries).cast(),
                Some(visit_dirent),
            )
        })?;
        entries
    }

    pub fn read(&self, path: &str, offset: u64, length: u64) -> Result<Bytes> {
        let mut out = sys::Buffer::default();
        // SAFETY: the namespace and path are live and out is writable.
        let status = unsafe {
            sys::siphon_namespace_read(
                self.0.as_ptr(),
                slice(path.as_bytes()),
                offset,
                length,
                &mut out,
            )
        };
        let data = take(out);
        check(status)?;
        Ok(data.into())
    }

    pub fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        // SAFETY: the namespace, path and data are live.
        check(unsafe {
            sys::siphon_namespace_write(self.0.as_ptr(), slice(path.as_bytes()), slice(data))
        })
    }

    pub fn mkdir(&self, path: &str) -> Result<()> {
        // SAFETY: the namespace and path are live.
        check(unsafe { sys::siphon_namespace_mkdir(self.0.as_ptr(), slice(path.as_bytes())) })
    }
}

impl Drop for Namespace {
    fn drop(&mut self) {
        // SAFETY: the Rust owner releases the namespace once.
        unsafe { sys::siphon_namespace_free(self.0.as_ptr()) };
    }
}

unsafe extern "C" fn visit_file(context: *mut c_void, view: *const sys::FileStat) {
    // SAFETY: these references and any borrowed fields are live for this call.
    let (out, view) = unsafe { (&mut *context.cast::<Result<Option<Stat>>>(), &*view) };
    *out = FileKind::decode(view.kind).map(|kind| {
        Some(Stat {
            kind,
            executable: view.executable != 0,
            size: view.size,
            modified_ms: view.modified_ms,
            file_id: unsafe { view.file_id.as_ref().copied() },
        })
    });
}

unsafe extern "C" fn visit_dirent(context: *mut c_void, name: sys::Slice, kind: u32) {
    // SAFETY: this visitor borrows the output exclusively for the call.
    let out = unsafe { &mut *context.cast::<Result<Vec<Dirent>>>() };
    if let Ok(entries) = out {
        match FileKind::decode(kind) {
            Ok(kind) => {
                let name = Bytes::copy_from_slice(unsafe { borrow_slice(name) });
                entries.push(Dirent { name, kind });
            }
            Err(error) => *out = Err(error),
        }
    }
}

/// Whether commands can be confined to namespace permissions on this host.
pub fn machine_confines() -> bool {
    // SAFETY: the probe takes no arguments or handles.
    unsafe { sys::siphon_machine_confines() != 0 }
}
