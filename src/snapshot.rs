use std::{ffi::c_void, ptr};

use crate::{Error, Fiber, Outcome, Result, borrow_slice, check, present, slice, sys, take};
use std::ptr::NonNull;

/// Directory labels and annotations are byte pairs. Sequences refer to committed volume changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Directory {
    pub labels: Vec<(Vec<u8>, Vec<u8>)>,
    pub annotations: Vec<(Vec<u8>, Vec<u8>)>,
    pub created_seq: u64,
    pub changed_seq: u64,
}

impl Directory {
    pub fn label(&self, name: &[u8]) -> Option<&[u8]> {
        self.labels
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_slice())
    }

    pub fn annotation(&self, name: &[u8]) -> Option<&[u8]> {
        self.annotations
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_slice())
    }
}

/// An owned entry from a snapshot. Directory metadata is present for directories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: Vec<u8>,
    pub length: u64,
    /// Expiration in milliseconds since the Unix epoch, when set.
    pub expires_at: Option<i64>,
    pub directory: Option<Directory>,
}

/// Limits a directory page by byte name, with an exclusive cursor in the requested direction.
#[derive(Debug, Clone, Copy, Default)]
pub struct Range<'a> {
    pub prefix: &'a [u8],
    pub after: Option<&'a [u8]>,
    pub reverse: bool,
    pub limit: usize,
}

#[derive(Debug)]
pub struct Snapshot(pub(crate) SnapshotHandle);
// SAFETY: native snapshots are immutable and support concurrent reads.
unsafe impl Send for Snapshot {}
// SAFETY: as above; Rust ownership prevents concurrent free.
unsafe impl Sync for Snapshot {}
impl Snapshot {
    /// Returns the entry at this byte path, if present.
    pub fn stat(&self, path: &[u8]) -> Result<Option<Entry>> {
        self.0.stat(path)
    }

    /// Reads a page in byte name order. A missing directory yields None and an empty page yields Some.
    pub fn list(&self, path: &[u8], range: &Range<'_>) -> Result<Option<Vec<Entry>>> {
        self.0.list(path, range)
    }

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
    pub fn outcome<P: Fiber>(&self, process: &[u8]) -> Result<Option<Outcome<P::Output>>> {
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
pub(crate) struct SnapshotHandle(pub(crate) NonNull<sys::Snapshot>);

impl SnapshotHandle {
    pub(crate) fn stat(&self, path: &[u8]) -> Result<Option<Entry>> {
        let mut entries = Vec::new();
        // SAFETY: the snapshot and path are live. collect_entry receives exclusive access to entries.
        let status = unsafe {
            sys::siphon_snapshot_stat(
                self.0.as_ptr(),
                slice(path),
                ptr::from_mut(&mut entries).cast(),
                Some(collect_entry),
            )
        };
        if !present(status)? {
            return Ok(None);
        }
        entries.pop().map(Some).ok_or_else(|| Error {
            code: 4,
            message: "Siphon returned no entry for a successful stat".into(),
        })
    }

    pub(crate) fn list(&self, path: &[u8], range: &Range<'_>) -> Result<Option<Vec<Entry>>> {
        let mut entries = Vec::new();
        let range = sys::SnapshotRange {
            prefix: slice(range.prefix),
            after: range.after.map_or_else(sys::Slice::default, slice),
            limit: range.limit,
            reverse: u8::from(range.reverse),
        };
        // SAFETY: the snapshot, path and range slices are live. The callback is synchronous.
        let status = unsafe {
            sys::siphon_snapshot_list(
                self.0.as_ptr(),
                slice(path),
                range,
                ptr::from_mut(&mut entries).cast(),
                Some(collect_entry),
            )
        };
        Ok(present(status)?.then_some(entries))
    }

    pub(crate) fn sequence(&self) -> Result<u64> {
        let mut seq = 0;
        // SAFETY: live snapshot and writable output.
        check(unsafe { sys::siphon_snapshot_seq(self.0.as_ptr(), &mut seq) })?;
        Ok(seq)
    }
    pub(crate) fn read_range(
        &self,
        path: &[u8],
        offset: u64,
        length: u64,
    ) -> Result<Option<Vec<u8>>> {
        let mut out = sys::Buffer::default();
        // SAFETY: snapshot is live, path borrowed for the call, output is writable.
        let status = unsafe {
            sys::siphon_snapshot_read(self.0.as_ptr(), slice(path), offset, length, &mut out)
        };
        let data = take(out);
        Ok(present(status)?.then_some(data))
    }
    pub(crate) fn outcome<P: Fiber>(&self, process: &[u8]) -> Result<Option<Outcome<P::Output>>> {
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

/// Safety: the native metadata view and its bytes must be valid for this visit.
unsafe fn copy_named(values: &sys::NamedSlice) -> Vec<(Vec<u8>, Vec<u8>)> {
    if values.len == 0 {
        return Vec::new();
    }
    // SAFETY: the native library keeps the array and all of its byte slices live for the visit.
    unsafe { std::slice::from_raw_parts(values.data, values.len) }
        .iter()
        .map(|item| {
            // SAFETY: both byte slices belong to this live metadata array.
            unsafe {
                (
                    borrow_slice(item.name).to_vec(),
                    borrow_slice(item.value).to_vec(),
                )
            }
        })
        .collect()
}

unsafe extern "C" fn collect_entry(context: *mut c_void, view: *const sys::EntryView) {
    // SAFETY: the wrapper passes a live Vec<Entry> and the library supplies a borrowed view.
    let (entries, view) = unsafe { (&mut *context.cast::<Vec<Entry>>(), &*view) };
    // SAFETY: nested pointers are null or refer to data held until this function returns.
    let entry = unsafe {
        Entry {
            name: borrow_slice(view.name).to_vec(),
            length: view.length,
            expires_at: view.expires_at.as_ref().copied(),
            directory: view.directory.as_ref().map(|directory| Directory {
                labels: copy_named(&directory.labels),
                annotations: copy_named(&directory.annotations),
                created_seq: directory.created_seq,
                changed_seq: directory.changed_seq,
            }),
        }
    };
    entries.push(entry);
}

fn decode<T: serde::de::DeserializeOwned>(bytes: Option<&[u8]>) -> Result<Option<T>> {
    Ok(bytes.map(serde_json::from_slice).transpose()?)
}
