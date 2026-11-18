use std::{
    ffi::c_void,
    ptr::{self, NonNull},
};

use crate::{
    Error, Fiber, Outcome, Result, borrow_slice, check, optional_slice, present, slice, sys, take,
};

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

/// Selects directories of one kind by their indexed label.
#[derive(Debug, Clone, Copy, Default)]
pub struct Query<'a> {
    pub kind: &'a [u8],
    pub name: &'a [u8],
    /// An exact label value when present. None matches every value.
    pub value: Option<&'a [u8]>,
    /// The subtree to search. An empty path searches the root.
    pub under: &'a [u8],
    /// An exclusive cursor containing the previous match's full path.
    pub after: Option<&'a [u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub path: Vec<u8>,
    pub value: Vec<u8>,
    pub entry: Entry,
}

/// The paths changed in a single commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeSet {
    pub seq: u64,
    pub paths: Vec<Vec<u8>>,
}

/// An owned page of the volume journal read through a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Journal {
    pub change_sets: Vec<ChangeSet>,
    /// The cursor after the commits read. Empty commits can advance this cursor.
    pub through: u64,
    /// The oldest sequence the volume retains at the time of the read.
    pub oldest: Option<u64>,
    /// The sequence of the snapshot.
    pub latest: u64,
    /// Some commits after the cursor are no longer retained.
    pub missed: bool,
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

    /// Finds indexed directories in full byte path order, up to the limit.
    pub fn find(&self, query: &Query<'_>, limit: usize) -> Result<Vec<Found>> {
        self.0.find(query, limit)
    }

    /// Reads at most limit commits strictly after the cursor.
    pub fn journal(&self, after: u64, limit: usize) -> Result<Journal> {
        self.0.journal(after, limit)
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
            after: optional_slice(range.after),
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

    pub(crate) fn find(&self, query: &Query<'_>, limit: usize) -> Result<Vec<Found>> {
        let mut matches = Vec::new();
        let query = sys::SnapshotQuery {
            kind: slice(query.kind),
            name: slice(query.name),
            value: optional_slice(query.value),
            under: slice(query.under),
            after: optional_slice(query.after),
            limit,
        };
        // SAFETY: the snapshot and query slices are live. The visitor has exclusive access to matches.
        check(unsafe {
            sys::siphon_snapshot_find(
                self.0.as_ptr(),
                query,
                ptr::from_mut(&mut matches).cast(),
                Some(collect_found),
            )
        })?;
        Ok(matches)
    }

    fn journal(&self, after: u64, limit: usize) -> Result<Journal> {
        let mut page = None;
        // SAFETY: the snapshot is live. The synchronous visitor writes into page exclusively.
        check(unsafe {
            sys::siphon_snapshot_journal(
                self.0.as_ptr(),
                after,
                limit,
                ptr::from_mut(&mut page).cast(),
                Some(collect_journal),
            )
        })?;
        page.ok_or_else(|| Error {
            code: 4,
            message: "Siphon returned no page for a successful journal read".into(),
        })
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

/// Safety: a nonempty array must be readable for the returned borrow.
unsafe fn borrow_array<'a, T>(data: *const T, len: usize) -> &'a [T] {
    if len == 0 {
        return &[];
    }
    // SAFETY: the native library keeps nonempty arrays readable for the visit.
    unsafe { std::slice::from_raw_parts(data, len) }
}

/// Safety: the native metadata view and its bytes must be valid for this visit.
unsafe fn copy_named(values: &sys::NamedSlice) -> Vec<(Vec<u8>, Vec<u8>)> {
    // SAFETY: the native library keeps the array and all of its byte slices live for the visit.
    unsafe { borrow_array(values.data, values.len) }
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

/// Safety: the native entry and nested views must remain live for this copy.
unsafe fn copy_entry(view: &sys::EntryView) -> Entry {
    // SAFETY: nested pointers are null or refer to data held through this copy.
    unsafe {
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
    }
}

unsafe extern "C" fn collect_entry(context: *mut c_void, view: *const sys::EntryView) {
    // SAFETY: the wrapper passes a live Vec<Entry> and the library supplies a borrowed entry.
    unsafe { &mut *context.cast::<Vec<Entry>>() }.push(unsafe { copy_entry(&*view) });
}

unsafe extern "C" fn collect_found(
    context: *mut c_void,
    path: sys::Slice,
    value: sys::Slice,
    view: *const sys::EntryView,
) {
    // SAFETY: the wrapper passes a live Vec<Found> and the result is borrowed during this visit.
    let matches = unsafe { &mut *context.cast::<Vec<Found>>() };
    matches.push(unsafe {
        Found {
            path: borrow_slice(path).to_vec(),
            value: borrow_slice(value).to_vec(),
            entry: copy_entry(&*view),
        }
    });
}

unsafe extern "C" fn collect_journal(context: *mut c_void, view: *const sys::JournalView) {
    // SAFETY: the wrapper passes a live output slot and the library supplies a borrowed page.
    let (page, view) = unsafe { (&mut *context.cast::<Option<Journal>>(), &*view) };
    // SAFETY: the native page owns all the change set views for this visit.
    let change_sets = unsafe { borrow_array(view.change_sets, view.change_sets_len) }
        .iter()
        .map(|set| {
            // SAFETY: the path array and its byte slices remain readable through this visit.
            let paths = unsafe { borrow_array(set.paths, set.paths_len) }
                .iter()
                .map(|path| unsafe { borrow_slice(*path) }.to_vec())
                .collect();
            ChangeSet {
                seq: set.seq,
                paths,
            }
        })
        .collect();
    *page = Some(Journal {
        change_sets,
        through: view.through,
        // SAFETY: oldest is null or points into the live journal page.
        oldest: unsafe { view.oldest.as_ref().copied() },
        latest: view.latest,
        missed: view.missed != 0,
    });
}

fn decode<T: serde::de::DeserializeOwned>(bytes: Option<&[u8]>) -> Result<Option<T>> {
    Ok(bytes.map(serde_json::from_slice).transpose()?)
}
