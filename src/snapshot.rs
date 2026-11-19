use std::{
    ffi::c_void,
    ptr::{self, NonNull},
};

use bytes::Bytes;

use crate::{
    Error, Fiber, FiberId, Outcome, Result, borrow_slice, check, optional_slice, present, slice,
    sys, take,
};

/// Directory labels and annotations are byte pairs. Sequences refer to committed volume changes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Directory {
    pub labels: Vec<(Bytes, Bytes)>,
    pub annotations: Vec<(Bytes, Bytes)>,
    pub created_seq: u64,
    pub changed_seq: u64,
}

impl Directory {
    pub fn label(&self, name: &[u8]) -> Option<&Bytes> {
        self.labels
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
    }

    pub fn annotation(&self, name: &[u8]) -> Option<&Bytes> {
        self.annotations
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
    }
}

/// An owned entry from a snapshot. Directory metadata is present for directories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: Bytes,
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
    pub path: Bytes,
    pub value: Bytes,
    pub entry: Entry,
}

/// The paths changed in a single commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeSet {
    pub seq: u64,
    pub paths: Vec<Bytes>,
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

/// A Fiber's identity, committed result, and current reaping eligibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiberRecord {
    pub id: FiberId,
    pub kind: Bytes,
    pub version: u32,
    pub created_seq: u64,
    /// A wait deadline in milliseconds since the Unix epoch, when set.
    pub waits_until: Option<i64>,
    pub usage: Vec<(Bytes, u64)>,
    /// The Fiber has ended and its record can be reaped.
    pub reapable: bool,
    /// The recorded outcome. It can be available while descendants are still exiting.
    pub outcome: Option<Outcome>,
}

impl FiberRecord {
    /// Decodes a recorded outcome using the Fiber's output type. The kind and version must match.
    pub fn decode_outcome<P: Fiber>(&self) -> Result<Option<Outcome<P::Output>>> {
        if self.kind != P::KIND.as_bytes() || self.version != P::VERSION {
            return Err(Error {
                code: 2,
                message: "the Fiber record has a different kind or version".into(),
            });
        }
        let Some(outcome) = &self.outcome else {
            return Ok(None);
        };
        let outcome = match outcome {
            Outcome::Completed(result) => Outcome::Completed(decode(result.as_deref())?),
            Outcome::Failed { error, reason } => Outcome::Failed {
                error: error.clone(),
                reason: reason.clone(),
            },
            Outcome::Stopped { result, reason } => Outcome::Stopped {
                result: decode(result.as_deref())?,
                reason: reason.clone(),
            },
        };
        Ok(Some(outcome))
    }
}

/// A view of one committed revision. It may be retained or borrowed from a Fiber phase.
#[repr(transparent)]
#[derive(Debug)]
pub struct Snapshot(sys::Snapshot);

// SAFETY: snapshots are immutable and native supports concurrent reads of a live view.
unsafe impl Send for Snapshot {}
// SAFETY: as above.
unsafe impl Sync for Snapshot {}

/// A retained snapshot, independent of the runtime's subsequent commits.
#[derive(Debug)]
pub struct OwnedSnapshot(pub(crate) NonNull<Snapshot>);

// SAFETY: the view is immutable and its native resources are released by the owner.
unsafe impl Send for OwnedSnapshot {}
// SAFETY: as above. Freeing a snapshot requires exclusive Rust ownership.
unsafe impl Sync for OwnedSnapshot {}

impl std::ops::Deref for OwnedSnapshot {
    type Target = Snapshot;

    fn deref(&self) -> &Snapshot {
        // SAFETY: the owner holds this native snapshot until Drop.
        unsafe { self.0.as_ref() }
    }
}

impl Drop for OwnedSnapshot {
    fn drop(&mut self) {
        // SAFETY: the owner releases the native snapshot once.
        unsafe { sys::siphon_snapshot_free(self.0.as_ptr().cast()) };
    }
}

impl Snapshot {
    pub(crate) fn as_ptr(&self) -> *const sys::Snapshot {
        ptr::from_ref(self).cast()
    }

    pub fn read(&self, path: &[u8]) -> Result<Option<Bytes>> {
        self.read_range(path, 0, u64::MAX)
    }

    /// Lists direct child Fibers of the stated kind. An empty kind lists every child Fiber.
    pub fn fibers(&self, parent: &[u8], kind: &[u8]) -> Result<Vec<FiberId>> {
        let mut ids: Vec<FiberId> = Vec::new();
        // SAFETY: the snapshot and both inputs stay live. The visitor exclusively borrows ids.
        check(unsafe {
            sys::siphon_snapshot_fibers(
                self.as_ptr(),
                slice(parent),
                slice(kind),
                ptr::from_mut(&mut ids).cast(),
                Some(collect_fiber_id),
            )
        })?;
        Ok(ids)
    }

    pub fn stat(&self, path: &[u8]) -> Result<Option<Entry>> {
        let mut entries = Vec::new();
        // SAFETY: the snapshot and path are live. collect_entry receives exclusive access to entries.
        let status = unsafe {
            sys::siphon_snapshot_stat(
                self.as_ptr(),
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

    pub fn list(&self, path: &[u8], range: &Range<'_>) -> Result<Option<Vec<Entry>>> {
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
                self.as_ptr(),
                slice(path),
                range,
                ptr::from_mut(&mut entries).cast(),
                Some(collect_entry),
            )
        };
        Ok(present(status)?.then_some(entries))
    }

    pub fn find(&self, query: &Query<'_>, limit: usize) -> Result<Vec<Found>> {
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
                self.as_ptr(),
                query,
                ptr::from_mut(&mut matches).cast(),
                Some(collect_found),
            )
        })?;
        Ok(matches)
    }

    pub fn journal(&self, after: u64, limit: usize) -> Result<Journal> {
        let mut page = None;
        // SAFETY: the snapshot is live. The synchronous visitor writes into page exclusively.
        check(unsafe {
            sys::siphon_snapshot_journal(
                self.as_ptr(),
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

    pub fn sequence(&self) -> Result<u64> {
        let mut seq = 0;
        // SAFETY: live snapshot and writable output.
        check(unsafe { sys::siphon_snapshot_seq(self.as_ptr(), &mut seq) })?;
        Ok(seq)
    }
    pub fn read_range(&self, path: &[u8], offset: u64, length: u64) -> Result<Option<Bytes>> {
        let mut out = sys::Buffer::default();
        // SAFETY: snapshot is live, path borrowed for the call, output is writable.
        let status = unsafe {
            sys::siphon_snapshot_read(self.as_ptr(), slice(path), offset, length, &mut out)
        };
        let data = take(out);
        Ok(present(status)?.then_some(Bytes::from(data)))
    }
    pub fn fiber(&self, process: &[u8]) -> Result<Option<FiberRecord>> {
        let mut record: Option<Result<FiberRecord>> = None;
        // SAFETY: the snapshot and path are live. The visitor writes into record exclusively.
        let status = unsafe {
            sys::siphon_snapshot_fiber(
                self.as_ptr(),
                slice(process),
                ptr::from_mut(&mut record).cast(),
                Some(collect_fiber),
            )
        };
        if !present(status)? {
            return Ok(None);
        }
        record
            .ok_or_else(|| Error {
                code: 4,
                message: "Siphon returned no record for a successful Fiber read".into(),
            })?
            .map(Some)
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
unsafe fn copy_named(values: &sys::NamedSlice) -> Vec<(Bytes, Bytes)> {
    // SAFETY: the native library keeps the array and all of its byte slices live for the visit.
    unsafe { borrow_array(values.data, values.len) }
        .iter()
        .map(|item| {
            // SAFETY: both byte slices belong to this live metadata array.
            unsafe {
                (
                    Bytes::copy_from_slice(borrow_slice(item.name)),
                    Bytes::copy_from_slice(borrow_slice(item.value)),
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
            name: Bytes::copy_from_slice(borrow_slice(view.name)),
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
            path: Bytes::copy_from_slice(borrow_slice(path)),
            value: Bytes::copy_from_slice(borrow_slice(value)),
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
                .map(|path| Bytes::copy_from_slice(unsafe { borrow_slice(*path) }))
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

/// Safety: a present slice must remain readable for this copy.
unsafe fn copy_optional(value: sys::Slice) -> Option<Bytes> {
    // SAFETY: the native record keeps present byte slices readable during the visit.
    (!value.data.is_null()).then(|| Bytes::copy_from_slice(unsafe { borrow_slice(value) }))
}

/// Safety: the native record and its fields must remain live for this copy.
unsafe fn copy_fiber(view: &sys::FiberRecordView) -> Result<FiberRecord> {
    // SAFETY: the record holds both optional values until the visitor returns.
    let (payload, reason) = unsafe { (copy_optional(view.payload), copy_optional(view.reason)) };
    let outcome = match view.outcome {
        sys::OUTCOME_PENDING => None,
        sys::OUTCOME_COMPLETED => Some(Outcome::Completed(payload)),
        sys::OUTCOME_FAILED => Some(Outcome::Failed {
            error: payload,
            reason,
        }),
        sys::OUTCOME_STOPPED => Some(Outcome::Stopped {
            result: payload,
            reason,
        }),
        kind => {
            return Err(Error {
                code: 4,
                message: format!("library returned an unknown outcome kind: {kind}"),
            });
        }
    };
    // SAFETY: the native visitor keeps the usage array and every name live for this copy.
    let usage = unsafe { borrow_array(view.usage, view.usage_len) }
        .iter()
        .map(|entry| {
            (
                Bytes::copy_from_slice(unsafe { borrow_slice(entry.name) }),
                entry.amount,
            )
        })
        .collect();
    // SAFETY: kind is readable and waits_until is null or borrowed for this visit.
    let (kind, waits_until) = unsafe {
        (
            Bytes::copy_from_slice(borrow_slice(view.kind)),
            view.waits_until.as_ref().copied(),
        )
    };
    Ok(FiberRecord {
        // SAFETY: the record keeps its identity and path live through the visit.
        id: unsafe { FiberId::from_view(&view.id) },
        kind,
        version: view.version,
        created_seq: view.created_seq,
        waits_until,
        usage,
        reapable: view.reapable != 0,
        outcome,
    })
}

unsafe extern "C" fn collect_fiber(context: *mut c_void, view: *const sys::FiberRecordView) {
    // SAFETY: the wrapper supplies an exclusive slot and the library lends a live record.
    unsafe { *context.cast::<Option<Result<FiberRecord>>>() = Some(copy_fiber(&*view)) };
}

fn decode<T: serde::de::DeserializeOwned>(bytes: Option<&[u8]>) -> Result<Option<T>> {
    Ok(bytes.map(serde_json::from_slice).transpose()?)
}

unsafe extern "C" fn collect_fiber_id(context: *mut c_void, id: *const sys::FiberId) {
    // SAFETY: the identity and path are live and the visitor holds the output exclusively.
    unsafe { &mut *context.cast::<Vec<FiberId>>() }.push(unsafe { FiberId::from_view(&*id) });
}
