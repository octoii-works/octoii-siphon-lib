use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use bytes::Bytes;

use crate::{Error, Fiber, Result, check, handle, optional_slice, slice, sys};

/// Sets a label or annotation, or removes it when its value is absent.
pub type Named = (Bytes, Option<Bytes>);

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

/// Named mail addressed by a byte key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mail {
    pub key: Bytes,
    pub name: Bytes,
    pub payload: Bytes,
}

/// An owned set of changes to commit together.
#[derive(Debug)]
pub struct Changes(Batch<'static>);

// SAFETY: an owned batch can move between threads. Mutations require exclusive access.
unsafe impl Send for Changes {}

impl Changes {
    /// Allocates an empty batch.
    pub fn new() -> Self {
        let mut out = std::ptr::null_mut();
        // SAFETY: out is writable. Allocation has no other input that could be rejected.
        check(unsafe { sys::siphon_changes_new(&mut out) })
            .expect("a writable result allocates a batch");
        Self(Batch::new(
            handle(out).expect("successful allocation returns a batch"),
        ))
    }

    pub(crate) fn as_ptr(&self) -> *const sys::Changes {
        self.0.raw.0.as_ptr()
    }
}

impl Default for Changes {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for Changes {
    type Target = Batch<'static>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Changes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for Changes {
    fn drop(&mut self) {
        // SAFETY: the owned batch is freed once.
        unsafe { sys::siphon_changes_free(self.0.raw.0.as_ptr()) };
    }
}

/// Changes within an owned transaction or an executing Fiber phase. A staging failure
/// rejects the batch or throws the whole phase.
#[derive(Debug)]
pub struct Batch<'a> {
    raw: ChangeHandle,
    failure: Option<Error>,
    scope: PhantomData<&'a mut sys::Changes>,
}

impl Batch<'_> {
    pub(crate) fn new(raw: NonNull<sys::Changes>) -> Self {
        Self {
            raw: ChangeHandle(raw),
            failure: None,
            scope: PhantomData,
        }
    }

    pub(crate) fn ready(&self) -> Result<()> {
        self.failure
            .as_ref()
            .map_or(Ok(()), |error| Err(error.clone()))
    }

    fn stage(&mut self, status: Result<()>) -> &mut Self {
        if let Err(error) = status
            && self.failure.is_none()
        {
            self.failure = Some(error);
        }
        self
    }

    pub fn is_empty(&self) -> bool {
        // SAFETY: the batch is live and immutably borrowed.
        self.failure.is_none() && unsafe { sys::siphon_changes_is_empty(self.raw.0.as_ptr()) != 0 }
    }

    pub fn extend(&mut self, mut other: Changes) -> &mut Self {
        if let Some(failure) = other.failure.take() {
            return self.stage(Err(failure));
        }
        // SAFETY: these are live and distinct handles, both borrowed exclusively.
        let status =
            unsafe { sys::siphon_changes_extend(self.raw.0.as_ptr(), other.raw.0.as_ptr()) };
        self.stage(check(status))
    }

    pub fn mkdir(&mut self, path: impl AsRef<[u8]>) -> &mut Self {
        let status = self.raw.mkdir(path.as_ref());
        self.stage(status)
    }

    pub fn write(&mut self, path: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> &mut Self {
        let status = self.raw.write(path.as_ref(), data.as_ref());
        self.stage(status)
    }

    pub fn append(&mut self, path: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> &mut Self {
        let status = self.raw.append(path.as_ref(), data.as_ref());
        self.stage(status)
    }

    pub fn seal(&mut self, path: impl AsRef<[u8]>) -> &mut Self {
        let status = self.raw.seal(path.as_ref());
        self.stage(status)
    }

    pub fn expire(&mut self, path: impl AsRef<[u8]>, deadline: Option<i64>) -> &mut Self {
        let status = self.raw.expire(path.as_ref(), deadline);
        self.stage(status)
    }

    pub fn delete(&mut self, path: impl AsRef<[u8]>) -> &mut Self {
        let status = self.raw.delete(path.as_ref());
        self.stage(status)
    }

    pub fn meta(
        &mut self,
        path: impl AsRef<[u8]>,
        labels: impl AsRef<[Named]>,
        annotations: impl AsRef<[Named]>,
    ) -> &mut Self {
        let status = self
            .raw
            .meta(path.as_ref(), labels.as_ref(), annotations.as_ref());
        self.stage(status)
    }

    pub fn start<P: Fiber>(
        &mut self,
        parent: impl AsRef<[u8]>,
        key: impl AsRef<[u8]>,
        input: &P::Input,
    ) -> Result<&mut Self> {
        self.raw.start::<P>(parent.as_ref(), key.as_ref(), input)?;
        Ok(self)
    }

    pub fn send(
        &mut self,
        process: impl AsRef<[u8]>,
        mail: Mail,
        started: Option<u64>,
    ) -> &mut Self {
        let status = self.raw.send(process.as_ref(), &mail, started);
        self.stage(status)
    }

    pub fn signal(
        &mut self,
        process: impl AsRef<[u8]>,
        signal: Signal,
        reason: Option<Bytes>,
    ) -> &mut Self {
        let status = self.raw.signal(process.as_ref(), signal, reason.as_deref());
        self.stage(status)
    }

    pub fn remove(&mut self, path: impl AsRef<[u8]>, reason: Option<Bytes>) -> &mut Self {
        let status = self.raw.remove(path.as_ref(), reason.as_deref());
        self.stage(status)
    }

    pub fn force_remove(&mut self, path: impl AsRef<[u8]>, reason: Option<Bytes>) -> &mut Self {
        let status = self.raw.force_remove(path.as_ref(), reason.as_deref());
        self.stage(status)
    }

    pub fn reap(&mut self, process: impl AsRef<[u8]>) -> &mut Self {
        let status = self.raw.reap(process.as_ref());
        self.stage(status)
    }

    pub fn charge(
        &mut self,
        process: impl AsRef<[u8]>,
        counters: impl AsRef<[(Bytes, u64)]>,
    ) -> &mut Self {
        let status = self.raw.charge(process.as_ref(), counters.as_ref());
        self.stage(status)
    }

    pub fn limit(&mut self, process: impl AsRef<[u8]>, name: &[u8], maximum: u64) -> &mut Self {
        // SAFETY: the batch is exclusive and the borrowed slices stay live for the call.
        let status = unsafe {
            sys::siphon_changes_limit(
                self.raw.0.as_ptr(),
                slice(process.as_ref()),
                slice(name),
                maximum,
            )
        };
        self.stage(check(status))
    }
}

#[derive(Debug)]
struct ChangeHandle(NonNull<sys::Changes>);

impl ChangeHandle {
    fn mkdir(&mut self, path: &[u8]) -> Result<()> {
        // SAFETY: batch is exclusively borrowed, path lives for the call.
        check(unsafe { sys::siphon_changes_mkdir(self.0.as_ptr(), slice(path)) })
    }
    fn meta(&mut self, path: &[u8], labels: &[Named], annotations: &[Named]) -> Result<()> {
        let encode = |updates: &[Named]| {
            updates
                .iter()
                .map(|(name, value)| sys::NamedBytes {
                    name: slice(name),
                    value: optional_slice(value.as_deref()),
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
    fn send(&mut self, process: &[u8], mail: &Mail, started: Option<u64>) -> Result<()> {
        let mail = sys::MailView {
            key: slice(&mail.key),
            name: slice(&mail.name),
            payload: slice(&mail.payload),
        };
        let started = started
            .as_ref()
            .map_or(std::ptr::null(), std::ptr::from_ref);
        // SAFETY: the batch is exclusively borrowed. The library copies all the live inputs.
        check(unsafe { sys::siphon_changes_send(self.0.as_ptr(), slice(process), mail, started) })
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
    fn charge(&mut self, process: &[u8], counters: &[(Bytes, u64)]) -> Result<()> {
        let counters: Vec<_> = counters
            .iter()
            .map(|(name, amount)| sys::CounterView {
                name: slice(name),
                amount: *amount,
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
