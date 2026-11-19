use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::{self, NonNull},
};

use bytes::Bytes;

use crate::{
    Fiber, FiberId, Result, Snapshot, check, compatible, handle, optional_slice, slice, sys,
};

/// Sets a label or annotation, or removes it when its value is absent.
pub type Named = (Bytes, Option<Bytes>);

/// A signal for a Fiber and its descendants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Signal {
    Hangup = sys::SIGNAL_HANGUP,
    Interrupt = sys::SIGNAL_INTERRUPT,
    Terminate = sys::SIGNAL_TERMINATE,
    Kill = sys::SIGNAL_KILL,
    Stop = sys::SIGNAL_STOP,
    Continue = sys::SIGNAL_CONTINUE,
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
    pub fn new() -> Result<Self> {
        compatible()?;
        let mut out = ptr::null_mut();
        // SAFETY: out points to writable storage.
        check(unsafe { sys::siphon_changes_new(&mut out) })?;
        Ok(Self(Batch::new(handle(out)?)))
    }

    pub(crate) fn as_ptr(&self) -> *const sys::Changes {
        self.0.raw.as_ptr()
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
        unsafe { sys::siphon_changes_free(self.0.raw.as_ptr()) };
    }
}

/// Changes within an owned transaction or an executing Fiber phase.
#[derive(Debug)]
pub struct Batch<'a> {
    raw: NonNull<sys::Changes>,
    scope: PhantomData<&'a mut sys::Changes>,
}

impl Batch<'_> {
    pub(crate) fn new(raw: NonNull<sys::Changes>) -> Self {
        Self {
            raw,
            scope: PhantomData,
        }
    }

    pub fn is_empty(&self) -> bool {
        // SAFETY: the batch is live and immutably borrowed.
        unsafe { sys::siphon_changes_is_empty(self.raw.as_ptr()) != 0 }
    }

    /// Requires this snapshot's open volume and exact head when the batch commits.
    pub fn expect(&mut self, snapshot: &Snapshot) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed and the snapshot is live for the call.
        check(unsafe { sys::siphon_changes_expect(self.raw.as_ptr(), snapshot.as_ptr()) })?;
        Ok(self)
    }

    pub fn extend(&mut self, other: Changes) -> Result<&mut Self> {
        // SAFETY: both handles are live, distinct and exclusively borrowed.
        check(unsafe { sys::siphon_changes_extend(self.raw.as_ptr(), other.0.raw.as_ptr()) })?;
        Ok(self)
    }

    pub fn mkdir(&mut self, path: impl AsRef<[u8]>) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed and the path is readable for the call.
        check(unsafe { sys::siphon_changes_mkdir(self.raw.as_ptr(), slice(path.as_ref())) })?;
        Ok(self)
    }

    pub fn write(&mut self, path: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed and the slices are readable for the call.
        check(unsafe {
            sys::siphon_changes_write(
                self.raw.as_ptr(),
                slice(path.as_ref()),
                slice(data.as_ref()),
            )
        })?;
        Ok(self)
    }

    pub fn append(&mut self, path: impl AsRef<[u8]>, data: impl AsRef<[u8]>) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed and the slices are readable for the call.
        check(unsafe {
            sys::siphon_changes_append(
                self.raw.as_ptr(),
                slice(path.as_ref()),
                slice(data.as_ref()),
            )
        })?;
        Ok(self)
    }

    pub fn seal(&mut self, path: impl AsRef<[u8]>) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed and the path is readable for the call.
        check(unsafe { sys::siphon_changes_seal(self.raw.as_ptr(), slice(path.as_ref())) })?;
        Ok(self)
    }

    pub fn expire(&mut self, path: impl AsRef<[u8]>, deadline: Option<i64>) -> Result<&mut Self> {
        let deadline = deadline.as_ref().map_or(ptr::null(), ptr::from_ref);
        // SAFETY: the batch is exclusively borrowed. The path and optional deadline are live.
        check(unsafe {
            sys::siphon_changes_expire(self.raw.as_ptr(), slice(path.as_ref()), deadline)
        })?;
        Ok(self)
    }

    pub fn delete(&mut self, path: impl AsRef<[u8]>) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed and the path is readable for the call.
        check(unsafe { sys::siphon_changes_delete(self.raw.as_ptr(), slice(path.as_ref())) })?;
        Ok(self)
    }

    pub fn meta(
        &mut self,
        path: impl AsRef<[u8]>,
        labels: impl AsRef<[Named]>,
        annotations: impl AsRef<[Named]>,
    ) -> Result<&mut Self> {
        let encode = |updates: &[Named]| {
            updates
                .iter()
                .map(|(name, value)| sys::NamedBytes {
                    name: slice(name),
                    value: optional_slice(value.as_deref()),
                })
                .collect::<Vec<_>>()
        };
        let labels = encode(labels.as_ref());
        let annotations = encode(annotations.as_ref());
        // SAFETY: the batch is exclusively borrowed. The library copies the live arrays and slices.
        check(unsafe {
            sys::siphon_changes_meta(
                self.raw.as_ptr(),
                slice(path.as_ref()),
                labels.as_slice().into(),
                annotations.as_slice().into(),
            )
        })?;
        Ok(self)
    }

    /// Starts a child of the identified parent, or a root Fiber when the parent is absent.
    pub fn start<P: Fiber>(
        &mut self,
        parent: Option<&FiberId>,
        key: impl AsRef<[u8]>,
        input: &P::Input,
    ) -> Result<&mut Self> {
        let input = serde_json::to_vec(input)?;
        let parent = parent.map(FiberId::view);
        let parent = parent.as_ref().map_or(ptr::null(), ptr::from_ref);
        // SAFETY: the batch is exclusively borrowed and every input is live for the call.
        check(unsafe {
            sys::siphon_changes_start(
                self.raw.as_ptr(),
                parent,
                slice(key.as_ref()),
                slice(P::KIND.as_bytes()),
                P::VERSION,
                slice(&input),
            )
        })?;
        Ok(self)
    }

    /// Stages mail to a specific Fiber generation.
    pub fn send(&mut self, fiber: &FiberId, mail: Mail) -> Result<&mut Self> {
        let mail = sys::MailView {
            key: slice(&mail.key),
            name: slice(&mail.name),
            payload: slice(&mail.payload),
        };
        // SAFETY: the batch is exclusively borrowed. The library copies all the live inputs.
        check(unsafe { sys::siphon_changes_send(self.raw.as_ptr(), &fiber.view(), mail) })?;
        Ok(self)
    }

    pub fn signal(
        &mut self,
        fiber: &FiberId,
        signal: Signal,
        reason: Option<Bytes>,
    ) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed. The identity and reason are live for the call.
        check(unsafe {
            sys::siphon_changes_signal(
                self.raw.as_ptr(),
                &fiber.view(),
                signal as u32,
                optional_slice(reason.as_deref()),
            )
        })?;
        Ok(self)
    }

    pub fn remove(&mut self, fiber: &FiberId, reason: Option<Bytes>) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed. The identity and reason are live for the call.
        check(unsafe {
            sys::siphon_changes_remove(
                self.raw.as_ptr(),
                &fiber.view(),
                optional_slice(reason.as_deref()),
            )
        })?;
        Ok(self)
    }

    pub fn force_remove(&mut self, fiber: &FiberId, reason: Option<Bytes>) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed. The identity and reason are live for the call.
        check(unsafe {
            sys::siphon_changes_force_remove(
                self.raw.as_ptr(),
                &fiber.view(),
                optional_slice(reason.as_deref()),
            )
        })?;
        Ok(self)
    }

    pub fn reap(&mut self, fiber: &FiberId) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed and the identity is live for the call.
        check(unsafe { sys::siphon_changes_reap(self.raw.as_ptr(), &fiber.view()) })?;
        Ok(self)
    }

    pub fn charge(
        &mut self,
        fiber: &FiberId,
        counters: impl AsRef<[(Bytes, u64)]>,
    ) -> Result<&mut Self> {
        let counters: Vec<_> = counters
            .as_ref()
            .iter()
            .map(|(name, amount)| sys::CounterView {
                name: slice(name),
                amount: *amount,
            })
            .collect();
        // SAFETY: the batch is exclusively borrowed. The library copies all the live inputs.
        check(unsafe {
            sys::siphon_changes_charge(
                self.raw.as_ptr(),
                &fiber.view(),
                counters.as_ptr(),
                counters.len(),
            )
        })?;
        Ok(self)
    }

    pub fn limit(&mut self, fiber: &FiberId, name: &[u8], maximum: u64) -> Result<&mut Self> {
        // SAFETY: the batch is exclusively borrowed. The identity and name are live for the call.
        check(unsafe {
            sys::siphon_changes_limit(self.raw.as_ptr(), &fiber.view(), slice(name), maximum)
        })?;
        Ok(self)
    }
}
