use std::{
    ffi::c_void,
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Weak},
};

use futures_util::FutureExt;
use serde::{Serialize, de::DeserializeOwned};
use tokio::{runtime::Handle, task::AbortHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    Builder, Error, Result, RuntimeHandle, borrow_slice, check, discard_panic, panic_message,
    release_context, slice, sys,
};

/// A request name, its typed payload, and the value returned by its handler.
pub trait Request: Serialize + DeserializeOwned + Send + 'static {
    const NAME: &'static str;
    type Answer: Serialize + DeserializeOwned + Send + 'static;
}

/// The durable identity of one request in a volume.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestId {
    process: Vec<u8>,
    started: u64,
    key: Vec<u8>,
    created_seq: u64,
}

impl RequestId {
    /// The path of the Fiber that posted the request.
    pub fn path(&self) -> &[u8] {
        &self.process
    }

    /// The request's mailbox key.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Encodes the identity for transport as an opaque string.
    pub fn to_token(&self) -> String {
        serde_json::to_string(&(&self.process, self.started, &self.key, self.created_seq))
            .expect("a request identity is serializable")
    }

    /// Decodes a token from `to_token`. The runtime checks the identity when an answer is submitted.
    pub fn from_token(token: &str) -> Option<Self> {
        let (process, started, key, created_seq) = serde_json::from_str(token).ok()?;
        Some(Self {
            process,
            started,
            key,
            created_seq,
        })
    }

    /// The native view and both slices must be readable.
    pub(crate) unsafe fn from_view(id: &sys::RequestId) -> Self {
        Self {
            process: unsafe { borrow_slice(id.process) }.to_vec(),
            started: id.started,
            key: unsafe { borrow_slice(id.key) }.to_vec(),
            created_seq: id.created_seq,
        }
    }

    pub(crate) fn view(&self) -> sys::RequestId {
        sys::RequestId {
            process: slice(&self.process),
            started: self.started,
            key: slice(&self.key),
            created_seq: self.created_seq,
        }
    }
}

/// A delivered request passed to its host handler.
#[derive(Debug)]
pub struct Asked<R> {
    pub id: RequestId,
    pub payload: R,
    pub runtime: RuntimeHandle,
    /// Signals a stop. The handler can return a partial answer or [`Unanswered::Interrupted`].
    pub cancelled: CancellationToken,
    delivery: Weak<Reply>,
}

impl<R> Asked<R> {
    /// Publishes bytes for this request while its delivery is open.
    pub fn publish(&self, topic: &str, data: &[u8]) -> Result<()> {
        let reply = self.delivery.upgrade().ok_or_else(|| Error {
            code: 3,
            message: "request delivery is closed".into(),
        })?;
        // SAFETY: reply is owned through this call, and topic and data remain readable.
        check(unsafe { sys::siphon_reply_publish(reply.0, slice(topic.as_bytes()), slice(data)) })
    }
}

/// The answer as read from a requesting process's mailbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer<A> {
    Given(A),
    Failed(String),
    Interrupted,
}

/// Why a host handler returned without a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unanswered {
    Failed(String),
    Interrupted,
}

impl fmt::Display for Unanswered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(reason) => f.write_str(reason),
            Self::Interrupted => f.write_str("interrupted"),
        }
    }
}

impl std::error::Error for Unanswered {}

impl Builder {
    /// Registers a request handler on a host Tokio executor.
    ///
    /// The executor must run while requests are handled. Delivery can begin during
    /// [`Builder::open`], before the caller receives the Siphon runtime. A process stop signals
    /// [`Asked::cancelled`] so the handler can cooperate. Closing a delivery asks the executor to
    /// abort its task and lets Siphon shut down without waiting for the host.
    pub fn handle<R, F, A>(&mut self, executor: &Handle, handler: F) -> Result<&mut Self>
    where
        R: Request,
        F: Fn(Asked<R>) -> A + Send + Sync + 'static,
        A: Future<Output = std::result::Result<R::Answer, Unanswered>> + Send + 'static,
    {
        let context = Box::into_raw(Box::new(Registration {
            executor: executor.clone(),
            handler: Arc::new(handler),
        }))
        .cast();
        // SAFETY: the builder is exclusively borrowed and the name is static. Native takes
        // ownership of the boxed context on all paths and keeps it live while delivering.
        check(unsafe {
            sys::siphon_builder_handle_request(
                self.0.as_ptr(),
                slice(R::NAME.as_bytes()),
                context,
                Some(deliver::<R, F, A>),
                Some(release_context::<Registration<F>>),
            )
        })?;
        Ok(self)
    }
}

struct Registration<F> {
    executor: Handle,
    handler: Arc<F>,
}

struct Delivery {
    cancelled: CancellationToken,
    task: AbortHandle,
}

struct Reply(*mut sys::Reply);

// SAFETY: the native reply serializes publication and submission. Arc owns its lifetime.
unsafe impl Send for Reply {}
// SAFETY: as above, free occurs only once after all references are dropped.
unsafe impl Sync for Reply {}

impl Reply {
    fn submit(&self, answer: std::result::Result<Vec<u8>, Unanswered>) {
        let (kind, payload) = match &answer {
            Ok(data) => (sys::ANSWER_GIVEN, data.as_slice()),
            Err(Unanswered::Failed(reason)) => (sys::ANSWER_FAILED, reason.as_bytes()),
            Err(Unanswered::Interrupted) => (sys::ANSWER_INTERRUPTED, &[][..]),
        };
        // SAFETY: the handle is owned here and payload is readable for this call. A closed
        // delivery can reject the answer. The native waiter decides whether to commit it.
        let _ = check(unsafe { sys::siphon_reply_submit(self.0, kind, slice(payload)) });
    }
}

impl Drop for Reply {
    fn drop(&mut self) {
        // SAFETY: this handle was transferred by native and is freed here once.
        unsafe { sys::siphon_reply_free(self.0) };
    }
}

unsafe extern "C" fn deliver<R, F, A>(
    context: *mut c_void,
    id: *const sys::RequestId,
    payload: sys::Slice,
    reply: *mut sys::Reply,
) -> sys::Watch
where
    R: Request,
    F: Fn(Asked<R>) -> A + Send + Sync + 'static,
    A: Future<Output = std::result::Result<R::Answer, Unanswered>> + Send + 'static,
{
    let reply = Arc::new(Reply(reply));
    let started = catch_unwind(AssertUnwindSafe(move || {
        // SAFETY: native keeps its registration and slices live throughout the callback.
        let registration = unsafe { &*context.cast::<Registration<F>>() };
        let (id, payload) = unsafe { (RequestId::from_view(&*id), borrow_slice(payload).to_vec()) };
        let cancelled = CancellationToken::new();
        let signal = cancelled.clone();
        let handler = Arc::clone(&registration.handler);
        let delivery = Arc::downgrade(&reply);
        let mut retained = std::ptr::null();
        // SAFETY: the reply is live and retained is writable.
        let runtime = check(unsafe { sys::siphon_reply_runtime(reply.0, &mut retained) })
            .and_then(|()| RuntimeHandle::from_retained(retained));
        let task = registration.executor.spawn(async move {
            let answer = AssertUnwindSafe(async move {
                let payload = serde_json::from_slice(&payload).map_err(|error| {
                    Unanswered::Failed(format!("the request could not be read: {error}"))
                })?;
                let runtime = runtime.map_err(|error| Unanswered::Failed(error.to_string()))?;
                let given = handler(Asked {
                    id,
                    payload,
                    runtime,
                    cancelled: signal,
                    delivery,
                })
                .await?;
                serde_json::to_vec(&given).map_err(|error| {
                    Unanswered::Failed(format!("the answer could not be written: {error}"))
                })
            })
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| {
                let reason = format!("the handler panicked: {}", panic_message(panic.as_ref()));
                discard_panic(panic);
                Err(Unanswered::Failed(reason))
            });
            reply.submit(answer);
        });
        let delivery = Delivery {
            cancelled,
            task: task.abort_handle(),
        };
        sys::Watch {
            context: Box::into_raw(Box::new(delivery)).cast(),
            notify: Some(notify),
            release: Some(release_context::<Delivery>),
        }
    }));
    started.unwrap_or_else(|panic| {
        discard_panic(panic);
        sys::Watch::default()
    })
}

unsafe extern "C" fn notify(context: *mut c_void, event: u32) {
    // SAFETY: native keeps the delivery context live until its notifications finish.
    let delivery = unsafe { &*context.cast::<Delivery>() };
    if let Err(panic) = catch_unwind(AssertUnwindSafe(|| match event {
        sys::REQUEST_STOP => delivery.cancelled.cancel(),
        sys::REQUEST_CLOSED => delivery.task.abort(),
        _ => {}
    })) {
        discard_panic(panic);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_tokens_preserve_bytes_and_full_sequence_numbers() {
        let id = RequestId {
            process: b"/threads/a/\xff".to_vec(),
            started: u64::MAX - 2,
            key: b"\xfe\0".to_vec(),
            created_seq: u64::MAX,
        };
        let encoded = serde_json::to_string(&id.to_token()).unwrap();
        let token: String = serde_json::from_str(&encoded).unwrap();
        assert_eq!(RequestId::from_token(&token), Some(id));
        assert_eq!(RequestId::from_token("mailbox-key"), None);
    }
}
