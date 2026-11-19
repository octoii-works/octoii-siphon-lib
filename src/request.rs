use std::{
    ffi::c_void,
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr::{self, NonNull},
    sync::Arc,
};

use futures_util::FutureExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{runtime::Handle, task::AbortHandle};
use tokio_util::sync::CancellationToken;

use crate::command::spawn_with;
use crate::{
    Builder, Command, CommandEvents, FiberId, Namespace, Result, Spec, borrow_slice, check,
    discard_panic, handle, panic_message, release_context, slice, sys,
};

/// A request name, its typed payload, and the value returned by its handler.
pub trait Request: Serialize + DeserializeOwned + Send + 'static {
    const NAME: &'static str;
    type Answer: Serialize + DeserializeOwned + Send + 'static;
}

/// The durable identity of one request in a volume.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId {
    fiber: FiberId,
    key: Vec<u8>,
    generation: [u8; 16],
    created_seq: u64,
}

impl RequestId {
    /// The exact Fiber instance that posted the request.
    pub fn fiber(&self) -> &FiberId {
        &self.fiber
    }

    /// The path of the Fiber that posted the request.
    pub fn path(&self) -> &[u8] {
        self.fiber.path()
    }

    /// The request's mailbox key.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Encodes the identity for transport as an opaque string.
    pub fn to_token(&self) -> String {
        serde_json::to_string(self).expect("a request identity is serializable")
    }

    /// Decodes a token from `to_token`. The runtime checks the identity when an answer is submitted.
    pub fn from_token(token: &str) -> Option<Self> {
        serde_json::from_str(token).ok()
    }

    /// The native view and both slices must be readable.
    pub(crate) unsafe fn from_view(id: &sys::RequestId) -> Self {
        Self {
            fiber: unsafe { FiberId::from_view(&id.fiber) },
            key: unsafe { borrow_slice(id.key) }.to_vec(),
            generation: id.generation,
            created_seq: id.created_seq,
        }
    }

    pub(crate) fn view(&self) -> sys::RequestId {
        sys::RequestId {
            fiber: self.fiber.view(),
            key: slice(&self.key),
            generation: self.generation,
            created_seq: self.created_seq,
        }
    }
}

/// Observes a request's cooperative stop signal.
#[derive(Debug, Clone)]
pub struct Stop(CancellationToken);

impl Stop {
    /// Returns whether the runtime has requested a stop.
    #[must_use]
    pub fn is_requested(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Waits for the runtime to request a stop.
    pub async fn requested(&self) {
        self.0.cancelled().await;
    }
}

/// A delivered request passed to its host handler.
#[derive(Debug)]
pub struct Asked<R> {
    pub id: RequestId,
    pub payload: R,
    /// Signals a stop. The handler can return a partial answer or [`Unanswered::Interrupted`].
    pub stop: Stop,
    delivery: Arc<Reply>,
}

impl<R> Asked<R> {
    /// Publishes bytes for this request while its delivery is open.
    pub fn publish(&self, topic: &str, data: &[u8]) -> Result<()> {
        // SAFETY: the reply is live, and topic and data remain readable during the call.
        check(unsafe {
            sys::siphon_reply_publish(self.delivery.0, slice(topic.as_bytes()), slice(data))
        })
    }

    /// Provides command launches owned by this exact request.
    pub fn commands(&self) -> Result<RequestCommands> {
        let mut out = ptr::null_mut();
        // SAFETY: the reply stays live and out is writable during the call.
        check(unsafe { sys::siphon_reply_commands(self.delivery.0, &mut out) })?;
        Ok(RequestCommands {
            handle: Arc::new(RequestCommandsHandle(handle(out)?)),
        })
    }
}

/// Starts commands owned by one request delivery.
///
/// The handle can outlive the handler. Launches require a pending, uninterrupted request.
#[derive(Debug, Clone)]
pub struct RequestCommands {
    handle: Arc<RequestCommandsHandle>,
}

#[derive(Debug)]
struct RequestCommandsHandle(NonNull<sys::RequestCommands>);

// SAFETY: the native handle can be used and freed from any thread.
unsafe impl Send for RequestCommandsHandle {}
// SAFETY: native synchronizes concurrent launches, and Arc keeps the handle live during calls.
unsafe impl Sync for RequestCommandsHandle {}

impl Drop for RequestCommandsHandle {
    fn drop(&mut self) {
        // SAFETY: only the last Arc owner frees this handle.
        unsafe { sys::siphon_request_commands_free(self.0.as_ptr()) };
    }
}

impl RequestCommands {
    /// Starts a command while the request accepts new commands. Requires [`Builder::local_machine`].
    pub fn spawn(&self, namespace: &Namespace, spec: &Spec) -> Result<(Command, CommandEvents)> {
        spawn_with(spec, |spec, command, events| {
            // SAFETY: both handles and the spec remain live. Outputs are writable and distinct.
            unsafe {
                sys::siphon_request_commands_spawn(
                    self.handle.0.as_ptr(),
                    namespace.0.as_ptr(),
                    spec,
                    command,
                    events,
                )
            }
        })
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
    /// [`Builder::start`]. A process stop signals [`Asked::stop`] so the handler can
    /// cooperate. Closing a delivery asks the executor to abort its task and lets Siphon
    /// shut down without waiting for the host.
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
    stop: CancellationToken,
    task: AbortHandle,
}

#[derive(Debug)]
struct Reply(*mut sys::Reply);

// SAFETY: native synchronizes reply operations, and Arc owns the handle's lifetime.
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
        let stopped = CancellationToken::new();
        let stop = Stop(stopped.clone());
        let handler = Arc::clone(&registration.handler);
        let delivery = Arc::clone(&reply);
        let task = registration.executor.spawn(async move {
            let answer = AssertUnwindSafe(async move {
                let payload = serde_json::from_slice(&payload).map_err(|error| {
                    Unanswered::Failed(format!("the request could not be read: {error}"))
                })?;
                let given = handler(Asked {
                    id,
                    payload,
                    stop,
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
            stop: stopped,
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
        sys::REQUEST_STOP => delivery.stop.cancel(),
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
        let view = sys::RequestId {
            fiber: sys::FiberId {
                path: slice(b"/threads/a/\xff"),
                generation: [1; 16],
            },
            key: slice(b"\xfe\0"),
            generation: [2; 16],
            created_seq: u64::MAX,
        };
        // SAFETY: both slices refer to static bytes.
        let id = unsafe { RequestId::from_view(&view) };
        let encoded = serde_json::to_string(&id.to_token()).unwrap();
        let token: String = serde_json::from_str(&encoded).unwrap();
        let decoded = RequestId::from_token(&token).unwrap();
        assert_eq!(decoded, id);
        assert_eq!(decoded.path(), b"/threads/a/\xff");
        assert_eq!(decoded.key(), b"\xfe\0");
        let decoded = decoded.view();
        assert_eq!(decoded.fiber.generation, view.fiber.generation);
        assert_eq!(decoded.generation, view.generation);
        assert_eq!(decoded.created_seq, view.created_seq);
        assert_eq!(RequestId::from_token("mailbox-key"), None);
        assert_eq!(RequestId::from_token("[[1],2,[3],4]"), None);
    }
}
