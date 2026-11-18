use std::{ffi::c_void, ptr::NonNull};

use tokio::sync::oneshot;

use crate::{
    Error, RequestId, Result, RuntimeHandle, borrow_slice, check, handle, release_context, slice,
    sys,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    pub topic: String,
    pub id: RequestId,
    pub offset: u64,
    pub data: Vec<u8>,
}

/// A change to the followed topic's stream view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamEvent {
    /// Replace the local view before consuming retained events again.
    Reset,
    Data(Published),
    Closed(RequestId),
}

type Event = Result<Option<StreamEvent>>;

#[derive(Debug)]
pub struct Following {
    handle: NonNull<sys::Following>,
    pending: Option<oneshot::Receiver<Event>>,
}

// SAFETY: native serializes reads and a unique Rust owner cancels the follower.
unsafe impl Send for Following {}

impl RuntimeHandle {
    /// Follows retained stream data and subsequent events on a topic.
    pub fn follow(&self, topic: &str) -> Result<Following> {
        let mut out = std::ptr::null_mut();
        // SAFETY: runtime and topic remain live and out is writable during the call.
        check(unsafe {
            sys::siphon_runtime_follow(self.0.as_ptr(), slice(topic.as_bytes()), &mut out)
        })?;
        Ok(Following {
            handle: handle(out)?,
            pending: None,
        })
    }
}

impl Following {
    /// Reads the next event. A cancelled wait leaves its event for the next call.
    pub async fn next(&mut self) -> Result<Option<StreamEvent>> {
        if self.pending.is_none() {
            let (send, receive) = oneshot::channel();
            let context = Box::into_raw(Box::new(Some(send))).cast();
            // SAFETY: the follower is live, and native owns the context on every path.
            let status = unsafe {
                sys::siphon_following_read(
                    self.handle.as_ptr(),
                    context,
                    Some(received),
                    Some(release_context::<Option<oneshot::Sender<Event>>>),
                )
            };
            match check(status) {
                Ok(()) => self.pending = Some(receive),
                Err(error) if error.code == 3 => return Ok(None),
                Err(error) => return Err(error),
            }
        }
        let event = self
            .pending
            .as_mut()
            .expect("a stream read is pending")
            .await;
        self.pending = None;
        event.unwrap_or(Ok(None))
    }
}

impl Drop for Following {
    fn drop(&mut self) {
        // SAFETY: this is the unique owner of the follower.
        unsafe { sys::siphon_following_free(self.handle.as_ptr()) };
    }
}

unsafe extern "C" fn received(context: *mut c_void, view: *const sys::StreamView) {
    // SAFETY: the callback context is live and only one invocation borrows it.
    let sender = unsafe { &mut *context.cast::<Option<oneshot::Sender<Event>>>() };
    if let Some(send) = sender.take() {
        // SAFETY: the native view is null at end and otherwise readable during the call.
        let event = unsafe { view.as_ref().map(|view| read_event(view)).transpose() };
        let _ = send.send(event);
    }
}

/// The view and its non-null fields must remain readable during decoding.
unsafe fn read_event(view: &sys::StreamView) -> Result<StreamEvent> {
    let id = unsafe { view.id.as_ref() };
    match (view.kind, id) {
        (sys::STREAM_RESET, _) => Ok(StreamEvent::Reset),
        (sys::STREAM_DATA, Some(id)) => {
            let topic =
                std::str::from_utf8(unsafe { borrow_slice(view.topic) }).map_err(|error| {
                    Error {
                        code: 4,
                        message: format!("Siphon returned an invalid stream topic: {error}"),
                    }
                })?;
            Ok(StreamEvent::Data(Published {
                topic: topic.to_owned(),
                id: unsafe { RequestId::from_view(id) },
                offset: view.offset,
                data: unsafe { borrow_slice(view.data) }.to_vec(),
            }))
        }
        (sys::STREAM_CLOSED, Some(id)) => {
            Ok(StreamEvent::Closed(unsafe { RequestId::from_view(id) }))
        }
        _ => Err(Error {
            code: 4,
            message: "Siphon returned an invalid stream event".into(),
        }),
    }
}
