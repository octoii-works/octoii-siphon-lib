use std::ffi::c_void;

use tokio::sync::oneshot;

use crate::{Result, Status, check, release_context, sys};

type Reply<Event> = oneshot::Sender<Result<Option<Event>>>;

#[derive(Debug)]
pub(crate) struct Events<Event> {
    pending: Option<oneshot::Receiver<Result<Option<Event>>>>,
}

impl<Event> Events<Event> {
    pub(crate) const fn new() -> Self {
        Self { pending: None }
    }

    pub(crate) fn begin(
        &mut self,
        request: impl FnOnce(*mut c_void, unsafe extern "C" fn(*mut c_void)) -> sys::Status,
    ) -> Result<()> {
        if self.pending.is_none() {
            let (sender, receiver) = oneshot::channel::<Result<Option<Event>>>();
            let context = Box::into_raw(Box::new(Some(sender))).cast();
            let status = request(context, release_context::<Option<Reply<Event>>>);
            match check(status) {
                Ok(()) => self.pending = Some(receiver),
                Err(error) if error.status() == Status::Closed => return Ok(()),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(crate) async fn wait(&mut self) -> Result<Option<Event>> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(None);
        };
        let event = pending.await;
        self.pending = None;
        event.unwrap_or(Ok(None))
    }
}

/// The context and the non-null view are readable exclusively during the callback.
pub(crate) unsafe fn receive<Event, View>(
    context: *mut c_void,
    view: *const View,
    decode: unsafe fn(&View) -> Result<Event>,
) {
    let sender = unsafe { &mut *context.cast::<Option<Reply<Event>>>() };
    if let Some(sender) = sender.take() {
        let event = unsafe { view.as_ref().map(|event| decode(event)).transpose() };
        let _ = sender.send(event);
    }
}
