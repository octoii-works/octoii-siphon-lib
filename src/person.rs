use std::{
    ffi::c_void,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
};

use serde_json::Value;

use crate::{
    Builder, Request, RequestId, Result, Runtime, Snapshot, borrow_slice, check, discard_panic,
    panic_message, release_context, slice, sys,
};

/// A request answered by a person, with a JSON Schema for the answer.
pub trait AskedOfPerson: Request {
    /// The schema for this payload. Siphon uses it both to present and validate the answer.
    fn answer_schema(&self) -> Value;
}

/// A pending request and the answer schema derived from its payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonRequest {
    pub id: RequestId,
    pub name: Vec<u8>,
    pub payload: Vec<u8>,
    pub answer_schema: Value,
}

impl Builder {
    /// Registers a request to be answered by a person.
    pub fn ask_person<R: AskedOfPerson>(&mut self) -> Result<&mut Self> {
        // SAFETY: the builder is exclusively borrowed, the name is static and the
        // monomorphized callback has no context.
        check(unsafe {
            sys::siphon_builder_ask_person(
                self.0.as_ptr(),
                slice(R::NAME.as_bytes()),
                ptr::null_mut(),
                Some(answer_schema::<R>),
                None,
            )
        })?;
        Ok(self)
    }
}

impl Runtime {
    /// Reads person requests under `under` from a snapshot of this volume.
    pub fn person_requests(&self, snapshot: &Snapshot, under: &[u8]) -> Result<Vec<PersonRequest>> {
        let mut prompts: Result<Vec<PersonRequest>> = Ok(Vec::new());
        // SAFETY: the handles and path are live. The synchronous callback receives exclusive
        // access to prompts.
        check(unsafe {
            sys::siphon_runtime_person_requests(
                self.0.as_ptr(),
                snapshot.0.0.as_ptr(),
                slice(under),
                ptr::from_mut(&mut prompts).cast(),
                Some(collect_request),
            )
        })?;
        prompts
    }

    /// Commits a person's answer to the exact request, returning the commit sequence.
    /// A missing or replaced request yields code 1. An unfit answer yields code 2.
    pub fn answer(&self, id: &RequestId, answer: &Value) -> Result<u64> {
        let mut seq = 0;
        let view = id.view();
        let answer = serde_json::to_vec(answer)?;
        // SAFETY: all arguments are readable for the call, and seq is writable.
        check(unsafe {
            sys::siphon_runtime_answer(self.0.as_ptr(), &view, slice(&answer), &mut seq)
        })?;
        Ok(seq)
    }
}

unsafe extern "C" fn answer_schema<R: AskedOfPerson>(
    _: *mut c_void,
    payload: sys::Slice,
) -> sys::SchemaResult {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the request payload lives for this synchronous callback.
        let request = serde_json::from_slice::<R>(unsafe { borrow_slice(payload) })?;
        serde_json::to_vec(&request.answer_schema())
    }));
    let (kind, bytes) = match result {
        Ok(Ok(data)) => (sys::ANSWER_GIVEN, data),
        Ok(Err(error)) => (sys::ANSWER_FAILED, error.to_string().into_bytes()),
        Err(panic) => {
            let reason = format!(
                "the answer schema panicked: {}",
                panic_message(panic.as_ref())
            );
            discard_panic(panic);
            (sys::ANSWER_FAILED, reason.into_bytes())
        }
    };
    let bytes = Box::new(bytes);
    let payload = slice(&bytes);
    sys::SchemaResult {
        kind,
        payload,
        context: Box::into_raw(bytes).cast(),
        release: Some(release_context::<Vec<u8>>),
    }
}

unsafe extern "C" fn collect_request(context: *mut c_void, view: *const sys::PersonRequestView) {
    // SAFETY: Siphon keeps the view and all its bytes live for this call. The caller
    // uses the synchronous visitor with an exclusive output context.
    let (out, request) = unsafe { (&mut *context.cast::<Result<Vec<PersonRequest>>>(), &*view) };
    let Ok(prompts) = out else {
        return;
    };
    match serde_json::from_slice(unsafe { borrow_slice(request.answer_schema) }) {
        Ok(answer_schema) => prompts.push(PersonRequest {
            id: unsafe { RequestId::from_view(&request.id) },
            name: unsafe { borrow_slice(request.name) }.to_vec(),
            payload: unsafe { borrow_slice(request.payload) }.to_vec(),
            answer_schema,
        }),
        Err(error) => *out = Err(error.into()),
    }
}
