use std::{ffi::c_void, ptr, ptr::NonNull};

use bytes::Bytes;
use serde::Serialize;

use crate::events::{Events, receive};
use crate::{
    Error, FiberId, Namespace, Result, RuntimeHandle, Signal, Status, borrow_slice, check, handle,
    slice, sys, take,
};

/// How to run a command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Exec {
    Argv(Vec<String>),
    Shell(String),
}

/// How a command's streams are wired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stdio {
    Pipe,
    Coprocess,
    Channel,
    Terminal {
        rows: u16,
        cols: u16,
        scrollback_lines: usize,
    },
}

/// A command and any overrides to the native execution defaults.
#[derive(Debug, Clone, Serialize)]
pub struct Spec {
    pub exec: Exec,
    pub cwd: String,
    pub set: Option<Vec<(String, String)>>,
    pub clear: Option<bool>,
    pub stdio: Option<Stdio>,
    pub head_bytes: Option<usize>,
    pub tail_bytes: Option<usize>,
    pub log: Option<(String, u64)>,
}

impl Spec {
    #[must_use]
    pub fn new(exec: Exec, cwd: impl Into<String>) -> Self {
        Self {
            exec,
            cwd: cwd.into(),
            set: None,
            clear: None,
            stdio: None,
            head_bytes: None,
            tail_bytes: None,
            log: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    Code(i32),
    Signal(i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandEvent {
    /// Bytes from the live stream with their starting offset.
    Output(Chunk),
    /// The reader skipped this range of live output, ending at the exclusive `through` offset.
    /// Use [`Command::read_output_at`] to recover retained bytes.
    Gap {
        from: u64,
        through: u64,
    },
    Title(String),
    Exit(Exit),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub offset: u64,
    pub bytes: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Screen {
    pub sequences: Bytes,
    pub through: u64,
}

#[derive(Debug)]
pub struct Command(NonNull<sys::Command>);

// SAFETY: native serializes access to a live command and Rust owns its release.
unsafe impl Send for Command {}
// SAFETY: as above.
unsafe impl Sync for Command {}

#[derive(Debug)]
pub struct CommandEvents {
    handle: NonNull<sys::CommandEvents>,
    events: Events<CommandEvent>,
}

// SAFETY: there is one Rust owner which advances and closes the event reader.
unsafe impl Send for CommandEvents {}

impl RuntimeHandle {
    /// Starts a command in the namespace with local execution enabled. A missing owner records
    /// the command at the runtime root.
    pub fn spawn(
        &self,
        owner: Option<&FiberId>,
        namespace: &Namespace,
        spec: &Spec,
    ) -> Result<(Command, CommandEvents)> {
        let owner = owner.map(FiberId::view);
        spawn_with(spec, |spec, command, events| {
            // SAFETY: handles, the optional owner and its path, and spec remain live during the
            // call. Both output references are writable and distinct.
            unsafe {
                sys::siphon_runtime_spawn(
                    self.0.as_ptr(),
                    namespace.0.as_ptr(),
                    owner.as_ref().map_or(ptr::null(), ptr::from_ref),
                    spec,
                    command,
                    events,
                )
            }
        })
    }
}

pub(crate) fn spawn_with(
    spec: &Spec,
    launch: impl FnOnce(sys::Slice, &mut *mut sys::Command, &mut *mut sys::CommandEvents) -> sys::Status,
) -> Result<(Command, CommandEvents)> {
    let encoded = serde_json::to_vec(spec)?;
    let (mut command, mut events) = (ptr::null_mut(), ptr::null_mut());
    check(launch(slice(&encoded), &mut command, &mut events))?;
    let command = Command(handle(command)?);
    Ok((
        command,
        CommandEvents {
            handle: handle(events)?,
            events: Events::new(),
        },
    ))
}

impl Command {
    /// Queues up to 64 KiB of input. [`Status::WouldBlock`] means the queue accepted no bytes.
    pub fn write(&self, data: &[u8]) -> Result<()> {
        // SAFETY: the command and data stay live during the call.
        check(unsafe { sys::siphon_command_write(self.0.as_ptr(), slice(data)) })
    }

    pub fn close_input(&self) -> Result<()> {
        // SAFETY: the command is live.
        check(unsafe { sys::siphon_command_close_input(self.0.as_ptr()) })
    }

    pub fn signal(&self, signal: Signal) -> Result<()> {
        // SAFETY: the command is live.
        check(unsafe { sys::siphon_command_signal(self.0.as_ptr(), signal as u32) })
    }

    pub fn resize(&self, rows: u16, cols: u16) -> Result<()> {
        // SAFETY: the command is live.
        check(unsafe { sys::siphon_command_resize(self.0.as_ptr(), rows, cols) })
    }

    pub fn output(&self) -> Result<Bytes> {
        let mut data = sys::Buffer::default();
        // SAFETY: the command is live and data is writable.
        let status = unsafe { sys::siphon_command_output(self.0.as_ptr(), &mut data) };
        let output = take(data);
        check(status)?;
        Ok(output.into())
    }

    /// Reads retained bytes starting at or after `from`. For coprocesses and channels,
    /// this offset counts diagnostic bytes and live events count protocol bytes.
    pub fn read_output_at(&self, from: u64) -> Result<Chunk> {
        let mut data = sys::Buffer::default();
        let mut offset = 0;
        // SAFETY: the command is live and the outputs are writable and distinct.
        let status = unsafe {
            sys::siphon_command_read_output(self.0.as_ptr(), from, &mut offset, &mut data)
        };
        let bytes = take(data).into();
        check(status)?;
        Ok(Chunk { offset, bytes })
    }

    pub fn screen(&self) -> Result<Screen> {
        let mut data = sys::Buffer::default();
        let mut through = 0;
        // SAFETY: the command is live and the outputs are writable and distinct.
        let status =
            unsafe { sys::siphon_command_screen(self.0.as_ptr(), &mut through, &mut data) };
        let sequences = take(data).into();
        check(status)?;
        Ok(Screen { sequences, through })
    }

    pub fn screen_text(&self) -> Result<String> {
        let mut data = sys::Buffer::default();
        // SAFETY: the command is live and data is writable.
        let status = unsafe { sys::siphon_command_screen_text(self.0.as_ptr(), &mut data) };
        let output = take(data);
        check(status)?;
        String::from_utf8(output).map_err(|error| Error::new(Status::Failed, error.to_string()))
    }
}

impl Drop for Command {
    fn drop(&mut self) {
        // SAFETY: the reference is uniquely owned and freed once.
        unsafe { sys::siphon_command_free(self.0.as_ptr()) };
    }
}

impl CommandEvents {
    /// A cancelled wait leaves its event for the next call.
    pub async fn next(&mut self) -> Result<Option<CommandEvent>> {
        self.events.begin(|context, release| {
            // SAFETY: the reader is live. Native owns context on every path.
            unsafe {
                sys::siphon_command_events_read(
                    self.handle.as_ptr(),
                    context,
                    Some(received),
                    Some(release),
                )
            }
        })?;
        self.events.wait().await
    }
}

impl Drop for CommandEvents {
    fn drop(&mut self) {
        // SAFETY: the event reader is uniquely owned and freed once.
        unsafe { sys::siphon_command_events_free(self.handle.as_ptr()) };
    }
}

unsafe extern "C" fn received(context: *mut c_void, view: *const sys::CommandView) {
    // SAFETY: if present, the view and all borrowed bytes are live during this callback.
    unsafe { receive(context, view, read_event) };
}

unsafe fn read_event(view: &sys::CommandView) -> Result<CommandEvent> {
    match view.kind {
        sys::COMMAND_OUTPUT => Ok(CommandEvent::Output(Chunk {
            offset: view.offset,
            bytes: Bytes::copy_from_slice(unsafe { borrow_slice(view.data) }),
        })),
        sys::COMMAND_GAP => Ok(CommandEvent::Gap {
            from: view.offset,
            through: view.through,
        }),
        sys::COMMAND_TITLE => std::str::from_utf8(unsafe { borrow_slice(view.data) })
            .map(|title| CommandEvent::Title(title.to_owned()))
            .map_err(|error| Error::new(Status::Failed, error.to_string())),
        sys::COMMAND_EXIT_CODE => Ok(CommandEvent::Exit(Exit::Code(view.value))),
        sys::COMMAND_EXIT_SIGNAL => Ok(CommandEvent::Exit(Exit::Signal(view.value))),
        _ => Err(Error::new(Status::Failed, "unknown command event")),
    }
}
