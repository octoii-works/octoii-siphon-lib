//! Checked-in declarations for include/siphon.h; consumer builds do not run bindgen.
use std::ffi::c_void;

#[repr(C)]
pub struct Builder {
    _private: [u8; 0],
}
#[repr(C)]
pub struct Runtime {
    _private: [u8; 0],
}
#[repr(C)]
pub struct CommitWatch {
    _private: [u8; 0],
}
#[repr(C)]
pub struct Namespace {
    _private: [u8; 0],
}
#[repr(C)]
pub struct Command {
    _private: [u8; 0],
}
#[repr(C)]
pub struct CommandEvents {
    _private: [u8; 0],
}
#[repr(C)]
pub struct Following {
    _private: [u8; 0],
}
#[repr(C)]
#[derive(Debug)]
pub struct Snapshot {
    _private: [u8; 0],
}
#[repr(C)]
pub struct Revision {
    _private: [u8; 0],
}
#[repr(C)]
pub struct Changes {
    _private: [u8; 0],
}
#[repr(C)]
pub struct Run {
    _private: [u8; 0],
}
#[repr(C)]
pub struct Reply {
    _private: [u8; 0],
}
#[repr(C)]
pub struct RequestCommands {
    _private: [u8; 0],
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Slice {
    pub data: *const u8,
    pub len: usize,
}
impl Default for Slice {
    fn default() -> Self {
        Self {
            data: std::ptr::null(),
            len: 0,
        }
    }
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct FiberId {
    pub path: Slice,
    pub generation: [u8; 16],
}
#[repr(C)]
pub struct MailView {
    pub key: Slice,
    pub name: Slice,
    pub payload: Slice,
}
type MailVisitor = unsafe extern "C" fn(*mut c_void, *const MailView);

#[repr(C)]
pub struct Buffer {
    pub data: *mut u8,
    pub len: usize,
}
impl Default for Buffer {
    fn default() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
        }
    }
}
#[repr(C)]
pub struct Status {
    pub code: u32,
    pub error: Buffer,
}

#[repr(C)]
pub struct NamedBytes {
    pub name: Slice,
    pub value: Slice,
}
#[repr(C)]
pub struct NamedSlice {
    pub data: *const NamedBytes,
    pub len: usize,
}
impl From<&[NamedBytes]> for NamedSlice {
    fn from(values: &[NamedBytes]) -> Self {
        Self {
            data: values.as_ptr(),
            len: values.len(),
        }
    }
}
#[repr(C)]
pub struct DirectoryView {
    pub labels: NamedSlice,
    pub annotations: NamedSlice,
    pub created_seq: u64,
    pub changed_seq: u64,
}
#[repr(C)]
pub struct EntryView {
    pub name: Slice,
    pub length: u64,
    pub expires_at: *const i64,
    pub directory: *const DirectoryView,
}
#[repr(C)]
pub struct SnapshotRange {
    pub prefix: Slice,
    pub after: Slice,
    pub limit: usize,
    pub reverse: u8,
}
#[repr(C)]
pub struct SnapshotQuery {
    pub kind: Slice,
    pub name: Slice,
    pub value: Slice,
    pub under: Slice,
    pub after: Slice,
    pub limit: usize,
}
#[repr(C)]
pub struct ChangeSetView {
    pub seq: u64,
    pub paths: *const Slice,
    pub paths_len: usize,
}
#[repr(C)]
pub struct JournalView {
    pub change_sets: *const ChangeSetView,
    pub change_sets_len: usize,
    pub through: u64,
    pub oldest: *const u64,
    pub latest: u64,
    pub missed: u8,
}
#[repr(C)]
pub struct CounterView {
    pub name: Slice,
    pub amount: u64,
}
#[repr(C)]
pub struct FiberRecordView {
    pub id: FiberId,
    pub kind: Slice,
    pub version: u32,
    pub created_seq: u64,
    pub waits_until: *const i64,
    pub usage: *const CounterView,
    pub usage_len: usize,
    pub reapable: u8,
    pub outcome: u32,
    pub payload: Slice,
    pub reason: Slice,
}
type FiberVisitor = unsafe extern "C" fn(*mut c_void, *const FiberRecordView);
type FiberIdVisitor = unsafe extern "C" fn(*mut c_void, *const FiberId);
type JournalVisitor = unsafe extern "C" fn(*mut c_void, *const JournalView);
type EntryVisitor = unsafe extern "C" fn(*mut c_void, *const EntryView);
type FoundVisitor = unsafe extern "C" fn(*mut c_void, Slice, Slice, *const EntryView);

pub const COMMIT_HEAD: u32 = 1;
pub const COMMIT_CLOSED: u32 = 2;
pub const COMMIT_FAULT: u32 = 3;

#[repr(C)]
pub struct CommitEvent {
    pub kind: u32,
    pub seq: u64,
    pub status: u32,
    pub error: Slice,
}

#[repr(C)]
pub struct RequestId {
    pub fiber: FiberId,
    pub key: Slice,
    pub generation: [u8; 16],
    pub created_seq: u64,
}

#[repr(C)]
pub struct SchemaResult {
    pub kind: u32,
    pub payload: Slice,
    pub context: *mut c_void,
    pub release: Option<ReleaseCallback>,
}
type SchemaCallback = unsafe extern "C" fn(*mut c_void, Slice) -> SchemaResult;

#[repr(C)]
pub struct PersonRequestView {
    pub id: RequestId,
    pub name: Slice,
    pub payload: Slice,
    pub answer_schema: Slice,
}
#[repr(C)]
pub struct FileStat {
    pub kind: u32,
    pub executable: u8,
    pub size: u64,
    pub modified_ms: i64,
    pub file_id: *const u64,
}
type FileVisitor = unsafe extern "C" fn(*mut c_void, *const FileStat);
type DirentVisitor = unsafe extern "C" fn(*mut c_void, Slice, u32);

type PersonVisitor = unsafe extern "C" fn(*mut c_void, *const PersonRequestView);

#[repr(C)]
pub struct StreamView {
    pub kind: u32,
    pub id: *const RequestId,
    pub topic: Slice,
    pub offset: u64,
    pub data: Slice,
}
pub const COMMAND_OUTPUT: u32 = 1;
pub const COMMAND_TITLE: u32 = 2;
pub const COMMAND_EXIT_CODE: u32 = 3;
pub const COMMAND_EXIT_SIGNAL: u32 = 4;
pub const COMMAND_GAP: u32 = 5;

#[repr(C)]
pub struct CommandView {
    pub kind: u32,
    pub value: i32,
    pub data: Slice,
    pub offset: u64,
    pub through: u64,
}
type CommandCallback = unsafe extern "C" fn(*mut c_void, *const CommandView);

type StreamCallback = unsafe extern "C" fn(*mut c_void, *const StreamView);
pub const STREAM_RESET: u32 = 1;
pub const STREAM_DATA: u32 = 2;
pub const STREAM_CLOSED: u32 = 3;

#[repr(C)]
pub struct Watch {
    pub context: *mut c_void,
    pub notify: Option<NotifyCallback>,
    pub release: Option<ReleaseCallback>,
}
impl Default for Watch {
    fn default() -> Self {
        Self {
            context: std::ptr::null_mut(),
            notify: None,
            release: None,
        }
    }
}

pub const WAIT_CHILD: u32 = 1;
pub const WAIT_CHANGE: u32 = 2;
pub const WAIT_MAIL: u32 = 3;
pub const END_NEXT: u32 = 1;
pub const END_WAIT: u32 = 2;
pub const END_DONE: u32 = 3;
pub const END_FAIL: u32 = 4;
pub const END_STOPPED: u32 = 5;
pub const END_THROW: u32 = 6;
pub const OUTCOME_PENDING: u32 = 0;
pub const OUTCOME_COMPLETED: u32 = 1;
pub const OUTCOME_FAILED: u32 = 2;
pub const OUTCOME_STOPPED: u32 = 3;
pub const SIGNAL_HANGUP: u32 = 1;
pub const SIGNAL_INTERRUPT: u32 = 2;
pub const SIGNAL_TERMINATE: u32 = 3;
pub const SIGNAL_KILL: u32 = 4;
pub const SIGNAL_STOP: u32 = 5;
pub const SIGNAL_CONTINUE: u32 = 6;
pub const ANSWER_GIVEN: u32 = 1;
pub const ANSWER_FAILED: u32 = 2;
pub const ANSWER_INTERRUPTED: u32 = 3;
pub const REQUEST_STOP: u32 = 1;
pub const REQUEST_CLOSED: u32 = 2;

type PhaseCallback = unsafe extern "C" fn(*mut c_void, *mut Run, Slice, Slice);
type ReleaseCallback = unsafe extern "C" fn(*mut c_void);
type CommitCallback = unsafe extern "C" fn(*mut c_void, *const CommitEvent);
type NotifyCallback = unsafe extern "C" fn(*mut c_void, u32);
type DeliverCallback =
    unsafe extern "C" fn(*mut c_void, *const RequestId, Slice, *mut Reply) -> Watch;

unsafe extern "C" {
    pub fn siphon_abi_version() -> u32;
    pub fn siphon_buffer_free(buffer: Buffer);
    pub fn siphon_runtime_open(
        directory: Slice,
        journal_entries: *const u64,
        out: *mut *mut Runtime,
    ) -> Status;
    pub fn siphon_builder_new(runtime: *const Runtime, out: *mut *mut Builder) -> Status;
    pub fn siphon_builder_local_machine(builder: *mut Builder) -> Status;
    pub fn siphon_builder_free(builder: *mut Builder);
    pub fn siphon_builder_fiber(
        builder: *mut Builder,
        kind: Slice,
        version: u32,
        stop_handler: u8,
        retries: u32,
        delay_min_ms: i64,
        delay_max_ms: i64,
        context: *mut c_void,
        run: Option<PhaseCallback>,
        release: Option<ReleaseCallback>,
    ) -> Status;
    pub fn siphon_builder_handle_request(
        builder: *mut Builder,
        name: Slice,
        context: *mut c_void,
        deliver: Option<DeliverCallback>,
        release: Option<ReleaseCallback>,
    ) -> Status;
    pub fn siphon_builder_ask_person(
        builder: *mut Builder,
        name: Slice,
        context: *mut c_void,
        schema: Option<SchemaCallback>,
        release: Option<ReleaseCallback>,
    ) -> Status;
    pub fn siphon_builder_start(builder: *mut Builder) -> Status;
    pub fn siphon_runtime_close(runtime: *const Runtime) -> Status;
    pub fn siphon_runtime_free(runtime: *const Runtime) -> Status;
    pub fn siphon_runtime_retain(runtime: *const Runtime);
    pub fn siphon_runtime_release(runtime: *const Runtime);
    pub fn siphon_runtime_commit(
        runtime: *const Runtime,
        changes: *const Changes,
        out: *mut *mut Revision,
    ) -> Status;
    pub fn siphon_revision_seq(revision: *const Revision, out: *mut u64) -> Status;
    pub fn siphon_revision_free(revision: *mut Revision);
    pub fn siphon_runtime_flush(
        runtime: *const Runtime,
        revision: *const Revision,
        through: *mut u64,
    ) -> Status;
    pub fn siphon_runtime_snapshot(runtime: *const Runtime, out: *mut *mut Snapshot) -> Status;
    pub fn siphon_runtime_follow(
        runtime: *const Runtime,
        topic: Slice,
        out: *mut *mut Following,
    ) -> Status;
    pub fn siphon_following_read(
        following: *const Following,
        context: *mut c_void,
        receive: Option<StreamCallback>,
        release: Option<ReleaseCallback>,
    ) -> Status;
    pub fn siphon_following_free(following: *mut Following);
    pub fn siphon_runtime_person_requests(
        runtime: *const Runtime,
        snapshot: *const Snapshot,
        under: Slice,
        context: *mut c_void,
        visit: Option<PersonVisitor>,
    ) -> Status;
    pub fn siphon_runtime_answer(
        runtime: *const Runtime,
        id: *const RequestId,
        answer: Slice,
        out: *mut *mut Revision,
    ) -> Status;
    pub fn siphon_runtime_new_id(runtime: *const Runtime, out: *mut Buffer) -> Status;
    pub fn siphon_runtime_now_ms(runtime: *const Runtime, out: *mut i64) -> Status;
    pub fn siphon_runtime_watch_commits(
        runtime: *const Runtime,
        context: *mut c_void,
        notify: Option<CommitCallback>,
        release: Option<ReleaseCallback>,
        out: *mut *mut CommitWatch,
    ) -> Status;
    pub fn siphon_commit_watch_free(watch: *mut CommitWatch);
    pub fn siphon_namespace_new(out: *mut *mut Namespace) -> Status;
    pub fn siphon_namespace_free(namespace: *mut Namespace);
    pub fn siphon_namespace_mount_host(
        namespace: *const Namespace,
        at: Slice,
        host: Slice,
        writable: u8,
    ) -> Status;
    pub fn siphon_namespace_hide(namespace: *const Namespace, at: Slice) -> Status;
    pub fn siphon_namespace_mount_labels(
        namespace: *const Namespace,
        runtime: *const Runtime,
        at: Slice,
        kind: Slice,
        label: Slice,
        under: Slice,
    ) -> Status;
    pub fn siphon_namespace_stat(
        namespace: *const Namespace,
        path: Slice,
        context: *mut c_void,
        visit: Option<FileVisitor>,
    ) -> Status;
    pub fn siphon_namespace_list(
        namespace: *const Namespace,
        path: Slice,
        range: SnapshotRange,
        context: *mut c_void,
        visit: Option<DirentVisitor>,
    ) -> Status;
    pub fn siphon_namespace_read(
        namespace: *const Namespace,
        path: Slice,
        offset: u64,
        length: u64,
        out: *mut Buffer,
    ) -> Status;
    pub fn siphon_namespace_write(namespace: *const Namespace, path: Slice, data: Slice) -> Status;
    pub fn siphon_namespace_mkdir(namespace: *const Namespace, path: Slice) -> Status;
    pub fn siphon_machine_confines() -> u8;
    pub fn siphon_runtime_spawn(
        runtime: *const Runtime,
        namespace: *const Namespace,
        owner: *const FiberId,
        spec: Slice,
        command: *mut *mut Command,
        events: *mut *mut CommandEvents,
    ) -> Status;
    pub fn siphon_command_events_read(
        events: *const CommandEvents,
        context: *mut c_void,
        receive: Option<CommandCallback>,
        release: Option<ReleaseCallback>,
    ) -> Status;
    pub fn siphon_command_events_free(events: *mut CommandEvents);
    pub fn siphon_command_free(command: *mut Command);
    pub fn siphon_command_write(command: *const Command, data: Slice) -> Status;
    pub fn siphon_command_close_input(command: *const Command) -> Status;
    pub fn siphon_command_signal(command: *const Command, signal: u32) -> Status;
    pub fn siphon_command_resize(command: *const Command, rows: u16, cols: u16) -> Status;
    pub fn siphon_command_output(command: *const Command, out: *mut Buffer) -> Status;
    pub fn siphon_command_read_output(
        command: *const Command,
        from: u64,
        offset: *mut u64,
        out: *mut Buffer,
    ) -> Status;
    pub fn siphon_command_screen(
        command: *const Command,
        through: *mut u64,
        out: *mut Buffer,
    ) -> Status;
    pub fn siphon_command_screen_text(command: *const Command, out: *mut Buffer) -> Status;
    pub fn siphon_changes_new(out: *mut *mut Changes) -> Status;
    pub fn siphon_changes_expect(changes: *mut Changes, snapshot: *const Snapshot) -> Status;
    pub fn siphon_changes_is_empty(changes: *const Changes) -> u8;
    pub fn siphon_changes_extend(changes: *mut Changes, other: *mut Changes) -> Status;
    pub fn siphon_changes_limit(
        changes: *mut Changes,
        fiber: *const FiberId,
        counter: Slice,
        maximum: u64,
    ) -> Status;
    pub fn siphon_changes_free(changes: *mut Changes);
    pub fn siphon_changes_mkdir(changes: *mut Changes, path: Slice) -> Status;
    pub fn siphon_changes_meta(
        changes: *mut Changes,
        path: Slice,
        labels: NamedSlice,
        annotations: NamedSlice,
    ) -> Status;
    pub fn siphon_changes_write(changes: *mut Changes, path: Slice, data: Slice) -> Status;
    pub fn siphon_changes_append(changes: *mut Changes, path: Slice, data: Slice) -> Status;
    pub fn siphon_changes_seal(changes: *mut Changes, path: Slice) -> Status;
    pub fn siphon_changes_expire(
        changes: *mut Changes,
        path: Slice,
        deadline: *const i64,
    ) -> Status;
    pub fn siphon_changes_delete(changes: *mut Changes, path: Slice) -> Status;
    pub fn siphon_changes_start(
        changes: *mut Changes,
        parent: *const FiberId,
        key: Slice,
        kind: Slice,
        version: u32,
        input: Slice,
    ) -> Status;
    pub fn siphon_changes_send(
        changes: *mut Changes,
        fiber: *const FiberId,
        mail: MailView,
    ) -> Status;
    pub fn siphon_changes_signal(
        changes: *mut Changes,
        fiber: *const FiberId,
        signal: u32,
        reason: Slice,
    ) -> Status;
    pub fn siphon_changes_remove(
        changes: *mut Changes,
        fiber: *const FiberId,
        reason: Slice,
    ) -> Status;
    pub fn siphon_changes_force_remove(
        changes: *mut Changes,
        fiber: *const FiberId,
        reason: Slice,
    ) -> Status;
    pub fn siphon_changes_reap(changes: *mut Changes, fiber: *const FiberId) -> Status;
    pub fn siphon_changes_charge(
        changes: *mut Changes,
        fiber: *const FiberId,
        counters: *const CounterView,
        len: usize,
    ) -> Status;
    pub fn siphon_fiber_path(parent: Slice, key: Slice, out: *mut Buffer) -> Status;
    pub fn siphon_snapshot_stat(
        snapshot: *const Snapshot,
        path: Slice,
        context: *mut c_void,
        visit: Option<EntryVisitor>,
    ) -> Status;
    pub fn siphon_snapshot_list(
        snapshot: *const Snapshot,
        path: Slice,
        range: SnapshotRange,
        context: *mut c_void,
        visit: Option<EntryVisitor>,
    ) -> Status;
    pub fn siphon_snapshot_find(
        snapshot: *const Snapshot,
        query: SnapshotQuery,
        context: *mut c_void,
        visit: Option<FoundVisitor>,
    ) -> Status;
    pub fn siphon_snapshot_read(
        snapshot: *const Snapshot,
        path: Slice,
        offset: u64,
        length: u64,
        out: *mut Buffer,
    ) -> Status;
    pub fn siphon_snapshot_seq(snapshot: *const Snapshot, out: *mut u64) -> Status;
    pub fn siphon_snapshot_journal(
        snapshot: *const Snapshot,
        after: u64,
        limit: usize,
        context: *mut c_void,
        visit: Option<JournalVisitor>,
    ) -> Status;
    pub fn siphon_snapshot_fiber(
        snapshot: *const Snapshot,
        process: Slice,
        context: *mut c_void,
        visit: Option<FiberVisitor>,
    ) -> Status;
    pub fn siphon_snapshot_fibers(
        snapshot: *const Snapshot,
        parent: Slice,
        kind: Slice,
        context: *mut c_void,
        visit: Option<FiberIdVisitor>,
    ) -> Status;
    pub fn siphon_snapshot_free(snapshot: *mut Snapshot);
    pub fn siphon_run_info(
        run: *const Run,
        id: *mut FiberId,
        time_ms: *mut i64,
        stop_handler: *mut u8,
    ) -> Status;
    pub fn siphon_run_new_id(run: *const Run, out: *mut Buffer) -> Status;
    pub fn siphon_run_changes(run: *mut Run) -> *mut Changes;
    pub fn siphon_run_snapshot(run: *const Run) -> *const Snapshot;
    pub fn siphon_run_request(run: *mut Run, key: Slice, name: Slice, payload: Slice) -> Status;
    pub fn siphon_run_cancel_request(run: *mut Run, key: Slice) -> Status;
    pub fn siphon_run_answer(
        run: *const Run,
        key: Slice,
        kind: *mut u32,
        payload: *mut Buffer,
    ) -> Status;
    pub fn siphon_run_mail(
        run: *const Run,
        key: Slice,
        context: *mut c_void,
        visit: Option<MailVisitor>,
    ) -> Status;
    pub fn siphon_run_take_mail(
        run: *mut Run,
        key: Slice,
        context: *mut c_void,
        visit: Option<MailVisitor>,
    ) -> Status;
    pub fn siphon_run_wait_for(run: *mut Run, kind: u32, value: Slice) -> Status;
    pub fn siphon_run_wait_until(run: *mut Run, deadline: i64) -> Status;
    pub fn siphon_run_end(run: *mut Run, kind: u32, payload: Slice) -> Status;
    pub fn siphon_reply_submit(reply: *const Reply, kind: u32, payload: Slice) -> Status;
    pub fn siphon_reply_publish(reply: *const Reply, topic: Slice, data: Slice) -> Status;
    pub fn siphon_reply_commands(reply: *const Reply, out: *mut *mut RequestCommands) -> Status;
    pub fn siphon_reply_free(reply: *mut Reply);
    pub fn siphon_request_commands_spawn(
        commands: *const RequestCommands,
        namespace: *const Namespace,
        spec: Slice,
        command: *mut *mut Command,
        events: *mut *mut CommandEvents,
    ) -> Status;
    pub fn siphon_request_commands_free(commands: *mut RequestCommands);
}
