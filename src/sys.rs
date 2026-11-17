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
pub struct Snapshot {
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

pub const WAIT_CHILD: u32 = 1;
pub const WAIT_CHANGE: u32 = 2;
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

type PhaseCallback = unsafe extern "C" fn(*mut c_void, *mut Run, Slice, Slice);
type ReleaseCallback = unsafe extern "C" fn(*mut c_void);

unsafe extern "C" {
    pub fn siphon_abi_version() -> u32;
    pub fn siphon_buffer_free(buffer: Buffer);
    pub fn siphon_builder_new(out: *mut *mut Builder) -> Status;
    pub fn siphon_builder_free(builder: *mut Builder);
    pub fn siphon_builder_program(
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
    pub fn siphon_builder_open(
        builder: *mut Builder,
        directory: Slice,
        out: *mut *mut Runtime,
    ) -> Status;
    pub fn siphon_runtime_close(runtime: *const Runtime) -> Status;
    pub fn siphon_runtime_free(runtime: *mut Runtime) -> Status;
    pub fn siphon_runtime_commit(
        runtime: *const Runtime,
        changes: *const Changes,
        seq: *mut u64,
    ) -> Status;
    pub fn siphon_runtime_snapshot(runtime: *const Runtime, out: *mut *mut Snapshot) -> Status;
    pub fn siphon_changes_new(out: *mut *mut Changes) -> Status;
    pub fn siphon_changes_free(changes: *mut Changes);
    pub fn siphon_changes_mkdir(changes: *mut Changes, path: Slice) -> Status;
    pub fn siphon_changes_write(changes: *mut Changes, path: Slice, data: Slice) -> Status;
    pub fn siphon_changes_append(changes: *mut Changes, path: Slice, data: Slice) -> Status;
    pub fn siphon_changes_delete(changes: *mut Changes, path: Slice) -> Status;
    pub fn siphon_changes_start(
        changes: *mut Changes,
        parent: Slice,
        key: Slice,
        kind: Slice,
        version: u32,
        input: Slice,
    ) -> Status;
    pub fn siphon_changes_terminate(changes: *mut Changes, process: Slice) -> Status;
    pub fn siphon_process_path(parent: Slice, key: Slice, out: *mut Buffer) -> Status;
    pub fn siphon_snapshot_read(
        snapshot: *const Snapshot,
        path: Slice,
        offset: u64,
        length: u64,
        out: *mut Buffer,
    ) -> Status;
    pub fn siphon_snapshot_seq(snapshot: *const Snapshot, out: *mut u64) -> Status;
    pub fn siphon_snapshot_outcome(
        snapshot: *const Snapshot,
        process: Slice,
        kind: *mut u32,
        payload: *mut Buffer,
        reason: *mut Buffer,
    ) -> Status;
    pub fn siphon_snapshot_free(snapshot: *mut Snapshot);
    pub fn siphon_run_info(
        run: *const Run,
        path: *mut Slice,
        time_ms: *mut i64,
        stop_handler: *mut u8,
    ) -> Status;
    pub fn siphon_run_changes(run: *mut Run) -> *mut Changes;
    pub fn siphon_run_snapshot(run: *const Run) -> *const Snapshot;
    pub fn siphon_run_wait_for(run: *mut Run, kind: u32, value: Slice) -> Status;
    pub fn siphon_run_wait_until(run: *mut Run, deadline: i64) -> Status;
    pub fn siphon_run_end(run: *mut Run, kind: u32, payload: Slice) -> Status;
}
