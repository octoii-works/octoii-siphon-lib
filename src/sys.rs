//! Checked-in declarations for include/siphon.h; consumer builds do not run bindgen.
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
pub struct Slice {
    pub data: *const u8,
    pub len: usize,
}
#[repr(C)]
pub struct Buffer {
    pub data: *mut u8,
    pub len: usize,
}
#[repr(C)]
pub struct Status {
    pub code: u32,
    pub error: Buffer,
}
unsafe extern "C" {
    pub fn siphon_abi_version() -> u32;
    pub fn siphon_buffer_free(buffer: Buffer);
    pub fn siphon_runtime_open(directory: Slice, out: *mut *mut Runtime) -> Status;
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
    pub fn siphon_snapshot_read(
        snapshot: *const Snapshot,
        path: Slice,
        offset: u64,
        length: u64,
        out: *mut Buffer,
    ) -> Status;
    pub fn siphon_snapshot_seq(snapshot: *const Snapshot, out: *mut u64) -> Status;
    pub fn siphon_snapshot_free(snapshot: *mut Snapshot);
}
