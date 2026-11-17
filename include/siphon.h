#ifndef SIPHON_H
#define SIPHON_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
/* ABI 1 is experimental until a release. All handles are opaque.
 * Slices are borrowed for a call. Returned buffers must be freed by this library.
 * Never free or mutate handles concurrently with their use.
 * Runtime calls serialize internally. Snapshot reads are thread-safe.
 * Close stops execution; retained snapshots may keep the storage directory locked.
 * Status: 0 success, 1 missing, 2 invalid, 3 closed, 4 failed, 5 panic.
 * Always free status.error, including on success (a null buffer is accepted).
 */
typedef struct siphon_runtime siphon_runtime;
typedef struct siphon_snapshot siphon_snapshot;
typedef struct siphon_changes siphon_changes;
typedef struct { const uint8_t *data; size_t len; } siphon_slice;
typedef struct { uint8_t *data; size_t len; } siphon_buffer;
typedef struct { uint32_t code; siphon_buffer error; } siphon_status;
uint32_t siphon_abi_version(void);
void siphon_buffer_free(siphon_buffer buffer);
siphon_status siphon_runtime_open(siphon_slice directory, siphon_runtime **out);
siphon_status siphon_runtime_close(const siphon_runtime *runtime);
siphon_status siphon_runtime_free(siphon_runtime *runtime);
siphon_status siphon_runtime_commit(const siphon_runtime *runtime, const siphon_changes *changes, uint64_t *seq);
siphon_status siphon_runtime_snapshot(const siphon_runtime *runtime, siphon_snapshot **out);
siphon_status siphon_changes_new(siphon_changes **out);
void siphon_changes_free(siphon_changes *changes);
siphon_status siphon_changes_mkdir(siphon_changes *changes, siphon_slice path);
siphon_status siphon_changes_write(siphon_changes *changes, siphon_slice path, siphon_slice data);
siphon_status siphon_changes_append(siphon_changes *changes, siphon_slice path, siphon_slice data);
siphon_status siphon_changes_delete(siphon_changes *changes, siphon_slice path);
siphon_status siphon_snapshot_read(const siphon_snapshot *snapshot, siphon_slice path, uint64_t offset, uint64_t length, siphon_buffer *out);
siphon_status siphon_snapshot_seq(const siphon_snapshot *snapshot, uint64_t *out);
void siphon_snapshot_free(siphon_snapshot *snapshot);
#ifdef __cplusplus
}
#endif
#endif
