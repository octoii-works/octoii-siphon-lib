#ifndef SIPHON_H
#define SIPHON_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
/* ABI 3 is experimental until a release. All handles are opaque.
 * Slices are borrowed for a call. Returned buffers must be freed by this library.
 * Never free or mutate handles concurrently with their use.
 * Runtime calls serialize internally. Snapshot reads are thread-safe.
 * Close stops execution; retained snapshots may keep the storage directory locked.
 * Status: 0 success, 1 missing, 2 invalid, 3 closed, 4 failed, 5 panic, 6 callback reentry.
 * Always free status.error, including on success (a null buffer is accepted).
 */
typedef struct siphon_runtime siphon_runtime;
typedef struct siphon_snapshot siphon_snapshot;
typedef struct siphon_changes siphon_changes;
typedef struct siphon_builder siphon_builder;
typedef struct siphon_run siphon_run;
typedef struct siphon_reply siphon_reply;
typedef struct { const uint8_t *data; size_t len; } siphon_slice;
typedef struct { uint8_t *data; size_t len; } siphon_buffer;
typedef struct { uint32_t code; siphon_buffer error; } siphon_status;
typedef void (*siphon_phase_callback)(void *context, siphon_run *run, siphon_slice input, siphon_slice checkpoint);
typedef void (*siphon_release_callback)(void *context);
typedef void (*siphon_request_notify)(void *context, uint32_t event);
typedef struct {
    void *context;
    siphon_request_notify notify;
    siphon_release_callback release;
} siphon_request_watch;
typedef siphon_request_watch (*siphon_request_deliver)(void *context, siphon_slice process, siphon_slice key,
    siphon_slice payload, siphon_reply *reply);
enum {
    SIPHON_END_NEXT = 1, SIPHON_END_WAIT, SIPHON_END_DONE, SIPHON_END_FAIL, SIPHON_END_STOPPED, SIPHON_END_THROW
};
enum { SIPHON_WAIT_CHILD = 1, SIPHON_WAIT_CHANGE, SIPHON_WAIT_MAIL };
enum { SIPHON_OUTCOME_PENDING = 0, SIPHON_OUTCOME_COMPLETED, SIPHON_OUTCOME_FAILED, SIPHON_OUTCOME_STOPPED };
enum { SIPHON_ANSWER_GIVEN = 1, SIPHON_ANSWER_FAILED, SIPHON_ANSWER_INTERRUPTED };
enum { SIPHON_REQUEST_STOP = 1, SIPHON_REQUEST_CLOSED };
uint32_t siphon_abi_version(void);
void siphon_buffer_free(siphon_buffer buffer);
siphon_status siphon_builder_new(siphon_builder **out);
void siphon_builder_free(siphon_builder *builder);
/* With non-null release, ownership of context transfers on every call, even on error.
 * Context must support concurrent phase invocations. Release runs after all invocations, or on
 * registration/startup failure. Phase and release callbacks must not unwind or wait for runtime calls.
 * Input and checkpoints are JSON; an empty checkpoint means none. Borrowed phase values are local
 * to the callback thread and valid only until the callback returns.
 */
siphon_status siphon_builder_program(siphon_builder *builder, siphon_slice kind, uint32_t version, uint8_t stop_handler,
    uint32_t retries, int64_t delay_min_ms, int64_t delay_max_ms, void *context, siphon_phase_callback run, siphon_release_callback release);
/* With release, context transfers even on registration error. Keep context ready before open and
 * thread-safe for concurrent deliveries. Copy the borrowed slices and transfer reply to host work.
 * Each call owns reply, even if it declines to handle the request. It may return an empty watch.
 * The watch transfers its context to Siphon if release is supplied. Stop allows an answer; Closed
 * means no more answers are accepted, not that an earlier answer committed. Siphon then releases
 * the watch. All delivery, notification and release callbacks return promptly and do not unwind.
 */
siphon_status siphon_builder_handle_request(siphon_builder *builder, siphon_slice name, void *context,
    siphon_request_deliver deliver, siphon_release_callback release);
/* Consumes builder on every call; directory is UTF-8, empty for memory storage.
 * Restored phases may invoke their callbacks before this call returns the runtime handle.
 */
siphon_status siphon_builder_open(siphon_builder *builder, siphon_slice directory, siphon_runtime **out);
/* Runtime open, explicit close, commit, and snapshot return status 6 from a callback.
 * Free in that context releases the handle immediately and the runtime shuts down asynchronously.
 */
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
siphon_status siphon_process_path(siphon_slice parent, siphon_slice key, siphon_buffer *out);
siphon_status siphon_changes_start(siphon_changes *changes, siphon_slice parent, siphon_slice key, siphon_slice kind,
    uint32_t version, siphon_slice input);
siphon_status siphon_changes_terminate(siphon_changes *changes, siphon_slice process);
siphon_status siphon_run_info(const siphon_run *run, siphon_slice *path, int64_t *time_ms, uint8_t *stop_handler);
/* Borrowed for this callback. Do not free, send to other threads, or separately commit. */
siphon_changes *siphon_run_changes(siphon_run *run);
const siphon_snapshot *siphon_run_snapshot(const siphon_run *run);
siphon_status siphon_run_wait_for(siphon_run *run, uint32_t kind, siphon_slice value);
siphon_status siphon_run_wait_until(siphon_run *run, int64_t deadline);
/* Stage a request and return its unique key. A missing answer returns status 1.
 * The outputs belong to the library; free them with siphon_buffer_free.
 */
siphon_status siphon_run_request(siphon_run *run, siphon_slice name, siphon_slice payload, siphon_buffer *key);
siphon_status siphon_run_answer(const siphon_run *run, siphon_slice key, uint32_t *kind, siphon_buffer *payload);
siphon_status siphon_run_take_mail(siphon_run *run, siphon_slice key);
/* Submit a JSON answer, UTF-8 failure or empty interruption. Success only means queued.
 * Status 3 means this reply is closed. Free exactly once; pending free abandons the delivery.
 */
siphon_status siphon_reply_submit(const siphon_reply *reply, uint32_t kind, siphon_slice payload);
void siphon_reply_free(siphon_reply *reply);
/* Set one ending and copy its payload. Fail/Throw take UTF-8 messages; other endings use JSON.
 * Empty payload is allowed for a missing checkpoint or stopped result. No ending means a throw.
 */
siphon_status siphon_run_end(siphon_run *run, uint32_t kind, siphon_slice payload);
siphon_status siphon_snapshot_read(const siphon_snapshot *snapshot, siphon_slice path, uint64_t offset, uint64_t length, siphon_buffer *out);
siphon_status siphon_snapshot_seq(const siphon_snapshot *snapshot, uint64_t *out);
/* Missing process returns status 1; absent payloads and reasons are empty buffers. */
siphon_status siphon_snapshot_outcome(const siphon_snapshot *snapshot, siphon_slice process, uint32_t *kind,
    siphon_buffer *payload, siphon_buffer *reason);
void siphon_snapshot_free(siphon_snapshot *snapshot);
#ifdef __cplusplus
}
#endif
#endif
