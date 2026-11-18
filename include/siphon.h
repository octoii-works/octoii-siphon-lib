#ifndef SIPHON_H
#define SIPHON_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
/* ABI 4 is experimental until a release. All handles are opaque.
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
typedef struct siphon_commit_watch siphon_commit_watch;
typedef struct { const uint8_t *data; size_t len; } siphon_slice;
typedef struct { uint8_t *data; size_t len; } siphon_buffer;
typedef struct { uint32_t code; siphon_buffer error; } siphon_status;
typedef struct {
    siphon_slice key;
    siphon_slice name;
    siphon_slice payload;
} siphon_mail_view;
typedef void (*siphon_mail_visitor)(void *context, const siphon_mail_view *mail);
typedef struct { siphon_slice name; siphon_slice value; } siphon_named_bytes;
typedef struct { const siphon_named_bytes *data; size_t len; } siphon_named_slice;
typedef struct {
    siphon_named_slice labels;
    siphon_named_slice annotations;
    uint64_t created_seq;
    uint64_t changed_seq;
} siphon_directory_view;
typedef struct {
    siphon_slice name;
    uint64_t length;
    const int64_t *expires_at;
    const siphon_directory_view *directory;
} siphon_entry_view;
typedef struct {
    siphon_slice prefix;
    siphon_slice after;
    size_t limit;
    uint8_t reverse;
} siphon_snapshot_range;
typedef struct {
    siphon_slice kind;
    siphon_slice name;
    siphon_slice value;
    siphon_slice under;
    siphon_slice after;
    size_t limit;
} siphon_snapshot_query;
typedef struct {
    uint64_t seq;
    const siphon_slice *paths;
    size_t paths_len;
} siphon_change_set_view;
typedef struct {
    const siphon_change_set_view *change_sets;
    size_t change_sets_len;
    uint64_t through;
    const uint64_t *oldest;
    uint64_t latest;
    uint8_t missed;
} siphon_journal_view;
typedef struct {
    siphon_slice name;
    uint64_t amount;
} siphon_counter_view;
typedef struct {
    siphon_slice kind;
    uint32_t version;
    uint64_t created_seq;
    const int64_t *waits_until;
    const siphon_counter_view *usage;
    size_t usage_len;
    uint8_t reapable;
    uint32_t outcome;
    siphon_slice payload;
    siphon_slice reason;
} siphon_fiber_record_view;
typedef void (*siphon_entry_visitor)(void *context, const siphon_entry_view *entry);
typedef void (*siphon_found_visitor)(void *context, siphon_slice path, siphon_slice value,
    const siphon_entry_view *entry);
typedef void (*siphon_journal_visitor)(void *context, const siphon_journal_view *page);
typedef void (*siphon_fiber_visitor)(void *context, const siphon_fiber_record_view *record);
typedef void (*siphon_phase_callback)(void *context, siphon_run *run, siphon_slice input, siphon_slice checkpoint);
typedef void (*siphon_release_callback)(void *context);
typedef void (*siphon_commit_callback)(void *context, uint64_t seq);
typedef struct {
    siphon_slice process;
    uint64_t started; /* Fiber creation sequence. */
    siphon_slice key;
    uint64_t created_seq; /* Request creation sequence. */
} siphon_request_id;
typedef void (*siphon_request_notify)(void *context, uint32_t event);
typedef struct {
    uint32_t kind; /* SIPHON_ANSWER_GIVEN for JSON Schema, SIPHON_ANSWER_FAILED for UTF-8 error text. */
    siphon_slice payload;
    void *context;
    siphon_release_callback release;
} siphon_schema_result;
typedef siphon_schema_result (*siphon_schema_callback)(void *context, siphon_slice payload);
typedef struct {
    siphon_request_id id;
    siphon_slice name;
    siphon_slice payload;
    siphon_slice answer_schema;
} siphon_person_request_view;
typedef void (*siphon_person_visitor)(void *context, const siphon_person_request_view *request);
typedef struct {
    void *context;
    siphon_request_notify notify;
    siphon_release_callback release;
} siphon_request_watch;
typedef siphon_request_watch (*siphon_request_deliver)(void *context, const siphon_request_id *id,
    siphon_slice payload, siphon_reply *reply);
enum {
    SIPHON_END_NEXT = 1, SIPHON_END_WAIT, SIPHON_END_DONE, SIPHON_END_FAIL, SIPHON_END_STOPPED, SIPHON_END_THROW
};
enum { SIPHON_WAIT_CHILD = 1, SIPHON_WAIT_CHANGE, SIPHON_WAIT_MAIL };
enum { SIPHON_OUTCOME_PENDING = 0, SIPHON_OUTCOME_COMPLETED, SIPHON_OUTCOME_FAILED, SIPHON_OUTCOME_STOPPED };
enum {
    SIPHON_SIGNAL_HANGUP = 1, SIPHON_SIGNAL_INTERRUPT, SIPHON_SIGNAL_TERMINATE, SIPHON_SIGNAL_KILL,
    SIPHON_SIGNAL_STOP, SIPHON_SIGNAL_CONTINUE
};
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
siphon_status siphon_builder_fiber(siphon_builder *builder, siphon_slice kind, uint32_t version, uint8_t stop_handler,
    uint32_t retries, int64_t delay_min_ms, int64_t delay_max_ms, void *context, siphon_phase_callback run, siphon_release_callback release);
/* With release, context transfers even on registration error. Keep context ready before open and
 * thread-safe for concurrent deliveries. The identity and its byte slices and the payload are valid
 * only during delivery. Copy any data needed by host work and transfer reply to it.
 * Each call owns reply, even if it declines to handle the request. It may return an empty watch.
 * The watch transfers its context to Siphon if release is supplied. Stop allows an answer; Closed
 * means no more answers are accepted, not that an earlier answer committed. Siphon then releases
 * the watch. All delivery, notification and release callbacks return promptly and do not unwind.
 */
siphon_status siphon_builder_handle_request(siphon_builder *builder, siphon_slice name, void *context,
    siphon_request_deliver deliver, siphon_release_callback release);
/* Registration transfers context with release even on error. The schema callback borrows the
 * request payload. Returned bytes remain readable until Siphon calls the result's release, or
 * have static lifetime if release is null. Callbacks return promptly and do not unwind.
 */
siphon_status siphon_builder_ask_person(siphon_builder *builder, siphon_slice name, void *context,
    siphon_schema_callback schema, siphon_release_callback release);
/* Consumes builder on every call; directory is UTF-8, empty for memory storage.
 * Restored phases may invoke their callbacks before this call returns the runtime handle.
 */
siphon_status siphon_builder_open(siphon_builder *builder, siphon_slice directory, siphon_runtime **out);
/* Blocking runtime calls return status 6 from a callback.
 * Free in that context releases the handle immediately and the runtime shuts down asynchronously.
 */
siphon_status siphon_runtime_close(const siphon_runtime *runtime);
siphon_status siphon_runtime_free(siphon_runtime *runtime);
siphon_status siphon_runtime_commit(const siphon_runtime *runtime, const siphon_changes *changes, uint64_t *seq);
siphon_status siphon_runtime_snapshot(const siphon_runtime *runtime, siphon_snapshot **out);
/* Reads prompts from a snapshot of this volume. Each visitor borrows the view for the call. */
siphon_status siphon_runtime_person_requests(const siphon_runtime *runtime, const siphon_snapshot *snapshot,
    siphon_slice under, void *context, siphon_person_visitor visit);
/* Returns status 1 for a missing or replaced request and status 2 for invalid JSON or an unfit answer. */
siphon_status siphon_runtime_answer(const siphon_runtime *runtime, const siphon_request_id *id,
    siphon_slice answer, uint64_t *seq);
/* IDs are UTF-8 UUIDv7 values from the volume. Free the buffer with siphon_buffer_free. */
siphon_status siphon_runtime_new_id(const siphon_runtime *runtime, siphon_buffer *out);
siphon_status siphon_runtime_now_ms(const siphon_runtime *runtime, int64_t *out);
/* Watch delivers its initial sequence before returning success. Subsequent notifications may
 * coalesce commits. The journal supplies change details. With release, ownership of context
 * transfers on every call. A non-null context requires release. Callbacks return promptly and
 * must not unwind or make blocking runtime calls.
 */
siphon_status siphon_runtime_watch_commits(const siphon_runtime *runtime, void *context,
    siphon_commit_callback notify, siphon_release_callback release, siphon_commit_watch **out);
/* Requests cancellation. Release runs after the last notification, including during runtime close. */
void siphon_commit_watch_free(siphon_commit_watch *watch);
siphon_status siphon_changes_new(siphon_changes **out);
void siphon_changes_free(siphon_changes *changes);
siphon_status siphon_changes_mkdir(siphon_changes *changes, siphon_slice path);
/* The batch copies the metadata pairs during the call. A null, zero-length value removes the name.
 * A non-null, zero-length value sets an empty value. An empty array leaves that metadata group intact.
 */
siphon_status siphon_changes_meta(siphon_changes *changes, siphon_slice path, siphon_named_slice labels,
    siphon_named_slice annotations);
siphon_status siphon_changes_write(siphon_changes *changes, siphon_slice path, siphon_slice data);
siphon_status siphon_changes_append(siphon_changes *changes, siphon_slice path, siphon_slice data);
siphon_status siphon_changes_seal(siphon_changes *changes, siphon_slice path);
/* Deadline is in milliseconds since the Unix epoch. Null clears expiration. */
siphon_status siphon_changes_expire(siphon_changes *changes, siphon_slice path, const int64_t *deadline);
siphon_status siphon_changes_delete(siphon_changes *changes, siphon_slice path);
siphon_status siphon_process_path(siphon_slice parent, siphon_slice key, siphon_buffer *out);
siphon_status siphon_changes_start(siphon_changes *changes, siphon_slice parent, siphon_slice key, siphon_slice kind,
    uint32_t version, siphon_slice input);
/* The batch copies the mail. A non-null started sequence fences delivery to that Fiber incarnation. */
siphon_status siphon_changes_send(siphon_changes *changes, siphon_slice process, siphon_mail_view mail,
    const uint64_t *started);
/* Reasons are optional bytes. A null, zero-length slice omits the reason.
 * Remove lets stop handlers run. Force remove kills live Fibers before discarding the subtree.
 */
siphon_status siphon_changes_signal(siphon_changes *changes, siphon_slice process, uint32_t signal, siphon_slice reason);
siphon_status siphon_changes_remove(siphon_changes *changes, siphon_slice path, siphon_slice reason);
siphon_status siphon_changes_force_remove(siphon_changes *changes, siphon_slice path, siphon_slice reason);
siphon_status siphon_changes_reap(siphon_changes *changes, siphon_slice process);
/* The batch copies the counter array and its names during the call. */
siphon_status siphon_changes_charge(siphon_changes *changes, siphon_slice process,
    const siphon_counter_view *counters, size_t len);
siphon_status siphon_run_info(const siphon_run *run, siphon_slice *path, int64_t *time_ms, uint8_t *stop_handler);
/* Allocates from the same volume inside the phase. Free the buffer with siphon_buffer_free. */
siphon_status siphon_run_new_id(const siphon_run *run, siphon_buffer *out);
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
/* A synchronous visitor borrows the phase's mail. An absent entry returns status 1.
 * The visitor must not unwind or mutate the phase. Mail keys, names and payloads are bytes.
 * Take stages consumption for the phase's commit.
 */
siphon_status siphon_run_mail(const siphon_run *run, siphon_slice key, void *context, siphon_mail_visitor visit);
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
/* The visitor runs synchronously on the calling thread. The entry and its fields are borrowed until
 * it returns. The visitor must not unwind or free the snapshot. Directory is null for a file;
 * expires_at is null or a time in milliseconds since the Unix epoch. Names and metadata are bytes.
 * Both calls return status 1 for a missing path. An empty page succeeds with no visits.
 * List selects byte names with prefix in ascending order (descending for nonzero reverse), up to
 * limit entries. A null, zero-length after slice starts at the beginning; any other cursor is exclusive.
 */
siphon_status siphon_snapshot_stat(const siphon_snapshot *snapshot, siphon_slice path, void *context,
    siphon_entry_visitor visit);
siphon_status siphon_snapshot_list(const siphon_snapshot *snapshot, siphon_slice path, siphon_snapshot_range range,
    void *context, siphon_entry_visitor visit);
/* Find returns up to limit matches in full byte path order. The visitor borrows path, value and entry.
 * A null, zero-length value matches any value. A present value with zero length matches the empty value.
 * An empty under path searches the root. After is an optional exclusive full path cursor.
 * No matches is success with zero visits.
 */
siphon_status siphon_snapshot_find(const siphon_snapshot *snapshot, siphon_snapshot_query query, void *context,
    siphon_found_visitor visit);
siphon_status siphon_snapshot_read(const siphon_snapshot *snapshot, siphon_slice path, uint64_t offset, uint64_t length, siphon_buffer *out);
siphon_status siphon_snapshot_seq(const siphon_snapshot *snapshot, uint64_t *out);
/* Read up to limit commits after the exclusive cursor. The visitor runs once on success, even for an
 * empty result. The page, arrays and byte slices are borrowed until it returns. The visitor must
 * not unwind or free the snapshot. Through advances over commits with no changed paths.
 * Oldest is null if no history is retained. Missed means some commits after the cursor were pruned.
 */
siphon_status siphon_snapshot_journal(const siphon_snapshot *snapshot, uint64_t after, size_t limit,
    void *context, siphon_journal_visitor visit);
/* A missing Fiber returns status 1. The synchronous visitor borrows the record and all its fields.
 * It must not unwind or free the snapshot. Waits_until is null or a Unix timestamp in milliseconds.
 * Optional byte slices are null when absent. An outcome can be available before the Fiber is reapable.
 */
siphon_status siphon_snapshot_fiber(const siphon_snapshot *snapshot, siphon_slice process, void *context,
    siphon_fiber_visitor visit);
void siphon_snapshot_free(siphon_snapshot *snapshot);
#ifdef __cplusplus
}
#endif
#endif
