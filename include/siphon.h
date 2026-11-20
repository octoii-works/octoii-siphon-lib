#ifndef SIPHON_H
#define SIPHON_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
/* Development ABI 6. All handles are opaque.
 * Slices are borrowed for a call. Returned buffers must be freed by this library.
 * Never free or mutate handles concurrently with their use.
 * Runtime calls serialize internally. Snapshot reads are thread-safe.
 * Close stops execution; retained snapshots may keep the storage directory locked.
 * Status codes are listed below.
 * Always free status.error, including on success (a null buffer is accepted).
 */
typedef struct siphon_runtime siphon_runtime;
typedef struct siphon_snapshot siphon_snapshot;
typedef struct siphon_revision siphon_revision;
typedef struct siphon_changes siphon_changes;
typedef struct siphon_builder siphon_builder;
typedef struct siphon_run siphon_run;
typedef struct siphon_reply siphon_reply;
typedef struct siphon_request_commands siphon_request_commands;
typedef struct siphon_commit_watch siphon_commit_watch;
typedef struct siphon_following siphon_following;
typedef struct siphon_namespace siphon_namespace;
typedef struct siphon_command siphon_command;
typedef struct siphon_command_events siphon_command_events;
typedef struct { const uint8_t *data; size_t len; } siphon_slice;
typedef struct { uint8_t *data; size_t len; } siphon_buffer;
typedef struct { uint32_t code; siphon_buffer error; } siphon_status;
typedef struct { siphon_revision *revision; uint64_t seq; } siphon_revision_receipt;
/* The generation identifies one Fiber at an execution address. */
typedef struct { siphon_slice path; uint8_t generation[16]; } siphon_fiber_id;
enum {
    SIPHON_OK = 0, SIPHON_NOT_FOUND, SIPHON_INVALID, SIPHON_CLOSED, SIPHON_FAILED, SIPHON_PANICKED,
    SIPHON_CALLBACK_REENTRY, SIPHON_DENIED, SIPHON_READ_ONLY, SIPHON_EXISTS, SIPHON_UNSUPPORTED,
    SIPHON_MISSING_PROGRAM, SIPHON_REFUSED, SIPHON_WRITE_ABORTED, SIPHON_REOPEN_REQUIRED,
    SIPHON_CONFLICT, SIPHON_WOULD_BLOCK
};
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
    siphon_fiber_id id;
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
typedef void (*siphon_fiber_id_visitor)(void *context, const siphon_fiber_id *id);
/* The view, its slices, snapshot and batch are borrowed on the callback thread until it returns.
 * Do not free the snapshot or batch, or commit the phase batch separately.
 */
typedef struct {
    siphon_fiber_id id;
    int64_t time_ms;
    uint8_t stop_handler;
    const siphon_snapshot *snapshot;
    siphon_changes *changes;
    siphon_slice input;
    siphon_slice checkpoint;
} siphon_phase_view;
typedef void (*siphon_phase_callback)(void *context, siphon_run *run, const siphon_phase_view *phase);
typedef void (*siphon_release_callback)(void *context);
enum { SIPHON_COMMIT_HEAD = 1, SIPHON_COMMIT_CLOSED, SIPHON_COMMIT_FAULT };
typedef struct {
    uint32_t kind;
    uint64_t seq;
    uint32_t status; /* Failure code for Fault. SIPHON_OK for Head and Closed. */
    siphon_slice error; /* Borrowed UTF-8 failure text, empty for Head and Closed. */
} siphon_commit_event;
typedef void (*siphon_commit_callback)(void *context, const siphon_commit_event *event);
typedef struct {
    siphon_fiber_id fiber;
    siphon_slice key;
    uint8_t generation[16];
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
enum { SIPHON_STREAM_RESET = 1, SIPHON_STREAM_DATA, SIPHON_STREAM_CLOSED };
typedef struct {
    uint32_t kind;
    const siphon_request_id *id; /* Null for Reset. */
    siphon_slice topic;
    uint64_t offset;
    siphon_slice data;
} siphon_stream_view;
typedef void (*siphon_stream_callback)(void *context, const siphon_stream_view *event);
enum { SIPHON_FILE_DIRECTORY = 1, SIPHON_FILE_REGULAR, SIPHON_FILE_LINK };
typedef struct {
    uint32_t kind;
    uint8_t executable;
    uint64_t size;
    int64_t modified_ms;
    const uint64_t *file_id;
} siphon_file_stat;
typedef void (*siphon_file_visitor)(void *context, const siphon_file_stat *stat);
typedef void (*siphon_dirent_visitor)(void *context, siphon_slice name, uint32_t kind);
enum { SIPHON_COMMAND_OUTPUT = 1, SIPHON_COMMAND_TITLE, SIPHON_COMMAND_EXIT_CODE, SIPHON_COMMAND_EXIT_SIGNAL, SIPHON_COMMAND_GAP };
typedef struct {
    uint32_t kind;
    int32_t value; /* Exit code or signal number. Zero for other events. */
    siphon_slice data; /* Output bytes or title text. Empty for Gap and Exit. */
    uint64_t offset; /* First byte of Output or Gap. Zero for other events. */
    uint64_t through; /* Exclusive end of Output or Gap. Zero for other events. */
} siphon_command_view;
typedef void (*siphon_command_callback)(void *context, const siphon_command_view *event);
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
typedef struct { const siphon_slice *data; size_t len; } siphon_slice_list;
typedef struct {
    siphon_slice_list children;
    siphon_slice_list changes;
    siphon_slice_list mail;
    const int64_t *deadline; /* Milliseconds since the Unix epoch. Null omits the deadline. */
} siphon_wait_view;
typedef struct {
    uint32_t kind;
    siphon_slice payload;
    const siphon_wait_view *wait; /* Required for SIPHON_END_WAIT. Null for other endings. */
} siphon_ending_view;
enum { SIPHON_OUTCOME_PENDING = 0, SIPHON_OUTCOME_COMPLETED, SIPHON_OUTCOME_FAILED, SIPHON_OUTCOME_STOPPED };
enum {
    SIPHON_SIGNAL_HANGUP = 1, SIPHON_SIGNAL_INTERRUPT, SIPHON_SIGNAL_TERMINATE, SIPHON_SIGNAL_KILL,
    SIPHON_SIGNAL_STOP, SIPHON_SIGNAL_CONTINUE
};
enum { SIPHON_ANSWER_GIVEN = 1, SIPHON_ANSWER_FAILED, SIPHON_ANSWER_INTERRUPTED };
enum { SIPHON_REQUEST_STOP = 1, SIPHON_REQUEST_CLOSED };
uint32_t siphon_abi_version(void);
void siphon_buffer_free(siphon_buffer buffer);
/* Opens a volume for state operations. Directory is UTF-8, empty for memory storage.
 * If journal_entries is null, the volume uses its default retention limit.
 */
siphon_status siphon_runtime_open(siphon_slice directory, const uint64_t *journal_entries, siphon_runtime **out);
/* Binds a builder to this opening without retaining the runtime. */
siphon_status siphon_builder_new(const siphon_runtime *runtime, siphon_builder **out);
/* Enables local command execution when the builder starts execution. */
siphon_status siphon_builder_local_machine(siphon_builder *builder);
void siphon_builder_free(siphon_builder *builder);
/* With non-null release, ownership of context transfers on every call, even on error.
 * Context must support concurrent phase invocations. Release runs after all invocations, or on
 * registration/startup failure. Phase and release callbacks must not unwind or wait for runtime calls.
 * The callback receives input bytes and an optional JSON checkpoint. An empty checkpoint means
 * none. Borrowed phase values are local to the callback thread and valid until it returns.
 */
siphon_status siphon_builder_fiber(siphon_builder *builder, siphon_slice kind, uint32_t version, uint8_t stop_handler,
    uint32_t retries, int64_t delay_min_ms, int64_t delay_max_ms, void *context, siphon_phase_callback run, siphon_release_callback release);
/* With release, context transfers even on registration error. Keep context ready before start and
 * thread-safe for concurrent deliveries. The identity and its byte slices and the payload are valid
 * only during delivery. Copy any data needed by host work and transfer reply to it.
 * Each call owns reply, even if it declines to handle the request. It may return an empty watch.
 * The watch transfers its context to Siphon if release is supplied. Stop asks the host to wind
 * down and still allows an answer. Closed ends the delivery. A submitted answer may still be
 * pending commit. Siphon releases the watch after Closed. All delivery, notification and release
 * callbacks return promptly and do not unwind.
 */
siphon_status siphon_builder_handle_request(siphon_builder *builder, siphon_slice name, void *context,
    siphon_request_deliver deliver, siphon_release_callback release);
/* Registration transfers context with release even on error. The schema callback borrows the
 * request payload. Returned bytes remain readable until Siphon calls the result's release, or
 * have static lifetime if release is null. Callbacks return promptly and do not unwind.
 */
siphon_status siphon_builder_ask_person(siphon_builder *builder, siphon_slice name, void *context,
    siphon_schema_callback schema, siphon_release_callback release);
/* Consumes builder on every call. Restored phases may invoke callbacks before it returns. */
siphon_status siphon_builder_start(siphon_builder *builder);
/* Blocking runtime calls return status 6 from a callback.
 * Free in that context releases the handle immediately and the runtime shuts down asynchronously.
 */
siphon_status siphon_runtime_close(const siphon_runtime *runtime);
/* Free closes the runtime and consumes the reference obtained from open. Retained references
 * may outlive the owner's free call. Pair each siphon_runtime_retain with siphon_runtime_release.
 */
siphon_status siphon_runtime_free(const siphon_runtime *runtime);
void siphon_runtime_retain(const siphon_runtime *runtime);
void siphon_runtime_release(const siphon_runtime *runtime);
/* Commit returns the visible sequence and its owned revision. Failure clears both output fields.
 * The revision does not retain the volume. Free it with siphon_revision_free.
 */
siphon_status siphon_runtime_commit(const siphon_runtime *runtime, const siphon_changes *changes, siphon_revision_receipt *out);
void siphon_revision_free(siphon_revision *revision);
/* Confirms the receipt in its original open volume. Through reports the durable sequence. */
siphon_status siphon_runtime_flush(const siphon_runtime *runtime, const siphon_revision *revision, uint64_t *through);
siphon_status siphon_runtime_snapshot(const siphon_runtime *runtime, siphon_snapshot **out);
/* A follower starts with Reset, then replays retained data and follows the topic. */
siphon_status siphon_runtime_follow(const siphon_runtime *runtime, siphon_slice topic, siphon_following **out);
/* Queue a next read without blocking. A non-null context requires release, which takes ownership
 * on every call. The callback borrows the event (null at end) only until it returns. Releasing
 * the follower can discard queued callbacks, and always releases their contexts.
 */
siphon_status siphon_following_read(const siphon_following *following, void *context,
    siphon_stream_callback receive, siphon_release_callback release);
void siphon_following_free(siphon_following *following);
/* Namespace paths and host directories are UTF-8. An empty mount point denotes the root. */
siphon_status siphon_namespace_new(siphon_namespace **out);
void siphon_namespace_free(siphon_namespace *namespace_);
siphon_status siphon_namespace_mount_host(const siphon_namespace *namespace_, siphon_slice at,
    siphon_slice host, uint8_t writable);
siphon_status siphon_namespace_hide(const siphon_namespace *namespace_, siphon_slice at);
siphon_status siphon_namespace_mount_labels(const siphon_namespace *namespace_, const siphon_runtime *runtime,
    siphon_slice at, siphon_slice kind, siphon_slice label, siphon_slice under);
siphon_status siphon_namespace_stat(const siphon_namespace *namespace_, siphon_slice path, void *context,
    siphon_file_visitor visit);
siphon_status siphon_namespace_list(const siphon_namespace *namespace_, siphon_slice path, siphon_snapshot_range range,
    void *context, siphon_dirent_visitor visit);
siphon_status siphon_namespace_read(const siphon_namespace *namespace_, siphon_slice path, uint64_t offset,
    uint64_t length, siphon_buffer *out);
siphon_status siphon_namespace_write(const siphon_namespace *namespace_, siphon_slice path, siphon_slice data);
siphon_status siphon_namespace_mkdir(const siphon_namespace *namespace_, siphon_slice path);
uint8_t siphon_machine_confines(void);
/* Requires siphon_builder_local_machine. A command starts within the mounts configured on namespace_.
 * Pass the owning Fiber's exact identity, or null to record the command at the runtime root.
 * The JSON spec carries exec, cwd, set, clear, stdio, head_bytes, tail_bytes and log.
 */
siphon_status siphon_runtime_spawn(const siphon_runtime *runtime, const siphon_namespace *namespace_,
    const siphon_fiber_id *owner, siphon_slice spec, siphon_command **command, siphon_command_events **events);
/* A read transfers context with release on every call. A null event indicates the end.
 * Pipes and terminals report skipped live output as Gap. Use siphon_command_read_output for
 * retained bytes. Coprocesses and channels may wait on a slow reader.
 */
siphon_status siphon_command_events_read(const siphon_command_events *events, void *context,
    siphon_command_callback receive, siphon_release_callback release);
void siphon_command_events_free(siphon_command_events *events);
void siphon_command_free(siphon_command *command);
/* Enqueues up to 64 KiB. SIPHON_WOULD_BLOCK means the input queue is full and no bytes were accepted. */
siphon_status siphon_command_write(const siphon_command *command, siphon_slice data);
siphon_status siphon_command_close_input(const siphon_command *command);
siphon_status siphon_command_signal(const siphon_command *command, uint32_t signal);
siphon_status siphon_command_resize(const siphon_command *command, uint16_t rows, uint16_t cols);
siphon_status siphon_command_output(const siphon_command *command, siphon_buffer *out);
siphon_status siphon_command_read_output(const siphon_command *command, uint64_t from, uint64_t *offset, siphon_buffer *out);
siphon_status siphon_command_screen(const siphon_command *command, uint64_t *through, siphon_buffer *out);
siphon_status siphon_command_screen_text(const siphon_command *command, siphon_buffer *out);
/* Reads prompts from a snapshot of this volume. Each visitor borrows the view for the call. */
siphon_status siphon_runtime_person_requests(const siphon_runtime *runtime, const siphon_snapshot *snapshot,
    siphon_slice under, void *context, siphon_person_visitor visit);
/* Returns the visible sequence and its owned revision. Status 1 means missing or replaced request.
 * Status 2 means invalid JSON or an unfit answer. Failure clears both output fields.
 */
siphon_status siphon_runtime_answer(const siphon_runtime *runtime, const siphon_request_id *id,
    siphon_slice answer, siphon_revision_receipt *out);
/* IDs are UTF-8 UUIDv7 values from the volume. Free the buffer with siphon_buffer_free. */
siphon_status siphon_runtime_new_id(const siphon_runtime *runtime, siphon_buffer *out);
siphon_status siphon_runtime_now_ms(const siphon_runtime *runtime, int64_t *out);
/* Watch delivers its initial event before returning success. Head notifications may coalesce
 * commits. Closed and Fault carry the final visible sequence and end the subscription. A watch
 * registered after shutdown starts may wait for the terminal event. A terminal initial event
 * is delivered once, with its context released before this call returns. The journal supplies
 * change details. The event and its error are valid only during the callback.
 * With release, ownership of context transfers on every call. A non-null context requires
 * release. Callbacks return promptly and must not unwind or make blocking runtime calls.
 */
siphon_status siphon_runtime_watch_commits(const siphon_runtime *runtime, void *context,
    siphon_commit_callback notify, siphon_release_callback release, siphon_commit_watch **out);
/* Frees the watch and requests cancellation. Release runs after the last notification. */
void siphon_commit_watch_free(siphon_commit_watch *watch);
siphon_status siphon_changes_new(siphon_changes **out);
/* Conditions the batch on this snapshot's head without retaining the snapshot. */
siphon_status siphon_changes_expect(siphon_changes *changes, const siphon_snapshot *snapshot);
uint8_t siphon_changes_is_empty(const siphon_changes *changes);
siphon_status siphon_changes_extend(siphon_changes *changes, siphon_changes *other);
siphon_status siphon_changes_limit(siphon_changes *changes, const siphon_fiber_id *fiber, siphon_slice counter, uint64_t maximum);
void siphon_changes_free(siphon_changes *changes);
/* Directory and file operations address user data relative to its root. */
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
/* An empty parent selects the execution root. Free the path with siphon_buffer_free. */
siphon_status siphon_fiber_path(siphon_slice parent, siphon_slice key, siphon_buffer *out);
/* A null parent starts a root Fiber. A child start resolves the supplied parent identity.
 * The kind must be nonempty and the version positive. The batch copies the input bytes.
 */
siphon_status siphon_changes_start(siphon_changes *changes, const siphon_fiber_id *parent, siphon_slice key, siphon_slice kind,
    uint32_t version, siphon_slice input);
/* Execution changes copy the Fiber identity and any other byte fields. They resolve
 * the identity at commit time and reject a missing or replaced instance.
 */
siphon_status siphon_changes_send(siphon_changes *changes, const siphon_fiber_id *fiber, siphon_mail_view mail);
/* Reasons are optional bytes. A null, zero-length slice omits the reason.
 * Remove lets stop handlers run. Force remove kills live Fibers before discarding the subtree.
 */
siphon_status siphon_changes_signal(siphon_changes *changes, const siphon_fiber_id *fiber, uint32_t signal, siphon_slice reason);
siphon_status siphon_changes_remove(siphon_changes *changes, const siphon_fiber_id *fiber, siphon_slice reason);
siphon_status siphon_changes_force_remove(siphon_changes *changes, const siphon_fiber_id *fiber, siphon_slice reason);
siphon_status siphon_changes_reap(siphon_changes *changes, const siphon_fiber_id *fiber);
/* The batch copies the counter array and its names during the call. */
siphon_status siphon_changes_charge(siphon_changes *changes, const siphon_fiber_id *fiber,
    const siphon_counter_view *counters, size_t len);
/* Allocates from the same volume inside the phase. Free the buffer with siphon_buffer_free. */
siphon_status siphon_run_new_id(const siphon_run *run, siphon_buffer *out);
/* Stages a request with a nonempty UTF-8 key and name. The payload is JSON. */
siphon_status siphon_run_request(siphon_run *run, siphon_slice key, siphon_slice name, siphon_slice payload);
siphon_status siphon_run_cancel_request(siphon_run *run, siphon_slice key);
/* Takes the observed answer once. The phase's commit consumes it.
 * Status 1 means absent or already taken. Free the returned payload with siphon_buffer_free.
 */
siphon_status siphon_run_take_answer(siphon_run *run, siphon_slice key, uint32_t *kind, siphon_buffer *payload);
/* A synchronous visitor borrows the phase's mail. An absent entry returns status 1.
 * The visitor must not unwind or mutate the phase. Mail keys, names and payloads are bytes.
 * Take stages consumption once for the phase's commit and visits that delivery if visit is set.
 * Take returns status 1 if the delivery is absent or already taken in this phase.
 */
siphon_status siphon_run_mail(const siphon_run *run, siphon_slice key, void *context, siphon_mail_visitor visit);
siphon_status siphon_run_take_mail(siphon_run *run, siphon_slice key, void *context, siphon_mail_visitor visit);
/* Submit a JSON answer, UTF-8 failure or empty interruption. Success only means queued.
 * Status 3 means this reply is closed. Free exactly once; pending free abandons the delivery.
 */
siphon_status siphon_reply_submit(const siphon_reply *reply, uint32_t kind, siphon_slice payload);
/* Copies the reply's command capability into an owned handle, even during delivery.
 * The handle can outlive the reply and runtime. Release it with siphon_request_commands_free.
 */
siphon_status siphon_reply_commands(const siphon_reply *reply, siphon_request_commands **out);
/* Starts a command owned by the exact request. Uses the same namespace, JSON spec, and output
 * ownership as siphon_runtime_spawn and requires siphon_builder_local_machine. A closed or
 * interrupted request refuses new commands. Returns SIPHON_CLOSED when the runtime has closed.
 * Call from host work outside a Siphon callback.
 */
siphon_status siphon_request_commands_spawn(const siphon_request_commands *commands, const siphon_namespace *namespace_,
    siphon_slice spec, siphon_command **command, siphon_command_events **events);
void siphon_request_commands_free(siphon_request_commands *commands);
/* Publishes bytes on a UTF-8 topic for this reply's request. Status 3 means delivery is closed. */
siphon_status siphon_reply_publish(const siphon_reply *reply, siphon_slice topic, siphon_slice data);
void siphon_reply_free(siphon_reply *reply);
/* Set one ending and copy its payload. Fail/Throw take UTF-8 messages; other endings use JSON.
 * Empty payload is allowed for a missing checkpoint or stopped result. No ending means a throw.
 */
siphon_status siphon_run_end(siphon_run *run, const siphon_ending_view *ending);
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
siphon_status siphon_snapshot_fibers(const siphon_snapshot *snapshot, siphon_slice parent, siphon_slice kind,
    void *context, siphon_fiber_id_visitor visit);
void siphon_snapshot_free(siphon_snapshot *snapshot);
#ifdef __cplusplus
}
#endif
#endif
