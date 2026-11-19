use bytes::Bytes;
use octoii_siphon_lib::Snapshot;

use std::{
    panic::panic_any,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use octoii_siphon_lib::{
    Builder, Changes, Ending, Error, Fiber, Mail, Options, Outcome, Query, Range, Run, Runtime,
    Signal, Wait, process_path,
};

const TIMEOUT: Duration = Duration::from_secs(10);

fn until(runtime: &Runtime, ready: impl Fn(&Snapshot) -> bool) -> octoii_siphon_lib::OwnedSnapshot {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let snapshot = runtime.snapshot().unwrap();
        if ready(&snapshot) {
            return snapshot;
        }
        assert!(Instant::now() < deadline, "timed out waiting for the phase");
        thread::sleep(Duration::from_millis(10));
    }
}

fn finished(runtime: &Runtime, path: &[u8]) -> octoii_siphon_lib::OwnedSnapshot {
    until(runtime, |snapshot| {
        snapshot
            .fiber(path)
            .unwrap()
            .is_some_and(|record| record.reapable)
    })
}

fn start<P: Fiber>(runtime: &Runtime, key: &[u8], input: &P::Input) -> Bytes {
    let mut changes = Changes::new();
    changes.start::<P>(b"", key, input).unwrap();
    runtime.commit_blocking(&changes).unwrap();
    process_path(b"", key)
}

struct Parent;

impl Fiber for Parent {
    const KIND: &'static str = "contract.parent";
    const VERSION: u32 = 2;
    type Input = (String, u32);
    type Checkpoint = u32;
    type Output = (String, u32);

    fn run(
        &self,
        run: &mut Run<'_>,
        (label, start): Self::Input,
        checkpoint: Option<u32>,
    ) -> anyhow::Result<Ending<u32, Self::Output>> {
        assert_eq!(run.path(), process_path(b"", b"parent"));
        assert!(run.time_ms() > 0 && run.snapshot().sequence()? > 0);
        assert!(!run.is_stop_handler());
        let record = run.snapshot().fiber(run.path())?.unwrap();
        assert_eq!(record.kind, Self::KIND.as_bytes());
        assert_eq!(record.version, Self::VERSION);
        assert!(!record.reapable);
        assert_eq!(record.outcome, None);
        let gate = Query {
            kind: b"gate",
            name: b"state",
            ..Query::default()
        };
        match checkpoint {
            None => {
                run.changes
                    .mkdir(b"gate")
                    .meta(
                        b"gate",
                        &[(
                            Bytes::copy_from_slice(b"state"),
                            Some(Bytes::copy_from_slice(b"ready")),
                        )],
                        &[],
                    )
                    .write(b"log", b"next");
                assert_eq!(run.snapshot().read(b"log")?, None);
                assert!(run.snapshot().find(&gate, 1)?.is_empty());
                Ok(Ending::Next(start))
            }
            Some(value) if value == start => {
                let matches = run.snapshot().find(&gate, 1)?;
                assert_eq!(matches.len(), 1);
                assert_eq!(matches[0].path, Bytes::from_static(b"gate"));
                assert_eq!(matches[0].value, Bytes::from_static(b"ready"));
                let entry = run
                    .snapshot()
                    .stat(b"log")?
                    .expect("the previous phase wrote log");
                assert_eq!(entry.length, 4);
                assert!(entry.directory.is_none());
                let own = run.snapshot().stat(run.path())?.unwrap().directory.unwrap();
                assert!(
                    own.labels
                        .iter()
                        .any(|(_, value)| value == Self::KIND.as_bytes())
                );
                let page = run.snapshot().list(
                    b"gate",
                    &Range {
                        limit: 1,
                        ..Range::default()
                    },
                )?;
                assert_eq!(page, Some(Vec::new()));
                assert_eq!(
                    run.snapshot().read_range(b"log", 1, 2)?,
                    Some(Bytes::from_static(b"ex"))
                );
                run.changes.append(b"log", b" change");
                Ok(Ending::Wait(
                    value + 1,
                    Wait::new().change(b"gate").deadline(run.time_ms() + 60_000),
                ))
            }
            Some(value) if value == start + 1 => {
                assert_eq!(
                    run.snapshot().read(b"gate/signal")?,
                    Some(Bytes::from_static(b"go"))
                );
                let parent = run.path().to_vec();
                run.changes
                    .delete(b"gate/signal")
                    .start::<Child>(&parent, b"child", &value)?;
                run.changes.append(b"log", b" child");
                Ok(Ending::Wait(value + 1, Wait::new().child(b"child")))
            }
            Some(value) if value == start + 2 => {
                let child = process_path(run.path(), b"child");
                assert_eq!(
                    run.snapshot()
                        .fiber(&child)?
                        .expect("child record")
                        .decode_outcome::<Child>()?,
                    Some(Outcome::Completed(Some(value)))
                );
                run.changes.append(b"log", b" deadline");
                Ok(Ending::Wait(
                    value + 1,
                    Wait::new().deadline(run.time_ms() + 10),
                ))
            }
            Some(value) => {
                assert_eq!(value, start + 3);
                run.changes.append(b"log", b" done");
                Ok(Ending::Done((label, value)))
            }
        }
    }
}

struct Child;

impl Fiber for Child {
    const KIND: &'static str = "contract.child";
    type Input = u32;
    type Checkpoint = ();
    type Output = u32;

    fn run(&self, _: &mut Run<'_>, value: u32, _: Option<()>) -> anyhow::Result<Ending<(), u32>> {
        Ok(Ending::Done(value + 1))
    }
}

#[test]
fn checkpoints_waits_and_phase_writes_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let mut builder = Builder::new().unwrap();
    builder.fiber(Parent).unwrap().fiber(Child).unwrap();
    let runtime = builder.open(directory.path()).unwrap();
    let path = process_path(b"", b"parent");
    assert_eq!(runtime.snapshot().unwrap().fiber(&path).unwrap(), None);
    assert_eq!(
        start::<Parent>(&runtime, b"parent", &("answer".into(), 42)),
        path
    );

    let waiting = until(&runtime, |snapshot| {
        snapshot.read(b"log").unwrap().as_deref() == Some(b"next change")
    });
    let record = waiting.fiber(&path).unwrap().unwrap();
    assert_eq!(record.decode_outcome::<Parent>().unwrap(), None);
    assert_eq!(record.kind, Parent::KIND.as_bytes());
    assert_eq!(record.version, Parent::VERSION);
    assert!(record.waits_until.is_some_and(|deadline| deadline > 0));
    assert!(!record.reapable);
    assert_eq!(record.decode_outcome::<Child>().unwrap_err().code, 2);
    let mut wake = Changes::new();
    wake.write(b"gate/signal", b"go");
    runtime.commit_blocking(&wake).unwrap();

    let completed = Some(Outcome::Completed(Some(("answer".into(), 45))));
    let log = Some(Bytes::from_static(b"next change child deadline done"));
    let finished = finished(&runtime, &path);
    assert_eq!(
        finished
            .fiber(&path)
            .unwrap()
            .unwrap()
            .decode_outcome::<Parent>()
            .unwrap(),
        completed
    );
    assert_eq!(finished.read(b"log").unwrap(), log);
    assert_eq!(finished.read(b"gate/signal").unwrap(), None);
    assert_eq!(
        waiting.read(b"log").unwrap(),
        Some(Bytes::from_static(b"next change"))
    );
    runtime.close().unwrap();
    drop((waiting, finished, runtime));

    let reopened = Builder::new().unwrap().open(directory.path()).unwrap();
    let snapshot = reopened.snapshot().unwrap();
    assert_eq!(
        snapshot
            .fiber(&path)
            .unwrap()
            .unwrap()
            .decode_outcome::<Parent>()
            .unwrap(),
        completed
    );
    assert_eq!(snapshot.read(b"log").unwrap(), log);
}

struct VolumeIds;

impl Fiber for VolumeIds {
    const KIND: &'static str = "contract.volume-ids";
    type Input = ();
    type Checkpoint = ();
    type Output = (String, i64);

    fn run(
        &self,
        run: &mut Run<'_>,
        (): (),
        _: Option<()>,
    ) -> anyhow::Result<Ending<(), Self::Output>> {
        let id = run.new_id()?;
        let deadline = run.time_ms() + 60_000;
        let process = run.path().to_vec();
        run.changes
            .charge(&process, [(Bytes::from_static(b"phase-units"), 4)]);
        run.changes
            .write(b"phase-id", id.as_bytes())
            .seal(b"phase-id")
            .expire(b"phase-id", Some(deadline));
        Ok(Ending::Done((id, deadline)))
    }
}

#[test]
fn a_phase_and_its_runtime_allocate_from_the_same_volume() {
    let mut builder = Builder::new().unwrap();
    builder.fiber(VolumeIds).unwrap();
    let runtime = builder.memory().unwrap();
    let before = runtime.new_id().unwrap();
    let path = start::<VolumeIds>(&runtime, b"work", &());
    let snapshot = finished(&runtime, &path);
    let Some(Outcome::Completed(Some((id, deadline)))) = snapshot
        .fiber(&path)
        .unwrap()
        .unwrap()
        .decode_outcome::<VolumeIds>()
        .unwrap()
    else {
        panic!("the phase returned its ID and deadline")
    };
    let after = runtime.new_id().unwrap();
    assert!(before < id && id < after);
    let record = snapshot.fiber(&path).unwrap().unwrap();
    assert!(
        record
            .usage
            .contains(&(Bytes::from_static(b"phase-units"), 4))
    );
    assert_eq!(
        snapshot.read(b"phase-id").unwrap().as_deref(),
        Some(id.as_bytes())
    );
    assert_eq!(
        snapshot.stat(b"phase-id").unwrap().unwrap().expires_at,
        Some(deadline)
    );
    let mut append = Changes::new();
    append.append(b"phase-id", b"forbidden");
    assert!(runtime.commit_blocking(&append).is_err());
}

const MAIL_KEY: &[u8] = b"report\xff";

struct ReceivesMail;

impl Fiber for ReceivesMail {
    const KIND: &'static str = "contract.ordinary-mail";
    type Input = ();
    type Checkpoint = bool;
    type Output = ();

    fn run(
        &self,
        run: &mut Run<'_>,
        (): (),
        consumed: Option<bool>,
    ) -> anyhow::Result<Ending<bool, ()>> {
        let incoming = run.mail(MAIL_KEY)?;
        if consumed == Some(true) {
            assert_eq!(incoming, None);
            assert_eq!(
                run.mail(b"ack")?,
                Some(Mail {
                    key: Bytes::from_static(b"ack"),
                    name: Bytes::from_static(b"received"),
                    payload: Bytes::new(),
                })
            );
            return Ok(Ending::Done(()));
        }
        let Some(mail) = incoming else {
            run.changes.write(b"mail-ready", b"yes");
            return Ok(Ending::Wait(false, Wait::new().mail(MAIL_KEY)));
        };
        assert_eq!(mail.key, MAIL_KEY);
        assert_eq!(mail.name, Bytes::from_static(b"notice\xfe"));
        assert_eq!(mail.payload, Bytes::from_static(b"\x00\xff"));
        let process = run.path().to_vec();
        let started = run.snapshot().fiber(&process)?.unwrap().created_seq;
        run.take_mail(MAIL_KEY)?;
        run.changes.send(
            &process,
            Mail {
                key: Bytes::from_static(b"ack"),
                name: Bytes::from_static(b"received"),
                payload: Bytes::new(),
            },
            Some(started),
        );
        assert_eq!(run.mail(MAIL_KEY)?, Some(mail));
        assert_eq!(run.mail(b"ack")?, None);
        Ok(Ending::Next(true))
    }
}

#[test]
fn byte_keyed_mail_wakes_the_right_fiber_and_is_consumed_on_commit() {
    let mut builder = Builder::new().unwrap();
    builder.fiber(ReceivesMail).unwrap();
    let runtime = builder.memory().unwrap();
    let process = start::<ReceivesMail>(&runtime, b"receiver", &());
    let waiting = until(&runtime, |snapshot| {
        snapshot.read(b"mail-ready").unwrap().is_some()
    });
    let started = waiting.fiber(&process).unwrap().unwrap().created_seq;
    let mail = Mail {
        key: Bytes::from_static(MAIL_KEY),
        name: Bytes::from_static(b"notice\xfe"),
        payload: Bytes::from_static(b"\x00\xff"),
    };
    let mut stale = Changes::new();
    stale
        .write(b"rolled-back", b"no")
        .send(&process, mail.clone(), Some(started + 1));
    assert_eq!(
        runtime.commit_blocking(&stale).unwrap_err().status(),
        octoii_siphon_lib::Status::Refused
    );
    assert_eq!(
        runtime.snapshot().unwrap().read(b"rolled-back").unwrap(),
        None
    );

    let mut send = Changes::new();
    send.send(&process, mail.clone(), Some(started));
    runtime.commit_blocking(&send).unwrap();
    assert_eq!(
        finished(&runtime, &process)
            .fiber(&process)
            .unwrap()
            .unwrap()
            .decode_outcome::<ReceivesMail>()
            .unwrap(),
        Some(Outcome::Completed(Some(())))
    );
}

struct ExitsFirst;

impl Fiber for ExitsFirst {
    const KIND: &'static str = "contract.exits-first";
    type Input = ();
    type Checkpoint = ();
    type Output = ();

    fn run(&self, run: &mut Run<'_>, (): (), _: Option<()>) -> anyhow::Result<Ending<(), ()>> {
        let parent = run.path().to_vec();
        run.changes
            .mkdir(b"exit-gate")
            .start::<WaitsForRelease>(&parent, b"child", &())?;
        Ok(Ending::Done(()))
    }
}

struct WaitsForRelease;

impl Fiber for WaitsForRelease {
    const KIND: &'static str = "contract.waits-for-release";
    type Input = ();
    type Checkpoint = ();
    type Output = ();

    fn run(&self, run: &mut Run<'_>, (): (), _: Option<()>) -> anyhow::Result<Ending<(), ()>> {
        if run.snapshot().read(b"exit-gate/release")?.is_some() {
            return Ok(Ending::Done(()));
        }
        Ok(Ending::Wait((), Wait::new().change(b"exit-gate")))
    }
}

#[test]
fn a_recorded_outcome_can_precede_reaping_eligibility() {
    let mut builder = Builder::new().unwrap();
    builder
        .fiber(ExitsFirst)
        .unwrap()
        .fiber(WaitsForRelease)
        .unwrap();
    let runtime = builder.memory().unwrap();
    let path = start::<ExitsFirst>(&runtime, b"parent", &());
    let exiting = until(&runtime, |snapshot| {
        snapshot
            .fiber(&path)
            .unwrap()
            .is_some_and(|record| matches!(record.outcome, Some(Outcome::Completed(_))))
    });
    let before = exiting.fiber(&path).unwrap().unwrap();
    assert!(!before.reapable);
    assert_eq!(
        before.decode_outcome::<ExitsFirst>().unwrap(),
        Some(Outcome::Completed(Some(())))
    );

    let mut wake = Changes::new();
    wake.write(b"exit-gate/release", b"go");
    runtime.commit_blocking(&wake).unwrap();
    let ended = finished(&runtime, &path).fiber(&path).unwrap().unwrap();
    assert!(ended.reapable);
    assert_eq!(ended.created_seq, before.created_seq);
    assert_eq!(ended.outcome, before.outcome);
    assert_eq!(exiting.fiber(&path).unwrap(), Some(before));
}

struct Stoppable;

impl Fiber for Stoppable {
    const KIND: &'static str = "contract.stoppable";
    const OPTIONS: Options = Options {
        stop_handler: true,
        ..Options::DEFAULT
    };
    type Input = String;
    type Checkpoint = String;
    type Output = String;

    fn run(
        &self,
        run: &mut Run<'_>,
        input: String,
        checkpoint: Option<String>,
    ) -> anyhow::Result<Ending<String, String>> {
        if run.is_stop_handler() {
            assert_eq!(
                run.snapshot().read(b"stoppable")?,
                Some(Bytes::from_static(b"waiting"))
            );
            run.changes.write(b"stoppable", b"stopped");
            return Ok(Ending::Stopped(checkpoint));
        }
        assert_eq!(checkpoint, None);
        run.changes.write(b"stoppable", b"waiting");
        Ok(Ending::Wait(
            input,
            Wait::new().deadline(run.time_ms() + 60_000),
        ))
    }
}

#[test]
fn a_waiting_process_preserves_its_stop_handler_result() {
    let mut builder = Builder::new().unwrap();
    builder.fiber(Stoppable).unwrap();
    let runtime = builder.memory().unwrap();
    let path = start::<Stoppable>(&runtime, b"stop", &"partial output".into());
    until(&runtime, |snapshot| {
        snapshot.read(b"stoppable").unwrap().is_some()
    });
    let mut premature = Changes::new();
    premature.reap(&path);
    assert_eq!(
        runtime.commit_blocking(&premature).unwrap_err().status(),
        octoii_siphon_lib::Status::Refused
    );

    let mut changes = Changes::new();
    changes
        .charge(&path, [(Bytes::from_static(b"units"), 9)])
        .signal(
            &path,
            Signal::Terminate,
            Some(Bytes::from_static(b"test stop")),
        );
    runtime.commit_blocking(&changes).unwrap();

    let stopped = finished(&runtime, &path);
    let record = stopped.fiber(&path).unwrap().unwrap();
    assert_eq!(
        record.decode_outcome::<Stoppable>().unwrap(),
        Some(Outcome::Stopped {
            result: Some("partial output".into()),
            reason: Some(Bytes::from_static(b"test stop"))
        })
    );
    assert_eq!(
        stopped.read(b"stoppable").unwrap(),
        Some(Bytes::from_static(b"stopped"))
    );
    assert!(record.usage.contains(&(Bytes::from_static(b"units"), 9)));

    let mut reap = Changes::new();
    reap.reap(&path);
    runtime.commit_blocking(&reap).unwrap();
    assert_eq!(runtime.snapshot().unwrap().fiber(&path).unwrap(), None);
    assert_eq!(stopped.fiber(&path).unwrap(), Some(record));
}

#[test]
fn graceful_removal_runs_stop_handlers_and_force_removal_kills_the_subtree() {
    let mut builder = Builder::new().unwrap();
    builder.fiber(Stoppable).unwrap();
    let runtime = builder.memory().unwrap();
    let start_group = |group: &[u8]| {
        let mut changes = Changes::new();
        changes
            .mkdir(group)
            .start::<Stoppable>(group, b"child", &"partial output".into())
            .unwrap();
        runtime.commit_blocking(&changes).unwrap();
        until(&runtime, |snapshot| {
            snapshot.read(b"stoppable").unwrap().as_deref() == Some(b"waiting")
        });
    };

    start_group(b"graceful");
    let mut graceful = Changes::new();
    graceful.remove(b"graceful", Some(Bytes::from_static(b"cleanup")));
    runtime.commit_blocking(&graceful).unwrap();
    let stopped = until(&runtime, |snapshot| {
        snapshot.stat(b"graceful").unwrap().is_none()
    });
    assert_eq!(
        stopped.read(b"stoppable").unwrap().as_deref(),
        Some(b"stopped".as_slice())
    );

    start_group(b"forced");
    let mut forced = Changes::new();
    forced.force_remove(b"forced", Some(Bytes::from_static(b"discard")));
    runtime.commit_blocking(&forced).unwrap();
    let stopped = until(&runtime, |snapshot| {
        snapshot.stat(b"forced").unwrap().is_none()
    });
    assert_eq!(
        stopped.read(b"stoppable").unwrap().as_deref(),
        Some(b"waiting".as_slice())
    );
}

struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("panic while dropping the payload");
    }
}

struct Fails;

impl Fiber for Fails {
    const KIND: &'static str = "contract.failure";
    type Input = String;
    type Checkpoint = ();
    type Output = ();

    fn run(
        &self,
        run: &mut Run<'_>,
        input: String,
        _: Option<()>,
    ) -> anyhow::Result<Ending<(), ()>> {
        run.changes.write(input.as_bytes(), b"staged");
        match input.as_str() {
            "fail" => Ok(Ending::Fail("explicit failure".into())),
            "error" => Err(Error {
                code: 4,
                message: "callback error".into(),
            }
            .into()),
            "panic" => panic_any(PanicOnDrop),
            _ => unreachable!(),
        }
    }
}

#[test]
fn explicit_failure_commits_writes_while_errors_and_panics_discard_them() {
    let mut builder = Builder::new().unwrap();
    builder.fiber(Fails).unwrap();
    let runtime = builder.memory().unwrap();
    for (kind, message, file) in [
        (
            "fail",
            "explicit failure",
            Some(Bytes::from_static(b"staged")),
        ),
        ("error", "callback error", None),
        ("panic", "the phase panicked", None),
    ] {
        let path = start::<Fails>(&runtime, kind.as_bytes(), &kind.into());
        let snapshot = finished(&runtime, &path);
        let Some(Outcome::Failed {
            error: Some(error),
            reason: None,
        }) = snapshot
            .fiber(&path)
            .unwrap()
            .unwrap()
            .decode_outcome::<Fails>()
            .unwrap()
        else {
            panic!("expected a failed outcome for {kind}")
        };
        assert!(String::from_utf8_lossy(&error).contains(message));
        assert_eq!(snapshot.read(kind.as_bytes()).unwrap(), file);
    }
}

struct Tracked(Arc<AtomicUsize>);

impl Fiber for Tracked {
    const KIND: &'static str = "contract.tracked";
    type Input = ();
    type Checkpoint = ();
    type Output = ();

    fn run(&self, _: &mut Run<'_>, _: (), _: Option<()>) -> anyhow::Result<Ending<(), ()>> {
        Ok(Ending::Done(()))
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn registration_ownership_covers_rejection_failed_open_and_close() {
    let releases = Arc::new(AtomicUsize::new(0));
    let count = || releases.load(Ordering::SeqCst);
    let mut builder = Builder::new().unwrap();
    builder.fiber(Tracked(releases.clone())).unwrap();
    assert_eq!(
        builder.fiber(Tracked(releases.clone())).unwrap_err().code,
        2
    );
    assert_eq!(count(), 1);
    drop(builder);
    assert_eq!(count(), 2);

    let directory = tempfile::tempdir().unwrap();
    let _locked = Builder::new().unwrap().open(directory.path()).unwrap();
    let mut builder = Builder::new().unwrap();
    builder.fiber(Tracked(releases.clone())).unwrap();
    assert!(builder.open(directory.path()).is_err());
    assert_eq!(count(), 3);

    let mut builder = Builder::new().unwrap();
    builder.fiber(Tracked(releases.clone())).unwrap();
    let runtime = builder.memory().unwrap();
    assert_eq!(count(), 3);
    runtime.close().unwrap();
    assert_eq!(count(), 4);
    drop(runtime);
    assert_eq!(count(), 4);
}

struct CallbackOwner {
    runtime: Arc<Mutex<Option<Runtime>>>,
    report: mpsc::Sender<(u32, u32, u32, u32)>,
    released: mpsc::Sender<u32>,
}

impl Fiber for CallbackOwner {
    const KIND: &'static str = "contract.reentry";
    type Input = ();
    type Checkpoint = ();
    type Output = ();

    fn run(&self, _: &mut Run<'_>, _: (), _: Option<()>) -> anyhow::Result<Ending<(), ()>> {
        let runtime = self.runtime.lock().unwrap().take().unwrap();
        let open = Builder::new().and_then(Builder::memory).unwrap_err().code;
        let commit = runtime.commit_blocking(&Changes::new()).unwrap_err().code;
        let snapshot = runtime.snapshot().unwrap_err().code;
        assert_eq!(runtime.new_id().unwrap_err().code, 6);
        assert_eq!(runtime.now_ms().unwrap_err().code, 6);
        let close = runtime.close().unwrap_err().code;
        drop(runtime);
        self.report.send((open, commit, snapshot, close)).unwrap();
        Ok(Ending::Done(()))
    }
}

impl Drop for CallbackOwner {
    fn drop(&mut self) {
        let code = Builder::new().and_then(Builder::memory).unwrap_err().code;
        let _ = self.released.send(code);
        panic_any(PanicOnDrop);
    }
}

#[test]
fn callbacks_reject_blocking_calls_and_can_drop_the_last_runtime_owner() {
    let (report, ran) = mpsc::channel();
    let (released, release) = mpsc::channel();
    let slot = Arc::new(Mutex::new(None));
    let mut builder = Builder::new().unwrap();
    builder
        .fiber(CallbackOwner {
            runtime: slot.clone(),
            report,
            released,
        })
        .unwrap();
    {
        let mut guard = slot.lock().unwrap();
        let runtime = guard.insert(builder.memory().unwrap());
        start::<CallbackOwner>(runtime, b"reenter", &());
    }
    assert_eq!(ran.recv_timeout(TIMEOUT).unwrap(), (6, 6, 6, 6));
    assert_eq!(release.recv_timeout(TIMEOUT).unwrap(), 6);
}
