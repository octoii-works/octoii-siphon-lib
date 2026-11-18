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
    Builder, Changes, Ending, Error, Fiber, Options, Outcome, Result, Run, Runtime, Snapshot, Wait,
    process_path,
};

const TIMEOUT: Duration = Duration::from_secs(10);

fn until(runtime: &Runtime, ready: impl Fn(&Snapshot) -> bool) -> Snapshot {
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

fn finished<P: Fiber>(runtime: &Runtime, path: &[u8]) -> Snapshot {
    until(runtime, |snapshot| {
        !matches!(snapshot.outcome::<P>(path).unwrap(), Some(Outcome::Pending))
    })
}

fn start<P: Fiber>(runtime: &Runtime, key: &[u8], input: &P::Input) -> Vec<u8> {
    let mut changes = Changes::new().unwrap();
    changes.start::<P>(b"", key, input).unwrap();
    runtime.commit(&changes).unwrap();
    process_path(b"", key).unwrap()
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
    ) -> Result<Ending<u32, Self::Output>> {
        assert_eq!(run.path(), process_path(b"", b"parent")?);
        assert!(run.time_ms() > 0 && run.sequence()? > 0);
        assert!(!run.is_stop_handler());
        match checkpoint {
            None => {
                run.mkdir(b"gate")?.write(b"log", b"next")?;
                assert_eq!(run.read(b"log")?, None);
                Ok(Ending::Next(start))
            }
            Some(value) if value == start => {
                assert_eq!(run.read_range(b"log", 1, 2)?, Some(b"ex".to_vec()));
                run.append(b"log", b" change")?;
                Ok(Ending::Wait(value + 1, Wait::new().change(b"gate")))
            }
            Some(value) if value == start + 1 => {
                assert_eq!(run.read(b"gate/signal")?, Some(b"go".to_vec()));
                let parent = run.path().to_vec();
                run.delete(b"gate/signal")?
                    .start::<Child>(&parent, b"child", &value)?;
                run.append(b"log", b" child")?;
                Ok(Ending::Wait(value + 1, Wait::new().child(b"child")))
            }
            Some(value) if value == start + 2 => {
                let child = process_path(run.path(), b"child")?;
                assert_eq!(
                    run.outcome::<Child>(&child)?,
                    Some(Outcome::Completed(Some(value)))
                );
                run.append(b"log", b" deadline")?;
                Ok(Ending::Wait(
                    value + 1,
                    Wait::new().deadline(run.time_ms() + 10),
                ))
            }
            Some(value) => {
                assert_eq!(value, start + 3);
                run.append(b"log", b" done")?;
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

    fn run(&self, _: &mut Run<'_>, value: u32, _: Option<()>) -> Result<Ending<(), u32>> {
        Ok(Ending::Done(value + 1))
    }
}

#[test]
fn checkpoints_waits_and_phase_writes_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let mut builder = Builder::new().unwrap();
    builder.fiber(Parent).unwrap().fiber(Child).unwrap();
    let mut runtime = builder.open(directory.path()).unwrap();
    let path = process_path(b"", b"parent").unwrap();
    assert_eq!(
        runtime
            .snapshot()
            .unwrap()
            .outcome::<Parent>(&path)
            .unwrap(),
        None
    );
    assert_eq!(
        start::<Parent>(&runtime, b"parent", &("answer".into(), 42)),
        path
    );

    let waiting = until(&runtime, |snapshot| {
        snapshot.read(b"log").unwrap().as_deref() == Some(b"next change")
    });
    assert_eq!(
        waiting.outcome::<Parent>(&path).unwrap(),
        Some(Outcome::Pending)
    );
    let mut wake = Changes::new().unwrap();
    wake.write(b"gate/signal", b"go").unwrap();
    runtime.commit(&wake).unwrap();

    let completed = Some(Outcome::Completed(Some(("answer".into(), 45))));
    let log = Some(b"next change child deadline done".to_vec());
    let finished = finished::<Parent>(&runtime, &path);
    assert_eq!(finished.outcome::<Parent>(&path).unwrap(), completed);
    assert_eq!(finished.read(b"log").unwrap(), log);
    assert_eq!(finished.read(b"gate/signal").unwrap(), None);
    assert_eq!(waiting.read(b"log").unwrap(), Some(b"next change".to_vec()));
    runtime.close().unwrap();
    drop((waiting, finished, runtime));

    let reopened = Runtime::open(directory.path()).unwrap();
    let snapshot = reopened.snapshot().unwrap();
    assert_eq!(snapshot.outcome::<Parent>(&path).unwrap(), completed);
    assert_eq!(snapshot.read(b"log").unwrap(), log);
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
    ) -> Result<Ending<String, String>> {
        if run.is_stop_handler() {
            assert_eq!(run.read(b"stoppable")?, Some(b"waiting".to_vec()));
            run.write(b"stoppable", b"stopped")?;
            return Ok(Ending::Stopped(checkpoint));
        }
        assert_eq!(checkpoint, None);
        run.write(b"stoppable", b"waiting")?;
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
    let mut changes = Changes::new().unwrap();
    changes.terminate(&path).unwrap();
    runtime.commit(&changes).unwrap();

    let stopped = finished::<Stoppable>(&runtime, &path);
    assert_eq!(
        stopped.outcome::<Stoppable>(&path).unwrap(),
        Some(Outcome::Stopped {
            result: Some("partial output".into()),
            reason: None
        })
    );
    assert_eq!(
        stopped.read(b"stoppable").unwrap(),
        Some(b"stopped".to_vec())
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

    fn run(&self, run: &mut Run<'_>, input: String, _: Option<()>) -> Result<Ending<(), ()>> {
        run.write(input.as_bytes(), b"staged")?;
        match input.as_str() {
            "fail" => Ok(Ending::Fail("explicit failure".into())),
            "error" => Err(Error {
                code: 4,
                message: "callback error".into(),
            }),
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
        ("fail", "explicit failure", Some(b"staged".to_vec())),
        ("error", "callback error", None),
        ("panic", "the phase panicked", None),
    ] {
        let path = start::<Fails>(&runtime, kind.as_bytes(), &kind.into());
        let snapshot = finished::<Fails>(&runtime, &path);
        let Some(Outcome::Failed {
            error: Some(error),
            reason: None,
        }) = snapshot.outcome::<Fails>(&path).unwrap()
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

    fn run(&self, _: &mut Run<'_>, _: (), _: Option<()>) -> Result<Ending<(), ()>> {
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
    let _locked = Runtime::open(directory.path()).unwrap();
    let mut builder = Builder::new().unwrap();
    builder.fiber(Tracked(releases.clone())).unwrap();
    assert!(builder.open(directory.path()).is_err());
    assert_eq!(count(), 3);

    let mut builder = Builder::new().unwrap();
    builder.fiber(Tracked(releases.clone())).unwrap();
    let mut runtime = builder.memory().unwrap();
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

    fn run(&self, _: &mut Run<'_>, _: (), _: Option<()>) -> Result<Ending<(), ()>> {
        let mut runtime = self.runtime.lock().unwrap().take().unwrap();
        let open = Runtime::memory().unwrap_err().code;
        let commit = runtime.commit(&Changes::new()?).unwrap_err().code;
        let snapshot = runtime.snapshot().unwrap_err().code;
        let close = runtime.close().unwrap_err().code;
        drop(runtime);
        self.report.send((open, commit, snapshot, close)).unwrap();
        Ok(Ending::Done(()))
    }
}

impl Drop for CallbackOwner {
    fn drop(&mut self) {
        let code = Runtime::memory().unwrap_err().code;
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
