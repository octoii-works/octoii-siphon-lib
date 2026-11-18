use std::{
    future::{pending, ready},
    panic::panic_any,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Sender},
    },
    thread,
    time::{Duration, Instant},
};

use octoii_siphon_lib::{
    Answer, Asked, Builder, Changes, Ending, Error, Fiber, Mail, Options, Outcome, Request, Result,
    Run, Runtime, Signal, Unanswered, Wait, process_path,
};
use serde::{Deserialize, Serialize};

const TIMEOUT: Duration = Duration::from_secs(10);

fn host() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("siphon-request-host")
        .build()
        .unwrap()
}

fn start<P: Fiber>(runtime: &Runtime, key: &[u8], input: &P::Input) -> Vec<u8> {
    let mut changes = Changes::new().unwrap();
    changes.start::<P>(b"", key, input).unwrap();
    runtime.commit(&changes).unwrap();
    process_path(b"", key).unwrap()
}

fn finish<P: Fiber>(runtime: &Runtime, process: &[u8]) -> Outcome<P::Output> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let outcome = runtime.snapshot().unwrap().outcome::<P>(process).unwrap();
        if let Some(outcome) = outcome
            && !matches!(outcome, Outcome::Pending)
        {
            return outcome;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the process"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[derive(Deserialize, Serialize)]
struct Lookup(u32);

impl Request for Lookup {
    const NAME: &'static str = "contract.lookup";
    type Answer = u32;
}

struct Twice;

#[derive(Deserialize, Serialize)]
enum Turn {
    First(String),
    Second {
        first_key: String,
        second_key: String,
        first: u32,
    },
}

impl Fiber for Twice {
    const KIND: &'static str = "contract.requests";
    type Input = u32;
    type Checkpoint = Turn;
    type Output = (u32, u32);

    fn run(
        &self,
        run: &mut Run<'_>,
        input: u32,
        checkpoint: Option<Turn>,
    ) -> Result<Ending<Turn, Self::Output>> {
        match checkpoint {
            None => {
                let key = run.request(&Lookup(input))?;
                assert_eq!(run.answer::<Lookup>(&key)?, None);
                Ok(Ending::Wait(
                    Turn::First(key.clone()),
                    Wait::new().mail(key),
                ))
            }
            Some(Turn::First(first_key)) => {
                let Some(Answer::Given(first)) = run.answer::<Lookup>(&first_key)? else {
                    panic!("first answer missing");
                };
                run.take_mail(&first_key)?;
                assert_eq!(
                    run.answer::<Lookup>(&first_key)?,
                    Some(Answer::Given(first))
                );
                let second_key = run.request(&Lookup(first))?;
                assert_ne!(first_key, second_key);
                Ok(Ending::Wait(
                    Turn::Second {
                        first_key,
                        second_key: second_key.clone(),
                        first,
                    },
                    Wait::new().mail(second_key),
                ))
            }
            Some(Turn::Second {
                first_key,
                second_key,
                first,
            }) => {
                assert_eq!(run.answer::<Lookup>(&first_key)?, None);
                let Some(Answer::Given(second)) = run.answer::<Lookup>(&second_key)? else {
                    panic!("second answer missing");
                };
                run.take_mail(&second_key)?
                    .write(b"answers", format!("{first}, {second}").as_bytes())?;
                Ok(Ending::Done((first, second)))
            }
        }
    }
}

struct Throws;

impl Fiber for Throws {
    const KIND: &'static str = "contract.throwing-request";
    type Input = ();
    type Checkpoint = ();
    type Output = ();

    fn run(&self, run: &mut Run<'_>, _: (), _: Option<()>) -> Result<Ending<(), ()>> {
        run.request(&Lookup(100))?;
        run.write(b"throw", b"staged")?;
        Err(Error {
            code: 4,
            message: "discard this request".into(),
        })
    }
}

#[test]
fn phases_commit_ordered_requests_and_discard_thrown_requests() {
    let executor = host();
    let (deliveries, received) = mpsc::channel();
    let mut builder = Builder::new().unwrap();
    builder
        .fiber(Throws)
        .unwrap()
        .fiber(Twice)
        .unwrap()
        .handle::<Lookup, _, _>(executor.handle(), move |asked| {
            assert_eq!(thread::current().name(), Some("siphon-request-host"));
            deliveries
                .send((asked.key, asked.process, asked.payload.0))
                .unwrap();
            ready(Ok(asked.payload.0 * 2))
        })
        .unwrap();
    let runtime = builder.memory().unwrap();
    let thrown = start::<Throws>(&runtime, b"throw", &());
    assert!(matches!(
        finish::<Throws>(&runtime, &thrown),
        Outcome::Failed { .. }
    ));
    let process = start::<Twice>(&runtime, b"twice", &21);
    let (first_key, first_process, first_input) = received.recv_timeout(TIMEOUT).unwrap();
    let (second_key, second_process, second_input) = received.recv_timeout(TIMEOUT).unwrap();
    assert_ne!(first_key, second_key);
    assert_eq!(
        (first_process, second_process),
        (process.clone(), process.clone())
    );
    assert_eq!((first_input, second_input), (21, 42));
    assert_eq!(
        finish::<Twice>(&runtime, &process),
        Outcome::Completed(Some((42, 84)))
    );
    let snapshot = runtime.snapshot().unwrap();
    assert_eq!(
        snapshot.read(b"answers").unwrap().as_deref(),
        Some(b"42, 84".as_slice())
    );
    assert_eq!(snapshot.read(b"throw").unwrap(), None);
    assert_eq!(
        received.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
}

struct Once;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
enum Response {
    Given(u32),
    Failed(String),
    Interrupted,
}

impl From<Answer<u32>> for Response {
    fn from(answer: Answer<u32>) -> Self {
        match answer {
            Answer::Given(value) => Self::Given(value),
            Answer::Failed(reason) => Self::Failed(reason),
            Answer::Interrupted => Self::Interrupted,
        }
    }
}

impl Fiber for Once {
    const KIND: &'static str = "contract.one-request";
    const OPTIONS: Options = Options {
        stop_handler: true,
        ..Options::DEFAULT
    };
    type Input = u32;
    type Checkpoint = String;
    type Output = Response;

    fn run(
        &self,
        run: &mut Run<'_>,
        input: u32,
        checkpoint: Option<String>,
    ) -> Result<Ending<String, Response>> {
        let Some(key) = checkpoint else {
            let key = run.request(&Lookup(input))?;
            return Ok(Ending::Wait(key.clone(), Wait::new().mail(key)));
        };
        let Some(answer) = run.answer::<Lookup>(&key)?.map(Response::from) else {
            // A stop handler can run before the async host has given its partial result.
            return Ok(Ending::Wait(key.clone(), Wait::new().mail(key)));
        };
        run.take_mail(&key)?;
        if run.is_stop_handler() {
            Ok(Ending::Stopped(Some(answer)))
        } else {
            Ok(Ending::Done(answer))
        }
    }
}

#[test]
fn stop_signals_the_handler_and_the_stop_phase_reads_its_partial_answer() {
    let executor = host();
    let (started, running) = mpsc::channel();
    let mut builder = Builder::new().unwrap();
    builder
        .fiber(Once)
        .unwrap()
        .handle::<Lookup, _, _>(executor.handle(), move |asked| {
            let started = started.clone();
            async move {
                started.send((asked.process, asked.key)).unwrap();
                asked.cancelled.cancelled().await;
                Ok(asked.payload.0)
            }
        })
        .unwrap();
    let runtime = builder.memory().unwrap();
    let process = start::<Once>(&runtime, b"stop", &42);
    let (asked_process, key) = running.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(asked_process, process);
    let mut conflicting = Changes::new().unwrap();
    conflicting
        .send(
            &process,
            &Mail {
                key: key.into_bytes(),
                name: b"ordinary".to_vec(),
                payload: b"reserved".to_vec(),
            },
            None,
        )
        .unwrap();
    let error = runtime.commit(&conflicting).unwrap_err();
    assert_eq!(error.code, 4);
    assert!(error.message.contains("request-pending"), "{error}");

    let mut changes = Changes::new().unwrap();
    changes.signal(&process, Signal::Terminate, None).unwrap();
    runtime.commit(&changes).unwrap();
    assert_eq!(
        finish::<Once>(&runtime, &process),
        Outcome::Stopped {
            result: Some(Response::Given(42)),
            reason: None
        },
    );
}

struct Dropped(Sender<()>);

impl Drop for Dropped {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[test]
fn close_leaves_host_work_to_its_executor_and_reopen_interrupts_the_request() {
    let directory = tempfile::tempdir().unwrap();
    let executor = host();
    let (started, running) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let (dropped, task_dropped) = mpsc::channel();
    let blocked = Mutex::new(Some(blocked));
    let mut builder = Builder::new().unwrap();
    builder
        .fiber(Once)
        .unwrap()
        .handle::<Lookup, _, _>(executor.handle(), move |_| {
            let blocked = blocked.lock().unwrap().take().unwrap();
            let dropped = Dropped(dropped.clone());
            let started = started.clone();
            async move {
                let _dropped = dropped;
                started.send(()).unwrap();
                // A poll already on the host executor can outlive native shutdown.
                blocked.recv().unwrap();
                pending().await
            }
        })
        .unwrap();
    let mut runtime = builder.open(directory.path()).unwrap();
    let process = start::<Once>(&runtime, b"restart", &1);
    running.recv_timeout(TIMEOUT).unwrap();
    let (closed, done) = mpsc::channel();
    let close = thread::spawn(move || {
        runtime.close().unwrap();
        drop(runtime);
        closed.send(()).unwrap();
    });
    done.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(task_dropped.try_recv(), Err(mpsc::TryRecvError::Empty));
    close.join().unwrap();

    let deliveries = Arc::new(AtomicUsize::new(0));
    let mut builder = Builder::new().unwrap();
    let seen = deliveries.clone();
    builder
        .fiber(Once)
        .unwrap()
        .handle::<Lookup, _, _>(executor.handle(), move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            ready(Ok(2))
        })
        .unwrap();
    let runtime = builder.open(directory.path()).unwrap();
    assert_eq!(
        finish::<Once>(&runtime, &process),
        Outcome::Completed(Some(Response::Interrupted))
    );
    assert_eq!(deliveries.load(Ordering::SeqCst), 0);
    release.send(()).unwrap();
    task_dropped.recv_timeout(TIMEOUT).unwrap();
}

struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("panic while dropping the payload");
    }
}

#[test]
fn host_handler_panics_become_failed_answers() {
    let executor = host();
    let mut builder = Builder::new().unwrap();
    builder
        .fiber(Once)
        .unwrap()
        .handle::<Lookup, _, _>(executor.handle(), |asked| {
            assert!(asked.payload.0 != 0, "constructing the handler");
            async move {
                match asked.payload.0 {
                    1 => panic_any(PanicOnDrop),
                    2 => Err(Unanswered::Failed("explicit failure".into())),
                    3 => Err(Unanswered::Interrupted),
                    value => Ok(value),
                }
            }
        })
        .unwrap();
    let runtime = builder.memory().unwrap();
    for (input, expected) in [
        (
            0,
            Response::Failed("the handler panicked: constructing the handler".into()),
        ),
        (
            1,
            Response::Failed("the handler panicked: no message".into()),
        ),
        (2, Response::Failed("explicit failure".into())),
        (3, Response::Interrupted),
        (4, Response::Given(4)),
    ] {
        let process = start::<Once>(&runtime, input.to_string().as_bytes(), &input);
        assert_eq!(
            finish::<Once>(&runtime, &process),
            Outcome::Completed(Some(expected))
        );
    }
}

fn tracked(
    released: Sender<()>,
) -> impl Fn(Asked<Lookup>) -> std::future::Ready<std::result::Result<u32, Unanswered>> {
    let tracker = Dropped(released);
    move |asked| {
        let _tracker = &tracker;
        ready(Ok(asked.payload.0))
    }
}

#[test]
fn request_registrations_are_released_on_rejection_and_failed_open() {
    let directory = tempfile::tempdir().unwrap();
    let _locked = Runtime::open(directory.path()).unwrap();
    let executor = host();
    let (released, dropped) = mpsc::channel();
    let mut builder = Builder::new().unwrap();
    builder
        .handle::<Lookup, _, _>(executor.handle(), tracked(released.clone()))
        .unwrap();
    assert_eq!(
        builder
            .handle::<Lookup, _, _>(executor.handle(), tracked(released))
            .unwrap_err()
            .code,
        2
    );
    dropped.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(dropped.try_recv(), Err(mpsc::TryRecvError::Empty));
    assert!(builder.open(directory.path()).is_err());
    dropped.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(dropped.try_recv(), Err(mpsc::TryRecvError::Disconnected));
}
