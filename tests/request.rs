use bytes::Bytes;

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
    Answer, Asked, AskedOfPerson, Builder, Changes, Ending, Error, Fiber, Following, Mail, Options,
    Outcome, PersonRequest, Published, Request, Run, Runtime, Signal, Status, StreamEvent,
    Unanswered, Wait, process_path,
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

fn start<P: Fiber>(runtime: &Runtime, key: &[u8], input: &P::Input) -> Bytes {
    let mut changes = Changes::new();
    changes.start::<P>(b"", key, input).unwrap();
    runtime.commit_blocking(&changes).unwrap();
    process_path(b"", key)
}

fn finish<P: Fiber>(runtime: &Runtime, process: &[u8]) -> Outcome<P::Output> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let record = runtime.snapshot().unwrap().fiber(process).unwrap().unwrap();
        if let Some(outcome) = record.decode_outcome::<P>().unwrap() {
            return outcome;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for the process"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Lookup(u32);

impl Request for Lookup {
    const NAME: &'static str = "contract.lookup";
    type Answer = u32;
}

impl AskedOfPerson for Lookup {
    fn answer_schema(&self) -> serde_json::Value {
        serde_json::json!({"const": self.0})
    }
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
    ) -> anyhow::Result<Ending<Turn, Self::Output>> {
        match checkpoint {
            None => {
                let key = run.new_id()?;
                run.request(&key, &Lookup(input))?;
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
                let second_key = run.new_id()?;
                run.request(&second_key, &Lookup(first))?;
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
                run.take_mail(&second_key)?;
                run.changes
                    .write(b"answers", format!("{first}, {second}").as_bytes());
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

    fn run(&self, run: &mut Run<'_>, _: (), _: Option<()>) -> anyhow::Result<Ending<(), ()>> {
        let key = run.new_id()?;
        run.request(&key, &Lookup(100))?;
        run.changes.write(b"throw", b"staged");
        Err(Error {
            code: 4,
            message: "discard this request".into(),
        }
        .into())
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
            deliveries.send((asked.id, asked.payload.0)).unwrap();
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
    let (first, first_input) = received.recv_timeout(TIMEOUT).unwrap();
    let (second, second_input) = received.recv_timeout(TIMEOUT).unwrap();
    assert_ne!(first.key, second.key);
    assert_eq!(first.process, process);
    assert_eq!(second.process, process);
    assert_eq!((first_input, second_input), (21, 42));
    assert_eq!(
        finish::<Twice>(&runtime, &process),
        Outcome::Completed(Some((42, 84)))
    );
    let snapshot = runtime.snapshot().unwrap();
    let created = snapshot.fiber(&process).unwrap().unwrap().created_seq;
    assert_eq!((first.started, second.started), (created, created));
    assert!(created < first.created_seq && first.created_seq < second.created_seq);
    assert!(second.created_seq <= snapshot.sequence().unwrap());
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
    ) -> anyhow::Result<Ending<String, Response>> {
        let Some(key) = checkpoint else {
            let key = run.new_id()?;
            run.request(&key, &Lookup(input))?;
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
                started.send(asked.id).unwrap();
                asked.cancelled.cancelled().await;
                Ok(asked.payload.0)
            }
        })
        .unwrap();
    let runtime = builder.memory().unwrap();
    let process = start::<Once>(&runtime, b"stop", &42);
    let request = running.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(request.process, process);
    let mut conflicting = Changes::new();
    conflicting.send(
        &process,
        Mail {
            key: request.key.into(),
            name: Bytes::from_static(b"ordinary"),
            payload: Bytes::from_static(b"reserved"),
        },
        None,
    );
    let error = runtime.commit_blocking(&conflicting).unwrap_err();
    assert_eq!(error.status(), Status::Refused);
    assert!(error.message.contains("request-pending"), "{error}");

    let mut changes = Changes::new();
    changes.signal(&process, Signal::Terminate, None);
    runtime.commit_blocking(&changes).unwrap();
    assert_eq!(
        finish::<Once>(&runtime, &process),
        Outcome::Stopped {
            result: Some(Response::Given(42)),
            reason: None
        },
    );
}

fn person_builder() -> Builder {
    let mut builder = Builder::new().unwrap();
    builder.fiber(Once).unwrap().ask_person::<Lookup>().unwrap();
    builder
}

fn wait_for_prompt(runtime: &Runtime, process: &[u8]) -> PersonRequest {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let snapshot = runtime.snapshot().unwrap();
        let mut prompts = runtime.person_requests(&snapshot, process).unwrap();
        if let Some(prompt) = prompts.pop() {
            assert!(prompts.is_empty());
            return prompt;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a person request"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn a_person_request_survives_reopening_and_accepts_only_a_fitting_current_answer() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = person_builder().open(directory.path()).unwrap();
    let process = start::<Once>(&runtime, b"person", &42);
    let original = wait_for_prompt(&runtime, &process);
    assert_eq!(original.id.process, process);
    assert_eq!(original.name, Lookup::NAME.as_bytes());
    assert_eq!(original.payload, b"42");
    assert_eq!(original.answer_schema, serde_json::json!({"const": 42}));
    runtime.close().unwrap();

    let runtime = person_builder().open(directory.path()).unwrap();
    let standing = runtime.snapshot().unwrap();
    assert_eq!(
        runtime
            .person_requests(&standing, &process)
            .unwrap()
            .as_slice(),
        std::slice::from_ref(&original),
    );
    assert!(
        runtime
            .person_requests(&standing, b"unrelated")
            .unwrap()
            .is_empty()
    );

    let valid = serde_json::json!(42);
    let mut other = original.id.clone();
    other.created_seq += 1;
    assert_eq!(runtime.answer_blocking(&other, &valid).unwrap_err().code, 1);
    other = original.id.clone();
    other.started += 1;
    assert_eq!(runtime.answer_blocking(&other, &valid).unwrap_err().code, 1);
    let error = runtime
        .answer_blocking(&original.id, &serde_json::json!(43))
        .unwrap_err();
    assert_eq!(error.code, 2);
    let still_waiting = runtime.snapshot().unwrap();
    assert_eq!(
        runtime
            .person_requests(&still_waiting, &process)
            .unwrap()
            .as_slice(),
        std::slice::from_ref(&original),
    );

    assert!(runtime.answer_blocking(&original.id, &valid).unwrap() > original.id.created_seq);
    assert_eq!(
        runtime
            .answer_blocking(&original.id, &valid)
            .unwrap_err()
            .code,
        1
    );
    assert_eq!(
        finish::<Once>(&runtime, &process),
        Outcome::Completed(Some(Response::Given(42)))
    );
    assert!(
        runtime
            .person_requests(&runtime.snapshot().unwrap(), &process)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        runtime.person_requests(&standing, &process).unwrap(),
        vec![original]
    );
}

async fn next_stream(following: &mut Following) -> Option<StreamEvent> {
    tokio::time::timeout(TIMEOUT, following.next())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_waits_can_be_cancelled_and_retain_one_request_through_close() {
    let (published, first_ready) = tokio::sync::oneshot::channel();
    let (proceed, wait) = tokio::sync::oneshot::channel();
    let (hold, held) = tokio::sync::oneshot::channel();
    let gates = Mutex::new(Some((published, wait, hold)));
    let mut builder = Builder::new().unwrap();
    builder
        .fiber(Once)
        .unwrap()
        .handle::<Lookup, _, _>(&tokio::runtime::Handle::current(), move |asked| {
            let (published, wait, hold) = gates.lock().unwrap().take().unwrap();
            async move {
                asked.publish("thread", b"first").unwrap();
                published.send(asked.id.clone()).unwrap();
                wait.await.unwrap();
                asked.publish("thread", b"second").unwrap();
                let answer = asked.payload.0;
                hold.send(asked).unwrap();
                Ok(answer)
            }
        })
        .unwrap();
    let runtime = builder.memory().unwrap();
    let mut live = runtime.follow("thread").unwrap();
    assert_eq!(next_stream(&mut live).await, Some(StreamEvent::Reset));
    let process = start::<Once>(&runtime, b"streaming", &42);
    let id = tokio::time::timeout(TIMEOUT, first_ready)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id.process, process);
    let part = |offset, data: &[u8]| {
        Some(StreamEvent::Data(Published {
            topic: "thread".into(),
            id: id.clone(),
            offset,
            data: data.to_vec(),
        }))
    };
    assert_eq!(next_stream(&mut live).await, part(0, b"first"));

    let mut replay = runtime.follow("thread").unwrap();
    assert_eq!(next_stream(&mut replay).await, Some(StreamEvent::Reset));
    assert_eq!(next_stream(&mut replay).await, part(0, b"first"));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), live.next())
            .await
            .is_err()
    );

    proceed.send(()).unwrap();
    for following in [&mut live, &mut replay] {
        assert_eq!(next_stream(following).await, part(1, b"second"));
        assert_eq!(
            next_stream(following).await,
            Some(StreamEvent::Closed(id.clone()))
        );
    }
    let asked = tokio::time::timeout(TIMEOUT, held).await.unwrap().unwrap();
    assert_eq!(asked.publish("thread", b"late").unwrap_err().code, 3);
    assert_eq!(
        finish::<Once>(&runtime, &process),
        Outcome::Completed(Some(Response::Given(42)))
    );
    runtime.close().unwrap();
    assert_eq!(next_stream(&mut live).await, None);
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
    let prepared = builder.runtime_handle().unwrap();
    assert_eq!(prepared.snapshot().unwrap_err().code, 3);
    builder
        .fiber(Once)
        .unwrap()
        .handle::<Lookup, _, _>(executor.handle(), move |asked| {
            let blocked = blocked.lock().unwrap().take().unwrap();
            let dropped = Dropped(dropped.clone());
            let started = started.clone();
            async move {
                let _dropped = dropped;
                let record = asked
                    .runtime
                    .snapshot()
                    .unwrap()
                    .fiber(&asked.id.process)
                    .unwrap()
                    .unwrap();
                assert_eq!(record.created_seq, asked.id.started);
                started.send(asked.runtime.clone()).unwrap();
                // A poll already on the host executor can outlive native shutdown.
                blocked.recv().unwrap();
                pending().await
            }
        })
        .unwrap();
    let runtime = builder.open(directory.path()).unwrap();
    let process = start::<Once>(&runtime, b"restart", &1);
    assert!(
        prepared
            .snapshot()
            .unwrap()
            .fiber(&process)
            .unwrap()
            .is_some()
    );
    let delivered = running.recv_timeout(TIMEOUT).unwrap();
    let (closed, done) = mpsc::channel();
    let close = thread::spawn(move || {
        runtime.close().unwrap();
        drop(runtime);
        closed.send(()).unwrap();
    });
    done.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(task_dropped.try_recv(), Err(mpsc::TryRecvError::Empty));
    close.join().unwrap();
    assert_eq!(prepared.snapshot().unwrap_err().code, 3);
    assert_eq!(delivered.snapshot().unwrap_err().code, 3);

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
    let _locked = Builder::new().unwrap().open(directory.path()).unwrap();
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
