use bytes::Bytes;

use std::{
    future::{pending, ready},
    panic::panic_any,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Sender},
    },
    thread,
    time::{Duration, Instant},
};

use octoii_siphon_lib::{
    Answer, Asked, AskedOfPerson, Builder, Changes, Ending, Error, Fiber, FiberId, Following, Mail,
    Options, Outcome, PersonRequest, Published, Request, RequestId, Retries, Run, Runtime, Signal,
    Status, StreamEvent, Unanswered, Wait, fiber_path,
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

fn start<P: Fiber>(runtime: &Runtime, key: &[u8], input: &P::Input) -> FiberId {
    let mut changes = Changes::new().unwrap();
    changes.start::<P>(None, key, input).unwrap();
    runtime.commit_blocking(&changes).unwrap();
    runtime
        .snapshot()
        .unwrap()
        .fiber(&fiber_path(b"", key))
        .unwrap()
        .unwrap()
        .id
}

fn finish<P: Fiber>(runtime: &Runtime, process: &FiberId) -> Outcome<P::Output> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let record = runtime
            .snapshot()
            .unwrap()
            .fiber(process.path())
            .unwrap()
            .unwrap();
        assert_eq!(&record.id, process);
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

#[derive(Default)]
struct Twice {
    threw_after_first_answer: AtomicBool,
}

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
    const OPTIONS: Options = Options {
        retries: Retries {
            count: 1,
            delay_min_ms: 1,
            delay_max_ms: 1,
        },
        ..Options::DEFAULT
    };
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
                assert_eq!(run.take_answer::<Lookup>(&key)?, None);
                Ok(Ending::Wait(
                    Turn::First(key.clone()),
                    Wait::new().mail(key),
                ))
            }
            Some(Turn::First(first_key)) => {
                let mail = run.mail(first_key.as_bytes())?.expect("first answer mail");
                let Some(Answer::Given(first)) = run.take_answer::<Lookup>(&first_key)? else {
                    panic!("first answer missing");
                };
                assert_eq!(run.take_answer::<Lookup>(&first_key)?, None);
                assert_eq!(run.take_mail(first_key.as_bytes())?, None);
                assert_eq!(run.mail(first_key.as_bytes())?, Some(mail));
                if !self.threw_after_first_answer.swap(true, Ordering::Relaxed) {
                    anyhow::bail!("retry after staging the first answer");
                }
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
                assert_eq!(run.take_answer::<Lookup>(&first_key)?, None);
                let Some(Answer::Given(second)) = run.take_answer::<Lookup>(&second_key)? else {
                    panic!("second answer missing");
                };
                run.changes
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

    fn run(&self, run: &mut Run<'_>, _: (), _: Option<()>) -> anyhow::Result<Ending<(), ()>> {
        let key = run.new_id()?;
        run.request(&key, &Lookup(100))?;
        run.changes.write(b"throw", b"staged")?;
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
    let runtime = Runtime::open(None, None).unwrap();
    let mut builder = Builder::new(&runtime).unwrap();
    builder
        .fiber(Throws)
        .unwrap()
        .fiber(Twice::default())
        .unwrap()
        .handle::<Lookup, _, _>(executor.handle(), move |asked| {
            assert_eq!(thread::current().name(), Some("siphon-request-host"));
            deliveries.send((asked.id, asked.payload.0)).unwrap();
            ready(Ok(asked.payload.0 * 2))
        })
        .unwrap();
    builder.start().unwrap();
    let thrown = start::<Throws>(&runtime, b"throw", &());
    assert!(matches!(
        finish::<Throws>(&runtime, &thrown),
        Outcome::Failed { .. }
    ));
    let process = start::<Twice>(&runtime, b"twice", &21);
    let (first, first_input) = received.recv_timeout(TIMEOUT).unwrap();
    let (second, second_input) = received.recv_timeout(TIMEOUT).unwrap();
    assert_ne!(first.key(), second.key());
    assert_eq!(first.fiber(), &process);
    assert_eq!(second.fiber(), &process);
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
    ) -> anyhow::Result<Ending<String, Response>> {
        let Some(key) = checkpoint else {
            let key = "lookup".to_owned();
            run.request(&key, &Lookup(input))?;
            return Ok(Ending::Wait(key.clone(), Wait::new().mail(key)));
        };
        let Some(answer) = run.take_answer::<Lookup>(&key)?.map(Response::from) else {
            // A stop handler can run before the async host has given its partial result.
            return Ok(Ending::Wait(key.clone(), Wait::new().mail(key)));
        };
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
    let runtime = Runtime::open(None, None).unwrap();
    let mut builder = Builder::new(&runtime).unwrap();
    builder
        .fiber(Once)
        .unwrap()
        .handle::<Lookup, _, _>(executor.handle(), move |asked| {
            let started = started.clone();
            async move {
                assert!(!asked.stop.is_requested());
                let stop = asked.stop.clone();
                started.send(asked.id).unwrap();
                stop.requested().await;
                assert!(asked.stop.is_requested());
                Ok(asked.payload.0)
            }
        })
        .unwrap();
    builder.start().unwrap();
    let process = start::<Once>(&runtime, b"stop", &42);
    let request = running.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(request.fiber(), &process);
    let mut conflicting = Changes::new().unwrap();
    conflicting
        .send(
            &process,
            Mail {
                key: Bytes::copy_from_slice(request.key()),
                name: Bytes::from_static(b"ordinary"),
                payload: Bytes::from_static(b"reserved"),
            },
        )
        .unwrap();
    let error = runtime.commit_blocking(&conflicting).unwrap_err();
    assert_eq!(error.status(), Status::Refused);
    assert!(error.message.contains("request-pending"), "{error}");

    let mut changes = Changes::new().unwrap();
    changes.signal(&process, Signal::Terminate, None).unwrap();
    runtime.commit_blocking(&changes).unwrap();
    assert_eq!(
        finish::<Once>(&runtime, &process),
        Outcome::Stopped {
            result: Some(Response::Given(42)),
            reason: None
        },
    );
}

fn start_person_requests(runtime: &Runtime) {
    let mut builder = Builder::new(runtime).unwrap();
    builder.fiber(Once).unwrap().ask_person::<Lookup>().unwrap();
    builder.start().unwrap();
}

fn wait_for_prompt(runtime: &Runtime, process: &FiberId) -> PersonRequest {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let snapshot = runtime.snapshot().unwrap();
        let mut prompts = runtime.person_requests(&snapshot, process.path()).unwrap();
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
    let runtime = Runtime::open(Some(directory.path()), None).unwrap();
    start_person_requests(&runtime);
    let process = start::<Once>(&runtime, b"person", &42);
    let original = wait_for_prompt(&runtime, &process);
    assert_eq!(original.id.fiber(), &process);
    let token = original.id.to_token();
    assert_eq!(original.name, Lookup::NAME.as_bytes());
    assert_eq!(original.payload, b"42");
    assert_eq!(original.answer_schema, serde_json::json!({"const": 42}));
    runtime.close().unwrap();

    let runtime = Runtime::open(Some(directory.path()), None).unwrap();
    start_person_requests(&runtime);
    let standing = runtime.snapshot().unwrap();
    assert_eq!(
        runtime
            .person_requests(&standing, process.path())
            .unwrap()
            .as_slice(),
        std::slice::from_ref(&original),
    );
    let unrelated = fiber_path(b"", b"unrelated");
    assert!(
        runtime
            .person_requests(&standing, &unrelated)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        runtime
            .person_requests(&standing, b"unrelated")
            .unwrap_err()
            .status(),
        octoii_siphon_lib::Status::Invalid
    );

    let valid = serde_json::json!(42);
    let id = RequestId::from_token(&token).unwrap();
    assert_eq!(id, original.id);
    let error = runtime
        .answer_blocking(&id, &serde_json::json!(43))
        .unwrap_err();
    assert_eq!(error.code, 2);
    let still_waiting = runtime.snapshot().unwrap();
    assert_eq!(
        runtime
            .person_requests(&still_waiting, process.path())
            .unwrap()
            .as_slice(),
        std::slice::from_ref(&original),
    );

    let answered = runtime.answer_blocking(&id, &valid).unwrap();
    assert!(answered.seq() > standing.sequence().unwrap());
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
            .person_requests(&runtime.snapshot().unwrap(), process.path())
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        runtime.person_requests(&standing, process.path()).unwrap(),
        vec![original]
    );

    let mut reap = Changes::new().unwrap();
    reap.reap(&process).unwrap();
    runtime.commit_blocking(&reap).unwrap();
    let replacement = start::<Once>(&runtime, b"person", &42);
    assert_eq!(replacement.path(), process.path());
    assert_ne!(replacement, process);
    let process = replacement;
    let current = wait_for_prompt(&runtime, &process);
    assert_eq!(current.id.path(), id.path());
    assert_eq!(current.id.key(), id.key());
    assert_ne!(current.id, id);
    let stale = runtime.answer_blocking(&id, &valid).unwrap_err();
    assert_eq!(stale.status(), Status::NotFound);
    runtime.answer_blocking(&current.id, &valid).unwrap();
    assert_eq!(
        finish::<Once>(&runtime, &process),
        Outcome::Completed(Some(Response::Given(42)))
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
    let runtime = Runtime::open(None, None).unwrap();
    let mut builder = Builder::new(&runtime).unwrap();
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
    builder.start().unwrap();
    let mut live = runtime.follow("thread").unwrap();
    assert_eq!(next_stream(&mut live).await, Some(StreamEvent::Reset));
    let process = start::<Once>(&runtime, b"streaming", &42);
    let id = tokio::time::timeout(TIMEOUT, first_ready)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id.fiber(), &process);
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
    let runtime = Runtime::open(Some(directory.path()), None).unwrap();
    let prepared = runtime.runtime_handle();
    assert!(
        prepared
            .snapshot()
            .unwrap()
            .fiber(b"restart")
            .unwrap()
            .is_none()
    );
    let handler_runtime = prepared.clone();
    let mut builder = Builder::new(&runtime).unwrap();
    builder
        .fiber(Once)
        .unwrap()
        .handle::<Lookup, _, _>(executor.handle(), move |asked| {
            let blocked = blocked.lock().unwrap().take().unwrap();
            let dropped = Dropped(dropped.clone());
            let started = started.clone();
            let runtime = handler_runtime.clone();
            async move {
                let _dropped = dropped;
                assert!(
                    runtime
                        .snapshot()
                        .unwrap()
                        .fiber(asked.id.path())
                        .unwrap()
                        .is_some()
                );
                started.send(runtime).unwrap();
                // A poll already on the host executor can outlive native shutdown.
                blocked.recv().unwrap();
                pending().await
            }
        })
        .unwrap();
    builder.start().unwrap();
    let process = start::<Once>(&runtime, b"restart", &1);
    assert!(
        prepared
            .snapshot()
            .unwrap()
            .fiber(process.path())
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
    let runtime = Runtime::open(Some(directory.path()), None).unwrap();
    let mut builder = Builder::new(&runtime).unwrap();
    let seen = deliveries.clone();
    builder
        .fiber(Once)
        .unwrap()
        .handle::<Lookup, _, _>(executor.handle(), move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            ready(Ok(2))
        })
        .unwrap();
    builder.start().unwrap();
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
    let runtime = Runtime::open(None, None).unwrap();
    let mut builder = Builder::new(&runtime).unwrap();
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
    builder.start().unwrap();
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
fn request_registrations_are_released_on_rejection_and_failed_start() {
    let runtime = Runtime::open(None, None).unwrap();
    let executor = host();
    let (released, dropped) = mpsc::channel();
    let mut builder = Builder::new(&runtime).unwrap();
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
    runtime.close().unwrap();
    assert_eq!(builder.start().unwrap_err().status(), Status::Closed);
    dropped.recv_timeout(TIMEOUT).unwrap();
    assert_eq!(dropped.try_recv(), Err(mpsc::TryRecvError::Disconnected));
}

#[cfg(unix)]
mod machine_requests {
    use octoii_siphon_lib::{
        Command, CommandEvent, Exec, Exit, Namespace, RequestCommands, Spec, Stdio,
    };
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::timeout;

    use super::{
        Answer, Arc, Asked, Builder, Bytes, Changes, Deserialize, Ending, Fiber, Lookup, Mail,
        Once, Outcome, Request, RequestId, Response, Run, Runtime, Serialize, Status, TIMEOUT,
        Unanswered, Wait, finish, host, pending, start,
    };

    const PAGE_KEYS: [&str; 2] = ["cover", "body"];

    #[derive(Deserialize, Serialize)]
    struct RenderPage(String);

    impl Request for RenderPage {
        const NAME: &'static str = "contract.render-page";
        type Answer = String;
    }

    struct Document;

    impl Fiber for Document {
        const KIND: &'static str = "contract.document";
        type Input = ();
        type Checkpoint = [Option<String>; 2];
        type Output = String;

        fn run(
            &self,
            run: &mut Run<'_>,
            (): (),
            checkpoint: Option<Self::Checkpoint>,
        ) -> anyhow::Result<Ending<Self::Checkpoint, Self::Output>> {
            if checkpoint.is_none() {
                run.request("cover", &RenderPage("Draft".into()))?;
                run.request("body", &RenderPage("Chapters".into()))?;
            } else if run.take_mail(b"revise")?.is_some() {
                run.request("cover", &RenderPage("Cover".into()))?;
            }

            let mut pages = checkpoint.unwrap_or_default();
            let mut wait = Wait::new().mail(b"revise".as_slice());
            for (key, page) in PAGE_KEYS.into_iter().zip(&mut pages) {
                if page.is_some() {
                    continue;
                }
                match run.take_answer::<RenderPage>(key)? {
                    Some(Answer::Given(markup)) => *page = Some(markup),
                    Some(answer) => anyhow::bail!("{key} did not render: {answer:?}"),
                    None => wait = wait.mail(key.as_bytes()),
                }
            }
            Ok(match pages {
                [Some(cover), Some(body)] => Ending::Done(format!("<main>{cover}{body}</main>")),
                pages => Ending::Wait(pages, wait),
            })
        }
    }

    struct Rendering {
        id: RequestId,
        commands: RequestCommands,
        command: Command,
        exit: oneshot::Receiver<Exit>,
    }

    fn renderer(heading: String) -> Spec {
        let script = r#"IFS= read -r ready && printf '<section><h1>%s</h1></section>' "$1""#;
        let argv = vec![
            "/bin/sh".into(),
            "-c".into(),
            script.into(),
            "render-page".into(),
            heading,
        ];
        Spec {
            stdio: Some(Stdio::Coprocess),
            ..Spec::new(Exec::Argv(argv), "/")
        }
    }

    async fn render_page(
        asked: Asked<RenderPage>,
        namespace: Arc<Namespace>,
        started: mpsc::UnboundedSender<Rendering>,
    ) -> anyhow::Result<String> {
        let commands = asked.commands()?;
        let (command, mut events) = commands.spawn(&namespace, &renderer(asked.payload.0))?;
        let (exited, exit) = oneshot::channel();
        let rendering = Rendering {
            id: asked.id,
            commands,
            command,
            exit,
        };
        anyhow::ensure!(
            started.send(rendering).is_ok(),
            "the test stopped waiting for renderers"
        );

        // The reader reports exit even if replacement drops the request's handler.
        tokio::spawn(async move {
            let mut markup = Vec::new();
            loop {
                match events.next().await? {
                    Some(CommandEvent::Output(chunk)) => {
                        assert_eq!(chunk.offset, markup.len() as u64);
                        markup.extend_from_slice(&chunk.bytes);
                    }
                    Some(CommandEvent::Exit(exit)) => {
                        let _ = exited.send(exit);
                        anyhow::ensure!(exit == Exit::Code(0), "renderer exited with {exit:?}");
                        return Ok(String::from_utf8(markup)?);
                    }
                    other => anyhow::bail!("unexpected renderer event {other:?}"),
                }
            }
        })
        .await?
    }

    async fn next_render(started: &mut mpsc::UnboundedReceiver<Rendering>) -> Rendering {
        timeout(TIMEOUT, started.recv()).await.unwrap().unwrap()
    }

    #[test]
    fn retained_commands_do_not_keep_an_abandoned_reply_open() {
        let executor = host();
        let (started, retained) = std::sync::mpsc::channel();
        let runtime = Runtime::open(None, None).unwrap();
        let mut builder = Builder::new(&runtime).unwrap();
        builder
            .local_machine()
            .unwrap()
            .fiber(Once)
            .unwrap()
            .handle::<Lookup, _, _>(executor.handle(), move |asked| {
                let started = started.clone();
                async move {
                    started.send(asked.commands().unwrap()).unwrap();
                    pending().await
                }
            })
            .unwrap();
        builder.start().unwrap();
        let process = start::<Once>(&runtime, b"abandoned", &42);
        let commands = retained.recv_timeout(TIMEOUT).unwrap();

        drop(executor);
        assert_eq!(
            finish::<Once>(&runtime, &process),
            Outcome::Completed(Some(Response::Interrupted))
        );
        let namespace = Namespace::new().unwrap();
        namespace.mount_host("", "/", true).unwrap();
        let error = commands
            .spawn(&namespace, &renderer("Abandoned".into()))
            .unwrap_err();
        assert_eq!(error.status(), Status::Refused);
        runtime.close().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn revising_one_page_kills_its_command_and_renders_the_document() {
        let namespace = Arc::new(Namespace::new().unwrap());
        namespace.mount_host("", "/", true).unwrap();
        let (started, mut renders) = mpsc::unbounded_channel();
        let runtime = Runtime::open(None, None).unwrap();
        let mut builder = Builder::new(&runtime).unwrap();
        let render_namespace = Arc::clone(&namespace);
        builder
            .local_machine()
            .unwrap()
            .fiber(Document)
            .unwrap()
            .handle::<RenderPage, _, _>(&tokio::runtime::Handle::current(), move |asked| {
                let started = started.clone();
                let namespace = Arc::clone(&render_namespace);
                async move {
                    render_page(asked, namespace, started)
                        .await
                        .map_err(|error| Unanswered::Failed(error.to_string()))
                }
            })
            .unwrap();
        builder.start().unwrap();
        let document = start::<Document>(&runtime, b"document", &());

        let first = next_render(&mut renders).await;
        let second = next_render(&mut renders).await;
        let (draft, body) = if first.id.key() == b"cover" {
            (first, second)
        } else {
            (second, first)
        };
        assert_eq!(draft.id.fiber(), &document);
        assert_eq!(body.id.fiber(), &document);
        assert_eq!(draft.id.key(), b"cover");
        assert_eq!(body.id.key(), b"body");

        let mut revise = Changes::new().unwrap();
        revise
            .send(
                &document,
                Mail {
                    key: Bytes::from_static(b"revise"),
                    name: Bytes::from_static(b"revise"),
                    payload: Bytes::new(),
                },
            )
            .unwrap();
        runtime.commit(revise).await.unwrap();

        let cover = next_render(&mut renders).await;
        assert_eq!(cover.id.fiber(), draft.id.fiber());
        assert_eq!(cover.id.key(), draft.id.key());
        assert_ne!(cover.id, draft.id);
        assert_eq!(
            timeout(TIMEOUT, draft.exit).await.unwrap().unwrap(),
            Exit::Signal(9)
        );
        drop(draft.command);
        let stale = draft
            .commands
            .spawn(&namespace, &renderer("Stale".into()))
            .unwrap_err();
        assert_eq!(stale.status(), Status::Refused);

        body.command.write(b"render\n").unwrap();
        cover.command.write(b"render\n").unwrap();
        assert_eq!(
            finish::<Document>(&runtime, &document),
            Outcome::Completed(Some(
                "<main><section><h1>Cover</h1></section><section><h1>Chapters</h1></section></main>".into()
            ))
        );
        runtime.close().unwrap();
    }
}
