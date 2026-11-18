#![cfg(unix)]

use std::time::Duration;

use octoii_siphon_lib::{CommandEvent, Exec, Exit, Namespace, Runtime, Spec, Status, Stdio};

#[tokio::test(flavor = "multi_thread")]
async fn commands_use_the_runtime_namespace_and_keep_ordered_io() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().to_str().unwrap();
    let runtime = Runtime::memory().unwrap();
    let namespace = Namespace::new().unwrap();
    namespace.mount_host("", "/", true).unwrap();
    let mut spec = Spec::new(Exec::Shell("cat; printf retained >&2".into()), workspace);
    spec.stdio = Stdio::Coprocess;
    let (command, mut events) = runtime
        .spawn(runtime.now_ms().unwrap(), b"", &namespace, &spec)
        .unwrap();
    command.write(b"first ").unwrap();
    command.write(b"second").unwrap();
    command.close_input().unwrap();
    let mut output = Vec::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match event {
            CommandEvent::Output(data) => output.extend_from_slice(&data),
            CommandEvent::Exit(exit) => {
                assert_eq!(exit, Exit::Code(0));
                break;
            }
            CommandEvent::Title(title) => panic!("unexpected coprocess title {title}"),
        }
    }
    assert_eq!(output, b"first second");
    assert_eq!(command.output().unwrap().as_ref(), b"retained");
    assert_eq!(command.read_output_at(5).unwrap().bytes.as_ref(), b"ned");
    assert_eq!(events.next().await.unwrap(), None);

    let (running, _) = runtime
        .spawn(runtime.now_ms().unwrap(), b"", &namespace, &spec)
        .unwrap();
    runtime.close().unwrap();
    assert_eq!(running.write(b"late").unwrap_err().status(), Status::Closed);
}
