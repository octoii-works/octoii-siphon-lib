#![cfg(unix)]

use std::time::Duration;

use octoii_siphon_lib::{
    Builder, CommandEvent, Exec, Exit, Namespace, Runtime, Spec, Status, Stdio,
};

#[tokio::test(flavor = "multi_thread")]
async fn commands_use_the_runtime_namespace_and_keep_ordered_io() {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().to_str().unwrap();
    let runtime = Runtime::open(None, None).unwrap();
    let mut builder = Builder::new(&runtime).unwrap();
    builder.local_machine().unwrap();
    builder.start().unwrap();
    let namespace = Namespace::new().unwrap();
    namespace.mount_host("", "/", true).unwrap();
    let mut spec = Spec::new(Exec::Shell("cat; printf retained >&2".into()), workspace);
    spec.stdio = Some(Stdio::Coprocess);
    let (command, mut events) = runtime.spawn(None, &namespace, &spec).unwrap();
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
            CommandEvent::Output(chunk) => {
                assert_eq!(chunk.offset, output.len() as u64);
                output.extend_from_slice(&chunk.bytes);
            }
            CommandEvent::Exit(exit) => {
                assert_eq!(exit, Exit::Code(0));
                break;
            }
            other => panic!("unexpected coprocess event {other:?}"),
        }
    }
    assert_eq!(output, b"first second");
    assert_eq!(command.output().unwrap().as_ref(), b"retained");
    assert_eq!(command.read_output_at(5).unwrap().bytes.as_ref(), b"ned");
    assert_eq!(events.next().await.unwrap(), None);

    let (running, _) = runtime.spawn(None, &namespace, &spec).unwrap();
    runtime.close().unwrap();
    assert_eq!(running.write(b"late").unwrap_err().status(), Status::Closed);
}
