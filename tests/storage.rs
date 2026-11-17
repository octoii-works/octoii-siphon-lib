use octoii_siphon_lib::{Changes, Runtime};

#[test]
fn snapshots_are_consistent_and_data_survives_reopening() {
    let directory = tempfile::tempdir().unwrap();
    let mut runtime = Runtime::open(directory.path()).unwrap();
    let mut first = Changes::new().unwrap();
    first
        .write(b"file", b"one")
        .unwrap()
        .write(b"empty", b"")
        .unwrap();
    let seq = runtime.commit(&first).unwrap();
    let before = runtime.snapshot().unwrap();
    assert!(before.sequence().unwrap() >= seq);
    assert_eq!(before.read(b"empty").unwrap(), Some(vec![]));
    assert_eq!(before.read(b"missing").unwrap(), None);
    let mut second = Changes::new().unwrap();
    second.write(b"file", b"two").unwrap();
    runtime.commit(&second).unwrap();
    assert_eq!(before.read(b"file").unwrap(), Some(b"one".to_vec()));
    runtime.close().unwrap();
    assert_eq!(runtime.snapshot().unwrap_err().code, 3);
    assert_eq!(
        before.read_range(b"file", 1, 1).unwrap(),
        Some(b"n".to_vec())
    );
    drop(before);
    drop(runtime);
    let reopened = Runtime::open(directory.path()).unwrap();
    assert_eq!(
        reopened.snapshot().unwrap().read(b"file").unwrap(),
        Some(b"two".to_vec())
    );
}

#[test]
fn a_failed_batch_does_not_commit_earlier_writes() {
    let runtime = Runtime::memory().unwrap();
    let mut batch = Changes::new().unwrap();
    batch.write(b"file", b"before").unwrap();
    runtime.commit(&batch).unwrap();
    let mut invalid = Changes::new().unwrap();
    invalid
        .write(b"file", b"after")
        .unwrap()
        .write(b"missing/child", b"bad")
        .unwrap();
    assert!(runtime.commit(&invalid).is_err());
    assert_eq!(
        runtime.snapshot().unwrap().read(b"file").unwrap(),
        Some(b"before".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn failure_cleanup_and_drop_work_inside_an_async_host() {
    let directory = tempfile::tempdir().unwrap();
    for _ in 0..10 {
        let runtime = Runtime::open(directory.path()).unwrap();
        let error = Runtime::open(directory.path()).unwrap_err();
        assert_ne!(error.code, 5, "locked directory must not panic");
        drop(runtime);
    }
}
