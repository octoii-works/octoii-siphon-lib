use octoii_siphon_lib::{Changes, Range, Runtime};

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
fn directory_pages_and_entries_are_owned_after_runtime_close() {
    let mut runtime = Runtime::memory().unwrap();
    let mut changes = Changes::new().unwrap();
    changes
        .mkdir(b"data")
        .unwrap()
        .mkdir(b"data/empty")
        .unwrap()
        .write(b"data/p-b", b"other")
        .unwrap()
        .write(b"data/p-a", b"")
        .unwrap()
        .write(b"data/p\xff", b"raw")
        .unwrap();
    let seq = runtime.commit(&changes).unwrap();
    let snapshot = runtime.snapshot().unwrap();
    runtime.close().unwrap();

    let range = Range {
        prefix: b"p",
        limit: 2,
        ..Range::default()
    };
    let forward = snapshot.list(b"data", &range).unwrap().unwrap();
    assert_eq!(forward.len(), 2);
    assert_eq!(forward[0].name, b"p-a");
    assert_eq!(forward[0].length, 0);
    assert_eq!(forward[1].name, b"p-b");
    let next = snapshot
        .list(
            b"data",
            &Range {
                after: Some(&forward[1].name),
                ..range
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].name, b"p\xff");

    let reverse = snapshot
        .list(
            b"data",
            &Range {
                reverse: true,
                ..range
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(reverse.len(), 2);
    assert_eq!(reverse[0].name, b"p\xff");
    assert_eq!(reverse[1].name, b"p-b");
    let earlier = snapshot
        .list(
            b"data",
            &Range {
                reverse: true,
                after: Some(&reverse[1].name),
                ..range
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(earlier.len(), 1);
    assert_eq!(earlier[0].name, b"p-a");

    assert_eq!(
        snapshot.list(b"data/empty", &range).unwrap(),
        Some(Vec::new())
    );
    assert_eq!(snapshot.list(b"missing", &range).unwrap(), None);
    assert!(snapshot.list(b"data/p-a", &range).is_err());
    assert_eq!(snapshot.stat(b"missing").unwrap(), None);
    let file = snapshot.stat(b"data/p\xff").unwrap().unwrap();
    let directory = snapshot.stat(b"data").unwrap().unwrap().directory.unwrap();
    drop(snapshot);

    assert_eq!(file, next[0]);
    assert_eq!(file.length, 3);
    assert_eq!(file.expires_at, None);
    assert_eq!(file.directory, None);
    assert_eq!(directory.created_seq, seq);
    assert!(directory.labels.is_empty() && directory.annotations.is_empty());
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
