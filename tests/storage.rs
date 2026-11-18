use octoii_siphon_lib::{Changes, NameChange, Query, Range, Runtime};

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
fn metadata_and_index_pages_follow_their_snapshot() {
    let mut runtime = Runtime::memory().unwrap();
    let mut changes = Changes::new().unwrap();
    changes
        .mkdir(b"team")
        .unwrap()
        .mkdir(b"team-other")
        .unwrap();
    let entries: &[(&[u8], &[u8])] = &[
        (b"team/item:b", b"ready"),
        (b"team/item:a", b""),
        (b"team/item:\xff", b"\xfe"),
        (b"team-other/item:a", b""),
    ];
    let tag = |value: Option<&'static [u8]>| NameChange {
        name: b"tag",
        value,
    };
    for &(path, value) in entries {
        changes
            .mkdir(path)
            .unwrap()
            .meta(path, &[tag(Some(value))], &[])
            .unwrap();
    }
    changes
        .meta(
            b"team/item:a",
            &[],
            &[NameChange {
                name: b"note",
                value: Some(b"\x00\xff"),
            }],
        )
        .unwrap();
    runtime.commit(&changes).unwrap();
    let before = runtime.snapshot().unwrap();

    let query = Query {
        kind: b"item",
        name: b"tag",
        under: b"team",
        ..Query::default()
    };
    let first = before.find(&query, 1).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].path, b"team/item:a");
    assert!(first[0].value.is_empty());
    let entry = first[0].entry.directory.as_ref().unwrap();
    assert_eq!(entry.label(b"tag"), Some(b"".as_slice()));
    assert_eq!(entry.annotation(b"note"), Some(b"\x00\xff".as_slice()));

    let rest = before
        .find(
            &Query {
                after: Some(&first[0].path),
                ..query
            },
            10,
        )
        .unwrap();
    assert_eq!(rest.len(), 2);
    assert_eq!(rest[0].path, b"team/item:b");
    assert_eq!(rest[1].path, b"team/item:\xff");
    let exact = before
        .find(
            &Query {
                value: Some(b"\xfe"),
                ..query
            },
            10,
        )
        .unwrap();
    assert_eq!(exact, rest[1..]);
    assert!(before.find(&query, 0).unwrap().is_empty());

    let mut update = Changes::new().unwrap();
    update
        .meta(
            b"team/item:a",
            &[tag(None)],
            &[NameChange {
                name: b"note",
                value: None,
            }],
        )
        .unwrap()
        .meta(b"team/item:b", &[tag(Some(b""))], &[])
        .unwrap();
    runtime.commit(&update).unwrap();
    let after = runtime.snapshot().unwrap();
    runtime.close().unwrap();

    let empty = Query {
        value: Some(b""),
        ..query
    };
    assert_eq!(before.find(&empty, 10).unwrap(), first);
    let found = after.find(&empty, 10).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].path, b"team/item:b");
    let removed = after
        .stat(b"team/item:a")
        .unwrap()
        .unwrap()
        .directory
        .unwrap();
    assert_eq!(removed.label(b"tag"), None);
    assert_eq!(removed.annotation(b"note"), None);
}

#[test]
fn journal_pages_keep_commit_boundaries_and_advance_over_empty_commits() {
    let mut runtime = Runtime::memory().unwrap();
    let initial = runtime.snapshot().unwrap().journal(0, 1).unwrap();
    assert!(initial.change_sets.is_empty());
    assert_eq!(
        (initial.through, initial.oldest, initial.latest),
        (0, None, 0)
    );
    assert!(!initial.missed);

    let mut write = Changes::new().unwrap();
    write.write(b"raw\xff", b"one").unwrap();
    let first = runtime.commit(&write).unwrap();
    let retained = runtime.snapshot().unwrap();
    let empty = runtime.commit(&Changes::new().unwrap()).unwrap();
    assert!(empty > first);

    let mut rewrite = Changes::new().unwrap();
    rewrite
        .delete(b"raw\xff")
        .unwrap()
        .write(b"new", b"two")
        .unwrap();
    let last = runtime.commit(&rewrite).unwrap();
    let snapshot = runtime.snapshot().unwrap();
    runtime.close().unwrap();

    let page = snapshot.journal(0, 1).unwrap();
    assert_eq!(
        (page.through, page.oldest, page.latest),
        (first, Some(first), last)
    );
    assert!(!page.missed);
    assert_eq!(page.change_sets.len(), 1);
    assert_eq!(page.change_sets[0].seq, first);
    assert_eq!(page.change_sets[0].paths, [b"raw\xff".to_vec()]);

    let quiet = snapshot.journal(page.through, 1).unwrap();
    assert!(quiet.change_sets.is_empty());
    assert_eq!(quiet.through, empty);
    assert!(!quiet.missed);

    let next = snapshot.journal(quiet.through, 1).unwrap();
    assert_eq!(next.through, last);
    assert_eq!(next.change_sets.len(), 1);
    assert_eq!(next.change_sets[0].seq, last);
    assert_eq!(next.change_sets[0].paths.len(), 2);
    assert!(
        next.change_sets[0]
            .paths
            .iter()
            .any(|path| path == b"raw\xff")
    );
    assert!(next.change_sets[0].paths.iter().any(|path| path == b"new"));
    assert!(snapshot.journal(last, 1).unwrap().change_sets.is_empty());

    let old = retained.journal(0, 10).unwrap();
    assert_eq!((old.through, old.latest), (first, first));
    assert_eq!(old.change_sets, page.change_sets);
}

#[test]
fn volume_ids_and_file_retention_survive_reopening() {
    let directory = tempfile::tempdir().unwrap();
    let mut runtime = Runtime::open(directory.path()).unwrap();
    let first = runtime.new_id().unwrap();
    assert_eq!(first.len(), 36);
    assert_eq!(first.as_bytes()[14], b'7');
    let now = runtime.now_ms().unwrap();
    assert!(now > 0);
    let deadline = now + 60_000;

    let mut changes = Changes::new().unwrap();
    changes
        .write(b"log", b"first")
        .unwrap()
        .seal(b"log")
        .unwrap()
        .expire(b"log", Some(deadline))
        .unwrap();
    runtime.commit(&changes).unwrap();
    let before = runtime.snapshot().unwrap();
    assert_eq!(
        before.stat(b"log").unwrap().unwrap().expires_at,
        Some(deadline)
    );

    let mut blocked = Changes::new().unwrap();
    blocked.append(b"log", b"later").unwrap();
    assert_eq!(runtime.commit(&blocked).unwrap_err().code, 4);
    let mut clear = Changes::new().unwrap();
    clear.expire(b"log", None).unwrap();
    runtime.commit(&clear).unwrap();
    let after = runtime.snapshot().unwrap();
    assert_eq!(
        before.stat(b"log").unwrap().unwrap().expires_at,
        Some(deadline)
    );
    assert_eq!(after.stat(b"log").unwrap().unwrap().expires_at, None);
    assert_eq!(
        after.read(b"log").unwrap().as_deref(),
        Some(b"first".as_slice())
    );

    runtime.close().unwrap();
    assert_eq!(runtime.new_id().unwrap_err().code, 3);
    assert_eq!(runtime.now_ms().unwrap_err().code, 3);
    drop((before, after, runtime));

    let reopened = Runtime::open(directory.path()).unwrap();
    assert!(reopened.new_id().unwrap() > first);
    assert_eq!(
        reopened
            .snapshot()
            .unwrap()
            .stat(b"log")
            .unwrap()
            .unwrap()
            .expires_at,
        None
    );
    assert_eq!(reopened.commit(&blocked).unwrap_err().code, 4);
}

#[test]
fn a_failed_batch_does_not_commit_earlier_writes() {
    let runtime = Runtime::memory().unwrap();
    let mut batch = Changes::new().unwrap();
    let tag = |value: &'static [u8]| NameChange {
        name: b"tag",
        value: Some(value),
    };
    batch
        .write(b"file", b"before")
        .unwrap()
        .mkdir(b"record")
        .unwrap()
        .meta(b"record", &[tag(b"before")], &[])
        .unwrap();
    runtime.commit(&batch).unwrap();
    let mut invalid = Changes::new().unwrap();
    invalid
        .write(b"file", b"after")
        .unwrap()
        .meta(b"record", &[tag(b"after")], &[])
        .unwrap()
        .write(b"missing/child", b"bad")
        .unwrap();
    assert!(runtime.commit(&invalid).is_err());
    let snapshot = runtime.snapshot().unwrap();
    assert_eq!(snapshot.read(b"file").unwrap(), Some(b"before".to_vec()));
    let matches = snapshot
        .find(
            &Query {
                kind: b"record",
                name: b"tag",
                ..Query::default()
            },
            1,
        )
        .unwrap();
    assert_eq!(matches[0].value, b"before");
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
