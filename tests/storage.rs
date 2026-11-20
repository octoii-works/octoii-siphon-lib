use bytes::Bytes;

use std::time::Duration;

use octoii_siphon_lib::{
    Changes, CommitEvent, FileKind, Namespace, Query, Range, Runtime, Snapshot, Status,
};

#[test]
fn snapshots_are_consistent_and_data_survives_reopening() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(Some(directory.path()), None).unwrap();
    let mut first = Changes::new().unwrap();
    first
        .write(b"file", b"one")
        .unwrap()
        .write(b"empty", b"")
        .unwrap();
    let seq = runtime.commit_blocking(&first).unwrap().seq();
    let before = runtime.snapshot().unwrap();
    assert!(before.sequence().unwrap() >= seq);
    assert_eq!(before.read(b"empty").unwrap(), Some(Bytes::new()));
    assert_eq!(before.read(b"missing").unwrap(), None);
    let mut second = Changes::new().unwrap();
    second.write(b"file", b"two").unwrap();
    runtime.commit_blocking(&second).unwrap();
    assert_eq!(
        before.read(b"file").unwrap(),
        Some(Bytes::from_static(b"one"))
    );
    runtime.close().unwrap();
    assert_eq!(runtime.snapshot().unwrap_err().code, 3);
    assert_eq!(
        before.read_range(b"file", 1, 1).unwrap(),
        Some(Bytes::from_static(b"n"))
    );
    drop(before);
    drop(runtime);
    let reopened = Runtime::open(Some(directory.path()), None).unwrap();
    assert_eq!(
        reopened.snapshot().unwrap().read(b"file").unwrap(),
        Some(Bytes::from_static(b"two"))
    );
}

#[test]
fn offline_collector_retries_stale_readings_and_flushes_the_result() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(Some(directory.path()), Some(2)).unwrap();
    let initial = runtime.snapshot().unwrap().sequence().unwrap();

    let mut seed = Changes::new().unwrap();
    seed.write(b"total", b"0").unwrap();
    let seeded = runtime.commit_blocking(&seed).unwrap().seq();
    let north_view = runtime.snapshot().unwrap();
    let south_view = runtime.snapshot().unwrap();
    assert_eq!(north_view.sequence().unwrap(), seeded);
    assert_eq!(south_view.sequence().unwrap(), seeded);

    let collect = |view: &Snapshot, source: &[u8], reading: u64| {
        let prior = view.read(b"total").unwrap().unwrap();
        let total = serde_json::from_slice::<u64>(&prior).unwrap() + reading;
        let mut changes = Changes::new().unwrap();
        changes
            .write(source, reading.to_string())
            .unwrap()
            .write(b"total", total.to_string())
            .unwrap()
            .expect(view)
            .unwrap();
        changes
    };
    let north = collect(&north_view, b"north", 2);
    let stale = collect(&south_view, b"south", 3);
    drop(south_view);

    let north_receipt = runtime.commit_blocking(&north).unwrap();
    let first = north_receipt.seq();
    assert_eq!(
        runtime.commit_blocking(&stale).unwrap_err().status(),
        Status::Conflict
    );
    let after_north = runtime.snapshot().unwrap();
    assert_eq!(after_north.sequence().unwrap(), first);
    assert_eq!(
        after_north.read(b"total").unwrap().as_deref(),
        Some(&b"2"[..])
    );
    assert_eq!(after_north.read(b"south").unwrap(), None);

    let south = collect(&after_north, b"south", 3);
    let south_receipt = runtime.commit_blocking(&south).unwrap();
    let last = south_receipt.seq();
    let collected = runtime.snapshot().unwrap();

    let north_page = collected.journal(seeded, 1).unwrap();
    assert!(!north_page.missed);
    assert_eq!(
        (north_page.through, north_page.oldest, north_page.latest),
        (first, Some(first), last)
    );
    let south_page = collected.journal(north_page.through, 1).unwrap();
    assert!(!south_page.missed);
    assert_eq!(south_page.through, last);
    for (page, seq, source) in [
        (&north_page, first, b"north"),
        (&south_page, last, b"south"),
    ] {
        let [entry] = page.change_sets.as_slice() else {
            panic!("each reading should fill one journal page");
        };
        assert_eq!(entry.seq, seq);
        assert_eq!(entry.paths.len(), 2);
        assert!(entry.paths.iter().any(|path| path.as_ref() == source));
        assert!(entry.paths.iter().any(|path| path.as_ref() == b"total"));
    }
    let gap = collected.journal(initial, 1).unwrap();
    assert!(gap.missed);
    assert_eq!(gap.change_sets, north_page.change_sets);
    let caught_up = collected.journal(south_page.through, 1).unwrap();
    assert!(!caught_up.missed);
    assert!(caught_up.change_sets.is_empty());
    assert_eq!(caught_up.through, last);

    assert_eq!(runtime.flush(&south_receipt).unwrap(), last);
    assert_eq!(runtime.flush(&north_receipt).unwrap(), last);
    runtime.close().unwrap();
    assert_eq!(
        north_view.read(b"total").unwrap().as_deref(),
        Some(&b"0"[..])
    );
    assert_eq!(north_view.read(b"north").unwrap(), None);
    drop((north_view, after_north, collected, runtime));

    let reopened = Runtime::open(Some(directory.path()), Some(2)).unwrap();
    let snapshot = reopened.snapshot().unwrap();
    assert_eq!(snapshot.sequence().unwrap(), last);
    assert_eq!(snapshot.read(b"total").unwrap().as_deref(), Some(&b"5"[..]));
    assert_eq!(snapshot.read(b"north").unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(snapshot.read(b"south").unwrap().as_deref(), Some(&b"3"[..]));
    assert_eq!(south_receipt.seq(), last);
    assert_eq!(
        reopened.flush(&south_receipt).unwrap_err().status(),
        Status::Invalid
    );
}

#[test]
fn directory_pages_and_entries_are_owned_after_runtime_close() {
    let runtime = Runtime::open(None, None).unwrap();
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
    let seq = runtime.commit_blocking(&changes).unwrap().seq();
    let snapshot = runtime.snapshot().unwrap();
    runtime.close().unwrap();

    let range = Range {
        prefix: b"p",
        limit: 2,
        ..Range::default()
    };
    let forward = snapshot.list(b"data", &range).unwrap().unwrap();
    assert_eq!(forward.len(), 2);
    assert_eq!(forward[0].name, Bytes::from_static(b"p-a"));
    assert_eq!(forward[0].length, 0);
    assert_eq!(forward[1].name, Bytes::from_static(b"p-b"));
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
    assert_eq!(next[0].name, Bytes::from_static(b"p\xff"));

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
    assert_eq!(reverse[0].name, Bytes::from_static(b"p\xff"));
    assert_eq!(reverse[1].name, Bytes::from_static(b"p-b"));
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
    assert_eq!(earlier[0].name, Bytes::from_static(b"p-a"));

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
    let runtime = Runtime::open(None, None).unwrap();
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
    let tag =
        |value: Option<&'static [u8]>| (Bytes::from_static(b"tag"), value.map(Bytes::from_static));
    for &(path, value) in entries {
        changes
            .mkdir(path)
            .unwrap()
            .meta(path, &[tag(Some(value))], &[])
            .unwrap();
    }
    let label_name = b"system.removing";
    let label_value = b"true";
    changes
        .meta(
            b"team/item:a",
            &[(
                Bytes::from_static(label_name),
                Some(Bytes::from_static(label_value)),
            )],
            &[(
                Bytes::from_static(b"note"),
                Some(Bytes::from_static(b"\x00\xff")),
            )],
        )
        .unwrap();
    runtime.commit_blocking(&changes).unwrap();
    let before = runtime.snapshot().unwrap();

    let query = Query {
        kind: b"item",
        name: b"tag",
        under: b"team",
        ..Query::default()
    };
    let first = before.find(&query, 1).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].path, Bytes::from_static(b"team/item:a"));
    assert!(first[0].value.is_empty());
    let entry = first[0].entry.directory.as_ref().unwrap();
    assert_eq!(entry.label(b"tag").map(Bytes::as_ref), Some(b"".as_slice()));
    assert_eq!(
        entry.annotation(b"note").map(Bytes::as_ref),
        Some(b"\x00\xff".as_slice())
    );

    assert_eq!(
        entry.label(label_name).map(Bytes::as_ref),
        Some(label_value.as_slice())
    );
    let stat = before.stat(b"team/item:a").unwrap().unwrap();
    assert_eq!(stat, first[0].entry);
    let listed = before
        .list(
            b"team",
            &Range {
                prefix: b"item:a",
                limit: 10,
                ..Range::default()
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(listed, [stat]);

    let indexed = before
        .find(
            &Query {
                name: label_name,
                value: Some(label_value),
                ..query
            },
            10,
        )
        .unwrap();
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].path, first[0].path);
    assert_eq!(indexed[0].value, Bytes::from_static(label_value));

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
    assert_eq!(rest[0].path, Bytes::from_static(b"team/item:b"));
    assert_eq!(rest[1].path, Bytes::from_static(b"team/item:\xff"));
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
            &[(Bytes::from_static(b"note"), None)],
        )
        .unwrap()
        .meta(b"team/item:b", &[tag(Some(b""))], &[])
        .unwrap();
    runtime.commit_blocking(&update).unwrap();
    let after = runtime.snapshot().unwrap();
    runtime.close().unwrap();

    let empty = Query {
        value: Some(b""),
        ..query
    };
    assert_eq!(before.find(&empty, 10).unwrap(), first);
    let found = after.find(&empty, 10).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].path, Bytes::from_static(b"team/item:b"));
    let removed = after
        .stat(b"team/item:a")
        .unwrap()
        .unwrap()
        .directory
        .unwrap();
    assert_eq!(removed.label(b"tag"), None);
    assert_eq!(removed.annotation(b"note"), None);
    assert_eq!(
        removed.label(label_name).map(Bytes::as_ref),
        Some(label_value.as_slice())
    );
}

#[test]
fn journal_pages_keep_commit_boundaries_and_advance_over_empty_commits() {
    let runtime = Runtime::open(None, None).unwrap();
    let initial = runtime.snapshot().unwrap().journal(0, 1).unwrap();
    assert!(initial.change_sets.is_empty());
    assert_eq!(
        (initial.through, initial.oldest, initial.latest),
        (0, None, 0)
    );
    assert!(!initial.missed);

    let mut write = Changes::new().unwrap();
    write.write(b"raw\xff", b"one").unwrap();
    let first = runtime.commit_blocking(&write).unwrap().seq();
    let retained = runtime.snapshot().unwrap();
    let empty = runtime
        .commit_blocking(&Changes::new().unwrap())
        .unwrap()
        .seq();
    assert!(empty > first);

    let mut rewrite = Changes::new().unwrap();
    rewrite
        .delete(b"raw\xff")
        .unwrap()
        .write(b"new", b"two")
        .unwrap();
    let last = runtime.commit_blocking(&rewrite).unwrap().seq();
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
            .any(|path| path.as_ref() == b"raw\xff")
    );
    assert!(
        next.change_sets[0]
            .paths
            .iter()
            .any(|path| path.as_ref() == b"new")
    );
    assert!(snapshot.journal(last, 1).unwrap().change_sets.is_empty());

    let old = retained.journal(0, 10).unwrap();
    assert_eq!((old.through, old.latest), (first, first));
    assert_eq!(old.change_sets, page.change_sets);
}

#[test]
fn volume_ids_and_file_retention_survive_reopening() {
    let directory = tempfile::tempdir().unwrap();
    let runtime = Runtime::open(Some(directory.path()), None).unwrap();
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
    runtime.commit_blocking(&changes).unwrap();
    let before = runtime.snapshot().unwrap();
    assert_eq!(
        before.stat(b"log").unwrap().unwrap().expires_at,
        Some(deadline)
    );

    let mut blocked = Changes::new().unwrap();
    blocked.append(b"log", b"later").unwrap();
    assert_eq!(
        runtime.commit_blocking(&blocked).unwrap_err().status(),
        Status::ReadOnly
    );
    let mut clear = Changes::new().unwrap();
    clear.expire(b"log", None).unwrap();
    runtime.commit_blocking(&clear).unwrap();
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

    let reopened = Runtime::open(Some(directory.path()), None).unwrap();
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
    assert_eq!(
        reopened.commit_blocking(&blocked).unwrap_err().status(),
        Status::ReadOnly
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_subscription_reports_progress_and_closes_with_its_runtime() {
    let runtime = Runtime::open(None, None).unwrap();
    let mut watch = runtime.commits().unwrap();
    let initial = runtime.snapshot().unwrap().sequence().unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .unwrap()
            .unwrap(),
        Some(CommitEvent::Head(seq)) if seq == initial
    ));

    let mut rejected = Changes::new().unwrap();
    rejected.write(b"absent/child", b"no").unwrap();
    assert!(runtime.commit_blocking(&rejected).is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(25), watch.next())
            .await
            .is_err()
    );

    let mut batch = Changes::new().unwrap();
    batch.write(b"file", b"one").unwrap();
    let first = runtime.commit(batch).await.unwrap().seq();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .unwrap()
            .unwrap(),
        Some(CommitEvent::Head(seq)) if seq == first
    ));

    let mut cancelled = runtime.commits().unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), cancelled.next())
            .await
            .unwrap()
            .unwrap(),
        Some(CommitEvent::Head(seq)) if seq == first
    ));
    drop(cancelled);
    let mut next = Changes::new().unwrap();
    next.write(b"file", b"two").unwrap();
    let second = runtime.commit(next).await.unwrap().seq();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .unwrap()
            .unwrap(),
        Some(CommitEvent::Head(seq)) if seq == second
    ));
    let journal = runtime.snapshot().unwrap().journal(first, 1).unwrap();
    assert_eq!(journal.through, second);
    assert_eq!(journal.change_sets[0].paths, [b"file".to_vec()]);

    let retained = runtime.runtime_handle();
    runtime.close().unwrap();
    let after_close = retained.commits().unwrap();
    drop(runtime);
    let after_owner_drop = retained.commits().unwrap();

    for mut subscription in [watch, after_close, after_owner_drop] {
        let terminal = tokio::time::timeout(Duration::from_secs(5), subscription.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal, Some(CommitEvent::Closed(second)));
        let end = tokio::time::timeout(Duration::from_secs(5), subscription.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(end, None);
    }
}

#[test]
fn a_failed_batch_does_not_commit_earlier_writes() {
    let runtime = Runtime::open(None, None).unwrap();
    let mut batch = Changes::new().unwrap();
    let tag = |value: &'static [u8]| (Bytes::from_static(b"tag"), Some(Bytes::from_static(value)));
    batch
        .write(b"file", b"before")
        .unwrap()
        .mkdir(b"record")
        .unwrap()
        .meta(b"record", &[tag(b"before")], &[])
        .unwrap();
    runtime.commit_blocking(&batch).unwrap();
    let mut invalid = Changes::new().unwrap();
    invalid
        .write(b"file", b"after")
        .unwrap()
        .meta(b"record", &[tag(b"after")], &[])
        .unwrap()
        .write(b"missing/child", b"bad")
        .unwrap();
    assert!(runtime.commit_blocking(&invalid).is_err());
    let snapshot = runtime.snapshot().unwrap();
    assert_eq!(
        snapshot.read(b"file").unwrap(),
        Some(Bytes::from_static(b"before"))
    );
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
    assert_eq!(matches[0].value, Bytes::from_static(b"before"));
}

#[tokio::test(flavor = "multi_thread")]
async fn failure_cleanup_and_drop_work_inside_an_async_host() {
    let directory = tempfile::tempdir().unwrap();
    for _ in 0..10 {
        let runtime = Runtime::open(Some(directory.path()), None).unwrap();
        let error = Runtime::open(Some(directory.path()), None).unwrap_err();
        assert_ne!(error.code, 5, "locked directory must not panic");
        drop(runtime);
    }
}

#[test]
fn a_namespace_mounts_host_and_read_only_volume_views() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().to_str().unwrap();
    let namespace = Namespace::new().unwrap();
    namespace.mount_host("", root, true).unwrap();
    namespace.mkdir("hidden").unwrap();
    namespace.write("hidden/notes", b"secret").unwrap();
    namespace.write("one", b"12345").unwrap();
    namespace.write("two", b"other").unwrap();
    assert_eq!(namespace.read("one", 2, 2).unwrap().as_ref(), b"34");
    namespace.hide("hidden").unwrap();
    assert_eq!(
        namespace.read("hidden/notes", 0, 64).unwrap_err().status(),
        Status::Denied
    );

    let stat = namespace.stat("one").unwrap().unwrap();
    assert_eq!((stat.kind, stat.size), (FileKind::File, 5));
    let range = Range {
        prefix: b"o",
        limit: 10,
        ..Range::default()
    };
    let names = namespace.list("", &range).unwrap();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].name.as_ref(), b"one");

    let runtime = Runtime::open(None, None).unwrap();
    let mut changes = Changes::new().unwrap();
    changes
        .mkdir(b"document:first")
        .unwrap()
        .meta(
            b"document:first",
            &[(
                Bytes::from_static(b"visible"),
                Some(Bytes::from_static(b"yes")),
            )],
            &[],
        )
        .unwrap()
        .write(b"document:first/note", b"from the volume")
        .unwrap();
    runtime.commit_blocking(&changes).unwrap();
    namespace
        .mount_labels(&runtime, "documents", "document", "visible", b"")
        .unwrap();
    assert_eq!(
        namespace
            .read("documents/first/note", 0, 128)
            .unwrap()
            .as_ref(),
        b"from the volume"
    );
    assert_eq!(
        namespace
            .write("documents/first/note", b"new")
            .unwrap_err()
            .status(),
        Status::ReadOnly
    );
}
