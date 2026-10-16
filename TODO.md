# TODO

## Soft-delete topic + scheduled GC

The topic delete flow is currently disabled. The previous in-progress
implementation has been commented out (see `// TODO:` markers across the
codebase) until the design below is implemented.

### Goal

Deleting a topic must not immediately destroy its data. Instead it is a
two-stage operation:

1. **SoftDelete** — the user marks a topic for deletion together with an
   expiration timestamp (`gc_after`). The topic is removed from the
   "active" topics list so producers/consumers stop seeing it, but its
   stored data (archives, indexes, the open tail) stays intact on
   disk. The soft-delete record is persisted in the topics snapshot so
   it survives a restart.
2. **Hard delete (GC)** — a background timer periodically scans the list
   of soft-deleted topics. Any whose `gc_after` is in the past gets its
   data permanently removed (delete `{namespace}/{topic}/` and the
   matching cold-storage keys, drop the in-memory `TopicData`, remove
   the soft-delete record). Until that
   moment a soft-deleted topic can also be **restored** (the soft-delete
   record is dropped and the topic becomes active again).

### Where the disabled code lives

- `src/operations/delete_topic.rs` — soft-delete operation
  (`delete_topic`) plus `TopicSoftDeleteMetadataBlobModel`.
- `src/operations/hard_delete_topic.rs` — GC operation
  (`hard_delete_topic`, `gc_expired_deleted_topics`).
- `src/operations/restore_topic.rs` — restore operation.
- `src/operations/mod.rs` — module declarations / re-exports.
- `src/timers/deleted_topics_gc.rs` + `src/timers/mod.rs` —
  `DeletedTopicsGcTimer` (30 s tick) that drives the GC.
- `src/main.rs` — registration of the GC timer.
- `src/topics_snapshot/current_snapshot.rs` — `add_deleted_topic`,
  `remove_deleted_topic` on `TopicsSnapshotData` /
  `CurrentTopicsSnapshot`.
- `src/topic_data/topics_data_list.rs` — `delete()` (Arc-swapped CoW
  variant; previously also kept a `deleted: HashSet<String>` to panic
  on later access — that mechanism should NOT be reintroduced as-is,
  see "Things to reconsider" below).
- `src/grpc/persistence_grpc_service.rs` — `delete_topic`,
  `restore_topic` gRPC methods (currently return
  `Status::unimplemented`).
- `src/http/controllers/topic_controller/delete_topic_action.rs` and
  `get_deleted_action.rs` — HTTP `DELETE /api/Topic` and
  `GET /api/Topic`.
- `src/http/controllers/topic_controller/mod.rs` — submodules.
- `src/http/builder.rs` — controller registration.
- `src/http/controllers/topic_controller/contracts.rs` —
  `DeleteTopicHttpContract` (kept with `#[allow(dead_code)]`).

The topics-snapshot persisted format already has a `deleted_topics`
field (`DeletedTopicProtobufModel { topic_id, message_id, gc_after,
namespace }`), so on-disk compatibility with previously soft-deleted
topics is preserved. The record is namespace-aware, so GC can never
delete a same-named topic in another namespace.

### Things to reconsider when re-enabling

- **Don't `panic!` on access to a deleted topic.** The previous
  `topics_data_list.get()`/`create_topic_data()` panicked when the topic
  id was in a `deleted: HashSet<String>`. That set was only ever
  *appended to*, never cleared by `hard_delete_topic`, so a topic with
  the same name could never be recreated. Either drop the set entirely
  (the snapshot is the source of truth) or make sure `hard_delete_topic`
  clears the entry, and never panic — return `None` / a typed error
  instead.
- **Restore vs. recreate race.** Decide what happens if a producer
  publishes to a topic that is currently soft-deleted: reject, auto-
  restore, or auto-create new? Currently this is undefined.
- **`delete_topic_secret_key`.** The HTTP handler used a settings-level
  shared secret. Confirm whether the gRPC handler should require a
  similar guard.
- **`hard_delete_topic` error handling.** The previous version logged
  the error on delete failure but had already removed the soft-delete
  record beforehand, so a transient storage error meant the data was
  orphaned forever. The flow should remove the soft-delete record only
  after the storage delete actually succeeds (or be idempotent and
  re-attempt safely).

---

## Other open items

- History-by-date implementation
  - `GetHistoryByDate` gRPC now returns an empty stream. Implement
    retrieval by timestamp range (likely leveraging index-by-minute)
    so clients can consume historical messages.

---

## Storage: files + S3

Implemented. Azure is gone - no `my-azure-*` crate is left in
[Cargo.toml](Cargo.toml) - and everything is written straight to files, with S3
as an optional cold tier.

### What landed

```text
{data_folder}/
    .layout-version               marks the folder as laid out by namespace
    {namespace}/
        topics-and-queue.yaml     that namespace's topics + queues, human readable
        {topic}/
            {:019}.archive        sealed sub pages: TOC + compressed blocks
            .{year}.yearindex     527 040 minutes x 8 bytes, addressed at minute*8
            active                the open tail - the sub page still being filled
```

- `{namespace}/{topic}/...` doubles as the S3 key; `app/storage_layout.rs` is the
  only place that spells either of them.
- `default` is not special. Pre-namespace folders sitting at the root are renamed
  into `default/` on first start (`operations::migrate_legacy_topics`) - a move
  within one mount, nothing copied. The marker is `{data_folder}/.layout-version`,
  and its *absence* is what matters: before namespaces the concept did not exist,
  so every folder at the root is a topic, with nothing to sniff or guess. The
  marker is written last, so a crash halfway is finished by the next start.
- One `data_folder` replaced the three connection strings, which also resolves the
  old tilde-expansion inconsistency.
- The uploader timer (60 s) keeps the highest-numbered archive and year index per
  topic and sends up all the rest - a backlog drains in one tick. No state is
  persisted: "highest on disk is current" holds by itself across rollovers and
  restarts. Upload, then delete locally; a crash in between costs a repeated PUT,
  which is idempotent.
- Reads look local first, then cold. A cold archive is never downloaded whole: the
  TOC is fetched once and cached forever (the object is immutable), then one ranged
  GET per sub page. A cold year index is instead pulled back to disk in full - 4 MB,
  addressed by offset - which makes a late write for a closed year work with no
  special case, and the uploader sends the updated copy back up.

- The cold tier is one bucket (`Bucket=x`, required) for every namespace, keys under
  `/x/my-sb-persistence/{ns}/{topic}/{file}`. The per-namespace `BucketPrefix` layout was dropped
  before production.
- Uploads are streamed from the file in 512 KB chunks, so memory does not depend on the size of the
  archive. This closed the production OOM: a 260 MB archive read whole into a 512 MB container was
  killed by SIGKILL, which leaves no panic and no log line.
- Settling the bucket is **best effort and never fatal**. It asks `check_if_bucket_exists`
  first and only creates what is missing - a key scoped to one bucket is routinely allowed to use it
  and denied `CreateBucket`. A failure is logged and stepped over; a transient one is tried again on
  the next operation, a deterministic one is not retried until a restart.
- `Debug=1` in `s3_conn_string` traces every S3 request to the console, so a refusing cold tier can
  be diagnosed on a running deployment without a rebuild.

### Audits

Two adversarial passes have run over this code. The first produced 20 findings, all 20 verified by
refutation, 8 confirmed and fixed:

- the year index was never registered on the write path, so nothing was ever indexed (4 findings,
  one defect);
- a resumed migration nested `default/` under `default/default`;
- a topic id containing `/` or `..` escaped its namespace folder - topic ids are now validated at
  every entry point, and `get_local_path` refuses an escaping component as a last line of defence;
- archive handles were cached in two independent lists, so a second `FileStorage` could be handed
  out for a file another writer already held - there is one cache now;
- a successful S3 DELETE (204) was read as a failure.

A second pass over the migration, snapshot and settings code produced 14 findings, all verified,
2 confirmed and fixed:

- the legacy `topicsdata` was imported whenever a `legacy` section was configured but converted
  only when `.layout-version` was absent, so starting once without the section and adding it
  afterwards stranded the whole topic list in a file nothing reads. The conversion is now keyed on
  the file being present, and merges rather than overwrites - the live snapshot is the newer one;
- `restore_legacy` deleted `.active-pages` even when it could not decode it. It holds the open tail
  of every topic and is the only copy by then; an undecodable one is now left in place, matching
  what the per-topic path already did.

Both migration bugs were found by *running* the service, not by the tests: each function behaved
correctly on its own and only their composition was wrong.

### The archiver's locking

`StorageLocks` holds one `tokio::sync::RwLock` per topic, one set for archives and one for year
indexes. The background archiver uses them in two phases:

1. **read** lock while the file is read and uploaded - the slow part, and readers pass through it
   freely since the file is still on disk and unchanged;
2. **write** lock to delete the local copy and drop the cached handle - it waits out every reader,
   so nothing is left holding a handle to a file that is about to disappear.

Hard delete takes both exclusively, since the whole topic folder goes.

`tokio::sync::RwLock` rather than `parking_lot` on purpose: the guard spans the upload and the
ranged read, both of which are awaits. No path takes the same lock twice, which matters because
tokio's RwLock is write-preferring and a re-entrant read behind a queued writer would deadlock.

### Upstream: `SortedVecOfArcWith2StrKey::remove` leaves an empty partition

`rust-extensions`' `PartitionOfArc::get_key` is `rows.get(0).unwrap()`, and `remove(primary,
secondary)` takes the row out without dropping a partition that just became empty. The next lookup
of *any* key then panics inside the binary search. Deleting the last topic of a namespace took the
whole service down until `TopicsDataList::remove` was changed to rebuild without the item instead
of rebuilding-then-removing - a partition only exists once something is inserted into it, so the
empty one is never created. Worth fixing in the crate; anything else using that container has the
same landmine.

Found by hard-deleting a topic on a running process. Neither audit saw it - it lives in a
dependency - and no unit test removed the last topic of a namespace.

### Still open

- **`active`: dump on shutdown, or an append-only journal?** It is still a dump,
  written in `before_shut_down`, so only a graceful restart is survived: `kill -9`,
  OOM or power loss lose the tail. And the tail is unbounded in time, not just in
  size - a topic that goes quiet mid-sub-page keeps those messages in RAM until
  shutdown, because GC only evicts a sub page once a newer one exists. On a plain
  file the tail could be appended to as messages arrive, shrinking the loss window
  to the last fsync and removing the shutdown path entirely. It changes the file
  format, so it is a deliberate decision rather than a refactor.
- **`ARCHIVE_MESSAGES_PER_FILE`** (10M) drives both the local disk peak and the size of a single
  upload. It can **not** simply be turned into a setting: `ArchiveFileNo::from_sub_page_id` divides
  by it, so changing it re-numbers every existing archive and silently misaddresses stored data.
  Changing it needs a layout version and a migration.
- **Nothing has run against a real AWS/Hetzner/MinIO endpoint yet.** The client is exercised against an
  in-process S3-compatible server (`cold_storage::fake_s3`), which covers SigV4 signing, the
  `Range` header, 200/206/204/404 handling, the key spelling and the cold archive read - but not
  a real provider's quirks. One smoke test against the actual bucket is still worth doing.
- **No automated end-to-end test.** The storage primitives, the layout and the migration are
  covered by 91 unit tests, and the whole flow has been driven by hand against a live process
  (migration, two namespaces, archive, restart, year index, per-namespace YAML). Nothing runs it
  automatically, and `cargo test` is still commented out in CI.
- **A cold read still panics on a storage failure.** `AppContext::restore_year_index_from_cold_storage`
  and `try_open_archive` panic when S3 answers anything other than success or "not there"
  (`app/app_ctx.rs`). Creating a bucket is best-effort and never fatal, so a deployment whose key
  can not create *or read* now starts happily and dies on the first archive read instead. The
  alternative - treating a 403 as "no data" - would look exactly like message loss to the bus node,
  which is worse, so this wants a deliberate answer rather than a quiet `unwrap_or_default`.
- **`PagesGcTimer` is driven by the topics snapshot, not by memory.** `gc_pages` iterates
  `topics_snapshot.snapshot.data`, so a topic that has received messages but is not in a saved
  snapshot never has its sub pages evicted or archived - they accumulate in RAM. The bus node
  pushes a snapshot every couple of seconds, so in practice the window is seconds; but the same
  shape already caused a real bug in `restore()` and is worth making memory-driven, using the
  snapshot entry only for the `persist` flag.
