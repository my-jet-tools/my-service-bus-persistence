# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this service is

`my-sb-persistence` is the durable-storage node for [`my-service-bus`](https://github.com/MyJetTools/my-service-bus).
The main bus node is the **only** canonical client: it calls this service over gRPC (port `7124`) to persist topics,
queue snapshots and messages **straight to files**, with S3 as an optional cold tier for sealed data. Do not call
this service directly from application code.

Topics are keyed by a `(namespace, topic_id)` pair — a topic name is unique only within its namespace. A missing or
empty namespace always means `default`, so a bus node that knows nothing about namespaces keeps working unchanged.

The README ([README.md](README.md)) is the authoritative operator-facing reference: settings, endpoints, ports,
storage layout, lifecycle/timers. Read it before changing config, endpoints, or the storage layout.

## Build / run / test

```sh
cargo check                 # fast feedback loop (preferred while iterating)
cargo build --release        # what CI builds; Docker image consumes target/release/my-sb-persistence
cargo run --release          # local run; reads YAML config from $HOME/.myservicebus-persistence
```

- 55 unit tests cover the storage primitives, the layout and the migration; `cargo test` is still commented out in CI.
- `build.rs` downloads `MyServicePersistenceGrpcService.proto` from the `my-sb-proto-files` GitHub repo at
  build time (via `ci-utils::sync_and_build_proto_file`) and compiles it into the `crate::persistence_grpc`
  module (`tonic::include_proto!("persistence")`). The local `proto/` copy is a synced artifact — the proto
  contract lives in the remote repo, so edit it there, not here.
- Requires `$HOME` to be set: the settings filename is resolved relative to it. jemalloc is the global allocator on Linux
  (a target-specific dependency); other platforms, e.g. macOS for local dev, use the system allocator.
  It is tuned through `_rjem_malloc_conf` in `main.rs` (background purge thread, fast decay), which only takes
  effect while `tikv-jemallocator` stays prefixed — do not add `unprefixed_malloc_on_supported_platforms`.
- There is no Azure dependency any more. Storage is plain files under `data_folder`, plus `my-s3` for the cold tier.

## MyJetTools dependencies (important)

Nearly every dependency is a MyJetTools (`MyJetTools/*`) git crate pinned by tag in [Cargo.toml](Cargo.toml):
`my-service-bus-sdk`, `my-s3`, `my-http-server`, `my-grpc-extensions`, `rust-extensions`, `my-logger`. These evolve constantly — **never rely on memory for their API signatures.**
Per the global rules, consult the `development-best-practices` MCP resources first (e.g. `my-grpc-extensions`,
`rust-extensions`, `my-postgres`, `performance-considerations`) before writing code against them.

## Architecture

`main.rs` builds a single `Arc<AppContext>` (shared, read-mostly state) and wires up:
- gRPC server on `:7124` (+ optional Unix socket from `listen_unix_socket`). Every request carries an optional
  `Namespace`; empty means `default`.
- HTTP server on `:7123` (health/status, reads, Prometheus, static UI from `./wwwroot`).
- `operations::migrate_legacy_layout` — runs in `main.rs` **before** `AppContext::new`, since it converts the
  snapshot the context then reads.
- `data_initializer::init` — restores the open tail of every topic that has a folder on disk (the disk, not the
  snapshot: a topic that never made it into a saved snapshot would otherwise be skipped). Until it finishes,
  `app.app_states` is *un-initialized* and gRPC/HTTP reject requests via `contracts::check_flags`.
- Background timers (`rust_extensions::MyTimer`): 3 s (topics-snapshot saver, min-index saver),
  1 s (pages GC, metrics updater), 60 s (cold-storage uploader, a no-op without an `s3` section).
  The 30 s `DeletedTopicsGc` timer is disabled (see TODO below).
- Graceful shutdown via `before_shut_down::execute_before_shutdown` (flush yearly index, archive in-flight
  sub-pages, persist topics snapshot).

### Module map (`src/`)
- `app/` — `AppContext` (global state, storage entry points), `storage_layout` (the single place mapping
  `(namespace, topic_id)` onto a path / S3 key), Prometheus metrics.
- `file_storage/` — random-access file (`read`/`write` at offset, `append`, `read_all`/`write_all`). Reads past EOF
  come back zero-filled, which is what the offset-addressed TOC and year index rely on.
- `cold_storage/` — thin wrapper over `my-s3`: upload, ranged download, exists, delete. One bucket
  (`Bucket=x` in `s3_conn_string`, required) for every namespace, keys under
  `/x/my-sb-persistence/{ns}/{topic}/{file}` — the fixed root is `KEY_ROOT` in `cold_storage.rs`,
  the rest is `storage_layout::get_relative_path`.
  The bucket is settled on first touch, best-effort - `check_if_bucket_exists`
  first, and only what is missing is created: a failure is logged and stepped over (never
  fatal - a key scoped to one bucket is often denied `CreateBucket` yet can use it), retried later
  only if the error was transient; uploads are streamed in 512 KB chunks, so
  memory does not depend on the size of the archive. `Debug=1` in `s3_conn_string` turns on
  per-request tracing to stdout (`S3Client::debug_to_console`).
- `topic_key/` — `Namespace` (validated `[a-z0-9-]`, 1..=63, no leading `-`), `TopicKey` / `TopicKeyRef`.
- `grpc/` — tonic service impl (`persistence_grpc_service.rs`), server bootstrap, request/response mappers and contracts.
- `http/` — `my-http-server` controllers (api/home/logs/prometheus/read/topic), builder, start_up.
- `topics_snapshot/` — the topics+queues snapshot. Flat in memory (`TopicSnapshotProtobufModel`), one readable
  YAML file per namespace on disk at `{namespace}/topics-and-queue.yaml`, written atomically via rename. Flat in
  memory because `GetQueueSnapshot` is deliberately one stream for every namespace. Source of truth for which
  topics exist. The pre-YAML protobuf blob (`topicsdata`, V3/V2/V1) is converted once by the migration.
- `topic_data/` — in-memory per-topic state. `TopicsDataList` is the registry; `TopicData` holds `PagesList`,
  yearly index, archive list, metrics.
- `message_pages/` — message storage in **sub-pages** (`SubPageId` derived from `MessageId`). `SubPage` is an enum:
  `Active` (mutable, in-memory under a `parking_lot::Mutex`), `FromArchive` (read-only restored), `Missing`.
- `archive_storage/` — rotated archive files per topic, indexed by `ArchiveFileNo` with a TOC at the head. A sub page
  is written once, already closed: data is appended first, the TOC entry second — the TOC write is the commit point.
  A file that has been uploaded is frozen and refuses writes.
- `index_by_minute/` — per-topic per-year minute index that powers time-range reads (`ListFromDate`).
- `operations/` — the business-logic layer (read/write/GC/init/shutdown flows). gRPC and HTTP handlers should
  delegate here rather than touching storage structures directly.
- `timers/` — the registered `MyTimer` jobs, including `cold_storage_uploader` (60 s).
- `settings.rs` — `SettingsModel` (YAML at `$HOME/.myservicebus-persistence`, read once at startup): one
  `data_folder` plus an optional `s3` section.

### Storage layout

```text
{data_folder}/
    .layout-version
    {namespace}/
        topics-and-queue.yaml
        {topic}/
            {:019}.archive      .{year}.yearindex      active
```

Locally the namespace is a folder; in the cold tier it is a key segment inside the one bucket
(`/{bucket}/my-sb-persistence/{namespace}/...`), never a bucket of its own. `default` is not special — it has its own folder, and pre-namespace
data sitting at the root is renamed into `default/` on first start (`operations::migrate_legacy_layout`), guarded by
`.layout-version` — its *absence* is the marker, because before namespaces every folder at the root was a topic.
A second marker, `.layout-migrating`, makes an interrupted run resumable: without it a resumed migration can not
tell a topic named `default` from the namespace folder a previous run created.

Mutable files (highest-numbered archive, newest year index, `active`) live locally; everything below them is sealed
and uploaded. The uploader keeps the highest-numbered file per topic and sends up **all** the rest, so a backlog
drains in one tick. No state is persisted — "highest on disk is current" holds by itself.

### Concurrency conventions

The coding/concurrency conventions are **not duplicated here** — they live in the `development-best-practices`
MCP (`performance-considerations`, `application-architecture-best-practices`, `rust-extensions`). Read those
before writing code; the global rules require it. This codebase follows them: e.g. `TopicsDataList` uses
`ArcSwap` + copy-on-write, and storage structs lock with `parking_lot` while handing out `Arc` snapshots.

## TODO / disabled features (read before touching delete/restore)

The **soft-delete topic + GC** flow is intentionally disabled and partially commented out across the codebase
(grep `// TODO:`). The on-disk snapshot format keeps `deleted_topics` for compatibility. The gRPC
`DeleteTopic`/`RestoreTopic` were `unimplemented`; note `HardDeleteTopic` gRPC has since been added (see recent
commits). `GetHistoryByDate` returns an empty stream. Before reworking any of this, read [TODO.md](TODO.md) —
it documents the intended two-stage design and the specific hazards to avoid (don't `panic!` on deleted-topic
access; only drop the soft-delete record after storage delete succeeds).

## Release

Two CI paths exist: `.github/workflows/` (GitHub Actions) and `.gitlab-ci.yml`. Both build the release binary and
publish a Docker image tagged from the `Cargo.toml` version (`amigin/my-sb-persistence:<version>`). The Dockerfile
copies the prebuilt `target/release/my-sb-persistence` and `./wwwroot` — build the binary before building the image.
See the `release-guide` MCP resource for the standard release/deploy flow.
