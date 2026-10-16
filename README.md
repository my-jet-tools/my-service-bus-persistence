# my-service-bus-persistence

Companion persistence node for [`my-service-bus`](https://github.com/MyJetTools/my-service-bus).
The main node calls this service over gRPC for durable storage of
topics, queues and messages, written straight to files, with S3 as an
optional cold tier for sealed data.

Topics are addressed by a `(namespace, topic_id)` pair: a topic name is
unique only within its namespace, so `orders` in `default` and `orders`
in `alpha` are two different topics with independent messages, queues
and cursors. A missing or empty namespace always means `default`, which
is what a node that knows nothing about namespaces sends.

## Run

### 1. Install Rust

https://www.rust-lang.org/tools/install

### 2. Make sure `HOME` is set

The service reads its configuration file from
`$HOME/.myservicebus-persistence` on Linux/macOS and
`%HOME%\.myservicebus-persistence` on Windows. It is resolved at run time from
`$HOME`, so the variable must be present in the environment of whoever
runs the binary.

### 3. Create the config file

`$HOME/.myservicebus-persistence` is a YAML file. Minimal example:

```yaml
data: "~/my-sb-persistence-data"
max_response_records_amount: 500
delete_topic_secret_key: "some-shared-secret"

# Optional, omit if you don't want a Unix-socket gRPC endpoint:
# listen_unix_socket: "/tmp/my-sb-persistence.sock"

# Optional cold tier. Omit it and everything stays on the local disk forever.
# Everything goes into one bucket: /{Bucket}/my-sb-persistence/{namespace}/{topic}/{file}
# s3_conn_string: "Endpoint=https://fsn1.your-objectstorage.com;Region=fsn1;AccessKey=...;SecretKey=...;Bucket=sb-data"

# Only for the first start after upgrading from the three-folder layout. Remove it afterwards.
# legacy:
#   topics: "/home/runners/Topics"
#   messages: "/home/runners/Messages"
#   archive: "/home/runners/Archive"
```

`data` is all a local-only deployment needs — no external storage at
all.

### `s3_conn_string` format

One string of `Key=Value` pairs separated by `;`, the same shape the
Azure connection strings used:

```
Endpoint=https://s3.eu-central-1.amazonaws.com;Region=eu-central-1;AccessKey=AKIA...;SecretKey=...;Bucket=my-sb-persistence
```

| Key         | Meaning                                                        |
| ----------- | -------------------------------------------------------------- |
| `Endpoint`  | S3 endpoint URL, including the scheme.                          |
| `Region`    | Region used for the SigV4 signature.                            |
| `AccessKey` | Access key id.                                                  |
| `SecretKey` | Secret access key. Only the **first** `=` separates, so a base64 secret with its own `=` is fine. |
| `Bucket`    | The one bucket every namespace goes into — see below.           |
| `Debug`     | Optional. `Debug=1` traces every S3 request to the console — see below. Off when absent. |

`Endpoint`, `Region`, `AccessKey`, `SecretKey` and `Bucket` are all
required. A missing or misspelled key fails at startup rather than
silently disabling the cold tier.

### `Debug=1` — tracing the S3 traffic

Add `;Debug=1` to the connection string and restart, and every request
the cold tier makes is printed to stdout, so `docker logs` shows it.
There is no rebuild and no separate switch — it is a normal part of the
connection string, which is what makes it usable on a running
deployment.

`1`, `true`, `yes` and `on` all mean on (`0` / `false` / `no` / `off`
mean off, and any other value refuses to start rather than quietly
reading as off).

What gets printed is deliberately asymmetric:

- **The request** — verb, url and the size of the body. It is printed
  *before* the answer arrives, so a request that never got one — a
  timeout, a refused connection — still shows up. That is the case a
  trace is most needed for.
- **A failed answer** — the body in full, because that body is the
  `<Error><Code>` that says why. `AccessDenied`, `SignatureDoesNotMatch`
  and `NoSuchBucket` are indistinguishable without it.
- **A successful answer** — its size only. It is the archive that was
  just downloaded, and nobody wants a few hundred megabytes on the
  console.

The `Authorization` header is never printed: it carries the access key
id and the request signature. The connection string itself still holds
the secret key, so the usual care applies to the config file — but a log
gathered with `Debug=1` on is safe to hand around.

Leave it off in normal operation: an upload of a sealed archive is a
rare burst, but a cold read is one request per sub page, and those add
up in a log.

### Cold tier layout

One bucket, named by `Bucket`, holds every namespace. Inside it the key
is the local layout under a fixed `my-sb-persistence/` root, so the
bucket can be shared with other services:

```text
/{bucket}/my-sb-persistence/{namespace}/{topic}/{file}
```

A bucket name is unique across *every customer of the provider* — AWS
partition-wide, and on Hetzner "unique amongst all Hetzner Object
Storage users and across all locations" — so pick a distinctive one.
Pick it once and keep it in the config: changing it later means moving
every object.

Name rules, checked before the bucket is touched: 3–63 characters,
lowercase letters, digits and hyphens, first and last character
alphanumeric.

The bucket is settled once per process, at startup, so a wrong endpoint,
region or key pair shows up in the log straight away rather than only
at the first upload hours later.

The service asks whether the bucket is *there* (`HEAD /{bucket}`) and
only creates it if it is not. This matters for a key scoped to that
single bucket: such a key is routinely allowed to use it while being
denied `CreateBucket`, and trying to create it first would log a
permission error on every start of a perfectly healthy deployment.

**Creating the bucket is best effort and never stops the service.** Not
being able to create a bucket says very little about being able to use
one: an access key scoped to a single bucket is routinely denied
`CreateBucket` while reading and writing inside that bucket perfectly
well, and a bucket made by hand ahead of time is the normal case in a
managed deployment. So a failure is written to the log and the work goes
on — if the bucket really is unusable, the upload or the read says so on
its own terms, naming the file it was working on. The cold tier holds
sealed data, and refusing to start over it would turn a storage problem
into an outage.

Creating a bucket that is **already yours** is not a failure at all — so
a hand-made bucket is fine, and so is every restart after the first. Two
failures are told apart by whether asking again could ever help:

- **Transient** (a 5xx, a timeout, a refused connection) — tried again
  on the next operation.
- **Deterministic** (permission denied, or `BucketAlreadyExists`, which
  means the name belongs to *another account* — bucket names are unique
  across every customer of the provider) — logged once and not tried
  again until a restart, because repeating it on every upload would turn
  one problem into a flood of requests.

Uploads are streamed from the file in 512 KB chunks, so memory does not
depend on the size of the object: an archive is hundreds of megabytes,
and reading one whole used to be an OOM kill in a 512 MB container.

| Field                          | Type             | Required | Description                                                                                                                                          |
| ------------------------------ | ---------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data`                         | `string`         | yes      | Root of every file the service owns. A leading `~` is expanded to `$HOME`.                                                                            |
| `max_response_records_amount`  | `usize`          | yes      | Upper bound on records returned per HTTP read response.                                                                                              |
| `delete_topic_secret_key`      | `string`         | yes      | Shared secret for the HTTP `DELETE /api/Topic` endpoint. Currently unused while soft-delete + GC is being reworked — see [TODO.md](TODO.md).         |
| `listen_unix_socket`           | `string` (opt.)  | no       | If set, gRPC additionally listens on this Unix socket path (in addition to TCP `:7124`). Useful for sidecar deployments.                             |
| `s3_conn_string`               | `string` (opt.)  | no       | Cold tier — see the format above. Omit it to keep every file local forever.                                                                           |
| `legacy`                       | `object` (opt.)  | no       | One-time migration from the three-folder layout: `topics`, `messages`, `archive`. Either the whole section is absent or all three are given — none of them is optional, so a half-filled section fails to parse instead of migrating half the data. |

Notes:
- The settings file is read once at startup; changing it requires a
  process restart.

## Network endpoints

| Port    | Protocol | Purpose                                                                |
| ------- | -------- | ---------------------------------------------------------------------- |
| `7123`  | HTTP     | Health, status, reads, Prometheus metrics, static UI / Swagger.        |
| `7124`  | gRPC     | Persistence API consumed by the `my-service-bus` main node.            |
| `listen_unix_socket` | gRPC | Optional Unix-socket variant of the gRPC API.                       |

### HTTP endpoints (port 7123)

- `GET /api/IsAlive` — liveness probe.
- `GET /api/Status` — runtime status (initialization flag, queue
  snapshot id, per-topic loaded pages, system memory).
- `GET /Read/ById?...` — fetch a single message by id (JSON, payload
  is base64-encoded).
- `GET /Read/ListFromDate?...` — fetch messages by time range
  (JSON, base64 payload). Backed by per-year minute index.
- `GET /metrics` — Prometheus exposition.
- Static UI under `/` is served from `./wwwroot`. Swagger is
  available for the registered controllers.
- HTTP `DELETE /api/Topic` and `GET /api/Topic` (deleted topics
  list) are temporarily disabled — see [TODO.md](TODO.md).

### gRPC endpoints (port 7124)

Defined by the `persistence.proto` contract (compiled into
`crate::persistence_grpc`). The service exposes:

- `GetVersion`, `Ping`.
- Queue snapshot stream get/save.
- Message / page / sub-page reads (compressed and plain variants).
- `SaveMessages` (client-streaming).
- `GetHistoryByDate` — currently returns an empty stream (TODO).
- `DeleteTopic` / `RestoreTopic` — currently return
  `Status::unimplemented` while soft-delete + GC is being reworked
  (see [TODO.md](TODO.md)).

`my-service-bus` main node is the canonical client; do not call this
service directly from application code.

## Lifecycle & timers

- On startup, if `legacy` is configured, only the **working set** is
  brought over before the service opens: the snapshot, the shared tail,
  and per topic the one archive and the one year index still being
  written. Everything else follows in a background task, one file at a
  time, so the live traffic keeps the disk to itself.
- Then any pre-namespace topic folder is moved into `default/` (once,
  see below) and the open tail of every topic is restored; gRPC
  requests respond with `Initializing` until that finishes.
- Background timers:
  - 3 s tick — topic-snapshot saver, min-index saver.
  - 1 s tick — page GC, metrics updater.
  - 60 s tick — cold-storage uploader (no-op without an `s3` section).
- Graceful shutdown runs `before_shut_down` to flush the yearly index,
  archive in-flight sub-pages, and persist the topics snapshot before
  the process exits.

## Storage layout

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

In the cold tier the namespace is a key segment inside the one bucket:
`/{bucket}/my-sb-persistence/{namespace}/{topic}/{file}` — see the
settings section above.

`default` is not special — it gets its own folder like any other
namespace. Data written before namespaces existed sits directly at the
root; the first start after upgrading moves those folders into
`default/`. The marker is `.layout-version`, and its *absence* is what
matters: before namespaces the concept did not exist, so every folder at
the root is a topic, with nothing to guess. A second marker,
`.layout-migrating`, makes an interrupted run resumable — without it a
resumed migration could not tell a topic named `default` from the
namespace folder a previous run had already created.

### Migrating from the three-folder layout

Older deployments kept three roots (`topics`, `messages`, `archive`).
Point `legacy` at them for one start and the service folds them into
`data`:

```text
{topics}/topics/topicsdata          -> {data}/topicsdata  (then converted to YAML)
{topics}/topics/.active-pages       -> {data}/.active-pages
{messages}/{topic}/.{year}.yearindex\
{archive}/{topic}/{:019}.archive    -> {data}/default/{topic}/
```

The same topic existed as **two** folders — one under `messages`, one
under `archive` — so files are moved individually rather than by
renaming a folder onto another.

Files are moved, not copied: each one is written to its new home and
only then removed from the legacy folder. What is still in the legacy
folder is therefore exactly what has not been migrated yet, which is
what makes an interrupted run resumable with no bookkeeping. A file is
copied to a `.migrating` name next to its destination and renamed into
place, so a crash mid-copy can never leave a truncated archive under a
real name.

With `s3_conn_string` set, a sealed file goes from the legacy folder
**straight to S3** and never lands on the new local disk at all.

The snapshot is one YAML file per namespace, so a namespace folder is
self-contained — copy `alpha/` somewhere else and its snapshot travels
with it. In memory it stays flat, because `GetQueueSnapshot` is
deliberately one stream for every namespace at once: that is how the
node restores everything in one call and learns which namespaces exist.
It is written atomically (write next to it, rename), since it is the
source of truth for which topics exist.

### Hot and cold

A file is either **mutable** (local disk) or **sealed** (uploaded, then
read over ranged GETs and never changed again). S3 objects cannot be
appended to or partially overwritten, so only sealed files ever go
there:

- mutable: the highest-numbered `.archive`, the newest `.yearindex`,
  and `active`;
- sealed: everything below those.

The uploader timer applies exactly that rule per topic: whichever
archive carries the highest number is the one being written, so it
stays, and **every** other one goes up — not just the one below it,
since a backlog of two or three is normal after the cold tier was
unreachable or after a restart. Years follow the same rule. Nothing is
persisted to track it: "the highest number on disk is the current one"
stays true by itself across rollovers and restarts.

Reads look local first and fall back to the cold tier. A cold archive is
never downloaded whole — the TOC is fetched once (it can be cached
forever, the object is immutable) and each sub page is a single ranged
GET. A cold year index is the exception: it is 4 MB and addressed by
offset, so it is pulled back to the local disk in full, which also makes
a late write for a closed year work without any special case.

## Development

- `cargo check` — fast feedback loop.
- `cargo run --release` — local run with the YAML config from
  `$HOME`.
- The repo expects a Tokio multi-threaded runtime (default
  `#[tokio::main]`) and uses jemalloc as the global allocator on Linux;
  other platforms use the system allocator.

## Project conventions

- Performance / concurrency rules: see
  [performance-considerations.md](performance-considerations.md)
  (ArcSwap for read-mostly state, `parking_lot` over `tokio::sync`
  whenever there is no `.await` under the guard, AHash instead of
  `std::HashMap`/`HashSet`, no heavy CPU work under a Mutex, etc.).
- Outstanding work: [TODO.md](TODO.md).
