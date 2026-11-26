# leaseq

A durable job queue in Rust: a write-ahead log, a deterministic state machine, a
single-writer engine with group commit, an HTTP API with a live dashboard, and a
worker library. One binary, no external services, no API keys.

## What is it for?

Some work should not keep a user waiting: sending a welcome e-mail, resizing an
upload, building a report. With a job queue the application records the task,
answers immediately, and separate worker processes carry it out.

```text
your app --enqueue--> leaseq --lease--> worker
                        ^                  |
                        +--- ack / nack ---+
```

- If a worker crashes, its lease expires and the job goes to another worker.
- A failed attempt is retried later, with a growing delay.
- A job that keeps failing is set aside for a person to inspect and retry.
- If the server stops, accepted jobs are still there when it starts again.

It fills the same role as tools such as Sidekiq or Amazon SQS at a much smaller
scale: a single machine and no dependencies. See [Limitations](#limitations).

## Highlights

- **Crash safe.** Every change is checksummed and synced before it is
  acknowledged. Torn writes are repaired; real corruption is refused loudly.
- **Fast where it matters.** Concurrent requests share each `fsync`
  (group commit).
- **Lease based, at-least-once delivery** with fencing tokens, heartbeats,
  capped exponential backoff, priorities, delayed jobs and a dead-letter state.
- **Deterministic core.** Queue rules live in a crate with no I/O and no clock,
  so they can be tested with randomised operation sequences.
- **Safe by default.** Loopback only, with defences against DNS rebinding and
  cross-site requests. `unsafe` code is forbidden across the workspace.

## Quick start

Requires a recent stable Rust toolchain (developed with 1.100).

```sh
cargo run --release -- serve
```

Open <http://127.0.0.1:7878>, then in other terminals:

```sh
cargo run --release -- enqueue mail '{"to":"someone"}' --priority 7
cargo run --release -- work mail --concurrency 8
cargo run --release -- stats
```

`work` runs a demonstration worker. A payload containing `"fail": true` fails
until it is dead-lettered, after which it can be retried from the dashboard or
with `leaseq retry <id>`.

(In PowerShell, quote JSON as `'{\"to\":\"someone\"}'` if your version strips
the inner quotes.)

## Writing a worker

Inside an async function:

```rust
use leaseq_client::{Client, Delivery, Worker, WorkerConfig};
use tokio_util::sync::CancellationToken;

let client = Client::new("http://127.0.0.1:7878")?;
let shutdown = CancellationToken::new(); // cancel it to stop the worker

Worker::new(client, WorkerConfig::new("mail".parse()?), |job: Delivery| async move {
    println!("attempt {} of job {}: {}", job.attempt, job.id, job.payload.get());
    Ok(()) // or Err("reason".to_owned()) to retry later
})
.run(shutdown)
.await;
```

The worker renews leases while a handler runs, drops the handler if its lease is
lost, and finishes jobs in progress before `run` returns.

## HTTP API

All request bodies are JSON. Errors look like
`{"error":{"code":"lease_lost","message":"..."}}`.

| Method and path | Body | Success |
|---|---|---|
| `POST /api/queues/{queue}/jobs` | `payload`, `priority?`, `delay_ms?`, `max_attempts?` | `201 {"id"}` |
| `POST /api/queues/{queue}/lease` | `max?`, `lease_ms?`, `wait_ms?` | `200 {"jobs":[...]}` |
| `POST /api/jobs/{id}/ack` | `token` | `204` |
| `POST /api/jobs/{id}/nack` | `token`, `error?` | `204` |
| `POST /api/jobs/{id}/heartbeat` | `token`, `lease_ms` | `204` |
| `POST /api/jobs/{id}/retry` | `{}` | `204` |
| `GET /api/jobs?queue=&state=&limit=` | | `200 {"jobs":[...]}` |
| `GET /api/stats` | | `200 {"queues":[...]}` |
| `GET /api/events` | | server-sent `stats` events |
| `GET /healthz` | | `200 ok` |

Queue names are 1 to 64 characters from `A-Z a-z 0-9 . _ -`.

## Architecture

```text
 producers / workers / browser
              |
       leaseq-server          HTTP, SSE, dashboard, request guard
              |  Engine handle (cloneable, async)
       leaseq-engine          one writer thread: batch -> apply -> fsync -> reply
          |         |
   leaseq-core   leaseq-wal   rules as pure state machine / framed log + snapshots
```

| Crate | Responsibility |
|---|---|
| `leaseq-wal` | Checksummed frames, torn-write recovery, atomic snapshots |
| `leaseq-core` | State machine: commands produce events, events change state |
| `leaseq-engine` | Durability, group commit, compaction, long polling |
| `leaseq-proto` | Wire types shared by server and client |
| `leaseq-server` | HTTP API and dashboard |
| `leaseq-client` | Typed client and worker runtime |
| `leaseq` (`leaseq-cli`) | Command line and benchmark |

Design decisions and failure handling are described in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Performance

`leaseq bench` drives the engine in process (no HTTP). Measured on an 8-core
laptop CPU with an NVMe SSD, Windows 11, release build; two runs agreed closely
except where a range is shown.

| Durability | Concurrency | Enqueue | Lease + ack |
|---|---|---|---|
| `fsync` per commit | 1 | ~1,040 jobs/s | ~520 jobs/s |
| `fsync` per commit | 64 | ~32,600 jobs/s | ~16,600 jobs/s |
| no `fsync` | 64 | ~410,000-490,000 jobs/s | ~310,000 jobs/s |

The step from 1 to 64 producers with `fsync` on (about 31x) is group commit.
Results depend heavily on the storage device.

```sh
cargo run --release -- bench --jobs 20000
```

## Testing

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The tests include:

- property tests that cut the log at arbitrary byte offsets and check that
  exactly the complete records come back;
- property tests over random operation sequences checking that replaying the log
  and restoring a snapshot both reproduce the live state, and that no job is lost
  or duplicated;
- concurrent producer/consumer tests against a real data directory;
- HTTP tests over TCP, including forged `Host`, cross-origin and non-JSON
  requests, and shutdown with open streams.

## Limitations

- **No authentication or TLS.** Keep it on loopback, or put an authenticating
  proxy in front before using `--allow-remote`.
- **Single node.** No replication.
- **State lives in memory** and is bounded by RAM. Compaction serialises the
  whole state on the writer thread, pausing requests while it does.
- **At-least-once delivery.** Handlers should be idempotent.
- Recovery treats damage anywhere before the final record as corruption. After a
  power failure that persisted an unacknowledged batch out of order, this can
  require manual repair; it never silently drops acknowledged data.
- On Windows, directory entries cannot be explicitly synced.
- Listing jobs scans every live job.

## License

[MIT](LICENSE)
