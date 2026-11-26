# Architecture

## Goals

1. An acknowledged change is never lost by a process crash, and, with `fsync`
   enabled, is on stable storage before the caller hears about it.
2. Queue behaviour is testable without disks, sockets or clocks.
3. Nothing needs to be installed besides the binary.

## Events, not commands

`leaseq-core` splits every change in two:

```text
decide_*(&State, input, now) -> Event      (reads only; may reject)
State::apply(&mut State, &Event)           (total; no clock, no configuration)
```

The log stores events. Anything that depends on time or settings, such as a retry
time or a lease expiry, is written into the event, so replay reproduces history
exactly even if the configuration has changed since.

Whether a pending job is *ready* or *delayed* depends on the current time, so it
is an index rather than an event: `State::promote(now)` maintains it.

Every state change removes the job from its indexes and inserts it again through
one pair of functions, so counters and schedules cannot drift apart.

## Writer loop and group commit

One thread owns the state and the log. Handles send it boxed closures:

```text
loop:
    wait for a request, or for the next deadline
    take everything else already queued (up to a batch limit)
    expire lapsed leases, promote due jobs
    run each closure          -> events appended to an in-memory buffer
    write the buffer, fsync   -> once for the whole batch
    publish counters, send the deferred replies, wake long polls
```

Replies are closures returned by each request and invoked only after the commit,
with its result. Reads go through the same path, so no caller can observe a
change that might still be lost.

The thread runs its own single-threaded async runtime, which lets it block on
disk I/O without stalling the application's runtime.

State is updated before the commit. If the commit fails, memory and disk may
disagree, so the engine **fails stop**: every later request is refused until the
directory is reopened. A failed `fsync` is never retried, because the kernel may
already have discarded the dirty pages.

## Log format and recovery

```text
frame = len:u32le | crc32(len || payload):u32le | payload
```

On open, frames are replayed in order. An incomplete frame, a bad checksum on the
last frame, or a zero-filled tail is an interrupted write and is truncated. A bad
frame followed by other data is reported as corruption and the file is left
untouched.

Payloads are JSON events. This costs some space compared with a binary encoding
and makes a damaged or surprising log easy to inspect.

## Compaction

```text
data/
  LOCK            exclusive while running
  snapshot.bin    state at the start of generation g
  wal-g.log       events since then
```

When the log passes its size limit:

1. create and sync an empty `wal-(g+1).log`;
2. write `snapshot.tmp`, sync it, rename it over `snapshot.bin` — the commit point;
3. delete `wal-g.log`.

| Crash after | Found on reopen | Result |
|---|---|---|
| step 1 | snapshot `g`, empty newer log | newer log removed, continue on `g` |
| step 2 | snapshot `g+1`, old log | old log removed, continue on `g+1` |
| step 3 | clean | continue on `g+1` |

A missing current log, or a non-empty log newer than the snapshot, means files
were removed or mixed by hand; opening refuses rather than guessing.

## Leases and fencing

A lease grants a job to one worker until an expiry time and carries a token that
only increases. `ack`, `nack` and `heartbeat` require the current token and an
unexpired lease, so a worker that stalled past its lease cannot overwrite the
outcome of whoever holds the job now.

Delivery is at least once. A worker can finish its side effect and crash before
acknowledging, so handlers should be idempotent.

## HTTP security

The server has no authentication and binds to loopback by default. A web page in
the user's browser can still send requests to loopback, so:

- the `Host` header must be a loopback name, which defeats DNS rebinding;
- mutations require `Content-Type: application/json`, which forces a CORS
  preflight that is never granted;
- a present `Origin` must match `Host`;
- responses carry a restrictive Content Security Policy, and the dashboard
  renders job data only as text.

Local processes and other users on the same machine can reach the port; that is
outside this model.
