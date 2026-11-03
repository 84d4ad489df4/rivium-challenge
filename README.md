# LinkSpool

**A durable TCP message transport for unreliable networks.**

LinkSpool accepts messages from local producers, persists them in SQLite, and delivers them to a remote TCP receiver. Producers can submit while the receiver is offline; accepted messages survive process restarts and are retried with backoff until the receiver acknowledges durable storage.

The same Rust core runs as a desktop daemon and inside an Android app. Android submits through the system **Share Sheet**, and a foreground service keeps delivery running after the UI is closed.

> Persist locally. Deliver reliably. Recover automatically.

## What it demonstrates

This is a submission for the [Rivium + Apalis challenge](https://github.com/apalis-dev/rivium-challenge). It connects all three required pieces:

- **Rivium** supervises the HTTP API, delivery worker, maintenance sweep, inbox and status services on desktop and Android.
- **Apalis** owns durable execution of delivery jobs in SQLite. One job type is enough: deliver message `M`, attempt `N`.
- **Android Share Sheet** is the producer feature. Shared text or a URL is written to a durable inbox and delivered by the embedded host.

The desktop build needs no phone. The Android build uses the same core through JNI.

## Features

- Desktop loopback HTTP API (Axum) with submit, list, fetch, retry and discard.
- Android Share Sheet target for `text/plain`.
- SQLite message store as the single authoritative record.
- Producer-scoped idempotency keys.
- Bounded admission by message count, queued bytes and payload size.
- Message expiry.
- Exponential retry with a configurable attempt limit.
- Dead-letter inspection, explicit retry and discard.
- Durable Apalis jobs written in the same transaction as the message.
- Raw TCP transport with four-byte big-endian length-prefixed JSON frames.
- Explicit receiver acknowledgements (`accepted`, `duplicate`, `rejected`).
- `linkspool-sink` companion receiver with SQLite deduplication and `--drop-ack`.
- Stale-delivery recovery after a worker or process disappears.
- Periodic status snapshot for the Android UI.
- Foreground service so delivery continues while the UI is closed.

## Versions

| Component | Version |
|---|---|
| Rust | 1.94.0 (pinned by `rust-toolchain.toml`) |
| Rivium / `rivium-host` / `rivium-jni` | 0.2.0 |
| Apalis / `apalis-sqlite` | 1.0.0 |
| Axum | 0.8.9 |
| SQLx | 0.9 |
| Tokio | 1.54 |
| Android Gradle Plugin | 8.9.1 |
| Kotlin | 2.1.20 |
| Gradle | 8.13 |
| Android compile/target SDK | 35 |
| Android min SDK | 26 |
| Android NDK | 27.x |

## Architecture

```mermaid
flowchart TB
    subgraph Producers
        HTTP["Desktop HTTP API"]
        SHARE["Android Share Sheet"]
        INBOX["Durable inbox file"]
    end

    subgraph Core["Shared Rust core"]
        SUBMIT["Validate + idempotency + admission"]
        DB[("SQLite messages")]
        JOBS[("Apalis Jobs")]
        WORKER["Delivery worker"]
        TCP["TCP transport"]
        MAINT["Expiry + recovery + purge"]
    end

    HTTP --> SUBMIT
    SHARE --> INBOX --> SUBMIT
    SUBMIT --> DB
    SUBMIT --> JOBS
    JOBS --> WORKER
    WORKER --> TCP
    TCP --> SINK["linkspool-sink"]
    SINK --> ACK["Acknowledgement"]
    ACK --> WORKER
    WORKER --> DB
    MAINT --> DB
```

Message ingestion and job creation commit in one SQLite transaction. A job contains only a message identifier and an attempt number; the payload stays in the message row. The worker re-reads the authoritative state before transmitting, so duplicate or stale jobs are safe.

## Repository layout

```text
linkspool/
├── Cargo.toml                  workspace
├── Cargo.lock
├── rust-toolchain.toml         Rust 1.94.0
├── linkspool.toml              desktop configuration
├── build.sh                    Android APK build
├── migrations/0001_init.sql    message table
├── src/
│   ├── main.rs                 desktop entry point
│   ├── lib.rs
│   ├── app.rs                  Rivium App composition
│   ├── api.rs                  Axum router and error mapping
│   ├── delivery.rs             Apalis worker service
│   ├── inbox.rs                file-drop ingestion
│   ├── maintenance.rs          expiry, recovery, purge
│   └── status.rs               JSON snapshot for Android
├── tests/
│   ├── api.rs                  HTTP contract tests
│   └── end_to_end.rs           core + sink over real TCP
├── crates/
│   ├── linkspool-core/
│   │   ├── src/{config,core,error,jobs,message,persistence,transport}.rs
│   │   └── tests/lifecycle.rs  ingestion, admission, retry, recovery
│   ├── linkspool-protocol/     frames, envelopes, acknowledgements
│   ├── linkspool-sink/         companion receiver
│   └── linkspool-jni/          five-method Android bridge
└── android/
    ├── settings.gradle.kts
    ├── build.gradle.kts
    └── app/src/main/
        ├── AndroidManifest.xml
        ├── java/dev/linkspool/app/
        │   ├── NativeHost.java
        │   ├── HostController.kt
        │   ├── HostService.kt
        │   ├── Inbox.kt
        │   ├── MainActivity.kt
        │   ├── Settings.kt
        │   └── ShareActivity.kt
        └── res/
```

## Desktop quick start

Prerequisites: Rust 1.94.0 via `rustup`, Cargo, and a C toolchain for the bundled SQLite build.

Terminal 1 — start the receiver:

```bash
cd linkspool
cargo run -p linkspool-sink -- --database /tmp/linkspool-sink.db
```

Terminal 2 — start LinkSpool:

```bash
cd linkspool
cargo run
```

`cargo run` defaults `--root` to the current directory, so `linkspool/linkspool.toml`, the database, inbox, status file and logs all live under `linkspool/`.

Submit a message:

```bash
curl -sS -X POST http://127.0.0.1:8080/v1/messages \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotency_key": "event-001",
    "payload": { "event": "device_status", "status": "online" }
  }'
```

The response is `201 Created` only after the message and its first Apalis job are durable. Repeating the same producer and key returns `200 OK` with the existing message and `"duplicate": true`.

Inspect state:

```bash
curl -sS 'http://127.0.0.1:8080/v1/messages?limit=5'
curl -sS 'http://127.0.0.1:8080/v1/messages?state=dead_lettered'
```

Other endpoints:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Liveness |
| `POST` | `/v1/messages` | Submit a message |
| `GET` | `/v1/messages` | List messages (`state`, `limit`) |
| `GET` | `/v1/messages/{id}` | Fetch one message |
| `POST` | `/v1/messages/{id}/retry` | Retry a dead-lettered message |
| `DELETE` | `/v1/messages/{id}` | Discard a dead-lettered or expired message |

The API binds to `127.0.0.1:8080` by default. It is disabled in the Android profile.

### Desktop configuration

`linkspool/linkspool.toml` contains the defaults. Relative paths resolve against `--root`.

```toml
[api]
enabled = true
listen = "127.0.0.1:8080"

[database]
path = "linkspool.db"

[queue]
destination = "local-sink"
max_payload_bytes = 262144
max_queued_bytes = 67108864
max_queued_messages = 10000
max_attempts = 8
retry_base_ms = 1000
retry_cap_ms = 60000

[transport]
sink_addr = "127.0.0.1:9000"
connect_timeout_ms = 3000
ack_timeout_ms = 5000
max_frame_bytes = 1048576

[maintenance]
interval_ms = 30000
terminal_retention_hours = 168
stale_delivery_ms = 300000
```

Runtime data:

- Database: `linkspool/linkspool.db`
- Logs: `linkspool/logs/linkspool/linkspool.log`
- Inbox (Android profile): `linkspool/inbox/`
- Status snapshot (Android profile): `linkspool/status.json`

## Android

### Build the APK

```bash
bash linkspool/build.sh
```

The script:

1. Finds the Android SDK and NDK (or uses `ANDROID_HOME` / `ANDROID_NDK_HOME`).
2. Installs the `aarch64-linux-android` and `x86_64-linux-android` Rust targets when missing.
3. Cross-compiles `linkspool-jni` with the NDK clang linker and the bundled SQLite amalgamation.
4. Copies `liblinkspool_jni.so` and `libc++_shared.so` into `jniLibs/`.
5. Downloads a pinned Gradle 8.13 distribution if needed.
6. Assembles the debug APK and writes `linkspool/debug.apk`.

Install it:

```bash
adb install -r linkspool/debug.apk
```

### Run it

Start the sink on the laptop:

```bash
cd linkspool
cargo run -p linkspool-sink -- \
  --listen 0.0.0.0:9000 \
  --database /tmp/linkspool-sink.db
```

- **Emulator:** the app defaults to `10.0.2.2:9000`, which reaches the host loopback.
- **Real phone:** enter the laptop's LAN address (for example `192.168.1.20:9000`) in the app and press **Start**. Ensure the sink listens on `0.0.0.0` and the firewall allows port 9000.

### Share Sheet demo

1. Open Chrome (or any app with selectable text).
2. Select a link or paragraph and choose **Share**.
3. Pick **LinkSpool**.
4. LinkSpool writes the text to a durable inbox file, starts the foreground host, and shows `Queued for delivery`.
5. Open the LinkSpool app to see queued, delivered and dead-lettered counts.
6. Close the UI. The foreground service keeps the host running; the notification shows that delivery is active.
7. Reopen the app: counts come from `status.json`, and unfinished messages resume from SQLite.

You can also type or paste text into the compose field and press **Queue message**.

### Android storage

All state is application-private:

- `filesDir/linkspool.db` — authoritative messages and Apalis jobs
- `filesDir/inbox/` — durable share hand-off
- `filesDir/status.json` — counts for the UI
- `filesDir/logs/linkspool/linkspool.log` — host log

Inspect with:

```bash
adb shell run-as dev.linkspool.app ls -R files
adb shell run-as dev.linkspool.app cat files/status.json
```

### Why a file inbox instead of a second JNI method?

`rivium-jni` exposes a fixed five-method ABI and the workspace forbids `unsafe_code`. Adding a bespoke native submission method would mean hand-written JNI declarations outside that ABI. Instead, Android writes a complete file to `filesDir/inbox/` using a temporary name and an atomic rename, then starts the embedded host. The inbox service ingests the file and deletes it only after the message is durable. The file stem is the idempotency key, so a crash between ingestion and deletion produces the same message rather than a duplicate.

The embedded arguments disable the HTTP API:

```text
--root <filesDir>
--set api.enabled=false
--set inbox.enabled=true
--set status.enabled=true
--set transport.sink_addr=<host:port>
```

## Reliability semantics

### Persist before acknowledging

`Core::submit` validates the payload, idempotency key, destination and expiry, then opens `BEGIN IMMEDIATE`. It inserts the message and calls `apalis_sqlite::queries::push_tasks` for the first delivery job in the same transaction. The producer receives `201` only after commit.

### Idempotency

A unique partial index on `(producer, idempotency_key)` scopes keys to the producer. A repeat returns the existing message with `duplicate: true`; it does not create another row or another job.

### Bounded admission

A submission is rejected with `QueueFull` when it would exceed `max_queued_messages` or `max_queued_bytes`. Capacity is occupied by `pending`, `delivering`, `retry_scheduled` and `dead_lettered`. Delivered, expired and discarded rows release capacity. Dead letters are retained for inspection for `terminal_retention_hours` and then purged by maintenance.

### Expiry

`expires_at` must be in the future at submission. Maintenance moves due messages to `expired`; a worker that claims a message just past its deadline expires it instead of transmitting.

### Retry and dead-lettering

A transient transport failure schedules attempt `N + 1` with exponential backoff:

```text
delay = min(retry_base_ms * 2^(attempt - 1), retry_cap_ms)
```

The retry state update and the next Apalis job commit together. When `attempts >= max_attempts`, or the receiver returns `rejected`, the message becomes `dead_lettered`. `POST /v1/messages/{id}/retry` re-enqueues it with an extended budget; `DELETE` discards it.

### Recovery

A claim increments `attempts` and sets `delivering` before transmission. If the worker or process disappears, maintenance finds `delivering` rows older than `stale_delivery_ms`, rewinds the attempt, sets `retry_scheduled`, and pushes a recovery job. This keeps the message row and the Apalis job consistent.

The delivery worker uses a unique per-process Apalis worker name (`linkspool-delivery-<pid>`) so a quick restart cannot collide with the previous worker registration. The queue polls every 500 ms.

### At-least-once delivery

LinkSpool does not claim exactly-once delivery. If the receiver persists a message but the acknowledgement is lost, LinkSpool retries. The sink deduplicates by `message_id`, increments `duplicate_count`, and acknowledges the duplicate without repeating its application-side effect.

## Lost-acknowledgement demo

This is the most interesting failure mode and is covered by `tests/end_to_end.rs`.

1. Start the sink with `--drop-ack`:

   ```bash
   cargo run -p linkspool-sink -- \
     --database /tmp/linkspool-sink.db \
     --drop-ack
   ```

2. Submit a message:

   ```bash
   curl -sS -X POST http://127.0.0.1:8080/v1/messages \
     -H 'Content-Type: application/json' \
     -d '{"idempotency_key":"lost-ack","payload":{"event":"lost_ack"}}'
   ```

3. The first attempt records the message in the sink but drops the acknowledgement. After the 5 s ack timeout LinkSpool schedules a retry. The message state is `retry_scheduled` with a timeout error.

4. The retry reaches the sink as a duplicate. The sink acknowledges it, and LinkSpool marks the message `delivered` on attempt 2.

5. Confirm on the sink: `received=1`, `duplicates=1`.

The same script without `--drop-ack` demonstrates the ordinary path: one attempt, `received=1`, `duplicates=0`.

## Tests

```bash
cd linkspool
cargo fmt --all --check
cargo clippy --workspace --all-targets
cargo test --workspace
```

Coverage:

- `crates/linkspool-core/tests/lifecycle.rs` — persistence, idempotency, admission, payload limits, expiry, retry, dead-letter, stale-job skipping, recovery, retry/discard, counts and listing.
- `linkspool/tests/api.rs` — health, `201`/`200` idempotency, list/fetch, error mapping (`400`, `404`, `409`, `413`, `429`), invalid JSON.
- `linkspool/tests/end_to_end.rs` — real TCP delivery, terminal-job skipping, lost acknowledgement and duplicate detection, receiver rejection, sink database reopening.
- `crates/linkspool-protocol/src/frame.rs` — frame round trips, clean EOF, oversized and empty frames, version validation.
- `crates/linkspool-core/src/config.rs` — defaults, backoff, path resolution.

## Protocol

Each frame is a four-byte unsigned big-endian payload length followed by JSON.

```text
+----------------------+-------------------------+
| Payload length (u32) | Encoded message payload |
+----------------------+-------------------------+
         4 bytes             N bytes
```

Envelope fields: `version`, `message_id`, `idempotency_key`, `created_at`, `expires_at`, `payload`.

Acknowledgement fields: `version`, `message_id`, `status` (`accepted`, `duplicate`, `rejected`), optional `reason`.

The sink persists the envelope before sending an acknowledgement. An acknowledgement therefore means durable receiver-side storage, not merely a successful socket read.

## Design notes and trade-offs

- **One authoritative record.** The `messages` table owns payload, destination, state, attempts and errors. Apalis rows contain only `message_id` and `attempt`.
- **Transactional ingestion.** `push_tasks` is called on the same SQLite connection as the message insert, so an accepted message cannot be stranded without a job.
- **Worker idempotency.** Every job re-checks the message state. Delivered, expired, dead-lettered and stale-attempt jobs are acknowledged without transmission.
- **Separate retry budgets.** Apalis infrastructure attempts (3) cover transient database/worker failures. Message-level retries are explicit LinkSpool jobs so backoff and attempt counts stay authoritative in the message row.
- **Android host lifecycle.** A foreground `dataSync` service keeps the host alive while the UI is closed. Android may still stop the service under memory pressure; sticky restart plus SQLite recovery handles that.
- **No TLS or authentication.** The TCP transport is for controlled development and trusted local networks. The desktop API is loopback-only and disabled on Android.

## Limitations and future work

- At-least-once, not exactly-once.
- One destination per instance.
- No TLS, peer authentication or encrypted payloads.
- No priority queues, batching or multi-destination routing.
- No distributed broker or clustering.
- Android delivery depends on the foreground service remaining alive; the durable store makes restarts safe but does not guarantee immediate background progress.

## References

- [Rivium](https://docs.rs/rivium/latest/rivium/)
- [Rivium JNI](https://docs.rs/rivium-jni/latest/rivium_jni/)
- [Apalis](https://apalis.dev/docs/introduction/overview)
- [Apalis SQLite](https://github.com/apalis-dev/apalis-sqlite)
- [Rivium Challenge](https://github.com/apalis-dev/rivium-challenge)

---

**LinkSpool — Durable delivery over unreliable networks.**
