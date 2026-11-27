# godis-kv

An in-memory **Key-Value database and Pub/Sub broker** built from scratch in Go, operating over raw TCP with zero external dependencies.

The goal of this project is to step away from high-level web frameworks and work directly inside the "engine room" of systems programming — building a minimalist Redis-inspired server for high-speed caching, session storage, and real-time messaging while handling raw TCP sockets, thread-safe concurrency, disk persistence, atomic log compaction, and controlled process shutdown.

## Core Engineering Concepts Practiced

Beyond implementing an in-memory map, each package exercises a core systems engineering and idiomatic Go concept:

- **Raw TCP Networking (`net`):** Custom TCP server listening on `:6379` (configurable via CLI flags) with no HTTP layer or web frameworks, powered by a custom line-based command parser.
- **Shared-Memory Concurrency (`goroutines` + `sync.RWMutex`):** Each client connection runs in its own goroutine. Concurrent reads (`GET`) share read locks (`RLock`), while mutations (`SET`, `SETEX`, `DEL`) acquire exclusive write locks (`Lock`) to eliminate race conditions.
- **Key Expiration (TTL):** Support for expiring keys combining passive expiration checks on read (`Get`) with an active background cleanup loop (`time.NewTicker`) that periodically evicts expired entries.
- **Append-Only File Persistence (AOF) & Atomic Compaction (`BGREWRITEAOF`):** Every effective write operation is appended sequentially to disk (`data/database.log`) and replayed on startup (`Replay`). Background compaction snapshots only live keys to a temporary file, calls `Sync()`, and replaces the log atomically via `os.Rename`, guarded by an `atomic.Bool` to prevent concurrent rewrites.
- **Real-Time Pub/Sub with Go Channels (`SUBSCRIBE` / `UNSUBSCRIBE` / `PUBLISH`):** Dedicated in-memory broker using buffered Go channels and non-blocking `select` statements so slow subscribers never stall publishers, with `sync.Once` and `sync.WaitGroup` ensuring leak-free cleanup on client disconnect.
- **Idiomatic Interface Decoupling:** Following Go's *"accept interfaces, return structs"* principle, `internal/server` defines the contracts it consumes (`KeyValueStore`, `AppendLogger`, and `PubSubBroker`) without importing `internal/storage` directly — enabling in-memory protocol testing with mocks and `net.Pipe()`.
- **Graceful Shutdown:** Captures `SIGINT` and `SIGTERM` via `os/signal` to stop accepting new connections, close the TCP listener, and flush/close the AOF file cleanly before exiting.
- **Automated Testing & Race Detection:** Unit and integration tests executed with `-race` covering CRUD, real TTL expiration, 1,000-goroutine stress tests, AOF replay and compaction (`t.TempDir()`), Pub/Sub concurrency, and full TCP protocol flows (`net.Pipe()`).
- **Load Benchmarking & Containerization:** Built-in worker-pool benchmark tool measuring throughput (`req/s`) and latency percentiles (`Min`, `Avg`, `P50`, `P95`, `P99`, `Max`), paired with a `Makefile` and a multi-stage `Dockerfile` producing a minimal non-root Alpine image with persistent AOF volumes.

## Tech Stack & Tools

- Go `1.26` (**100% standard library**, zero third-party dependencies)
- Standard packages: `net`, `bufio`, `sync`, `sync/atomic`, `os/signal`, `flag`, `slices`, `testing`
- Go Race Detector (`go test -race`)
- GNU Make (`Makefile`)
- Docker (multi-stage static build on `alpine:3.21`) & Docker Compose
- `netcat` (`nc`) for manual TCP interaction

## Project Structure

```text
godis-kv/
├── .dockerignore
├── .gitignore
├── Dockerfile                      # Multi-stage build (static Linux binary + non-root Alpine runtime)
├── docker-compose.yml              # Container orchestration with persistent named volume (godis_data)
├── Makefile                        # Automation targets: build, run, test, bench, clean, docker-up/down
├── go.mod
├── cmd/
│   ├── server/
│   │   └── main.go                 # Entrypoint: CLI flags (-addr, -aof), AOF replay, broker wiring, graceful shutdown
│   └── bench/
│       └── main.go                 # Worker-pool load generator with throughput and P50/P95/P99 latency metrics
└── internal/
    ├── pubsub/
    │   ├── broker.go               # Thread-safe Pub/Sub broker using buffered Go channels and non-blocking select
    │   └── broker_test.go          # Unit, slow-subscriber overflow, and concurrency tests
    ├── server/
    │   ├── tcp.go                  # Raw TCP server, command parser, and consumer-side interfaces
    │   └── tcp_test.go             # Protocol, BGREWRITEAOF, and multi-client Pub/Sub tests via net.Pipe()
    └── storage/
        ├── db.go                   # In-memory KV engine, sync.RWMutex, TTL cleanup routine, and live snapshot generator
        ├── db_test.go              # CRUD, TTL expiration, and 1,000-worker race condition tests
        ├── aof.go                  # Append-Only File persistence (auto-mkdir, Append, Replay, atomic Rewrite)
        └── aof_test.go             # Persistence, reopen/replay, and size-reduction compaction tests with t.TempDir()
```

## Architecture & Persistence Notes

The `internal/server` package owns its dependency contracts (`KeyValueStore`, `AppendLogger`, and `PubSubBroker`). Because the TCP layer never depends on concrete storage or filesystem structs, the entire command protocol and background rewrite trigger can be tested deterministically in memory using `net.Pipe()` and lightweight mocks.

For disk persistence, `storage.NewAOF` automatically creates parent directories via `os.MkdirAll`, preventing startup failures on fresh clones where `data/` is gitignored. During `BGREWRITEAOF`, `aof.Rewrite` receives a `snapshotFn` callback and executes it while holding the AOF mutex (respecting the `aof.mu -> db.mu` lock hierarchy), preventing both deadlocks and lost writes between snapshotting and the atomic `os.Rename` swap.

## Running Locally

### Prerequisites

- Go `1.22+` (`go version`)
- `make` (optional, for shortcut commands)
- `netcat` (`nc`) for manual TCP commands
- Docker & Docker Compose (optional, for containerized execution)

### Option 1: Run with Go / Make

1. Clone the repository:

```bash
git clone [https://github.com/supp3rguto/godis-kv.git](https://github.com/supp3rguto/godis-kv.git)
cd godis-kv
```

2. **Terminal 1** — start the TCP server (defaults to `:6379` and `data/database.log`):

```bash
make run
```

Or customize address and AOF path via CLI flags:

```bash
go run ./cmd/server -addr :6380 -aof data/custom.log
```

To compile stripped binaries into `bin/`:

```bash
make build
./bin/godis-server -addr :6379 -aof data/database.log
```

3. **Terminal 2** — connect via raw TCP using `netcat`:

```bash
nc localhost 6379
```

### Option 2: Run with Docker

Build the multi-stage image and start the container with persistent AOF storage (`godis_data` volume):

```bash
make docker-up
docker logs godis-kv
```

Stop and remove the container (persisted keys remain intact inside the Docker volume and are automatically replayed on the next start):

```bash
make docker-down
```

## Testing & Race Detection

Run the full test suite with Go's race detector and coverage report:

```bash
make test
```

Current internal package coverage:
- `internal/pubsub`: `100.0%`
- `internal/storage`: `76.2%`
- `internal/server`: `68.5%`

## Benchmarking

With the server running on `:6379`, execute the worker-pool benchmark tool:

```bash
# Default mixed workload (50 workers, 10,000 requests: 50% SET / 50% GET)
make bench

# Custom workloads
go run ./cmd/bench -addr localhost:6379 -workers 50 -requests 10000 -mode mixed
go run ./cmd/bench -addr localhost:6379 -workers 50 -requests 10000 -mode set
go run ./cmd/bench -addr localhost:6379 -workers 50 -requests 10000 -mode get
```

Each worker maintains a persistent TCP connection and records latencies in a local slice without lock contention, sorting all samples at the end to compute exact `P50`, `P95`, and `P99` tail latencies.

## Protocol & Commands

| Command | Syntax | Persisted to AOF | Responses |
| --- | --- | --- | --- |
| `SET` | `SET <key> <value>` | Yes | `OK`, `ERR syntax: SET <key> <value>` |
| `SETEX` | `SETEX <key> <seconds> <value>` | Yes | `OK`, `ERR invalid seconds`, `ERR syntax: SETEX <key> <seconds> <value>` |
| `GET` | `GET <key>` | No | `<value>`, `(nil)`, `ERR syntax: GET <key>` |
| `DEL` | `DEL <key>` | Yes (if key existed) | `1` (deleted), `0` (not found), `ERR syntax: DEL <key>` |
| `BGREWRITEAOF` | `BGREWRITEAOF` | Rewrites AOF | `Background append only file rewriting started`, `ERR rewrite already in progress`, `ERR AOF not configured` |
| `SUBSCRIBE` | `SUBSCRIBE <channel>` | No | `SUBSCRIBED <channel>`, pushes `MESSAGE <channel> <message>` |
| `UNSUBSCRIBE` | `UNSUBSCRIBE <channel>` | No | `UNSUBSCRIBED <channel>` |
| `PUBLISH` | `PUBLISH <channel> <message>` | No | `<delivered_count>`, `ERR syntax: PUBLISH <channel> <message>` |

Values and Pub/Sub messages containing spaces are supported. Unrecognized commands return `ERR unknown command`.

### Examples via `netcat`

**Key-Value, TTL & AOF Compaction:**

```text
$ nc localhost 6379
SET user guto
OK
GET user
guto
SET phrase in-memory database in go
OK
GET phrase
in-memory database in go
SETEX session 5 temp_token
OK
GET session
temp_token
# after 5 seconds:
GET session
(nil)
DEL user
1
BGREWRITEAOF
Background append only file rewriting started
```

**Real-Time Pub/Sub (two terminals):**

```text
# Terminal A (Subscriber)
$ nc localhost 6379
SUBSCRIBE alerts
SUBSCRIBED alerts
MESSAGE alerts deploy finished successfully
UNSUBSCRIBE alerts
UNSUBSCRIBED alerts
```

```text
# Terminal B (Publisher)
$ nc localhost 6379
PUBLISH alerts deploy finished successfully
1
```

## Roadmap & Milestones

- [x] Custom raw TCP server using Go's `net` package (goroutine per connection)
- [x] Thread-safe in-memory Key-Value engine with `sync.RWMutex`
- [x] Append-Only File (AOF) disk persistence with startup `Replay`
- [x] Key expiration (`SETEX`) with passive checks and active background eviction
- [x] Graceful Shutdown handling `SIGINT` and `SIGTERM`
- [x] Consumer-side interface decoupling (`KeyValueStore`, `AppendLogger`, `PubSubBroker`) and CLI flags
- [x] Unit & integration test suite with `-race` (`db_test.go`, `aof_test.go`, `broker_test.go`, `tcp_test.go`)
- [x] Atomic AOF compaction (`BGREWRITEAOF`) via temporary file + `Sync()` + `os.Rename`
- [x] Real-time Pub/Sub broker (`SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`) using Go channels
- [x] Worker-pool load benchmark with throughput and `P50` / `P95` / `P99` latency metrics
- [x] `Makefile` automation and multi-stage Docker setup with persistent volume

## Author

Augusto Ortigoso Barbosa

- GitHub: https://github.com/supp3rguto
- LinkedIn: https://www.linkedin.com/in/augusto-barbosa-769602194
