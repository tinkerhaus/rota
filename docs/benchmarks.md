# Benchmarking Rota

Rota ships three operator load tools:

- `rota bench` measures publish and drain throughput against an existing node.
- `rota soak` runs a time-boxed workload with optional retry, DLQ, workflow, and chaos pressure.
- `rota bench suite` starts disposable local nodes and produces a repeatable benchmark report across broker, workflow, cluster failover, and backup paths.

## Quick Smoke Run

Use the smoke profile before a release or after a local change:

```bash
go run ./cmd/rota bench suite --profile smoke
```

For machine-readable output:

```bash
go run ./cmd/rota bench suite --profile smoke --json
```

Progress is written to stderr so JSON on stdout stays parseable. Use `--quiet`
to suppress progress and print only the final report.

The suite creates a temporary run directory and removes it when the run completes. Pass `--keep-data` to inspect the Pebble/Raft data directories afterwards, or `--tmp-dir ./bench-runs` to put run data under a known parent.

## Standard Run

The standard profile is intended for local regression comparisons:

```bash
go run ./cmd/rota bench suite --profile standard
```

Useful tuning flags:

```bash
go run ./cmd/rota bench suite \
  --messages 20000 \
  --groups 250 \
  --workers 16 \
  --batch-size 500 \
  --payload-bytes 256 \
  --latency-samples 500 \
  --soak-duration 30s \
  --soak-rate 500 \
  --retry-every 25 \
  --deadletter-every 100 \
  --workflow-duration 15s \
  --workflow-rate 250 \
  --backup-messages 5000 \
  --timeout 2m
```

## What The Suite Measures

Single-node broker:

- `PublishBatch` throughput.
- Work-stream drain throughput.
- End-to-end publish-and-ack throughput.
- Unary publish latency percentiles.
- Lease wait latency percentiles.

Workflow storm:

- Workflow starts per second.
- Workflow task polls and responses.
- Activity task polls and completions.
- Worker-side command volume and errors.

Soak pressure:

- Multi-lane, multi-group publish and work streams.
- Long-lived workers under steady publish rate.
- Configurable retry pressure through `--retry-every`.
- Configurable DLQ pressure through `--deadletter-every`.
- Published, leased, acked, retried, dead-lettered, and error counters.

Three-node cluster:

- Broker throughput through the current Raft leader.
- Leader kill and re-election timing.
- Committed-but-unacked backlog survival across leader failover.
- Post-failover publish, lease, ack, and drain validation.

Backup:

- Offline archive creation time.
- Validation restore time and key counts.
- Full restore time.
- Archive size.

## Skipping Sections

During focused work, you can skip slower sections:

```bash
go run ./cmd/rota bench suite --profile smoke --skip-cluster
go run ./cmd/rota bench suite --profile smoke --skip-soak
go run ./cmd/rota bench suite --profile smoke --skip-workflow
go run ./cmd/rota bench suite --profile smoke --skip-backup
```

## Existing-Node Benchmarks

Use `rota bench` when you already have a node or cluster endpoint running:

```bash
go run ./cmd/rota bench \
  --grpc 127.0.0.1:7300 \
  --messages 10000 \
  --groups 100 \
  --workers 8 \
  --batch-size 250
```

Use `rota soak` for long-running pressure:

```bash
go run ./cmd/rota soak \
  --grpc 127.0.0.1:7300 \
  --duration 5m \
  --lanes 8 \
  --groups 200 \
  --publishers 4 \
  --workers 16 \
  --workflow-storm \
  --workflow-rate 100
```

## Reading Results

The suite is most useful as a regression detector. Compare runs on the same machine, with the same flags, and watch for:

- lower end-to-end message throughput,
- higher publish or lease p95/p99 latency,
- slower failover recovery,
- committed-but-unacked messages not draining after failover,
- retry or dead-letter counters unexpectedly staying at zero when enabled,
- workflow task errors under storm load,
- backup validation counts that do not match the seeded workload.

Absolute numbers depend heavily on CPU, disk, filesystem cache, and whether the Go race detector or other background load is active.
