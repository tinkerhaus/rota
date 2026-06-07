# Part 6 — Clustering & operations

**Goal:** run Rota as a real highly-available cluster, see leader-following and
failover work transparently, coordinate with singleton leases, and learn the
operational surface (back-pressure, health, metrics, the dashboard).

← [Part 5 — Durable workflows](05-durable-workflows.md) · [Tutorial index](README.md)

---

## 6.1 Run a 3-node cluster

A single dev node is its own leader. For HA you run a **3-** (or 5-) node Raft
quorum: writes go to the leader, state is replicated, and if the leader dies a new
one is elected automatically. Everything — messages, leases, timers, cron,
workflow history, *and* the Raft log — lives in each node's embedded Pebble
keyspace. No external coordinator.

Each node needs its own `--id`, `--raft` (Raft transport), `--grpc`, `--metrics`,
and `--data`. **One** node bootstraps the initial voter set with `--peers`; all
nodes carry the `--grpc-peers` map so any follower can tell a client the leader's
gRPC address. Open three terminals:

```bash
# terminal 1 — n1 bootstraps the cluster
$ go run ./cmd/rota serve --id n1 \
    --raft 127.0.0.1:8201 --grpc 127.0.0.1:7201 --metrics 127.0.0.1:7301 --data ./data-n1 \
    --bootstrap true \
    --peers n1=127.0.0.1:8201,n2=127.0.0.1:8202,n3=127.0.0.1:8203 \
    --grpc-peers n1=127.0.0.1:7201,n2=127.0.0.1:7202,n3=127.0.0.1:7203

# terminal 2 — n2 joins
$ go run ./cmd/rota serve --id n2 \
    --raft 127.0.0.1:8202 --grpc 127.0.0.1:7202 --metrics 127.0.0.1:7302 --data ./data-n2 \
    --bootstrap false \
    --grpc-peers n1=127.0.0.1:7201,n2=127.0.0.1:7202,n3=127.0.0.1:7203

# terminal 3 — n3 joins
$ go run ./cmd/rota serve --id n3 \
    --raft 127.0.0.1:8203 --grpc 127.0.0.1:7203 --metrics 127.0.0.1:7303 --data ./data-n3 \
    --bootstrap false \
    --grpc-peers n1=127.0.0.1:7201,n2=127.0.0.1:7202,n3=127.0.0.1:7203
```

Within a couple of seconds the three elect a leader. Check the cluster view:

```python
from rota import Control
info = Control("127.0.0.1:7201").describe_cluster()
print("leader:", info.leader_id, "term:", info.term)
for p in info.peers:
    print(" ", p.id, p.addr, p.suffrage)     # Voter / Nonvoter
```
```ts
import { Control } from "rota";
const info = await new Control("127.0.0.1:7201").describeCluster();
console.log("leader:", info.leaderId, "term:", info.term);
for (const p of info.peers) console.log(" ", p.id, p.addr, p.suffrage);
```

---

## 6.2 Leader-following is automatic

Writes must hit the Raft leader. You **don't** have to know which node that is — the
SDK handles it. Point a client at **any** node (even a follower): on a `NOT_LEADER`
fault it reads the advertised leader address, re-dials, and retries with bounded
backoff. The *Work* stream does the same via an in-stream redirect frame.

```python
# Pointed at n2, which may be a follower — this still succeeds:
Control("127.0.0.1:7202").set_group_config("notifications", "tenant-A", weight=2.0)

# Or seed the client with several addresses for resilience:
Publisher("127.0.0.1:7201,127.0.0.1:7202,127.0.0.1:7203").publish(
    "notifications", "tenant-A", b"hi")
```
```ts
await new Control("127.0.0.1:7202").setGroupConfig("notifications", "tenant-A", { weight: 2.0 });

await new Publisher(["127.0.0.1:7201", "127.0.0.1:7202", "127.0.0.1:7203"])
  .publish("notifications", "tenant-A", Buffer.from("hi"));
```

**Try a failover:** find the leader (`describe_cluster`), Ctrl-C that node, and keep
publishing through a *different* node's address. The remaining two elect a new
leader within ~a second and your client transparently follows it — no code change,
no dropped writes once the new leader is up. (This is exactly what the SDK's
`cluster` test suite asserts against a real 3-node cluster.)

For protected clusters, bootstrap the first administrator once, then manage
principals and grants through Rota itself. The bootstrap token can come from an
environment variable, a file, or an explicit flag; pass the same bootstrap source
to every protected node so followers wait for replicated auth state before
serving. After the first principal is replicated, runtime auth is no longer
file-based.

```bash
export ROTA_BOOTSTRAP_ADMIN_TOKEN="$(openssl rand -base64 32)"

go run ./cmd/rota serve --id n1 \
  --raft 127.0.0.1:8201 --grpc 127.0.0.1:7201 --metrics 127.0.0.1:7301 --data ./data-n1 \
  --bootstrap true \
  --peers n1=127.0.0.1:8201,n2=127.0.0.1:8202,n3=127.0.0.1:8203 \
  --grpc-peers n1=127.0.0.1:7201,n2=127.0.0.1:7202,n3=127.0.0.1:7203 \
  --bootstrap-admin-env ROTA_BOOTSTRAP_ADMIN_TOKEN

go run ./cmd/rota auth create --grpc 127.0.0.1:7201 --token "$ROTA_BOOTSTRAP_ADMIN_TOKEN" \
  --name dashboard --tag dashboard
go run ./cmd/rota auth create --grpc 127.0.0.1:7201 --token "$ROTA_BOOTSTRAP_ADMIN_TOKEN" \
  --name orders-worker --grant '^orders$:.*:publish,consume,complete'
go run ./cmd/rota auth list --grpc 127.0.0.1:7201 --token "$ROTA_BOOTSTRAP_ADMIN_TOKEN"
go run ./cmd/rota doctor --grpc 127.0.0.1:7201 --token "$ROTA_BOOTSTRAP_ADMIN_TOKEN"
```

```python
Publisher("127.0.0.1:7201", auth_token=os.environ["ROTA_WORKER_TOKEN"])
```
```ts
new Publisher("127.0.0.1:7201", { authToken: process.env.ROTA_WORKER_TOKEN });
```

Grant actions are `read`, `publish`, `consume`, `complete`, `workflow`,
`configure`, and `admin`. Lane and group patterns are RE2 regexes; tags
`dashboard` and `monitoring` grant read-only visibility, while `administrator`
is break-glass access. Add `--tls-cert` and `--tls-key` for server TLS, plus
`--client-ca` for mTLS. CLI clients verify TLS with `--tls-ca` and optional
`--tls-server-name`; SDKs accept the native gRPC credential objects for their
language.

---

## 6.3 Singleton leases: cluster-wide coordination

Need exactly one instance of something across the cluster — a cron driver, a
migration, a leader-elected job? A **singleton lease** gives mutual exclusion with a
**fencing token** (a strictly-increasing number you attach to side effects so a
stale holder can't act after losing the lease).

### Python

```python
ctl = Control("127.0.0.1:7201")
lease = ctl.acquire_singleton("nightly-job", holder="host-1", ttl=30.0)
if lease.fence:                                   # we hold it
    # ... do the exclusive work, tagging writes with lease.fence ...
    ctl.renew_singleton("nightly-job", "host-1", lease.fence, ttl=30.0)  # heartbeat
    ctl.release_singleton("nightly-job", "host-1", lease.fence)          # done
```

### TypeScript

```ts
const ctl = new Control("127.0.0.1:7201");
const lease = await ctl.acquireSingleton("nightly-job", "host-1", 30.0);
if (lease.fence) {                                // we hold it
  // ... exclusive work, tagging writes with lease.fence ...
  await ctl.renewSingleton("nightly-job", "host-1", lease.fence, 30.0); // heartbeat
  await ctl.releaseSingleton("nightly-job", "host-1", lease.fence);     // done
}
```

A second holder's `acquire` is **rejected** while the lease is held; `renew` with a
**stale fence** is rejected; after release (or TTL expiry) the next acquirer gets a
**strictly higher** fence. Heartbeat with `renew` before the TTL to keep it.

---

## 6.4 Back-pressure: rate limits and pausing

Two operator levers when a lane is too hot or a downstream is struggling:

**Rate-limit dequeue** — cap how fast a lane hands out leases (token bucket):

```python
ctl.set_lane_config("notifications", rate_per_sec=50, burst=10)   # <=0 = unlimited
```
```ts
await ctl.setLaneConfig("notifications", { ratePerSec: 50, burst: 10 });
```

**Pause/resume** a lane — a circuit-breaker hook. A paused lane leases nothing
(workers stay connected and idle); resume to let it flow again:

```python
ctl.pause_lane("notifications", duration=30.0)   # 0 = until explicitly resumed
ctl.resume_lane("notifications")
```
```ts
await ctl.pauseLane("notifications", 30.0);       // 0 = until resume_lane
await ctl.resumeLane("notifications");
```

You can also pause/resume an individual **group**, or `cancel`/`purge`/`teardown`
groups for cleanup (`cancel_group`, `purge_group`, `teardown_group` — the last
drops a group across *all* lanes at once).

---

## 6.5 Health, metrics, and the dashboard

**Health** (liveness + leadership) — for load balancers and probes:

```python
h = ctl.health()                 # serving / has_quorum / is_leader
```
```ts
const h = await ctl.health();    // serving / hasQuorum / isLeader
```

Standard **gRPC health** (`grpc.health.v1.Health`) is registered too, so
`grpc-health-probe` works. Over HTTP, each node serves:

- `http://<node>:<metrics>/healthz` — 200 when serving (k8s liveness/readiness).
- `http://<node>:<metrics>/metrics` — Prometheus metrics (publish/lease/ack rates,
  backlog, DLQ depth, policy faults, Raft state, …).

```bash
$ curl -s localhost:7301/healthz       # -> ok
$ curl -s localhost:7301/metrics | grep rota_ | head
```

**The dashboard** (`--web web/dist`, served on each node's metrics port) is the
human view: the fairness ribbon, per-group share bars, the starvation radar,
throughput sparklines, a DLQ inspector with **redrive**, and the per-run workflow
swimlanes. The home-page **Doctor** panel mirrors the CLI report and flags
not-serving nodes, quorum loss, unknown leaders, DLQ depth, paused backlogs,
expired/near-expired leases, and workflow runs whose pending task has no visible
workflow-task lane depth. The DLQ inspector and redrive live on the HTTP/JSON
gateway (the SDK intentionally exposes only the broker/control/workflow gRPC
surface), so the dashboard is the place to triage and re-drive dead letters.

**The CLI** gives the same operator primitives when you are in a shell:

```bash
# One-shot health report: cluster, lanes, leases, DLQ, and workflow task liveness.
go run ./cmd/rota doctor --grpc 127.0.0.1:7201
go run ./cmd/rota doctor --grpc 127.0.0.1:7201 --json

# Inspect and recover leases.
go run ./cmd/rota leases list --grpc 127.0.0.1:7201 --lane notifications
go run ./cmd/rota leases force-expire --grpc 127.0.0.1:7201 --lease-id 42 --delay 2s

# Redrive one dead letter as a fresh READY message.
go run ./cmd/rota dlq redrive --grpc 127.0.0.1:7201 \
  --lane notifications --group tenant-A --msg-id 7

# Back-pressure and cleanup.
go run ./cmd/rota lane pause --grpc 127.0.0.1:7201 --lane notifications --duration 30s
go run ./cmd/rota lane resume --grpc 127.0.0.1:7201 --lane notifications
go run ./cmd/rota group purge --grpc 127.0.0.1:7201 --lane notifications --group tenant-A
go run ./cmd/rota workflow cancel --grpc 127.0.0.1:7201 --run-id 99 --reason operator
```

Prometheus includes counters for publish, lease, ack, nack mode, DLQ reason,
leader redirects, workflow-task validation rejections, timer fires, and policy
faults. `GetStats` also reports EWMA publish/lease/ack rates and oldest live
message age per lane.

The repository includes a starter alert group at
[`ops/prometheus/rota-alerts.yml`](../../ops/prometheus/rota-alerts.yml). It
covers target-down, no-leader, no-quorum, rising dead letters, high retry
pressure, workflow-task rejections, leader-redirect spikes, and policy faults.

---

## 6.6 Backups and load smoke tests

Rota's data directory is embedded state, so backups are deliberately **offline**:
stop the node, archive the directory, then restart it. Restore refuses a
non-empty target unless you pass `--force`.

```bash
go run ./cmd/rota backup create --data ./data-n1 --out rota-n1.tar.gz
go run ./cmd/rota backup validate --in rota-n1.tar.gz
go run ./cmd/rota backup restore --in rota-n1.tar.gz --data ./data-restored
```

For a quick load smoke test, `bench` publishes a synthetic workload with
`PublishBatch`, drains it with concurrent Work streams, and reports publish,
drain, and end-to-end throughput.

```bash
go run ./cmd/rota bench --grpc 127.0.0.1:7201 \
  --lane bench --messages 10000 --groups 100 --workers 8 --batch-size 250
go run ./cmd/rota bench --grpc 127.0.0.1:7201 --json
```

For a longer pressure test, `soak` runs concurrent publishers/workers, optional
retry/DLQ pressure, optional workflow storms, and an optional chaos command during
active load:

```bash
go run ./cmd/rota soak --grpc 127.0.0.1:7201 \
  --duration 10m --lanes 8 --groups 200 --publishers 4 --workers 16 \
  --publish-rate 1000 --retry-every 50 --workflow-storm
```

The repo CI workflow runs Go tests, TypeScript SDK tests, dashboard checks, and
Python SDK tests on pushes and pull requests.

---

## What you learned

- A **3-node cluster** is just three `serve` processes — one bootstraps with
  `--peers`, all share `--grpc-peers`. No external coordinator.
- **Leader-following and failover are automatic** in both SDKs — point a client at
  any node (or seed several) and writes find the leader.
- **Singleton leases** give fenced, cluster-wide mutual exclusion.
- Operate with **replicated auth**, rate limits, **pause/resume**, **health**,
  **Prometheus metrics**, alerts, backup validation, load/soak tests, and the
  **dashboard**.

That's the whole system. Loop back to the [tutorial index](README.md), or go deep
with [`DESIGN.md`](../../DESIGN.md) and the [ADRs](../adr/).
