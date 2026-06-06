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
swimlanes. The DLQ inspector and redrive live on the HTTP/JSON gateway (the SDK
intentionally exposes only the broker/control/workflow gRPC surface), so the
dashboard is the place to triage and re-drive dead letters.

---

## What you learned

- A **3-node cluster** is just three `serve` processes — one bootstraps with
  `--peers`, all share `--grpc-peers`. No external coordinator.
- **Leader-following and failover are automatic** in both SDKs — point a client at
  any node (or seed several) and writes find the leader.
- **Singleton leases** give fenced, cluster-wide mutual exclusion.
- Operate with **rate limits**, **pause/resume**, **health**, **Prometheus
  metrics**, and the **dashboard**.

That's the whole system. Loop back to the [tutorial index](README.md), or go deep
with [`DESIGN.md`](../../DESIGN.md) and the [ADRs](../adr/).
