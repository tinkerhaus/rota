# Part 3 — Fairness & policies

**Goal:** control *how* groups share a lane — with weights, batch sizes, built-in
policies, and a custom policy written in CEL — and watch the effect live.

← [Part 2 — Broker basics](02-broker-basics.md) · Next: [Part 4 — Reliability & scheduling](04-reliability-and-scheduling.md) →

This is Rota's moat. Other brokers make you bolt fairness on after the fact; here
it's a first-class, programmable, per-lane decision.

---

## 3.1 Weights: proportional shares

By default every group has **weight 1.0** → equal shares (round-robin). Give a
group a higher weight and it gets a proportionally larger share of the lane.

The mechanism is **weighted fair queuing via virtual time**: each group has a
virtual clock, serving it advances the clock by `1/weight`, and the lowest clock
goes next. Equal weights → round-robin; weights `3:1` → the heavy group is served
~3× as often.

Set weights with the **Control** client. (You can also pass `weight=` on `publish`
as an upsert hint, but the authoritative knob is `set_group_config`.)

### Python

```python
from rota import Control
ctl = Control("127.0.0.1:7100")

ctl.set_group_config("notifications", "tenant-A", weight=3.0)  # 3× the share
ctl.set_group_config("notifications", "tenant-B", weight=1.0)
print(ctl.get_group_config("notifications", "tenant-A"))       # weight: 3.0
```

### TypeScript

```ts
import { Control } from "rota";
const ctl = new Control("127.0.0.1:7100");

await ctl.setGroupConfig("notifications", "tenant-A", { weight: 3.0 }); // 3× the share
await ctl.setGroupConfig("notifications", "tenant-B", { weight: 1.0 });
console.log(await ctl.getGroupConfig("notifications", "tenant-A"));     // weight: 3
```

Now publish a big burst to both tenants and drain it with one worker (Part 2's
`worker`). Over many deliveries you'll see **~3 of A for every 1 of B**. In a live
contention test, weights `5:4:3:2:1` produced `33 / 27 / 20 / 13 / 7 %` of
service — proportional to the decimal.

### `batch_size`

`batch_size` (default 1) is how many messages a group is handed *per turn* it wins.
Raise it for a group that benefits from locality (e.g. batch DB writes) without
changing its long-run share much:

```python
ctl.set_group_config("notifications", "tenant-A", batch_size=10)
```
```ts
await ctl.setGroupConfig("notifications", "tenant-A", { batchSize: 10 });
```

---

## 3.2 Built-in policies

The serving order is set by the lane's **policy**. Switch it by name — it
hot-reloads, no restart:

| Kind | Behaviour |
|---|---|
| `DRR` (default) / `WFQ` | Weighted-fair: each group's share ∝ its `weight`. |
| `STRICT_PRIORITY` | Higher-weight groups first; lower ones *may* starve (by design). |
| `LOTTERY` | Weighted-random selection. |
| `COMPLETION_AWARE` | WFQ that also accounts for **in-flight** work — a tenant holding many un-acked leases is throttled, not just one that takes many turns. |

### Python

```python
from rota._gen.rota.v1 import rota_pb2 as pb

info = ctl.set_policy("notifications", kind=pb.STRICT_PRIORITY)
print("policy version", info.policy_version)            # bumps on each set
print(ctl.get_policy("notifications").source.kind)      # STRICT_PRIORITY

ctl.set_policy("notifications", kind=pb.COMPLETION_AWARE)  # hot-reload to another
```

### TypeScript

```ts
import { PolicyKind } from "rota";

const info = await ctl.setPolicy("notifications", { kind: PolicyKind.STRICT_PRIORITY });
console.log("policy version", info.policyVersion);            // bumps on each set
console.log((await ctl.getPolicy("notifications")).source?.kind); // STRICT_PRIORITY

await ctl.setPolicy("notifications", { kind: PolicyKind.COMPLETION_AWARE }); // hot-reload
```

Try `STRICT_PRIORITY` with `tenant-A` at weight 3: A's backlog now drains
completely before B gets a turn. Switch back to `WFQ` and the interleave returns —
all without restarting anything.

---

## 3.3 A custom policy in CEL

When the built-ins aren't enough, write the scoring rule yourself in
[**CEL**](https://github.com/google/cel-spec) (or compile a WASM module). The
policy is a pure expression that returns **one score per group; higher = served
sooner**. It runs **only on the Raft leader**, so it's consistent and
side-effect-free.

Variables available to the expression include `weight` and `backlog` (the group's
ready depth). For example, *"serve the shortest queue first"* is `-backlog`.

**Always validate before installing** — `validate_policy` compiles and dry-runs the
expression against synthetic fixtures without touching the live lane:

### Python

```python
# Dry-run first
res = ctl.validate_policy("notifications", kind=pb.CUSTOM, engine="cel", code=b"-backlog")
print(res.ok, list(res.diagnostics))     # True []   (a malformed expr -> False + diagnostics)

# Install it
ctl.set_policy("notifications", kind=pb.CUSTOM, engine="cel", code=b"-backlog")
```

### TypeScript

```ts
import { PolicyKind } from "rota";

const res = await ctl.validatePolicy("notifications", {
  kind: PolicyKind.CUSTOM, engine: "cel", code: Buffer.from("-backlog"),
});
console.log(res.ok, res.diagnostics);    // true []   (malformed -> false + diagnostics)

await ctl.setPolicy("notifications", {
  kind: PolicyKind.CUSTOM, engine: "cel", code: Buffer.from("-backlog"),
});
```

A faulting policy is **bounded**: if it errors at runtime the broker falls back to
weighted-fair for that tick, and a repeatedly-faulting policy is *quarantined*
until you install a new one. You can't take the lane down with a bad expression.

> WASM works the same way: `engine="wasm"`, `code=<bytes of a wasip1 reactor
> module exporting `score`>`. Any language that compiles to WASM can author a
> policy.

---

## 3.4 Watch it live

Two ways to observe fairness:

**The dashboard** (`http://localhost:7101`, from Part 1). Publish a lopsided burst
and watch the **served-order ribbon** interleave, the **expected-vs-actual share**
bars track your weights, and the **starvation radar** light up if you switch to
`STRICT_PRIORITY`.

**From code**, poll `get_stats` for per-lane counters:

```python
for ls in ctl.get_stats("notifications").lanes:
    print(ls.lane, "leasable", ls.leasable, "inflight", ls.inflight,
          "groups", ls.group_count, "policy_v", ls.policy_version)
```
```ts
for (const ls of (await ctl.getStats("notifications")).lanes) {
  console.log(ls.lane, "leasable", ls.leasable, "inflight", ls.inflight,
              "groups", ls.groupCount, "policyV", ls.policyVersion);
}
```

---

## What you learned

- **Weights** set proportional shares; **`batch_size`** sets messages-per-turn.
- **Built-in policies** (`DRR`/`WFQ`, `STRICT_PRIORITY`, `LOTTERY`,
  `COMPLETION_AWARE`) switch by name and **hot-reload**.
- **Custom policies** in CEL/WASM return a score per group, run leader-only, and
  are sandboxed + quarantined on fault. **Always `validate_policy` first.**
- Fairness is observable live via the dashboard or `get_stats`.

Next: what happens when work *fails* — retries, dead-letters, and scheduling. →
[Part 4 — Reliability & scheduling](04-reliability-and-scheduling.md)
