# Part 4 — Reliability & scheduling

**Goal:** handle failure correctly (retries, dead-letters, requeue) and schedule
work in time (delays, cron), plus two power tools: complete-by-token and
producer-side dedup.

← [Part 3 — Fairness & policies](03-fairness-and-policies.md) · Next: [Part 5 — Durable workflows](05-durable-workflows.md) →

---

## 4.1 A message's lifecycle

Rota is **at-least-once, lease-based** (the SQS model):

1. A worker **leases** a message → it's invisible to others until a **visibility
   deadline**.
2. The handler runs, then one of:
   - **returns normally** → **ack** (done, deleted);
   - throws **`Requeue`** → nack *no-penalty* (back-pressure; doesn't burn a retry);
   - throws **`DeadLetter`** → terminal → the lane's **DLQ**;
   - throws **anything else** → nack *retry* (broker-owned `attempt`++ , redelivered).
3. Worker crashes / deadline passes → the lease is **reclaimed** and redelivered.
4. After **`max_attempts`**, the message is **dead-lettered** automatically.

### Outcome mapping

| handler does | broker frame |
|---|---|
| returns / resolves | `Ack` |
| throws `Requeue({delay})` | `Nack(REQUEUE_NO_PENALTY, delay)` |
| throws `DeadLetter({meta})` | `Nack(DEAD_LETTER, failureMeta)` |
| throws anything else | `Nack(RETRY)` (attempt++) |

### Python

```python
from rota import Worker, Requeue, DeadLetter

def handle(msg):
    if shutting_down():
        raise Requeue(delay=2.0)                 # try again later, no penalty
    if not parseable(msg.payload):
        raise DeadLetter(meta={"why": "unparseable"})  # straight to the DLQ
    msg.extend(30.0)                             # need more time? push the deadline out
    do_work(msg.payload)                         # return -> ack; raise -> RETRY

Worker("127.0.0.1:7100", lane="notifications", handler=handle,
       credit=1).run()
```

### TypeScript

```ts
import { Worker, Requeue, DeadLetter } from "rota";

const worker = new Worker("127.0.0.1:7100", "notifications", async (msg) => {
  if (shuttingDown()) throw new Requeue({ delay: 2.0 });          // retry later, no penalty
  if (!parseable(msg.payload)) throw new DeadLetter({ meta: { why: "unparseable" } });
  msg.extend(30.0);                                               // push the deadline out
  await doWork(msg.payload);                                      // resolve -> ack; throw -> RETRY
});
await worker.run();
```

> **`Requeue` vs a plain throw.** `Requeue` is for *transient back-pressure* you
> caused (rate-limited, circuit open, shutting down) — it does **not** advance
> `attempt`, so it can't dead-letter the message. A plain throw means *the work
> failed* — it advances `attempt` and eventually dead-letters. Pick deliberately.

---

## 4.2 Retries and the DLQ in action

Publish a message with a small `max_attempts` and a handler that always fails, then
watch it dead-letter. The SDK observes DLQ depth via `get_stats` (the dashboard's
DLQ inspector and redrive live on the HTTP gateway, not the SDK):

### Python

```python
from rota import Publisher, Worker, Control

Publisher("127.0.0.1:7100").publish("billing", group_id="t1",
                                    payload=b"bad", max_attempts=3)

attempts = []
def always_fail(msg):
    attempts.append(msg.attempt)        # 0, then 1, then 2
    raise RuntimeError("boom")          # plain throw -> RETRY

# run a worker until it dead-letters (stop it from your own logic / another thread)
Worker("127.0.0.1:7100", lane="billing", handler=always_fail).run()

# elsewhere:
ctl = Control("127.0.0.1:7100")
lane = next(l for l in ctl.get_stats("billing").lanes if l.lane == "billing")
print("dlq depth:", lane.dlq_depth)     # -> 1 after attempt 2 fails (max_attempts=3)
```

### TypeScript

```ts
import { Publisher, Worker, Control } from "rota";

await new Publisher("127.0.0.1:7100").publish("billing", "t1", Buffer.from("bad"),
  { maxAttempts: 3 });

const attempts = [];
const worker = new Worker("127.0.0.1:7100", "billing", (msg) => {
  attempts.push(msg.attempt);           // 0, then 1, then 2
  throw new Error("boom");              // plain throw -> RETRY
});
// ... run the worker; stop it once dead-lettered ...

const ctl = new Control("127.0.0.1:7100");
const stats = await ctl.getStats("billing");
const lane = stats.lanes.find((l) => l.lane === "billing");
console.log("dlq depth:", lane?.dlqDepth);  // -> 1 after attempt 2 fails
```

The `attempt` counter is **broker-owned** — publishers can't set it, and it
survives worker crashes. Two safety valves back this up: a **max lease lifetime**
caps total in-flight time so a wedged consumer can't pin a message forever, and a
**TTL** can auto-drop/dead-letter a message that's never delivered in time.

---

## 4.3 Complete-by-token: hand work to an external system

Sometimes the real work happens *elsewhere* — you submit to a third-party API and a
**webhook** tells you later whether it succeeded. Complete-by-token lets you lease a
message, kick off the external work, and resolve it **later, by token**, from a
process that never held the lease (e.g. your webhook handler). No side-table, no
polling.

Publish with `issue_token=True` (the broker mints a token and delivers it on the
lease) or supply your own `external_token`. Then complete it off-stream:

### Python

```python
from rota import Publisher, Control

# producer
Publisher("127.0.0.1:7100").publish("charges", group_id="t1",
                                    payload=b"charge-42", issue_token=True)

# worker leases it, reads msg.external_token, submits to the external API, returns.
# later — your webhook handler, holding only the token:
res = Control("127.0.0.1:7100").complete_by_token(token, success=True,
                                                  result_meta={"provider_id": "ch_123"})
print(res.resolved, res.unknown_token)   # True False   (unknown token -> benign)
```

### TypeScript

```ts
import { Publisher, Control } from "rota";

await new Publisher("127.0.0.1:7100").publish("charges", "t1", Buffer.from("charge-42"),
  { issueToken: true });

// worker leases it, reads msg.externalToken, submits externally, returns.
// later — your webhook handler, holding only the token:
const res = await new Control("127.0.0.1:7100").completeByToken(token, {
  success: true, resultMeta: { providerId: "ch_123" },
});
console.log(res.resolved, res.unknownToken); // true false   (unknown token -> benign)
```

You can also complete **in-stream** from the worker that holds the lease —
`msg.complete(success=True)` (Python) / `msg.complete({ success: true })` (TS).
Completing an unknown token is **benign** (`unknown_token=True`), so an
at-least-once webhook can safely fire twice.

---

## 4.4 Delayed publish

Make a message eligible later — a relative `delay` (seconds) or an absolute
`not_before` (unix epoch seconds). Exactly one of the two.

### Python

```python
pub.publish("notifications", "t1", b"reminder", delay=30.0)          # in 30s
pub.publish("notifications", "t1", b"at-noon", not_before=1_900_000_000)
```

### TypeScript

```ts
await pub.publish("notifications", "t1", Buffer.from("reminder"), { delay: 30.0 });
await pub.publish("notifications", "t1", Buffer.from("at-noon"), { notBefore: 1_900_000_000 });
```

---

## 4.5 Cron: recurring publishes

A cron entry publishes a message on a schedule, fired **exactly once
cluster-wide**. The schedule is a standard 5-field crontab *or* a shorthand like
`@every 1s`, `@hourly`. It's idempotent on `cron_id`.

### Python

```python
ctl.schedule_cron("nightly-digest", lane="notifications", group_id="ops",
                  schedule="0 3 * * *", payload=b"digest")
print([c.cron_id for c in ctl.list_cron().crons])
ctl.pause_cron("nightly-digest")     # stops firing; CronInfo.paused = True
ctl.delete_cron("nightly-digest")    # CronOpResult.existed = True
```

### TypeScript

```ts
await ctl.scheduleCron("nightly-digest", "notifications", "ops", "0 3 * * *",
  { payload: Buffer.from("digest") });
console.log((await ctl.listCron()).crons.map((c) => c.cronId));
await ctl.pauseCron("nightly-digest");   // CronInfo.paused = true
await ctl.deleteCron("nightly-digest");  // CronOpResult.existed = true
```

---

## 4.6 Producer idempotency: `dedup_key`

Publishing twice with the same `dedup_key` within a window collapses to **one**
message — the duplicate returns the *original* id. Use it to make retried publishes
safe.

### Python

```python
a = pub.publish("orders", "t1", b"order-7", dedup_key="order-7")
b = pub.publish("orders", "t1", b"order-7", dedup_key="order-7")
assert a == b           # same id: the second publish was deduplicated
```

### TypeScript

```ts
const a = await pub.publish("orders", "t1", Buffer.from("order-7"), { dedupKey: "order-7" });
const b = await pub.publish("orders", "t1", Buffer.from("order-7"), { dedupKey: "order-7" });
console.assert(a === b); // same id: deduplicated
```

---

## What you learned

- **At-least-once + leases**: ack / `Requeue` / `DeadLetter` / retry, with a
  broker-owned `attempt` and automatic dead-lettering at `max_attempts`. **Make
  handlers idempotent.**
- **`extend`** buys more time on a long task; **max lease lifetime** and **TTL**
  are the safety valves.
- **Complete-by-token** bridges to external/async systems without a side-table.
- **Delayed publish**, **cron**, and **`dedup_key`** cover scheduling and
  producer idempotency.

Next: the second layer — durable, replay-based workflows that run their activities
through this same fair scheduler. → [Part 5 — Durable workflows](05-durable-workflows.md)
