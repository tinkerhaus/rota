# Part 2 — Broker basics

**Goal:** install an SDK, then publish messages and consume them with a worker.
By the end you'll have a multi-tenant notifications service running.

← [Part 1 — Getting running](01-getting-running.md) · Next: [Part 3 — Fairness & policies](03-fairness-and-policies.md) →

Keep a node running from Part 1: `go run ./cmd/rota serve --grpc :7100 --metrics :7101`.

---

## 2.1 Install the SDK

### Python

```bash
$ pip install -e sdk/python        # editable install from the repo root
```

Python 3.9+. Runtime deps are just `grpcio` and `protobuf`.

### TypeScript

The package lives in-repo at `sdk/typescript`. Build it, then install it into your
project by path:

```bash
$ cd sdk/typescript && npm install && npm run build && cd -
$ mkdir notif && cd notif && npm init -y
$ npm pkg set type=module
$ npm install ../sdk/typescript     # installs the local 'rota' package
```

Node 18+. The `.proto` ships inside the package and loads at runtime — no codegen
step to consume the SDK.

---

## 2.2 The domain model, in one breath

A message is **opaque bytes** (`payload`) plus a string **`headers`** map,
addressed to a **`lane`** and a **`group`**. The SDK never interprets your
payload. For our notifications service:

- **lane** = `"notifications"` — the stream of "send a notification" jobs.
- **group** = the tenant id (`"tenant-A"`, `"tenant-B"`, …) — the fairness unit.
- **payload** = whatever your worker needs to send the notification (here, a tiny
  JSON blob).

---

## 2.3 Publish

### Python

```python
# publish.py
from rota import Publisher

pub = Publisher("127.0.0.1:7100")     # lazy connect; follows the leader

for tenant in ["tenant-A", "tenant-B"]:
    for i in range(3):
        msg_id = pub.publish(
            "notifications",
            group_id=tenant,
            payload=f'{{"to":"u{i}@{tenant}.com"}}'.encode(),
            headers={"kind": "welcome"},
        )
        print(f"published {tenant} #{i} -> id {msg_id}")

pub.close()
```

```bash
$ python publish.py
published tenant-A #0 -> id 1
...
```

### TypeScript

```ts
// publish.mjs
import { Publisher } from "rota";

const pub = new Publisher("127.0.0.1:7100"); // lazy connect; follows the leader

for (const tenant of ["tenant-A", "tenant-B"]) {
  for (let i = 0; i < 3; i++) {
    const id = await pub.publish(
      "notifications",
      tenant,
      Buffer.from(JSON.stringify({ to: `u${i}@${tenant}.com` })),
      { headers: { kind: "welcome" } },
    );
    console.log(`published ${tenant} #${i} -> id ${id}`);
  }
}
pub.close();
```

```bash
$ node publish.mjs
published tenant-A #0 -> id 1
...
```

`publish()` returns the broker-assigned **message id** (a *per-group* monotonic
sequence — so `tenant-A`'s first message and `tenant-B`'s first message are both
id `1`). The first publish to a group **implicitly creates** it — there is no
register step.

---

## 2.4 Consume with a worker

A **worker** opens the *Work* stream, leases messages one at a time, and runs your
handler. Returning from the handler **acks** (deletes) the message; throwing steers
the outcome (Part 4). `credit` is the in-flight budget the broker may grant; `1`
(the default) means strictly serial.

### Python

```python
# worker.py
from rota import Worker

def handle(msg):
    print(f"[{msg.group_id}] send -> {msg.payload.decode()} "
          f"(attempt {msg.attempt}, kind={msg.headers.get('kind')})")
    # ... actually send the notification here ...
    # returning normally ACKs the message

Worker("127.0.0.1:7100", lane="notifications", handler=handle, credit=1).run()
```

```bash
$ python worker.py
[tenant-A] send -> {"to":"u0@tenant-A.com"} (attempt 0, kind=welcome)
[tenant-B] send -> {"to":"u0@tenant-B.com"} (attempt 0, kind=welcome)
[tenant-A] send -> {"to":"u1@tenant-A.com"} (attempt 0, kind=welcome)
...
```

`run()` blocks. It installs SIGTERM/SIGINT handlers for a **graceful drain** (stop
taking new work, finish the in-flight message, then exit) and reconnects with
backoff if the connection drops. Ctrl-C to stop.

### TypeScript

```ts
// worker.mjs
import { Worker } from "rota";

const worker = new Worker(
  "127.0.0.1:7100",
  "notifications",
  async (msg) => {
    console.log(
      `[${msg.groupId}] send -> ${msg.payload.toString()} ` +
        `(attempt ${msg.attempt}, kind=${msg.headers.kind})`,
    );
    // ... actually send the notification here ...
    // resolving ACKs the message
  },
  { credit: 1 },
);

await worker.run(); // resolves on graceful drain / stop
```

```bash
$ node worker.mjs
[tenant-A] send -> {"to":"u0@tenant-A.com"} (attempt 0, kind=welcome)
[tenant-B] send -> {"to":"u0@tenant-B.com"} (attempt 0, kind=welcome)
...
```

The handler may be sync or async; `await worker.run()` resolves when the worker
drains or you call `worker.stop()`.

---

## 2.5 What just happened

Notice the **interleaving** in the worker output: `tenant-A`, `tenant-B`,
`tenant-A`, … even though you published all of A's messages, then all of B's. The
broker's per-lane policy picked groups fairly. With equal weights (the default)
that's round-robin across groups. In Part 3 you'll change those weights and watch
the ratios shift.

A few properties to internalise now:

- **Delivery is at-least-once.** If a worker crashes after doing the work but
  before acking, the lease expires and the message is redelivered. **Make your
  handlers idempotent** — key side effects on `msg.message_id` (Python) /
  `msg.messageId` (TS) or your own dedup id.
- **`attempt`** starts at 0 and climbs on each retry (Part 4).
- **Groups are opaque.** Rota doesn't know "tenant" means a customer — it just
  keeps each `group_id`'s share fair.

---

## 2.6 Exercises

1. Run **two** worker processes at once (same lane). Watch the broker hand
   different messages to each — leases are exclusive, so no message is processed
   twice concurrently.
2. Publish to a **third** tenant while the worker is running and watch it get
   folded into the fair rotation immediately (no registration needed).
3. Give a worker `credit=5` and publish a burst. The broker may now keep up to 5
   leases in flight to that consumer (the SDK still runs your handler serially).

Next: make the fairness *yours* — weights, policies, and watching it live. →
[Part 3 — Fairness & policies](03-fairness-and-policies.md)
