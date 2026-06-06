# The Rota Tutorial

A hands-on, build-it-as-you-go tour of **everything Rota does** — the fair broker
*and* the durable workflow engine — with working code in **Python and
TypeScript** side by side at every step.

If you want the *why* and the mental model first, read
[**getting-started.md**](../getting-started.md) (concepts, ~10 min). This tutorial
is the *how*: you'll actually run a broker and drive it.

## What you'll build

One running example carried through the whole tutorial: a multi-tenant
**notifications service** (a fair work queue) that grows into an **orders
workflow** (durable execution), and finally runs on a **3-node cluster**.

By the end you'll have used every surface of Rota: publish/consume, group
fairness, programmable policies, retries + dead-letters, delayed + cron publishes,
complete-by-token, durable workflows (activities, signals, timers,
continue-as-new), singleton leases, clustering with automatic leader-following,
and the operator dashboard.

## The path

| Part | You'll learn | Time |
|---|---|---|
| [1. Getting running](01-getting-running.md) | Build the binary, run a node, run the fairness demo, poke it with `grpcurl`, open the dashboard | 10 min |
| [2. Broker basics](02-broker-basics.md) | Install an SDK; publish and consume; the worker loop; headers; idempotency | 15 min |
| [3. Fairness & policies](03-fairness-and-policies.md) | Groups, weights, batch sizes; built-in policies; a custom CEL policy; watch fairness live | 20 min |
| [4. Reliability & scheduling](04-reliability-and-scheduling.md) | Visibility deadlines, retries, the DLQ, requeue vs dead-letter, complete-by-token, dedup, delayed publish, cron | 20 min |
| [5. Durable workflows](05-durable-workflows.md) | Runs, history & deterministic replay; workflow + activity workers; signals, timers, cancel, continue-as-new | 30 min |
| [6. Clustering & operations](06-clustering-and-operations.md) | A 3-node cluster, leader-following, failover, singleton coordination, metrics/health, back-pressure | 20 min |

Each part stands on the one before it. If you only have 30 minutes, do parts 1–3.

## Prerequisites

- **Go 1.24+** — to build and run the broker (one static binary, no other services).
- **Python 3.9+** *or* **Node 18+** — whichever SDK you'll follow. Snippets are
  given for both; pick one and stick with it (the APIs mirror each other).
- A terminal. That's it — there is no database, message broker, or scheduler to
  install. Rota *is* all of those, embedded.

## Conventions

- Every hands-on step shows **Python** and **TypeScript** under separate
  sub-headings. The two SDKs are deliberately symmetric, so you can read either.
- The broker listens on **`127.0.0.1:7100`** (gRPC) and **`:7101`** (metrics +
  dashboard) throughout — these are the SDK defaults, so client code can omit the
  address.
- `$` denotes a shell prompt; run commands from the repo root unless noted.

Ready? Start with [**Part 1 — Getting running**](01-getting-running.md).
