# SDK parity matrix

Rota's public SDKs are intentionally thin and domain-neutral. They expose the
broker, control, and workflow gRPC APIs; dashboard-only HTTP/JSON inspection
stays in the dashboard and CLI.

Legend: yes = supported directly; partial = possible but not wrapped as a
dedicated helper; dashboard/CLI = intentionally outside the SDK.

| Capability | Python SDK | TypeScript SDK | Notes |
|---|---:|---:|---|
| Publish one message | yes | yes | `Publisher.publish` |
| Publish batch, atomic or best-effort | yes | yes | `publish_batch` / `publishBatch` |
| Producer dedup key | yes | yes | publish option |
| Delay / absolute eligibility | yes | yes | `delay`, `not_before` / `notBefore` |
| TTL and max attempts | yes | yes | publish options |
| Group weight and batch size upsert | yes | yes | publish option or control-plane config |
| Work stream worker loop | yes | yes | serial handler dispatch with configurable credit |
| Ack on handler success | yes | yes | automatic |
| Retry on handler error | yes | yes | automatic `Nack(RETRY)` |
| No-penalty requeue | yes | yes | `Requeue` exception |
| Dead-letter from handler | yes | yes | `DeadLetter` exception |
| Extend visibility | yes | yes | `msg.extend(...)` |
| In-stream complete-by-token | yes | yes | `msg.complete(...)` |
| Off-stream complete-by-token | yes | yes | `Publisher.complete(...)` |
| Leader following for unary RPCs | yes | yes | retries `NOT_LEADER` against advertised leader |
| Leader following for work streams | yes | yes | stream redirect handling |
| TLS channel credentials | yes | yes | pass native gRPC credentials |
| Static auth token metadata | yes | yes | `auth_token=` / `{ authToken }` |
| Extra per-call metadata | yes | yes | `metadata=` / `{ metadata }` |
| Group config and lifecycle | yes | yes | set/get/pause/resume/cancel/purge/reap/teardown |
| Lane rate limit and pause/resume | yes | yes | control plane |
| Scheduling policy set/get/validate | yes | yes | built-in, CEL, WASM payloads |
| Cron schedule/list/pause/delete | yes | yes | control plane |
| Singleton leases with fencing | yes | yes | acquire/renew/release |
| Stats, cluster info, health | yes | yes | control plane |
| Start/signal/cancel workflow | yes | yes | workflow client |
| Get workflow run/history/list runs | yes | yes | workflow client |
| Workflow worker loop | yes | yes | computes prefix checksum before responding |
| Activity worker loop | yes | yes | reports result/failure into history |
| Workflow commands | yes | yes | activity, timer, continue-as-new, complete, fail |
| List groups / leases / DLQ | dashboard/CLI | dashboard/CLI | served by HTTP/JSON dashboard routes and `rota` CLI |
| Peek messages / redrive DLQ | dashboard/CLI | dashboard/CLI | operational surface, not app SDK |
| Fairness observatory and policy health reads | dashboard/CLI | dashboard/CLI | dashboard visualization surface |

## Compatibility expectations

- New broker/control/workflow RPCs should appear in both SDKs before being called
  stable.
- Runtime examples should exist for both languages when a feature affects normal
  application code.
- Dashboard-only endpoints may stay out of SDKs when they are operator concerns
  rather than application concerns.
- Transport features such as TLS, metadata, and auth tokens should remain
  available on every client type, including worker loops.

## Current example coverage

- [`examples/python/fair_email_worker.py`](../examples/python/fair_email_worker.py)
  shows fair broker publishing and worker consumption.
- [`examples/typescript/background-worker.ts`](../examples/typescript/background-worker.ts)
  shows the same broker pattern in Node.
- [`examples/python/payment_workflow.py`](../examples/python/payment_workflow.py)
  shows workflow/activity workers and a completed run.
- [`examples/typescript/payment-workflow.ts`](../examples/typescript/payment-workflow.ts)
  shows the same durable workflow pattern in Node.
