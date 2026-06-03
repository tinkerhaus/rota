# 0008. gRPC/HTTP-2 transport: bidi Work stream + unary control plane, with credit flow control

## Decision

Use gRPC over HTTP/2 with protobuf: a Broker service (Publish/PublishBatch + bidi Work RPC) and a Control service (all unary operator RPCs). Credit is the only flow-control primitive (additive integer; credit=1 = strictly serial). HTTP/2 keepalive PINGs hold idle pull streams alive during multi-minute tasks (no app-side heartbeat). Context cancellation = clean graceful drain. NotLeader redirects writes to the leader. One proto schema serves both wire and on-disk encoding.

## Rationale

Many consumers are strictly serial (they scale by process count rather than in-process parallelism), so credit is a sufficient flow-control primitive and credit=1 expresses a prefetch=1, strictly-serial consumer. HTTP/2 keepalive removes the need for an app-side heartbeat to keep an idle pull stream from being torn down during long-running task processing, which a client without transport-level keepalive would otherwise have to pump manually. Reusing the protobuf schema for storage avoids a second serialization stack and lets messages/specs evolve without migrations. Two services keep the streaming hot path uncluttered and let ops tooling depend only on Control.

## Consequences

Credit-as-delta can desync between SDK and broker across a reconnect (leases re-leased over the gap), needing a Credit reconciliation frame + absolute re-advertise on resume. Pushed PAUSE_LANE frames are best-effort to connected consumers, so a late-connecting consumer must learn a paused lane on (re)subscribe. Per-lane multi-raft later may make leader-ship per-lane, so the NotLeader redirect detail must stay forward-compatible.
