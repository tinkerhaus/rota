/**
 * Signal errors a worker handler can throw to steer a message's outcome.
 *
 * These are domain-neutral control-flow signals, not real errors:
 *
 * - Throw {@link Requeue} to put the message back with no attempt penalty
 *   (transient back-pressure: shutting down, rate-limited, circuit open).
 * - Throw {@link DeadLetter} to terminally fail the message into the DLQ.
 *
 * Returning normally from a handler acks the message. Throwing any *other*
 * error nacks it with RETRY (attempt++ and auto-dead-letter at max attempts).
 */

/** Base class for all SDK-raised errors and signals. */
export class RotaError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = "RotaError";
  }
}

/**
 * Requeue the in-flight message without penalizing its attempt count.
 *
 * Maps to a Nack with mode ``REQUEUE_NO_PENALTY``. An optional ``delay``
 * (seconds) requests a redelivery delay.
 */
export class Requeue extends RotaError {
  readonly delay?: number;
  readonly reason: string;

  constructor(opts: { delay?: number; reason?: string } = {}) {
    super(opts.reason || "requeue");
    this.name = "Requeue";
    this.delay = opts.delay;
    this.reason = opts.reason ?? "";
  }
}

/**
 * Terminally fail the in-flight message into the dead-letter queue.
 *
 * Maps to a Nack with mode ``DEAD_LETTER``. Optional ``meta`` is attached as
 * failure metadata and carried into the DLQ record.
 */
export class DeadLetter extends RotaError {
  readonly meta: Record<string, string>;
  readonly reason: string;

  constructor(opts: { meta?: Record<string, string>; reason?: string } = {}) {
    super(opts.reason || "dead_letter");
    this.name = "DeadLetter";
    this.meta = opts.meta ? { ...opts.meta } : {};
    this.reason = opts.reason ?? "";
  }
}

/** Raised internally when an RPC hits a follower; carries the leader address. */
export class NotLeaderError extends RotaError {
  readonly leaderAddr: string;
  readonly detail: string;

  constructor(leaderAddr = "", detail = "") {
    super(detail || "not leader");
    this.name = "NotLeaderError";
    this.leaderAddr = leaderAddr;
    this.detail = detail;
  }
}
