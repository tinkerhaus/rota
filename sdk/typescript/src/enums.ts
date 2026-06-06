/**
 * Re-exports of the proto enums as runtime constant objects (numeric values),
 * so callers can write, e.g., ``ev.eventType === HistoryEventType.HET_ACTIVITY_COMPLETED``
 * or ``ctl.setPolicy(lane, { kind: PolicyKind.WFQ })``.
 *
 * Each name is both a value (the const object) and a type (the numeric union),
 * exactly as generated from the proto.
 */

export { HistoryEventType } from "./generated/rota/v1/HistoryEventType.js";
export { WorkflowStatus } from "./generated/rota/v1/WorkflowStatus.js";
export { MessageState } from "./generated/rota/v1/MessageState.js";
export { NackMode } from "./generated/rota/v1/NackMode.js";
export { Outcome } from "./generated/rota/v1/Outcome.js";
export { ControlKind } from "./generated/rota/v1/ControlKind.js";
export { ErrorCode } from "./generated/rota/v1/ErrorCode.js";
export { PolicyKind } from "./generated/rota/v1/PolicyKind.js";
export { PolicyMode } from "./generated/rota/v1/PolicyMode.js";
export { MisfirePolicy } from "./generated/rota/v1/MisfirePolicy.js";
