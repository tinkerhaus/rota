#!/usr/bin/env tsx
/**
 * Durable workflow example: schedule one payment activity and complete.
 *
 * From this repository, run `npm run build` in sdk/typescript first. In an
 * application, replace the relative import with `from "rota"`.
 */

import {
  HistoryEventType,
  WorkflowClient,
  WorkflowStatus,
  completeWorkflow,
  runActivityWorker,
  runWorkflowWorker,
  scheduleActivity,
  type HistoryEvent,
} from "../../sdk/typescript/dist/index.js";

const args = new Map(
  process.argv.slice(2).flatMap((arg, i, all) => (arg.startsWith("--") ? [[arg, all[i + 1]]] : [])),
);

const addr = args.get("--addr") ?? process.env.ROTA_ADDR ?? "127.0.0.1:7100";
const workflowType = args.get("--workflow-type") ?? "example.payment";
const activityType = args.get("--activity-type") ?? "example.charge-card";
const tenant = args.get("--tenant") ?? "tenant-a";
const authToken = args.get("--auth-token") ?? process.env.ROTA_TOKEN;
const abort = new AbortController();

const decide = (_runId: number, history: HistoryEvent[]) => {
  const scheduled = history.some((e) => e.eventType === HistoryEventType.HET_ACTIVITY_SCHEDULED);
  const completed = history.some((e) => e.eventType === HistoryEventType.HET_ACTIVITY_COMPLETED);
  const failed = history.some((e) => e.eventType === HistoryEventType.HET_ACTIVITY_FAILED);
  if (failed) return [completeWorkflow(Buffer.from("payment failed"))];
  if (!scheduled) return [scheduleActivity(activityType, Buffer.from('{"amount":4200}'))];
  if (completed) return [completeWorkflow(Buffer.from("payment captured"))];
  return [];
};

const workflowLoop = runWorkflowWorker(addr, workflowType, "wf-example-1", decide, {
  signal: abort.signal,
  authToken,
});
const activityLoop = runActivityWorker(
  addr,
  activityType,
  "act-example-1",
  async (task) => {
    console.log(`charging card run=${task.runId} event=${task.scheduledEventId}`);
    return [Buffer.from("charge-id=ch_123"), true];
  },
  { signal: abort.signal, authToken },
);

const client = new WorkflowClient(addr, { authToken });
try {
  const runId = await client.startWorkflow(workflowType, {
    tenantId: tenant,
    input: Buffer.from("order-123"),
  });
  console.log(`started workflow run=${runId}`);

  const deadline = Date.now() + 15_000;
  let completed = false;
  while (Date.now() < deadline) {
    const run = await client.getRun(runId);
    console.log(`run=${runId} status=${run.status} seq=${run.curHistorySeq}`);
    if (run.status === WorkflowStatus.WF_COMPLETED) {
      completed = true;
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  if (!completed) throw new Error(`workflow ${runId} did not complete before timeout`);
} finally {
  abort.abort();
  client.close();
  await Promise.allSettled([workflowLoop, activityLoop]);
}
