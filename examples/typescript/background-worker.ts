#!/usr/bin/env tsx
/**
 * Fair broker example: publish background jobs across tenants and drain them.
 *
 * From this repository, run `npm run build` in sdk/typescript first. In an
 * application, replace the relative import with `from "rota"`.
 */

import { Publisher, Worker } from "../../sdk/typescript/dist/index.js";

const args = new Map(
  process.argv.slice(2).flatMap((arg, i, all) => (arg.startsWith("--") ? [[arg, all[i + 1]]] : [])),
);

const addr = args.get("--addr") ?? process.env.ROTA_ADDR ?? "127.0.0.1:7100";
const lane = args.get("--lane") ?? "example.background";
const messages = Number(args.get("--messages") ?? 18);
const authToken = args.get("--auth-token") ?? process.env.ROTA_TOKEN;
const tenants = ["tenant-a", "tenant-b", "tenant-c"];

let processed = 0;
let resolveDone!: () => void;
const done = new Promise<void>((resolve) => {
  resolveDone = resolve;
});

const worker = new Worker(
  addr,
  lane,
  async (msg) => {
    const job = JSON.parse(msg.payload.toString("utf8"));
    console.log(`handled job msg=${msg.messageId} tenant=${msg.groupId} task=${job.task}`);
    processed += 1;
    if (processed >= messages) resolveDone();
  },
  { credit: 1, installSignalHandler: false, authToken },
);

const run = worker.run();
const pub = new Publisher(addr, { authToken });

try {
  for (let i = 0; i < messages; i++) {
    const tenant = tenants[i % tenants.length];
    await pub.publish(
      lane,
      tenant,
      Buffer.from(JSON.stringify({ task: "sync-account", index: i })),
      { headers: { example: "background-worker" }, dedupKey: `job-${i}` },
    );
    console.log(`queued job tenant=${tenant} index=${i}`);
  }

  await Promise.race([
    done,
    new Promise((_, reject) => setTimeout(() => reject(new Error("timed out")), 10_000)),
  ]);
  console.log(`drained ${processed} jobs from ${tenants.length} tenants`);
} finally {
  worker.stop();
  pub.close();
  await run;
}
