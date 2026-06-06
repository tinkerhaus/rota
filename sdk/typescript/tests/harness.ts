/**
 * Test harness: build the `rota` binary, boot single-node or multi-node clusters
 * in subprocesses, and tear them down. Mirrors the Python SDK's e2e fixture and
 * the Go integration cluster setup.
 */

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createServer } from "node:net";
import { mkdtempSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { Control } from "../dist/index.js";

// tests/ -> sdk/typescript -> sdk -> repo root.
const REPO_ROOT = resolve(fileURLToPath(new URL("../../..", import.meta.url)));

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function haveGo(): boolean {
  return spawnSync("go", ["version"], { encoding: "utf8" }).status === 0;
}

export function buildBinary(): string {
  const out = join(tmpdir(), "rota_ts_e2e");
  const r = spawnSync("go", ["build", "-o", out, "./cmd/rota"], {
    cwd: REPO_ROOT,
    encoding: "utf8",
    timeout: 300_000,
  });
  if (r.status !== 0) throw new Error(`go build failed: ${(r.stderr || "").slice(0, 500)}`);
  return out;
}

function freePort(): Promise<number> {
  return new Promise((res, rej) => {
    const srv = createServer();
    srv.unref();
    srv.on("error", rej);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      if (addr && typeof addr === "object") srv.close(() => res(addr.port));
      else srv.close(() => rej(new Error("could not get a free port")));
    });
  });
}

const freeAddr = async () => `127.0.0.1:${await freePort()}`;

async function waitReady(addr: string, timeoutMs = 25_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    const ctl = new Control(addr, { timeout: 2.0, maxRetries: 0 });
    try {
      if ((await ctl.health()).serving) {
        ctl.close();
        return;
      }
    } catch (e) {
      lastErr = e;
    } finally {
      ctl.close();
    }
    await sleep(100);
  }
  throw new Error(`broker at ${addr} not ready in ${timeoutMs}ms: ${String(lastErr)}`);
}

// ── single node ──────────────────────────────────────────────────────────────

export interface Broker {
  addr: string;
  stop(): Promise<void>;
}

export async function startBroker(binary: string): Promise<Broker> {
  const grpcAddr = await freeAddr();
  const metricsAddr = await freeAddr();
  const dataDir = mkdtempSync(join(tmpdir(), "rota_ts_e2e_data_"));
  let output = "";
  const proc = spawn(
    binary,
    ["serve", "--grpc", grpcAddr, "--metrics", metricsAddr, "--data", dataDir],
    { cwd: REPO_ROOT, stdio: ["ignore", "pipe", "pipe"] },
  );
  proc.stdout?.on("data", (d) => (output += d));
  proc.stderr?.on("data", (d) => (output += d));

  try {
    await waitReady(grpcAddr);
  } catch (e) {
    proc.kill("SIGKILL");
    throw new Error(`${e}\n--- broker output ---\n${output}`);
  }
  return { addr: grpcAddr, stop: () => stopProc(proc, [dataDir]) };
}

// ── multi-node cluster ───────────────────────────────────────────────────────

export interface Cluster {
  /** node id -> gRPC address */
  grpcAddrs: Record<string, string>;
  /** Resolve the current leader's gRPC address. */
  leader(): Promise<{ id: string; addr: string }>;
  /** Resolve some follower's gRPC address. */
  follower(): Promise<{ id: string; addr: string }>;
  stop(): Promise<void>;
}

/**
 * Boot an N-node Raft cluster (default 3). Node n1 bootstraps the initial voter
 * set; the rest join. Mirrors test/integration/cluster_test.go's setup.
 */
export async function startCluster(binary: string, n = 3): Promise<Cluster> {
  const ids = Array.from({ length: n }, (_, i) => `n${i + 1}`);
  const raftAddrs: Record<string, string> = {};
  const grpcAddrs: Record<string, string> = {};
  const metricsAddrs: Record<string, string> = {};
  for (const id of ids) {
    raftAddrs[id] = await freeAddr();
    grpcAddrs[id] = await freeAddr();
    metricsAddrs[id] = await freeAddr();
  }
  const peersArg = ids.map((id) => `${id}=${raftAddrs[id]}`).join(",");
  const grpcPeersArg = ids.map((id) => `${id}=${grpcAddrs[id]}`).join(",");

  const procs: ChildProcess[] = [];
  const dataDirs: string[] = [];
  let output = "";
  for (const id of ids) {
    const dataDir = mkdtempSync(join(tmpdir(), `rota_ts_cluster_${id}_`));
    dataDirs.push(dataDir);
    const args = [
      "serve",
      "--id", id,
      "--raft", raftAddrs[id],
      "--grpc", grpcAddrs[id],
      "--metrics", metricsAddrs[id],
      "--data", dataDir,
      "--bootstrap", id === "n1" ? "true" : "false",
      "--grpc-peers", grpcPeersArg,
    ];
    if (id === "n1") args.push("--peers", peersArg);
    const proc = spawn(binary, args, { cwd: REPO_ROOT, stdio: ["ignore", "pipe", "pipe"] });
    proc.stdout?.on("data", (d) => (output += `[${id}] ${d}`));
    proc.stderr?.on("data", (d) => (output += `[${id}] ${d}`));
    procs.push(proc);
  }

  const stop = () => stopProc(procs, dataDirs);

  // Wait until some node reports an elected leader.
  const deadline = Date.now() + 40_000;
  let leaderId = "";
  while (Date.now() < deadline && !leaderId) {
    for (const id of ids) {
      const ctl = new Control(grpcAddrs[id], { timeout: 2.0, maxRetries: 0 });
      try {
        const info = await ctl.describeCluster();
        if (info.leaderId) {
          leaderId = info.leaderId;
          break;
        }
      } catch {
        // node not up yet
      } finally {
        ctl.close();
      }
    }
    if (!leaderId) await sleep(150);
  }
  if (!leaderId) {
    await stop();
    throw new Error(`cluster did not elect a leader\n--- output ---\n${output}`);
  }

  const currentLeaderId = async (): Promise<string> => {
    for (const id of ids) {
      const ctl = new Control(grpcAddrs[id], { timeout: 2.0, maxRetries: 0 });
      try {
        const info = await ctl.describeCluster();
        if (info.leaderId) return info.leaderId;
      } catch {
        // try next
      } finally {
        ctl.close();
      }
    }
    throw new Error("no leader currently reachable");
  };

  return {
    grpcAddrs,
    async leader() {
      const id = await currentLeaderId();
      return { id, addr: grpcAddrs[id] };
    },
    async follower() {
      const lid = await currentLeaderId();
      const fid = ids.find((id) => id !== lid);
      if (!fid) throw new Error("no follower found");
      return { id: fid, addr: grpcAddrs[fid] };
    },
    stop,
  };
}

// ── process teardown ─────────────────────────────────────────────────────────

async function stopProc(procs: ChildProcess | ChildProcess[], dataDirs: string[]): Promise<void> {
  const list = Array.isArray(procs) ? procs : [procs];
  await Promise.all(
    list.map(
      (proc) =>
        new Promise<void>((res) => {
          if (proc.exitCode !== null) return res();
          const t = setTimeout(() => {
            proc.kill("SIGKILL");
            res();
          }, 5000);
          proc.on("exit", () => {
            clearTimeout(t);
            res();
          });
          proc.kill("SIGTERM");
        }),
    ),
  );
  for (const d of dataDirs) if (existsSync(d)) rmSync(d, { recursive: true, force: true });
}
