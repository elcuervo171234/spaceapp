import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { WorkflowNotFoundError, type Client } from "@temporalio/client";
import { temporal } from "@temporalio/proto";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { bundleWorkflowCode, Worker, type WorkerOptions } from "@temporalio/worker";
import { MEMORY_MAINTENANCE_TASK_QUEUE } from "../../src/memory-maintenance-config.js";
import { MEMORY_MUTATION_TASK_QUEUE } from "../../src/memory-mutation-config.js";

export const recordingWorkflowHistories = process.env.SPACE_TEMPORAL_RECORD_HISTORIES === "1";
export const workflowHistoryDirectory = fileURLToPath(new URL("../fixtures/workflow-histories/", import.meta.url));

type WorkflowBundle = Awaited<ReturnType<typeof bundleWorkflowCode>>;
type WorkflowHistory = temporal.api.history.v1.IHistory;
type Activities = NonNullable<WorkerOptions["activities"]>;
type HistoryHandle = {
  workflowId: string;
  firstExecutionRunId?: string;
  fetchHistory(): Promise<WorkflowHistory>;
};

export type TemporalSandboxContext = {
  env: TestWorkflowEnvironment;
  client: Client;
  taskQueue: string;
};

export type RecordedWorkflowHistory = {
  format: 1;
  label: string;
  workflowId: string;
  workflowType: string;
  history: Record<string, unknown>;
};

let environment: TestWorkflowEnvironment | undefined;
const bundles = new Map<string, Promise<WorkflowBundle>>();

export function getTemporalWorkflowBundle(options: { legacy?: boolean; negative?: boolean } = {}): Promise<WorkflowBundle> {
  const name = options.negative
    ? "temporal-negative-workflow.ts"
    : options.legacy ? "temporal-legacy-workflows.ts" : "temporal-workflows.ts";
  let bundle = bundles.get(name);
  if (!bundle) {
    bundle = bundleWorkflowCode({ workflowsPath: fileURLToPath(new URL(name, import.meta.url)) });
    bundles.set(name, bundle);
  }
  return bundle;
}

export async function startTemporalSandbox(): Promise<void> {
  if (environment) throw new Error("The Temporal sandbox is already running.");
  environment = await TestWorkflowEnvironment.createTimeSkipping();
  // Build once per suite. Every case executes the real production bundle.
  await getTemporalWorkflowBundle();
}

export async function stopTemporalSandbox(): Promise<void> {
  const current = environment;
  environment = undefined;
  try {
    if (current) await current.teardown();
  } finally {
    bundles.clear();
  }
}

function sandboxEnvironment(): TestWorkflowEnvironment {
  if (!environment) throw new Error("Initialize the Temporal sandbox in beforeAll before running a case.");
  return environment;
}

// Bind every ordinary method to the genuine SDK object. Only start/execute are
// wrapped to remember IDs for cleanup; signals, results, histories and timers
// still cross the real Temporal client/server boundary.
function trackedClient(client: Client, workflowIds: Set<string>): Client {
  const workflows = new Proxy(client.workflow, {
    get(target, property) {
      const value = Reflect.get(target, property, target);
      if ((property === "start" || property === "execute") && typeof value === "function") {
        return (...args: unknown[]) => {
          const options = args[1] as { workflowId?: string } | undefined;
          if (options?.workflowId) workflowIds.add(options.workflowId);
          return Reflect.apply(value, target, args);
        };
      }
      return typeof value === "function" ? value.bind(target) : value;
    }
  });
  return new Proxy(client, {
    get(target, property) {
      if (property === "workflow") return workflows;
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    }
  });
}

async function terminateStartedWorkflows(client: Client, workflowIds: Set<string>): Promise<void> {
  const failures: unknown[] = [];
  for (const workflowId of workflowIds) {
    try {
      // No run ID: terminate the CURRENT execution, including a continued run.
      await client.workflow.getHandle(workflowId).terminate("Synthetic test case cleanup");
    } catch (error) {
      // A completed execution is not an active resource to clean up. Other
      // errors must fail the test rather than concealing a leaked execution.
      if (!(error instanceof WorkflowNotFoundError)) failures.push(error);
    }
  }
  if (failures.length) throw new AggregateError(failures, "Could not clean up synthetic workflow executions.");
}

export async function withTemporalWorker<T>(
  activities: Activities,
  run: (context: TemporalSandboxContext) => Promise<T>,
  options: { legacy?: boolean; memory?: boolean } = {}
): Promise<T> {
  const env = sandboxEnvironment();
  const taskQueue = `space-temporal-test-${randomUUID()}`;
  const workflowIds = new Set<string>();
  const client = trackedClient(env.client, workflowIds);
  const workflowBundle = await getTemporalWorkflowBundle({ legacy: options.legacy });
  const queues = options.memory
    ? [taskQueue, MEMORY_MAINTENANCE_TASK_QUEUE, MEMORY_MUTATION_TASK_QUEUE]
    : [taskQueue];
  const workers: Worker[] = [];
  try {
    for (const queue of new Set(queues)) {
      workers.push(await Worker.create({
        connection: env.nativeConnection,
        workflowBundle,
        taskQueue: queue,
        activities,
        // Tests exercise actual activity cancellation, with short heartbeats
        // instead of waiting for the production-sized throttling interval.
        maxHeartbeatThrottleInterval: "100 milliseconds",
        defaultHeartbeatThrottleInterval: "100 milliseconds",
        shutdownGraceTime: "1 second"
      }));
    }
    // The microtask starts only after all real workers have been constructed
    // and their runUntil calls registered. Secondary memory queues stay alive
    // for the same callback, so production executeChild/taskQueue routing runs.
    const result = Promise.resolve().then(async () => {
      let callbackFailure: unknown;
      try {
        return await run({ env, client, taskQueue });
      } catch (error) {
        callbackFailure = error;
        throw error;
      } finally {
        try {
          await terminateStartedWorkflows(env.client, workflowIds);
        } catch (cleanupFailure) {
          if (callbackFailure !== undefined) {
            throw new AggregateError([callbackFailure, cleanupFailure], "Temporal case and cleanup both failed.");
          }
          throw cleanupFailure;
        }
      }
    });
    const executions = workers.map((worker) => worker.runUntil(result));
    try {
      await Promise.all(executions);
      return await result;
    } finally {
      for (const worker of workers) if (worker.getState() === "RUNNING") worker.shutdown();
      await Promise.allSettled(executions);
    }
  } catch (error) {
    for (const worker of workers) if (worker.getState() === "RUNNING") worker.shutdown();
    throw error;
  }
}

export async function firstRunHistory(handle: HistoryHandle): Promise<WorkflowHistory> {
  if (!handle.firstExecutionRunId) throw new Error(`Missing first execution run ID for ${handle.workflowId}.`);
  return sandboxEnvironment().client.workflow.getHandle(handle.workflowId, handle.firstExecutionRunId).fetchHistory();
}

export async function recordWorkflowHistory(
  label: string,
  handle: HistoryHandle,
  options: { firstRun?: boolean } = {}
): Promise<void> {
  if (!recordingWorkflowHistories) return;
  if (!/^[A-Za-z][A-Za-z0-9.-]*$/.test(label)) throw new Error(`Invalid synthetic history label: ${label}`);
  const history = options.firstRun ? await firstRunHistory(handle) : await handle.fetchHistory();
  const workflowType = history.events?.find((event) => event.workflowExecutionStartedEventAttributes)
    ?.workflowExecutionStartedEventAttributes?.workflowType?.name;
  if (!workflowType) throw new Error(`No workflow start event in history for ${label}.`);
  // Protobuf's JSON form preserves 64-bit IDs and base64 payloads portably.
  // Normal test execution NEVER updates golden files.
  const portable = temporal.api.history.v1.History.fromObject(history as Record<string, unknown>).toJSON();
  const fixture: RecordedWorkflowHistory = {
    format: 1,
    label,
    workflowId: handle.workflowId,
    workflowType,
    history: portable
  };
  await mkdir(workflowHistoryDirectory, { recursive: true });
  await writeFile(join(workflowHistoryDirectory, `${label}.json`), `${JSON.stringify(fixture, null, 2)}\n`, "utf8");
}
