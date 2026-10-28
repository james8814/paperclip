import { afterEach, describe, expect, it, vi } from "vitest";
vi.mock("./execution-continuation.js", () => ({ buildExecutionContinuation: vi.fn(async () => ({})) }));
import { buildExecutionContinuation } from "./execution-continuation.js";
import { validateExecutionReconciliation } from "./execution-recovery-resolution.js";

const identity = { companyId: "company-fixture", issueId: "issue-fixture", agentId: "agent-fixture" };
const run = { id: "run-fixture", agentId: identity.agentId, nativeIssueId: identity.issueId,
  status: "cancelled", resultJson: null, processPid: 42001, processGroupId: 42000,
  processStartedAt: new Date("2026-10-27T07:15:02Z"), finishedAt: new Date("2026-10-27T07:44:43Z") };
function dbFor(coordinator: unknown, leases: unknown[]) {
  const rows = [[run], [{ assigneeAgentId: identity.agentId }], coordinator ? [coordinator] : [], leases];
  return { select: vi.fn(() => {
    const result = rows.shift();
    const query = { from: () => query, where: () => query, limit: async () => result,
      then: (resolve: (value: unknown) => unknown) => Promise.resolve(result).then(resolve) };
    return query;
  }) };
}
afterEach(() => { vi.restoreAllMocks(); vi.clearAllMocks(); });
describe("reconciliation preserves independent authority", () => {
  it.each([
    [{ leaseOwner: "coordinator-fixture" }, [], "coordinator"],
    [{ failureDetail: { successorRunId: "successor-fixture" } }, [], "coordinator"],
    [null, [{ id: "lease-fixture" }], "environment"],
  ])("retains coordinator/successor/lease hold %#", async (coordinator, leases, reason) => {
    // Absent PID and group are an already cleared liveness result. No OS call.
    vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(Error("gone"), { code: "ESRCH" }); });
    await expect(validateExecutionReconciliation({ ...identity, db: dbFor(coordinator, leases as unknown[]) as never,
      sourceRunId: run.id, decision: { runId: run.id } as never })).rejects.toThrow(String(reason));
    expect(buildExecutionContinuation).not.toHaveBeenCalled();
  });
});
