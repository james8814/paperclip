import type { IssueUnblockDescriptor } from "@paperclipai/shared";

export const ROUTABLE_BLOCKED_ROLLOUT_AT = new Date("2026-07-23T18:13:03.000Z");

type RoutableBlockedIssue = {
  id: string;
  status: string;
  unblockDescriptor?: IssueUnblockDescriptor | null;
  blockedTransitionAt?: Date | null;
  blockedOwnerNotifiedAt?: Date | null;
};

type ProspectiveBlockedIssue = RoutableBlockedIssue & {
  status: "blocked";
  blockedTransitionAt: Date;
};

export function isProspectiveBlockedTransition(issue: RoutableBlockedIssue): issue is ProspectiveBlockedIssue {
  return issue.status === "blocked" &&
    Boolean(issue.blockedTransitionAt && issue.blockedTransitionAt >= ROUTABLE_BLOCKED_ROLLOUT_AT);
}

export async function deliverAgentUnblockNotification(input: {
  issue: RoutableBlockedIssue;
  wakeup: (agentId: string, options: {
    source: "automation";
    triggerDetail: "system";
    reason: "issue_unblock_requested";
    idempotencyKey: string;
    payload: { issueId: string; action: string };
    contextSnapshot: { wakeReason: "issue_unblock_requested"; issueId: string; taskId: string };
  }) => Promise<unknown>;
  markNotified: (notifiedAt: Date) => Promise<unknown>;
  /** Present when the caller can identify the writer's own run; when it
   * returns true the current legitimate live run of this issue made itself
   * the unblock owner and is already waiting on the outcome, so no successor
   * run is queued for it. Writers other than the owner agent, owner runs on
   * other issues, and writes without a run keep the notification. */
  isOwnerCurrentIssueRun?: (ownerAgentId: string) => Promise<boolean>;
  now?: () => Date;
}) {
  const { issue } = input;
  if (!isProspectiveBlockedTransition(issue) || !issue.unblockDescriptor || issue.blockedOwnerNotifiedAt) {
    return false;
  }

  const owner = issue.unblockDescriptor.owner;
  if (owner === "board" || !("agentId" in owner)) return false;

  // This issue's current legitimate live run set itself as the unblock owner
  // and reported blocked; the wait is already owned by that run, so do not
  // queue a successor for it. Consulted only for this writer: anyone else,
  // or the same owner from a run on another issue, still gets notified.
  if (input.isOwnerCurrentIssueRun && (await input.isOwnerCurrentIssueRun(owner.agentId)))
    return false;

  await input.wakeup(owner.agentId, {
    source: "automation",
    triggerDetail: "system",
    reason: "issue_unblock_requested",
    idempotencyKey: `issue-unblock:${issue.id}:${issue.blockedTransitionAt.toISOString()}`,
    payload: { issueId: issue.id, action: issue.unblockDescriptor.action },
    contextSnapshot: { wakeReason: "issue_unblock_requested", issueId: issue.id, taskId: issue.id },
  });
  await input.markNotified((input.now ?? (() => new Date()))());
  return true;
}

// The writer's run must be the owner agent's own, currently running, and
// bound to this issue; an unreadable run or any mismatch counts as "not the
// owner's run" and keeps the notification.
export function isOwnerCurrentIssueRun(
  run: {
    id: string;
    companyId: string;
    agentId: string;
    status: string;
    nativeIssueId?: string | null;
    contextSnapshot?: Record<string, unknown> | null;
  } | null | undefined,
  input: {
    issue: {
      id: string;
      companyId: string;
      executionRunId?: string | null;
    };
    ownerAgentId: string;
  },
): boolean {
  if (!run || run.companyId !== input.issue.companyId || run.agentId !== input.ownerAgentId || run.status !== "running")
    return false;
  const context = run.contextSnapshot && typeof run.contextSnapshot === "object" ? run.contextSnapshot : {};
  return run.nativeIssueId === input.issue.id ||
    (context.issueId ?? context.taskId) === input.issue.id ||
    input.issue.executionRunId === run.id;
}
