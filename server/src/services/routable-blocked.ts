import { createHash } from "node:crypto";
import type { IssueUnblockDescriptor } from "@paperclipai/shared";

export const ROUTABLE_BLOCKED_ROLLOUT_AT = new Date("2026-07-23T18:13:03.000Z");

export type UnblockNotificationState = { fingerprint: string; generation: number };

type RoutableBlockedIssue = {
  id: string;
  status: string;
  unblockDescriptor?: IssueUnblockDescriptor | null;
  blockedTransitionAt?: Date | null;
  blockedOwnerNotifiedAt?: Date | null;
};

type BlockedContentIssue = RoutableBlockedIssue & {
  companyId: string;
  title?: string | null;
  description?: string | null;
  blockedNotificationState?: UnblockNotificationState | null;
};

// Only substantive content participates; status, updatedAt,
// blockedTransitionAt, notification markers, and status versions stay out of
// the key. Comments, card answers, dependency completions, and manual
// resumes are delivered by their own native events and never merge into
// this system notification.
export function blockedContentFingerprint(issue: BlockedContentIssue): string {
  const descriptor = issue.unblockDescriptor;
  const owner = descriptor?.owner;
  const canonicalOwner = !owner ? null : owner === "board" ? "board" :
    "agentId" in owner ? ["agent", owner.agentId] : ["user", owner.userId];
  return createHash("sha256").update(JSON.stringify([
    1, issue.companyId, issue.id, canonicalOwner, descriptor?.action ?? null,
    issue.title ?? "", issue.description ?? null,
  ])).digest("hex");
}

export function agentUnblockFingerprint(issue: BlockedContentIssue): string | null {
  if (!issue.unblockDescriptor || issue.unblockDescriptor.owner === "board" ||
    !("agentId" in issue.unblockDescriptor.owner) || !issue.unblockDescriptor.owner.agentId)
    return null;
  return blockedContentFingerprint(issue);
}

function assertNotificationState(state: UnblockNotificationState | null | undefined): void {
  if (state && (!Number.isSafeInteger(state.generation) || state.generation < 1 ||
    state.generation >= Number.MAX_SAFE_INTEGER || !/^[a-f0-9]{64}$/.test(state.fingerprint)))
    throw new Error("Invalid native unblock notification state");
}

// Updated only inside the native issue.update row lock. Leaving blocked
// keeps the status but not the public descriptor. generation is a content
// protocol sequence, not a cycle or retry threshold; A to B to A transitions
// each bump it, including intermediate board/user states.
export function advanceBlockedNotificationState(
  existing: BlockedContentIssue,
  patch: Record<string, unknown>,
): UnblockNotificationState | null {
  const state = existing.blockedNotificationState ?? null;
  assertNotificationState(state);
  // Drizzle ignores undefined update fields; the version follows only the
  // content that would actually be written.
  const next = { ...existing, ...Object.fromEntries(Object.entries(patch).filter(([, value]) => value !== undefined)) } as BlockedContentIssue;
  if (next.status !== "blocked")
    return state;
  const fingerprint = blockedContentFingerprint(next);
  return state?.fingerprint === fingerprint ? state : {
    fingerprint, generation: (state?.generation ?? 0) + 1,
  };
}

// The caller holds the native issue row lock; a stale route or queue payload
// must not notify for another content version.
export function planAgentUnblockNotification(
  issue: BlockedContentIssue,
  agentId: string,
  payload: Record<string, unknown> | null | undefined,
): { kind: "stale" } | { kind: "enqueue"; idempotencyKey: string; payload: Record<string, unknown> } {
  const fingerprint = agentUnblockFingerprint(issue);
  const state = issue.blockedNotificationState;
  assertNotificationState(state);
  const descriptor = issue.unblockDescriptor;
  const ownerAgentId = descriptor && descriptor.owner !== "board" && "agentId" in descriptor.owner
    ? descriptor.owner.agentId
    : undefined;
  if (!isProspectiveBlockedTransition(issue) ||
    ownerAgentId !== agentId ||
    !fingerprint || !state || payload?.issueId !== issue.id || state.fingerprint !== fingerprint ||
    payload?.unblockFingerprint !== fingerprint || payload?.unblockGeneration !== state.generation) {
    return { kind: "stale" };
  }
  return {
    kind: "enqueue",
    idempotencyKey: `issue-unblock-content:${issue.id}:${state.generation}:${fingerprint}`,
    payload: { ...payload, issueId: issue.id, action: descriptor.action,
      unblockFingerprint: fingerprint, unblockGeneration: state.generation },
  };
}

type ProspectiveBlockedIssue = RoutableBlockedIssue & {
  status: "blocked";
  blockedTransitionAt: Date;
};

export function isProspectiveBlockedTransition(issue: RoutableBlockedIssue): issue is ProspectiveBlockedIssue {
  return issue.status === "blocked" &&
    Boolean(issue.blockedTransitionAt && issue.blockedTransitionAt >= ROUTABLE_BLOCKED_ROLLOUT_AT);
}

export async function deliverAgentUnblockNotification(input: {
  issue: BlockedContentIssue;
  wakeup: (agentId: string, options: {
    source: "automation";
    triggerDetail: "system";
    reason: "issue_unblock_requested";
    idempotencyKey: string;
    payload: { issueId: string; action: string; unblockFingerprint: string; unblockGeneration: number };
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
  if (!isProspectiveBlockedTransition(issue) || !issue.unblockDescriptor) {
    return false;
  }
  const owner = issue.unblockDescriptor.owner;
  if (owner === "board" || !("agentId" in owner)) return false;
  const fingerprint = agentUnblockFingerprint(issue);
  const state = issue.blockedNotificationState;
  assertNotificationState(state);
  if (!state || state.fingerprint !== fingerprint)
    throw new Error("Missing native unblock content version");

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
    idempotencyKey: `issue-unblock-content:${issue.id}:${state.generation}:${fingerprint}`,
    payload: { issueId: issue.id, action: issue.unblockDescriptor.action,
      unblockFingerprint: fingerprint, unblockGeneration: state.generation },
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
