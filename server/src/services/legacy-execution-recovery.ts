import { isPreDispatchReviewWaitVerified } from "./pre-dispatch-review-wait.js";
import { hasWorkspaceRestoreFailure } from "@paperclipai/shared";
import { normalizeMaxTurnStopReason } from "./heartbeat-stop-metadata.js";
import { claimedAdapterType, hasConversationContinuationPolicy } from "./conversation-continuation.js";
import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { and, eq, inArray, isNull, or, sql } from "drizzle-orm";
import { environmentLeases, heartbeatRunEvents, heartbeatRuns, issueRecoveryActions, issues, nativeRunFinalizations, type Db } from "@paperclipai/db";
import { issueRecoveryActionService } from "./issue-recovery-actions.js";
import { parseIssueExecutionState } from "./issue-execution-policy.js";
import { executionFailureRetryCount } from "./execution-recovery-attempt.js";
import { logActivity } from "./activity-log.js";
import { isSupersededConversationRun } from "./agent-conversations.js";
import { issueService } from "./issues.js";
import { parseObject } from "../adapters/utils.js";

type Run = typeof heartbeatRuns.$inferSelect;
export const LEGACY_RECOVERY_CAUSE = "legacy_execution_requires_reconciliation";

type ExecutionEvidenceAssessment = { kind: "unknown" | "execution_evidence" | "never_started"; reason: string };

// Zero-execution is decided only from the run's own complete persisted
// fields: missing columns, startup traces, and unknown results all stay
// held for verification. Exactly three receipt families are released
// precisely, per the native write points: the enqueue-carried
// sessionIdBefore while startedAt is still null, the fixed liveness values
// written by terminal classification, and the dependency-gate cancellation
// codes; every other execution marker must still be empty.
export function assessLegacyExecutionStart(run: Partial<Run> | null | undefined): ExecutionEvidenceAssessment {
  const unknown = (reason: string): ExecutionEvidenceAssessment => ({ kind: "unknown", reason });
  if (!run || run.runtimeMode !== "legacy" || !["queued", "scheduled_retry", "cancelled"].includes(run.status ?? ""))
    return unknown("not_legacy_queue_cancellation");
  if (run.status === "cancelled" && !["issue_reassigned", "issue_assignee_changed", "issue_continuation_waiting_on_review", "execution_reconciliation_required", "issue_dependencies_blocked"].includes(run.errorCode ?? ""))
    return unknown("not_supported_queue_cancellation");
  // The running update writes a non-null startedAt and sessionIdBefore in
  // the same statement; while startedAt is still null that value can only
  // have been carried in at enqueue time.
  const enqueueSession = run.startedAt === null && typeof run.sessionIdBefore === "string";
  // Terminal classification derives the cancelled liveness output only from
  // status/errorCode; an exactly equal value carries no execution
  // information.
  const terminalLiveness = run.status === "cancelled" && run.livenessState === "failed" &&
    run.livenessReason === `Run ended with cancelled (${run.errorCode})` && run.lastUsefulActionAt === null && run.nextAction === null;
  const emptyFields = ["startedAt", "processPid", "processGroupId", "processStartedAt", "usageJson", "logBytes",
    "stdoutExcerpt", "stderrExcerpt", "lastOutputAt", "lastOutputBytes", "exitCode", "signal",
    "sessionIdBefore", "sessionIdAfter", "externalRunId", "controllerBootId", "controllerLeaseExpiresAt", "runnerInstanceId", "nativeSessionId",
    "logStore", "logRef", "logSha256", "lastOutputStream", "executionStage", "runtimeModeResolvedAt", "nativeIssueId",
    "executionControlDeadlineAt", "runtimeModeResolverVersion", "runtimeModeReason", "runnerProfileJson", "driverKind", "driverVersion",
    "completionContractId", "completionContractSha256", "nativePhase", "nativePhaseUpdatedAt", "livenessState", "livenessReason",
    "lastUsefulActionAt", "nextAction", "issueCommentSatisfiedByCommentId", "issueCommentRetryQueuedAt"];
  const fields = run as Record<string, unknown>;
  for (const field of emptyFields) {
    if (!Object.hasOwn(fields, field) || fields[field] === undefined) return unknown(`missing_execution_field:${field}`);
    if (fields[field] !== null && !(field === "sessionIdBefore" && enqueueSession) &&
      !(["livenessState", "livenessReason"].includes(field) && terminalLiveness))
      return { kind: "execution_evidence", reason: `execution_field_present:${field}` };
  }
  if (fields.lastOutputSeq !== 0) return unknown("output_sequence_not_proven_zero");
  if (fields.processLossRetryCount !== 0) return unknown("process_loss_not_proven_zero");
  if (fields.logCompressed !== false) return unknown("log_compression_not_proven_false");
  if (fields.issueCommentStatus !== "not_applicable") return unknown("issue_comment_work_not_excluded");
  const result = fields.resultJson;
  if (result !== null) {
    if (!result || typeof result !== "object" || Array.isArray(result)) return unknown("missing_or_invalid_result");
    const resultJson = result as Record<string, unknown>;
    // `cancellation` is the platform's own stop-intent record, not execution
    // evidence; runs that actually started are already rejected above.
    const allowed = new Set(["stopReason", "executionWait", "effectiveTimeoutSec", "timeoutConfigured", "timeoutFired", "timeoutSource", "executionRecovery", "cancellation"]);
    if (Object.keys(resultJson).some((key) => !allowed.has(key))) return unknown("unverified_result_fields");
    if (resultJson.stopReason !== undefined && resultJson.stopReason !== run.errorCode) return unknown("cancellation_reason_mismatch");
    if (resultJson.executionWait !== undefined && (run.errorCode !== "execution_reconciliation_required" ||
      !resultJson.executionWait || typeof resultJson.executionWait !== "object" || Array.isArray(resultJson.executionWait) ||
      Object.keys(resultJson.executionWait as Record<string, unknown>).some((key) => !["issueId", "recoveryActionId"].includes(key)))) return unknown("unexpected_execution_wait");
    if ((resultJson.effectiveTimeoutSec !== undefined && resultJson.effectiveTimeoutSec !== 0) ||
      (resultJson.timeoutConfigured !== undefined && resultJson.timeoutConfigured !== false) ||
      (resultJson.timeoutFired !== undefined && resultJson.timeoutFired !== false) ||
      (resultJson.timeoutSource !== undefined && resultJson.timeoutSource !== "stale_queued_run_gate" &&
        !(resultJson.timeoutSource === "dependency_gate" && run.errorCode === "issue_dependencies_blocked"))) return unknown("unverified_timeout_result");
    const recovery = resultJson.executionRecovery as Record<string, unknown> | undefined;
    if (recovery !== undefined &&
      (!recovery || recovery.kind !== "bootstrap" || recovery.providerWorkStarted !== false ||
      (recovery.queueCancellationVerified !== undefined && recovery.queueCancellationVerified !== true) ||
      Object.keys(recovery).some((key) => !["kind", "providerWorkStarted", "queueCancellationVerified"].includes(key)))) return unknown("unverified_recovery_result");
  }
  return { kind: "never_started", reason: "persisted_queue_cancellation_without_execution" };
}

/** Error families describe availability, not whether earlier actions happened. */
export function legacyExecutionNeedsReconciliation(
  run: Pick<Run, "runtimeMode" | "status" | "errorCode" | "resultJson"> & Partial<Run>,
): boolean {
  if (
    run.runtimeMode === "native" ||
    !["failed", "timed_out", "interrupted", "cancelled"].includes(run.status)
  )
    return false;
  // A review-participant recovery round is settled by the stage and status
  // version budget reconciliation, not by this generic branch; real business
  // runs keep the original reconciliation path.
  const retryReason = run.retryReason ?? parseObject(run.contextSnapshot).retryReason;
  if (retryReason === "execution_review_participant_recovery") return false;
  // Without an in-database verified receipt, a queue cancellation for these
  // ownership and reconciliation codes does not exempt itself.
  if (run.status === "cancelled" && ["issue_reassigned", "issue_assignee_changed", "issue_continuation_waiting_on_review", "execution_reconciliation_required"].includes(run.errorCode ?? ""))
    return true;
  // A fresh model turn cannot repair or verify unrestored files.
  if (run.resultJson?.workspaceRestoreFailure === "restore_unsafe_archive") return true;
  // A fresh conversation turn lets the agent decide what remains. The retry
  // scheduler, not an action-outcome hold, owns the automatic attempt limit.
  if (hasConversationContinuationPolicy(run.resultJson)) return false;
  // Productive turn-budget continuation is not a failed provider session.
  if (normalizeMaxTurnStopReason(run.resultJson?.stopReason) ?? normalizeMaxTurnStopReason(run.errorCode)) return false;
  const evidence = run.resultJson?.executionRecovery as
    Record<string, unknown> | undefined;
  if (run.status === "cancelled" && evidence?.kind === "interrupted"
      && evidence.providerStopped === true && evidence.sessionPreserved === true
      && evidence.actionOutcomes === "settled"
      && (run.resultJson?.executionCancellation as Record<string, unknown> | undefined)?.state === "acknowledged") return false;
  // Waiting for a subscription or workspace precedes provider execution. It is
  // a resource wait, not a failed provider attempt or permission to replay work.
  if (run.status === "cancelled" && (run.errorCode === "ai_connection_busy" || run.errorCode === "ai_connection_pool_exhausted") &&
      evidence?.kind === "ai_connection_wait" && evidence.providerWorkStarted === false) return false;
  if (run.status === "cancelled" && run.errorCode === "workspace_busy" &&
      evidence?.kind === "workspace_wait" && evidence.providerWorkStarted === false) return false;
  // Setup owns the bounded retry budget for temporary workspace scans. Its
  // exhaustion needs workspace repair, not reconciliation of provider actions
  // that the bootstrap evidence proves never started. Keep unknown outcomes held.
  if ((run.errorCode === "workspace_git_scan_timeout" || run.errorCode === "workspace_git_scan_saturated") &&
      evidence?.kind === "bootstrap" && evidence.providerWorkStarted === false) return false;
  if (executionFailureRetryCount(run) >= 2) return true;
  return !(
    evidence?.kind === "bootstrap" && evidence.providerWorkStarted === false
  );
}

// A native queue-cancellation receipt counts as positive evidence only when
// every field matches this exact run: message, result shape, timeout source,
// payload keys, and the current assignee. Other cancellation codes, other
// messages, missing keys, or extra keys are not accepted.
function queueCancellationReceiptMatches(run: Partial<Run>, payload: unknown): boolean {
  const nonEmpty = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0;
  const issueId = parseObject(run.contextSnapshot).issueId;
  const result = run.resultJson as Record<string, unknown> | null | undefined;
  const marked = (result?.executionRecovery as Record<string, unknown> | undefined)?.queueCancellationVerified === true;
  if (!nonEmpty(issueId) || !payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  const resultKeys = result ? Object.keys(result).filter((key) => key !== "executionRecovery").sort() : null;
  const gateResult = (source: string, extra: string[] = []) =>
    isDeepStrictEqual(resultKeys, ["effectiveTimeoutSec", "stopReason", "timeoutConfigured", "timeoutFired", "timeoutSource", ...extra].sort()) &&
    result?.stopReason === run.errorCode && result?.timeoutSource === source;
  // When the verified receipt is written, the row marker is written too and
  // the original reason is not; a marked row that still carries the original
  // reason receipt is a contradiction. When verification did not pass, the
  // original reason stays and the payload additionally carries that reason
  // plus the assessment recorded at cancellation time.
  const sameDetails = (details: Record<string, unknown>) =>
    !marked && (isDeepStrictEqual(payload, details) ||
      (isDeepStrictEqual(payload, { ...details, reason: run.error, executionAssessment: (payload as Record<string, unknown>).executionAssessment }) &&
        ["unknown", "execution_evidence"].includes((payload as Record<string, unknown>).executionAssessment?.kind as string) && nonEmpty((payload as Record<string, unknown>).executionAssessment?.reason) &&
        Object.keys((payload as Record<string, unknown>).executionAssessment as Record<string, unknown>).length === 2));
  if (run.errorCode === "issue_assignee_changed") {
    const current = (payload as Record<string, unknown>).currentAssigneeAgentId;
    return ["Cancelled because issue assignee changed before the queued run could start; the new owner will be woken instead",
      "Cancelled because resolved-interaction continuation issue changed assignee before the queued run could start"].includes(run.error ?? "") &&
      gateResult("stale_queued_run_gate") && (current === null || (nonEmpty(current) && current !== run.agentId)) &&
      sameDetails({ issueId, previousAssigneeAgentId: run.agentId, currentAssigneeAgentId: current });
  }
  if (run.errorCode === "execution_reconciliation_required") {
    const wait = result?.executionWait as Record<string, unknown> | undefined;
    const details = { issueId, recoveryActionId: wait?.recoveryActionId };
    return nonEmpty(run.error) && gateResult("stale_queued_run_gate", ["executionWait"]) && isDeepStrictEqual(wait, details) &&
      (details.recoveryActionId === null || nonEmpty(details.recoveryActionId)) && sameDetails(details);
  }
  if (run.errorCode !== "issue_dependencies_blocked") return false;
  const blockers = (payload as Record<string, unknown>).unresolvedBlockerIssueIds;
  if (!Array.isArray(blockers) || !blockers.length || !blockers.every(nonEmpty)) return false;
  if (run.error === "Cancelled because issue dependencies are still blocked; Paperclip will wake the assignee when blockers resolve")
    return gateResult("dependency_gate") && isDeepStrictEqual(payload, { issueId, unresolvedBlockerIssueIds: blockers });
  // A suppressed scheduled retry only changes status/finishedAt/error/errorCode
  // with the result left empty; the payload carries this row's three scheduled
  // values.
  const due = run.scheduledRetryAt instanceof Date || typeof run.scheduledRetryAt === "string" ? new Date(run.scheduledRetryAt) : null;
  return run.error === "Scheduled retry suppressed because issue dependencies are still blocked" && result === null &&
    Number.isSafeInteger(run.scheduledRetryAttempt) && (run.scheduledRetryAttempt ?? 0) > 0 && nonEmpty(run.scheduledRetryReason) &&
    due !== null && !Number.isNaN(due.getTime()) && Number.isSafeInteger((payload as Record<string, unknown>).unresolvedBlockerCount) &&
    ((payload as Record<string, unknown>).unresolvedBlockerCount as number) > 0 &&
    isDeepStrictEqual(payload, { issueId, unresolvedBlockerIssueIds: blockers, unresolvedBlockerCount: (payload as Record<string, unknown>).unresolvedBlockerCount,
      scheduledRetryAttempt: run.scheduledRetryAttempt, scheduledRetryAt: due.toISOString(), scheduledRetryReason: run.scheduledRetryReason });
}

// Cross-verification modeled on the upstream pre-dispatch review-wait design:
// empty row fields alone are not complete evidence. This tree does not adopt
// that upstream cleanup-receipt protocol, so any retained lease is
// conservatively rejected; a releasedAt value is never accepted in place of
// proof of remote termination.
export async function assessLegacyExecutionEvidence(db: Db, run: Partial<Run>): Promise<ExecutionEvidenceAssessment> {
  const row = assessLegacyExecutionStart(run);
  if (row.kind !== "never_started") return row;
  const [coordinator] = await db.select({ id: nativeRunFinalizations.runId }).from(nativeRunFinalizations)
    .where(and(eq(nativeRunFinalizations.companyId, run.companyId!), eq(nativeRunFinalizations.runId, run.id!))).limit(1);
  if (coordinator) return { kind: "unknown", reason: "native_coordinator_present" };
  const leases = await db.select({ id: environmentLeases.id }).from(environmentLeases)
    .where(and(eq(environmentLeases.companyId, run.companyId!), eq(environmentLeases.heartbeatRunId, run.id!))).limit(1);
  if (leases.length) return { kind: "unknown", reason: "execution_environment_retained" };
  // A retry is a new run in this version; promotion and retry-now receipts
  // only describe this row's queuing and never prove the parent turn did not
  // execute. Only the two native events whose complete payload matches this
  // row are accepted; lifecycle/system events or dynamic suppression reasons
  // are not generalized away.
  const queueIdentity = and(eq(heartbeatRunEvents.companyId, run.companyId!), eq(heartbeatRunEvents.agentId, run.agentId!),
    eq(heartbeatRunEvents.eventType, "lifecycle"), eq(heartbeatRunEvents.stream, "system"),
    isNull(heartbeatRunEvents.sourceInstanceId), isNull(heartbeatRunEvents.sourceEventId), isNull(heartbeatRunEvents.sourceSeq),
    isNull(heartbeatRunEvents.sourcePayloadSha256), isNull(heartbeatRunEvents.protocolSchemaVersion));
  const queueCancellation = and(queueIdentity, eq(heartbeatRunEvents.message, "Queued run cancelled before execution"),
    sql`${heartbeatRunEvents.payload}->'executionAssessment' = ${JSON.stringify({ kind: "never_started", reason: "queue_cancellation_and_retained_evidence_verified" })}::jsonb`);
  const nonEmpty = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0;
  const due = run.scheduledRetryAt instanceof Date || typeof run.scheduledRetryAt === "string"
    ? new Date(run.scheduledRetryAt) : null;
  let knownQueueEvent = queueCancellation;
  if ([run.id, run.companyId, run.agentId, run.scheduledRetryReason].every(nonEmpty) && Object.hasOwn(run, "retryOfRunId") &&
    (run.retryOfRunId === null || (nonEmpty(run.retryOfRunId) && run.retryOfRunId !== run.id)) &&
    Number.isSafeInteger(run.scheduledRetryAttempt) && (run.scheduledRetryAttempt ?? 0) > 0 &&
    due !== null && !Number.isNaN(due.getTime())) {
    const scheduledRetryAt = due.toISOString();
    const promotionPayload = { scheduledRetryAttempt: run.scheduledRetryAttempt, scheduledRetryAt, scheduledRetryReason: run.scheduledRetryReason };
    const infoQueueEvent = and(queueIdentity, eq(heartbeatRunEvents.level, "info"), isNull(heartbeatRunEvents.color));
    const promotion = and(infoQueueEvent,
      eq(heartbeatRunEvents.message, "Scheduled retry became due and was promoted to the queued run pool"),
      sql`${heartbeatRunEvents.payload} = ${JSON.stringify(promotionPayload)}::jsonb`);
    knownQueueEvent = or(knownQueueEvent, promotion);
    const context = parseObject(run.contextSnapshot);
    if (nonEmpty(context.issueId) &&
      context.scheduledRetryAt === scheduledRetryAt && context.retryNowRequestedAt === scheduledRetryAt &&
      Object.hasOwn(context, "retryNowRequestedByActorType") && Object.hasOwn(context, "retryNowRequestedByActorId") &&
      [null, "user", "agent", "system"].includes(context.retryNowRequestedByActorType as string | null) &&
      (context.retryNowRequestedByActorId === null || nonEmpty(context.retryNowRequestedByActorId))) {
      knownQueueEvent = or(knownQueueEvent, and(infoQueueEvent,
        eq(heartbeatRunEvents.message, "Scheduled retry was requested to run now"),
        sql`${heartbeatRunEvents.payload} = ${JSON.stringify({ issueId: context.issueId, ...promotionPayload,
          requestedByActorType: context.retryNowRequestedByActorType, requestedByActorId: context.retryNowRequestedByActorId })}::jsonb`));
    }
  }
  // The coarse SQL prefilter matches only the two event families by fixed
  // columns; each payload is verified below and anything unverified stays
  // held. Cancellation receipts: warn events whose message equals this run's
  // error. Hold-stop events: the two fixed messages of the recovery closer.
  const warnQueueEvent = and(queueIdentity, eq(heartbeatRunEvents.level, "warn"), isNull(heartbeatRunEvents.color));
  const receiptEvent = run.status === "cancelled" && nonEmpty(run.error)
    ? and(warnQueueEvent, eq(heartbeatRunEvents.message, run.error)) : undefined;
  const holdStopEvent = and(warnQueueEvent, inArray(heartbeatRunEvents.message, [
    "Automatic recovery stopped. Recorded work is preserved; actions with unverified outcomes will not be repeated.",
    "Recovery closed because the task's owner, execution, or status changed. No work was replayed."]));
  knownQueueEvent = or(knownQueueEvent, holdStopEvent, receiptEvent);
  // Restricted to this run's id; a stray fragment from the same run under the
  // wrong company must also block. NOT COALESCE closes missing or null event
  // fields.
  const events = await db.select({ id: heartbeatRunEvents.id }).from(heartbeatRunEvents)
    .where(and(eq(heartbeatRunEvents.runId, run.id!), sql`not coalesce(${knownQueueEvent}, false)`)).limit(1);
  if (events.length) return { kind: "execution_evidence", reason: "retained_execution_event" };
  // A stop event only proves the hold was settled: its payload must be
  // exactly the four native keys and the referenced action must share this
  // company, issue, and run, carry the legacy cause, and have been settled by
  // the same policy. The action's current replay state is not re-checked:
  // after the first verification it flips to not_applicable_never_started,
  // and a second stop event must still verify independently.
  const stops = await db.select({ payload: heartbeatRunEvents.payload }).from(heartbeatRunEvents)
    .where(and(eq(heartbeatRunEvents.runId, run.id!), holdStopEvent)).limit(21);
  const stopIds = stops.map((event) => (typeof (event.payload as Record<string, unknown> | null)?.recoveryActionId === "string"
    ? (event.payload as Record<string, unknown>).recoveryActionId as string : null));
  if (stops.length > 20 || stops.some((event, index) => {
    const id = stopIds[index];
    return typeof id !== "string" ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id) ||
      !isDeepStrictEqual(event.payload, { recoveryActionId: id, cause: LEGACY_RECOVERY_CAUSE,
        automaticRecovery: "preserve_without_replay_v1", replay: "blocked" });
  }))
    return { kind: "unknown", reason: "unverified_recovery_stop_event" };
  if (stopIds.length) {
    const holds = await db.select({ id: issueRecoveryActions.id, sourceIssueId: issueRecoveryActions.sourceIssueId,
      kind: issueRecoveryActions.kind, cause: issueRecoveryActions.cause, evidence: issueRecoveryActions.evidence })
      .from(issueRecoveryActions).where(and(eq(issueRecoveryActions.companyId, run.companyId!), inArray(issueRecoveryActions.id, [...new Set(stopIds)])));
    const issueId = parseObject(run.contextSnapshot).issueId;
    if (stopIds.some((id) => {
      const hold = holds.find((holdRow) => holdRow.id === id);
      const evidence = (hold?.evidence ?? null) as Record<string, unknown> | null;
      const automaticRecovery = evidence?.automaticRecovery as Record<string, unknown> | undefined;
      return !hold || !nonEmpty(issueId) || hold.sourceIssueId !== issueId || hold.kind !== "active_run_watchdog" ||
        hold.cause !== LEGACY_RECOVERY_CAUSE || evidence?.runId !== run.id ||
        automaticRecovery?.policy !== "preserve_without_replay_v1" || automaticRecovery?.runId !== run.id;
    })) return { kind: "unknown", reason: "unverified_recovery_stop_event" };
  }
  // Positive evidence: a cancelled row needs either the row marker written at
  // verification time or exactly one native cancellation receipt matching it
  // field by field; missing, multiple, or contradictory receipts never
  // classify as zero-execution.
  if (run.status === "cancelled") {
    const receipts = receiptEvent ? await db.select({ payload: heartbeatRunEvents.payload }).from(heartbeatRunEvents)
      .where(and(eq(heartbeatRunEvents.runId, run.id!), receiptEvent)).limit(2) : [];
    if (receipts.length > 1 || (receipts.length === 1 && !queueCancellationReceiptMatches(run, receipts[0].payload)))
      return { kind: "unknown", reason: "queue_cancellation_receipt_mismatch" };
    if (!receipts.length && ((run.resultJson as Record<string, unknown> | null)?.executionRecovery as Record<string, unknown> | undefined)?.queueCancellationVerified !== true)
      return { kind: "unknown", reason: "queue_cancellation_receipt_not_proven" };
  }
  return { kind: "never_started", reason: "queue_cancellation_and_retained_evidence_verified" };
}

/** Queue-cancellation ownership and reconciliation codes are only exempt
 * with a verified never-started assessment; review-wait receipts are only
 * exempt after retained execution evidence agrees. The synchronous
 * classifier stays conservative for callers without a DB proof. */
export async function legacyExecutionNeedsReconciliationWithEvidence(db: Db, run: Run): Promise<boolean> {
  if (run.runtimeMode === "legacy" && run.status === "cancelled" &&
    ["issue_reassigned", "issue_assignee_changed", "issue_continuation_waiting_on_review", "execution_reconciliation_required", "issue_dependencies_blocked"].includes(run.errorCode ?? ""))
    return (await assessLegacyExecutionEvidence(db, run)).kind !== "never_started";
  return legacyExecutionNeedsReconciliation(run) && !(await isPreDispatchReviewWaitVerified(db, run));
}

/** Persist the failed legacy run, owned lock release and operator decision together. */
export async function terminalizeLegacyExecution(input: {
  db: Db;
  run: Run;
  status: string;
  patch?: Partial<typeof heartbeatRuns.$inferInsert>;
  fromStatuses?: string[];
}) {
  const { db, run, status, patch } = input;
  const issueId =
    run.nativeIssueId ??
    (typeof run.contextSnapshot?.issueId === "string"
      ? run.contextSnapshot.issueId
      : null);
  return db.transaction(async (tx) => {
    await tx.execute(
      sql`select set_config('statement_timeout', '15000', true), set_config('lock_timeout', '1000', true)`,
    );
    const [task] = issueId
      ? await tx
          .select()
          .from(issues)
          .where(
            and(eq(issues.companyId, run.companyId), eq(issues.id, issueId)),
          )
          .for("update")
      : [];
    const [updated] = await tx
      .update(heartbeatRuns)
      .set({
        status,
        ...patch,
        executionStatusDeliveryId: randomUUID(),
        updatedAt: new Date(),
      })
      .where(
        and(
          eq(heartbeatRuns.id, run.id),
          eq(heartbeatRuns.companyId, run.companyId),
          inArray(heartbeatRuns.status, input.fromStatuses ?? [run.status]),
        ),
      )
      .returning();
    if (!updated) return null;
    if (task?.executionRunId === run.id)
      await tx
        .update(issues)
        .set({
          executionRunId: null,
          executionAgentNameKey: null,
          executionLockedAt: null,
        })
        .where(eq(issues.id, task.id));
    if (task?.checkoutRunId === run.id)
      await tx
        .update(issues)
        .set({ checkoutRunId: null })
        .where(eq(issues.id, task.id));
    const review = task?.status === "in_review" ? parseIssueExecutionState(task.executionState) : null;
    const isCurrentReviewer = review?.status === "pending" &&
      review.currentParticipant?.type === "agent" && review.currentParticipant.agentId === run.agentId;
    if (
      task &&
      (await legacyExecutionNeedsReconciliationWithEvidence(tx as unknown as Db, updated)) &&
      !isSupersededConversationRun(task, updated) &&
      (task.assigneeAgentId === run.agentId || isCurrentReviewer) &&
      !["done", "cancelled"].includes(task.status)
    ) {
      // Periodic stranded-work checks may revisit this terminal run before its
      // reconciled continuation is dispatched. Preserve the recorded decision
      // and an existing unsafe-workspace hold instead of creating another one.
      const [reconciled] = await tx.select({ id: issueRecoveryActions.id })
        .from(issueRecoveryActions).where(and(
          eq(issueRecoveryActions.companyId, run.companyId),
          eq(issueRecoveryActions.sourceIssueId, task.id),
          eq(issueRecoveryActions.status, "resolved"),
          or(
            sql`${issueRecoveryActions.evidence}->'executionReconciliation'->>'runId' = ${run.id}`,
            and(
              sql`${issueRecoveryActions.evidence}->>'runId' = ${run.id}`,
              sql`${issueRecoveryActions.evidence}->>'workspaceRestoreFailure' = 'restore_unsafe_archive'`,
              sql`${issueRecoveryActions.evidence}->'automaticRecovery'->>'replay' = 'blocked'`,
            ),
          ),
        )).limit(1);
      if (reconciled) return updated;
      await issueRecoveryActionService(tx as unknown as Db).upsertSourceScoped({
        companyId: run.companyId,
        sourceIssueId: task.id,
        kind: "active_run_watchdog",
        ownerType: "board",
        returnOwnerAgentId: task.assigneeAgentId,
        cause: LEGACY_RECOVERY_CAUSE,
        fingerprint: `legacy-execution:${run.id}`,
        evidence: {
          runId: run.id,
          ...(isCurrentReviewer ? { reviewParticipantAgentId: run.agentId } : {}),
          originalFailureCode: updated.errorCode,
          executionAssessment: await assessLegacyExecutionEvidence(tx as unknown as Db, updated),
          ...(hasWorkspaceRestoreFailure(updated.resultJson) ? { workspaceRestoreFailure: updated.resultJson!.workspaceRestoreFailure } : {}),
          adapterRecovery: "unsupported_or_unknown",
          attempt: executionFailureRetryCount(run) + 1,
        },
        nextAction: hasWorkspaceRestoreFailure(updated.resultJson)
          ? "Verify safe workspace staging or repair, then reconcile the stopped run before continuing. Saved work and approval decisions remain in force."
          : "Inspect the stopped provider and recorded actions, then reconcile their outcomes before continuing. This adapter has not established a safe resume checkpoint.",
        maxAttempts: 3,
        wakePolicy: null,
        supersedeOnIdentityChange: true,
      });
    }
    return updated;
  });
}

/** Called only by the exact executor's finally block after preparation/cleanup
 * settles. A shutdown can win the terminal-status race before the setup catch
 * records that no provider started. Retire only that mistaken recovery hold. */
export async function settleInterruptedNativeBootstrap(
  db: Db, input: { run: Run; providerDispatchStarted: boolean },
): Promise<Run | null> {
  if (input.providerDispatchStarted) return null;
  const issueId = input.run.contextSnapshot?.issueId;
  if (typeof issueId !== "string") return null;
  return db.transaction(async tx => {
    const [task] = await tx.select().from(issues).where(and(eq(issues.id, issueId), eq(issues.companyId, input.run.companyId))).for("update");
    const [run] = await tx.select().from(heartbeatRuns).where(and(eq(heartbeatRuns.id, input.run.id), eq(heartbeatRuns.companyId, input.run.companyId))).for("update");
    if (!task || !run || run.runtimeMode !== "legacy" || run.runtimeModeResolvedAt ||
        run.contextSnapshot?.issueId !== task.id ||
        run.status !== "interrupted" || run.errorCode !== "server_shutdown_interrupted" ||
        claimedAdapterType(run) !== "paperclip_runner" || run.processPid || run.processGroupId ||
        hasWorkspaceRestoreFailure(run.resultJson) || executionFailureRetryCount(run) >= 2) return null;
    const [native] = await tx.select({ id: nativeRunFinalizations.runId }).from(nativeRunFinalizations).where(eq(nativeRunFinalizations.runId, run.id)).limit(1);
    const [lease] = await tx.select({ id: environmentLeases.id }).from(environmentLeases).where(and(
      eq(environmentLeases.companyId, run.companyId), eq(environmentLeases.heartbeatRunId, run.id),
      or(isNull(environmentLeases.releasedAt), eq(environmentLeases.status, "pending_cleanup"), eq(environmentLeases.cleanupStatus, "failed")),
    )).limit(1);
    if (native || lease) return null;
    const holds = await tx.select().from(issueRecoveryActions).where(and(
      eq(issueRecoveryActions.companyId, run.companyId), eq(issueRecoveryActions.sourceIssueId, task.id),
      eq(issueRecoveryActions.cause, LEGACY_RECOVERY_CAUSE), eq(issueRecoveryActions.fingerprint, `legacy-execution:${run.id}`),
      sql`${issueRecoveryActions.evidence}->>'runId' = ${run.id}`,
      sql`not (${issueRecoveryActions.evidence} ? 'executionReconciliation')`,
      sql`coalesce(${issueRecoveryActions.evidence}->>'workspaceRestoreFailure', '') <> 'restore_unsafe_archive'`,
      or(inArray(issueRecoveryActions.status, ["active", "escalated"]), and(
        eq(issueRecoveryActions.status, "resolved"), eq(issueRecoveryActions.outcome, "blocked"),
        sql`${issueRecoveryActions.evidence}->'automaticRecovery'->>'policy' = 'preserve_without_replay_v1'`,
        sql`${issueRecoveryActions.evidence}->'automaticRecovery'->>'replay' = 'blocked'`,
      )),
    )).for("update");
    const ownedBlock = holds.map(hold => hold.evidence.nativeBootstrapFailureBlock as Record<string, unknown> | undefined)
      .find(receipt => receipt?.runId === run.id && receipt.statusVersion === task.statusVersion &&
        ["todo", "in_progress", "in_review"].includes(String(receipt.previousStatus)));
    if (task.status === "blocked" && ownedBlock && task.assigneeAgentId === run.agentId &&
        !isSupersededConversationRun(task, run) && !task.executionRunId && !task.checkoutRunId) {
      // Restore only the unchanged projection made by this exact incident.
      // The issue service still enforces dependency/assignee readiness. Human
      // reblocks, reassignment and conversation resets invalidate this receipt.
      await issueService(tx as unknown as Db).update(task.id, { status: String(ownedBlock.previousStatus) }, tx);
    }
    const now = new Date();
    const [settled] = await tx.update(heartbeatRuns).set({ resultJson: { ...run.resultJson,
      executionRecovery: { kind: "bootstrap", providerWorkStarted: false, preparationSettledAt: now.toISOString() },
    }, updatedAt: now }).where(eq(heartbeatRuns.id, run.id)).returning();
    const resolved = holds.length ? await tx.update(issueRecoveryActions).set({ status: "resolved", outcome: "false_positive",
      resolutionNote: "The interrupted executor finished preparation and cleanup without starting a provider.",
      resolvedAt: now, updatedAt: now,
      // An automatic disposition may already have resolved the incident but
      // left replay blocked. Preserve its history while correcting that verdict.
      evidence: sql`jsonb_set(${issueRecoveryActions.evidence}, '{automaticRecovery,replay}', '"allowed"'::jsonb, false) ||
        ${JSON.stringify({ bootstrapPreparationSettledAt: now.toISOString() })}::jsonb`,
    }).where(inArray(issueRecoveryActions.id, holds.map(hold => hold.id))).returning({ id: issueRecoveryActions.id }) : [];
    if (resolved.length) await logActivity(tx as unknown as Db, { companyId: run.companyId, actorType: "system", actorId: "system",
      action: "issue.recovery_action_resolved", entityType: "issue", entityId: task.id, agentId: run.agentId, runId: run.id,
      details: { reason: "native_bootstrap_stopped_before_provider", recoveryActionIds: resolved.map(r => r.id) },
    });
    return settled;
  });
}
