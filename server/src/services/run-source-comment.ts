import { and, eq } from "drizzle-orm";
import { issueComments, heartbeatRuns, type Db } from "@paperclipai/db";
import { extractWakeCommentIds } from "../modules/run-dispatch/domain/wake-context.js";

type SourceRun = Pick<typeof heartbeatRuns.$inferSelect, "status" | "companyId" | "contextSnapshot" | "nativeIssueId">;

// Accept a single host-normalized comment, never infer one from timing or pending replies.
const COMMENT_WAKE_SOURCES = new Set([
    "issue.comment",
    "issue.comment.reopen",
]);
const COMMENT_WAKE_REASONS = new Set([
    "issue_commented",
    "issue_reopened_via_comment",
]);

const PLUGIN_HUMAN_COMMENT_SOURCE = /^plugin:[^:\s]+$/;
const PLUGIN_HUMAN_COMMENT_REASON = "issue_commented";
const PLUGIN_FOREIGN_CONTEXT_KEYS = ["pluginId", "pluginKey", "interactionId", "approvalId"];

function objectOrEmpty(value: unknown): Record<string, unknown> {
    return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function nonEmptyString(value: unknown): string | null {
    return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function commentWakeKind(context: Record<string, unknown>, commentId: string) {
    const source = nonEmptyString(context.source);
    const reason = nonEmptyString(context.wakeReason);
    if (COMMENT_WAKE_SOURCES.has(source ?? "") && COMMENT_WAKE_REASONS.has(reason ?? ""))
        return "issue_comment";
    if (source && PLUGIN_HUMAN_COMMENT_SOURCE.test(source) &&
        reason === PLUGIN_HUMAN_COMMENT_REASON &&
        nonEmptyString(context.sourceCommentId) === commentId &&
        PLUGIN_FOREIGN_CONTEXT_KEYS.every((key) => context[key] == null)) {
        return "plugin_human_comment";
    }
    return null;
}

export function sourceCommentCandidate(run: SourceRun | null | undefined) {
    if (!run || run.status !== "running")
        return null;
    const context = objectOrEmpty(run.contextSnapshot);
    const ids = extractWakeCommentIds(context);
    if (ids.length !== 1)
        return null;
    const companyId = nonEmptyString(run.companyId);
    const issueId = nonEmptyString(context.issueId);
    const nativeIssueId = nonEmptyString(run.nativeIssueId);
    if (!companyId || !issueId || (nativeIssueId && nativeIssueId !== issueId))
        return null;
    const kind = commentWakeKind(context, ids[0]);
    if (!kind)
        return null;
    return { companyId, issueId, commentId: ids[0], kind };
}

// Verify ownership against the comment row; absent or unreadable evidence stays null.
export async function resolveVerifiedRunSourceCommentId(db: Db, run: SourceRun): Promise<string | null> {
    const candidate = sourceCommentCandidate(run);
    if (!candidate)
        return null;
    try {
        const rows = await db
            .select({
            id: issueComments.id,
            authorUserId: issueComments.authorUserId,
            authorAgentId: issueComments.authorAgentId,
        })
            .from(issueComments)
            .where(and(
            eq(issueComments.id, candidate.commentId),
            eq(issueComments.companyId, candidate.companyId),
            eq(issueComments.issueId, candidate.issueId),
        ))
            .limit(2);
        if (rows.length !== 1)
            return null;
        if (candidate.kind === "plugin_human_comment" &&
            (!nonEmptyString(rows[0].authorUserId) || rows[0].authorAgentId != null)) {
            return null;
        }
        return rows[0].id;
    }
    catch {
        return null;
    }
}
