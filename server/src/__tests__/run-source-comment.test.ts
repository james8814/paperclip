import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { PgDialect } from "drizzle-orm/pg-core";
import { issueComments, type Db } from "@paperclipai/db";
import { transformSync } from "esbuild";
import { describe, expect, it, vi } from "vitest";
import { resolveVerifiedRunSourceCommentId, sourceCommentCandidate } from "../services/run-source-comment.js";

type SourceRun = NonNullable<Parameters<typeof sourceCommentCandidate>[0]>;
const context = {
  issueId: "issue-1", source: "issue.comment", wakeReason: "issue_commented", wakeCommentIds: ["comment-1"],
};
const relay = {
  ...context, source: "plugin:example.chat", sourceCommentId: "comment-1",
};
const run = (overrides: Partial<SourceRun> = {}): SourceRun => ({
  companyId: "company-1", nativeIssueId: "issue-1", status: "running", contextSnapshot: context, ...overrides,
});
function database(rows: unknown[]) {
  const where = vi.fn();
  const from = vi.fn();
  const query = { from: (table: unknown) => { from(table); return query; }, where: (filter: unknown) => { where(filter); return query; }, limit: vi.fn().mockResolvedValue(rows) };
  return { db: { select: () => query } as unknown as Db, where, from, query };
}

describe("verified comment origin", () => {
  it("accepts only the single host-normalized candidate", () => {
    expect(sourceCommentCandidate(run())).toEqual({ companyId: "company-1", issueId: "issue-1", commentId: "comment-1", kind: "issue_comment" });
    expect(sourceCommentCandidate(run({ contextSnapshot: { ...context, wakeCommentIds: ["comment-1", "comment-1"] } }))?.commentId).toBe("comment-1");
    expect(sourceCommentCandidate(run({ contextSnapshot: relay }))?.kind).toBe("plugin_human_comment");
    expect(sourceCommentCandidate(run({ contextSnapshot: { ...context, source: "issue.comment.reopen", wakeReason: "issue_reopened_via_comment" } }))?.kind).toBe("issue_comment");
  });
  it.each([
    { ...context, wakeCommentIds: [] },
    { ...context, wakeCommentIds: ["comment-1", "comment-2"] },
    { ...context, wakeCommentIds: undefined, commentId: "comment-1", wakeCommentId: "comment-1" },
    { ...context, issueId: "other-issue" },
    { ...context, source: "assignment" },
    { ...context, source: "automation", wakeReason: "b1" },
    { ...context, source: "on_demand" },
    { ...relay, sourceCommentId: undefined },
    { ...relay, sourceCommentId: "comment-2" },
    { ...relay, pluginId: "plugin-1" },
    { ...relay, pluginKey: "example.chat" },
    { ...relay, interactionId: "interaction-1" },
    { ...relay, approvalId: "approval-1" },
    { ...relay, source: "plugin:example.chat:approval.approve" },
    { ...relay, source: "plugin:example.chat:interaction.accept" },
    { ...relay, wakeReason: "plugin_issue_wakeup_requested" },
  ])("rejects ambiguous or foreign wake context %#", (contextSnapshot) => {
    expect(sourceCommentCandidate(run({ contextSnapshot }))).toBeNull();
  });
  it("rejects missing company, inactive runs, and invalid context", () => {
    expect(sourceCommentCandidate(run({ companyId: "" }))).toBeNull();
    expect(sourceCommentCandidate(run({ status: "queued" }))).toBeNull();
    expect(sourceCommentCandidate(run({ contextSnapshot: [] }))).toBeNull();
    expect(sourceCommentCandidate(null)).toBeNull();
  });
  it("queries the exact company, issue and comment ownership tuple", async () => {
    const mock = database([{ id: "comment-1" }]);
    expect(await resolveVerifiedRunSourceCommentId(mock.db, run())).toBe("comment-1");
    expect(mock.from).toHaveBeenCalledWith(issueComments);
    expect(new PgDialect().sqlToQuery(mock.where.mock.calls[0][0]).params).toEqual(["comment-1", "company-1", "issue-1"]);
    expect(mock.query.limit).toHaveBeenCalledWith(2);
  });
  it.each([[], [{ id: "comment-1" }, { id: "comment-1" }]])("rejects missing or duplicate database evidence %#", async (rows) => {
    expect(await resolveVerifiedRunSourceCommentId(database(rows).db, run())).toBeNull();
  });
  it.each([
    { id: "comment-1", authorUserId: null, authorAgentId: "agent-1" },
    { id: "comment-1", authorUserId: null, authorAgentId: null },
    { id: "comment-1", authorUserId: "user-1", authorAgentId: "agent-1" },
  ])("rejects plugin relay comments without exclusive human authorship %#", async (row) => {
    expect(await resolveVerifiedRunSourceCommentId(database([row]).db, run({ contextSnapshot: relay }))).toBeNull();
  });
  it("accepts a verified plugin human comment and fails closed on database errors", async () => {
    const mock = database([{ id: "comment-1", authorUserId: "user-1", authorAgentId: null }]);
    expect(await resolveVerifiedRunSourceCommentId(mock.db, run({ contextSnapshot: relay }))).toBe("comment-1");
    mock.query.limit.mockRejectedValueOnce(new Error("database unavailable"));
    expect(await resolveVerifiedRunSourceCommentId(mock.db, run({ contextSnapshot: relay }))).toBeNull();
  });
});

// Exercise the actual private event publishers without booting unrelated server services.
const heartbeatSource = readFileSync(fileURLToPath(new URL("../services/heartbeat.ts", import.meta.url)), "utf8");
const publisherStart = heartbeatSource.indexOf("  async function publishRunLifecyclePluginEvent(");
const publisherEnd = heartbeatSource.indexOf("  async function setWakeupStatus(", publisherStart);
if (publisherStart < 0 || publisherEnd <= publisherStart) throw new Error("Lifecycle publishers not found");
const publisherCode = transformSync(heartbeatSource.slice(publisherStart, publisherEnd), { loader: "ts", target: "es2022" }).code;
function eventHarness(resolve = vi.fn().mockResolvedValue("comment-1")) {
  const publish = vi.fn();
  const warn = vi.fn();
  const functions = new Function("db", "resolveVerifiedRunSourceCommentId", "logger", "readNonEmptyString", "parseObject", "publishPluginDomainEvent", "randomUUID",
    publisherCode + "return { emit: publishRunLifecyclePluginEvent, emitData: publishRunLifecyclePluginEventData };")(
      {}, resolve, { warn }, (v: unknown) => typeof v === "string" && v.length ? v : null, (v: unknown) => v ?? {}, publish, () => "event-1",
    );
  return { ...functions, publish, resolve, warn };
}
const eventRun = { ...run(), id: "run-1", agentId: "agent-1", invocationSource: "on_demand", triggerDetail: null, error: null, errorCode: null, startedAt: new Date(0), finishedAt: null };

describe("lifecycle plugin event boundary", () => {
  it("awaits source verification and publishes it on the same started run", async () => {
    let finish!: (value: string) => void;
    const h = eventHarness(vi.fn().mockReturnValue(new Promise<string>((r) => { finish = r; })));
    const pending = h.emit(eventRun);
    expect(h.publish).not.toHaveBeenCalled();
    finish("comment-1"); await pending;
    expect(h.resolve.mock.calls[0][1]).toBe(eventRun);
    expect(h.publish).toHaveBeenCalledWith(expect.objectContaining({ entityId: "run-1", companyId: "company-1", eventType: "agent.run.started", payload: expect.objectContaining({ runId: "run-1", agentId: "agent-1", issueId: "issue-1", sourceCommentId: "comment-1" }) }));
  });
  it.each(["succeeded", "failed", "timed_out", "cancelled"])("keeps %s events source-free", async (status) => {
    const h = eventHarness(); await h.emit({ ...eventRun, status });
    expect(h.resolve).not.toHaveBeenCalled();
    expect(h.publish.mock.calls[0][0].payload.sourceCommentId).toBeNull();
  });
  it("publishes null on verification failure and on context-free post-commit effects", async () => {
    const h = eventHarness(vi.fn().mockRejectedValue(new Error("unavailable")));
    await h.emit(eventRun);
    expect(h.publish.mock.calls[0][0].payload.sourceCommentId).toBeNull();
    expect(h.warn).toHaveBeenCalledOnce();
    h.emitData({ ...eventRun, runId: "run-1", status: "cancelled", issueId: "issue-1" });
    expect(h.publish.mock.calls[1][0].payload.sourceCommentId).toBeNull();
  });
  it("awaits every row-based event call before its caller continues", () => {
    const sites = [...heartbeatSource.matchAll(/^\s*(await )?publishRunLifecyclePluginEvent\(/gm)];
    expect(sites).toHaveLength(5);
    expect(sites.every((site) => site[1] === "await ")).toBe(true);
  });
});
