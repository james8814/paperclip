import { describe, expect, it, vi } from "vitest";
import { persistedConversationProcessLiveness } from "./conversation-continuation.js";

const start = new Date("2026-10-27T07:15:02.876Z");
const end = new Date("2026-10-27T07:44:43.543Z");
const born = "2026-10-28T11:42:02.000Z";
const run = { processPid: 42001, processGroupId: 42000, processStartedAt: start, finishedAt: end };
const leader = { pid: 42000, startedAt: born };
function probes(overrides: Record<string, unknown> = {}) {
  return { isAlive: vi.fn(() => true), startedAt: vi.fn(async () => born),
    groupMembers: vi.fn(async () => [leader]), ...overrides };
}

describe("persisted group identity", () => {
  it("releases a recycled unequal group", async () => {
    expect(await persistedConversationProcessLiveness(run, probes())).toEqual({ pidAlive: false, groupAlive: false });
  });
  it.each([42001, null])("releases a new group when old PID %s is absent", async (processPid) => {
    const p = probes({ isAlive: vi.fn((pid) => pid < 0) });
    expect(await persistedConversationProcessLiveness({ ...run, processPid }, p)).toEqual({ pidAlive: false, groupAlive: false });
  });
  it("preserves the existing equal-ID release before terminal time", async () => {
    const beforeEnd = "2026-10-27T07:20:00.000Z";
    expect(await persistedConversationProcessLiveness({ ...run, processPid: 42000 }, probes({
      startedAt: vi.fn(async () => beforeEnd), groupMembers: vi.fn(async () => [{ ...leader, startedAt: beforeEnd }]),
    }))).toEqual({ pidAlive: false, groupAlive: false });
  });
  it("does not apply that weaker boundary to newly covered unequal groups", async () => {
    expect(await persistedConversationProcessLiveness(run, probes({
      groupMembers: vi.fn(async () => [{ ...leader, startedAt: "2026-10-27T07:20:00Z" }]),
    }))).toEqual({ pidAlive: false, groupAlive: true });
  });
  it("keeps a genuine original PID and skips group release", async () => {
    const p = probes({ startedAt: vi.fn(async () => start.toISOString()) });
    expect(await persistedConversationProcessLiveness(run, p)).toEqual({ pidAlive: true, groupAlive: true });
    expect(p.groupMembers).not.toHaveBeenCalled();
  });
  it.each(["2026-10-27T07:30:00Z", born])("keeps old/same-second descendants (%s)", async (startedAt) => {
    expect(await persistedConversationProcessLiveness(run, probes({ groupMembers: vi.fn(async () => [leader, { pid: 42002, startedAt }]) })))
      .toEqual({ pidAlive: false, groupAlive: true });
  });
  it("allows strictly later new children", async () => {
    expect(await persistedConversationProcessLiveness(run, probes({ groupMembers: vi.fn(async () => [leader, { pid: 42002, startedAt: "2026-10-28T11:42:03Z" }]) })))
      .toEqual({ pidAlive: false, groupAlive: false });
  });
  it.each([null, [], [{ pid: 42002, startedAt: born }], [{ ...leader, startedAt: "invalid" }], [leader, leader]])
    ("holds missing, malformed or duplicate membership %#", async (members) => {
      expect(await persistedConversationProcessLiveness(run, probes({ groupMembers: vi.fn(async () => members) })))
        .toEqual({ pidAlive: false, groupAlive: true });
    });
  it.each([null, undefined, new Date(NaN), new Date("2026-10-28T11:42:02.999Z")])
    ("requires a strictly later terminal second for new paths %#", async (finishedAt) => {
      expect(await persistedConversationProcessLiveness({ ...run, finishedAt }, probes())).toEqual({ pidAlive: false, groupAlive: true });
    });
  it("holds unreadable PID identity", async () => {
    expect(await persistedConversationProcessLiveness(run, probes({ startedAt: vi.fn(async () => { throw Error("unreadable"); }) })))
      .toEqual({ pidAlive: true, groupAlive: true });
  });
  it("holds unreadable group membership", async () => {
    expect(await persistedConversationProcessLiveness(run, probes({ groupMembers: vi.fn(async () => { throw Error("permission/timeout"); }) })))
      .toEqual({ pidAlive: false, groupAlive: true });
  });
});
