import { afterEach, describe, expect, it, vi } from "vitest";
const mocks = vi.hoisted(() => ({ execFile: vi.fn() }));
vi.mock("node:child_process", () => ({ execFile: mocks.execFile }));
import { readProcessStartedAt, runProcessCommand } from "./hot-restart.js";
import { getConversationOwnershipBlocker, persistedConversationProcessLiveness } from "./conversation-continuation.js";

const originalPlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
afterEach(() => { Object.defineProperty(process, "platform", originalPlatform); vi.restoreAllMocks(); vi.unstubAllEnvs(); mocks.execFile.mockReset(); });
const english = "Wed Oct 28 20:32:04 2026";
const chinese = "三 10月/28 20:32:04 2026";
const run = { processPid: 42000, processGroupId: 42000, processStartedAt: new Date("2026-10-27T07:15:02Z"), finishedAt: new Date("2026-10-27T07:44:43Z") };
function localizedPs(groupOutput?: string) {
  Object.defineProperty(process, "platform", { value: "darwin", configurable: true });
  vi.stubEnv("LC_ALL", "zh_CN.UTF-8"); vi.stubEnv("LANG", "zh_CN.UTF-8");
  mocks.execFile.mockImplementation((_cmd, args, opts, callback) => {
    const date = opts.env?.LC_ALL === "C" ? english : chinese;
    callback(null, args[0] === "-axo" ? (groupOutput ?? `42000 42000 ${date}\n`) : date);
  });
}
describe("native process probe locale", () => {
  it("demonstrates localized lstart parse failure with an injected command", async () => {
    await expect(readProcessStartedAt(42000, { platform: "darwin", runCommand: async () => chinese })).rejects.toThrow("Could not parse");
  });
  it("fixes default PID ps output without changing the caller locale", async () => {
    localizedPs();
    const value = await readProcessStartedAt(42000, { platform: "darwin" });
    expect(Number.isFinite(Date.parse(value!))).toBe(true);
    expect(mocks.execFile.mock.calls[0][2].env).toMatchObject({ LC_ALL: "C", LANG: "C" });
    expect(process.env.LC_ALL).toBe("zh_CN.UTF-8");
  });
  it("uses the same C-locale command for group membership", async () => {
    localizedPs(); vi.spyOn(process, "kill").mockReturnValue(true);
    expect(await persistedConversationProcessLiveness(run)).toEqual({ pidAlive: false, groupAlive: false });
    const groupCall = mocks.execFile.mock.calls.find((call) => call[1][0] === "-axo");
    expect(groupCall?.[2].env).toMatchObject({ LC_ALL: "C", LANG: "C" });
    expect(process.env.LANG).toBe("zh_CN.UTF-8");
  });
  it.each([`42000 42000 ${english}\n42002 42000\n`, `42000 42000 ${english}\ntruncated-row\n`, `42000 42000 ${chinese}\n`])
    ("holds incomplete or unparsable member output %#", async (output) => {
      localizedPs(output); vi.spyOn(process, "kill").mockReturnValue(true);
      expect(await persistedConversationProcessLiveness(run)).toEqual({ pidAlive: false, groupAlive: true });
    });
  it("does not override environment for other commands", async () => {
    mocks.execFile.mockImplementation((_cmd, _args, opts, callback) => { expect(opts.env).toBeUndefined(); callback(null, "ok"); });
    expect(await runProcessCommand("powershell.exe", ["fixture-only"])).toBe("ok");
  });
  it("retains ownership for an unreleased lease after process identity clears", async () => {
    const candidate = { run: { ...run, id: "run-fixture", agentId: "agent-fixture" }, activeLease: true };
    const db = { select: () => ({ from: () => ({ where: () => ({ orderBy: async () => [candidate] }) }) }) };
    vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(Error("gone"), { code: "ESRCH" }); });
    expect(await getConversationOwnershipBlocker(db as never, "company-fixture", "issue-fixture")).toMatchObject({ cause: "execution_owner_active", runId: "run-fixture" });
  });
});
