/**
 * Runs against a local cluster (`just service start`). Set
 * FRACTALBITS_MGMT_URL, FRACTALBITS_KEY_ID and FRACTALBITS_SECRET to point
 * elsewhere; the defaults are the test key `just service init` creates.
 */
import { describe, expect, it } from "vitest";
import { ApiError, FractalbitsClient, normalizePrefix } from "../src/index.js";

const client = new FractalbitsClient({
  mgmtUrl: process.env.FRACTALBITS_MGMT_URL ?? "http://127.0.0.1:18080",
  gatewayAddrs: ["127.0.0.1:8180"],
  keyId: process.env.FRACTALBITS_KEY_ID ?? "test_api_key",
  secret: process.env.FRACTALBITS_SECRET ?? "test_api_secret",
});

describe("drives", () => {
  it("creates, reads, lists and deletes", async () => {
    const name = "sdk-crud";
    await client.drives.delete(name, { force: true }).catch(() => undefined);
    expect(await client.drives.waitDeleted(name, 60_000)).toBe(true);

    const drive = await client.drives.create({ name, labels: { run: "r1" } });
    expect(drive.name).toBe(name);
    expect(drive.status).toBe("ready");
    expect(drive.labels).toEqual({ run: "r1" });

    const again = await client.drives.createIfNotExists({ name, labels: { run: "r1" } });
    expect(again.id).toBe(drive.id);
    await expect(client.drives.create({ name, labels: { run: "r2" } })).rejects.toMatchObject({
      status: 409,
      code: "already_exists",
    });

    expect((await client.drives.get(drive.id)).name).toBe(name);
    const names: string[] = [];
    for await (const d of client.drives.list({ limit: 2 })) names.push(d.name);
    expect(names).toContain(name);

    await client.drives.delete(name);
    await expect(client.drives.get(name)).rejects.toBeInstanceOf(ApiError);
  });

  it("refuses a bad name and a bad signature", async () => {
    await expect(client.drives.create({ name: "AB" })).rejects.toMatchObject({
      status: 400,
      code: "invalid_name",
    });
    const bad = new FractalbitsClient({ ...client.options, secret: "wrong" });
    await expect(bad.drives.listPage()).rejects.toMatchObject({ status: 401 });
  });

  it("force deletes in the background", async () => {
    const name = "sdk-force";
    await client.drives.delete(name, { force: true }).catch(() => undefined);
    expect(await client.drives.waitDeleted(name, 60_000)).toBe(true);
    await client.drives.create({ name });
    const accepted = await client.drives.delete(name, { force: true });
    expect(accepted?.status).toBe("deleting");
    expect(await client.drives.waitDeleted(name, 120_000)).toBe(true);
  });
});

describe("mountConfig", () => {
  it("renders the artfs-mount config", () => {
    const cfg = client.drives.mountConfig("task-1", {
      mountPath: "/mnt/data",
      prefix: "/agents/a/",
      readOnly: false,
    });
    expect(cfg.env.FS_MOUNT_BUCKET_NAME).toBe("task-1");
    expect(cfg.env.FS_MOUNT_PREFIX).toBe("/agents/a/");
    expect(cfg.env.FS_MOUNT_READ_WRITE).toBe("true");
    expect(cfg.toml).toContain('bucket_name = "task-1"');
    expect(cfg.toml).toContain('prefix = "/agents/a/"');
    expect(cfg.toml).toContain("read_write = true");
    expect(client.drives.mountConfig("t", { mountPath: "/m" }).env.FS_MOUNT_READ_WRITE).toBe(
      "false",
    );
  });

  it("validates prefix", () => {
    expect(normalizePrefix(undefined)).toBe("/");
    expect(normalizePrefix("/a/b/")).toBe("/a/b/");
    expect(() => normalizePrefix("a/")).toThrow();
    expect(() => normalizePrefix("/a")).toThrow();
    expect(() => normalizePrefix("/a//b/")).toThrow();
    expect(() => normalizePrefix("/../")).toThrow();
  });
});
