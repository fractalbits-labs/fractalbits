import type { ClientOptions } from "./client.js";

export interface MountOptions {
  /** Where the drive appears inside the sandbox. */
  mountPath: string;
  /** Directory of the drive to mount, `/` by default. Must start and end with `/`. */
  prefix?: string;
  /** Default true, as `artfs-mount` defaults to read-only. */
  readOnly?: boolean;
}

export interface MountConfig {
  /** A `artfs-mount --config` file; unset fields take the binary's defaults. */
  toml: string;
  /** The same values as `FS_MOUNT_*` overrides, for images that ship a base config. */
  env: Record<string, string>;
}

function tomlString(value: string): string {
  return JSON.stringify(value);
}

export function normalizePrefix(prefix: string | undefined): string {
  if (prefix === undefined || prefix === "" || prefix === "/") {
    return "/";
  }
  if (!prefix.startsWith("/") || !prefix.endsWith("/")) {
    throw new Error(`prefix must start and end with "/": ${prefix}`);
  }
  for (const segment of prefix.slice(1, -1).split("/")) {
    if (segment === "" || segment === "." || segment === "..") {
      throw new Error(`prefix has an empty, "." or ".." segment: ${prefix}`);
    }
  }
  return prefix;
}

/** The mount configuration for a drive, or a subtree of it, that the orchestrator runs inside a sandbox. */
export function mountConfig(
  client: ClientOptions,
  driveName: string,
  opts: MountOptions,
): MountConfig {
  const prefix = normalizePrefix(opts.prefix);
  const readWrite = !(opts.readOnly ?? true);
  const env: Record<string, string> = {
    FS_MOUNT_GATEWAY_ADDRS: client.gatewayAddrs.join(","),
    FS_MOUNT_BUCKET_NAME: driveName,
    FS_MOUNT_MOUNT_POINT: opts.mountPath,
    FS_MOUNT_API_KEY_ID: client.keyId,
    FS_MOUNT_API_KEY_SECRET: client.secret,
    FS_MOUNT_READ_WRITE: String(readWrite),
    FS_MOUNT_PREFIX: prefix,
  };
  const toml = [
    `gateway_addrs = [${client.gatewayAddrs.map(tomlString).join(", ")}]`,
    `bucket_name = ${tomlString(driveName)}`,
    `mount_point = ${tomlString(opts.mountPath)}`,
    `api_key_id = ${tomlString(client.keyId)}`,
    `api_key_secret = ${tomlString(client.secret)}`,
    `read_write = ${readWrite}`,
    `prefix = ${tomlString(prefix)}`,
    "",
  ].join("\n");
  return { toml, env };
}
