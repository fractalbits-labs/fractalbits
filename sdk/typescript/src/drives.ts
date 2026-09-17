import { ApiError } from "./errors.js";
import type { FractalbitsClient } from "./client.js";
import { mountConfig, type MountConfig, type MountOptions } from "./mount.js";

export type DriveStatus = "ready" | "deleting";

export interface Drive {
  id: string;
  name: string;
  displayName: string;
  labels: Record<string, string>;
  status: DriveStatus;
  createdAt: string;
  deletingSince?: string;
  staleDeleting?: boolean;
}

export interface CreateDriveOptions {
  name: string;
  displayName?: string;
  labels?: Record<string, string>;
}

export interface ListDrivesOptions {
  limit?: number;
  cursor?: string;
}

export interface Page<T> {
  data: T[];
  meta: { hasMore: boolean; nextCursor?: string };
}

export class Drives {
  constructor(private readonly client: FractalbitsClient) {}

  async create(opts: CreateDriveOptions): Promise<Drive> {
    return this.client.request<Drive>("POST", "/v1/drives", {
      name: opts.name,
      displayName: opts.displayName ?? "",
      labels: opts.labels ?? {},
    });
  }

  /** `create`, and on `409 already_exists` the existing drive. */
  async createIfNotExists(opts: CreateDriveOptions): Promise<Drive> {
    try {
      return await this.create(opts);
    } catch (e) {
      if (e instanceof ApiError && e.code === "already_exists") {
        return this.get(opts.name);
      }
      throw e;
    }
  }

  async listPage(opts: ListDrivesOptions = {}): Promise<Page<Drive>> {
    const query = new URLSearchParams();
    if (opts.limit !== undefined) query.set("limit", String(opts.limit));
    if (opts.cursor !== undefined) query.set("cursor", opts.cursor);
    const qs = query.size > 0 ? `?${query}` : "";
    return this.client.request<Page<Drive>>("GET", `/v1/drives${qs}`);
  }

  /** Every drive the key can read, following cursors. */
  async *list(opts: ListDrivesOptions = {}): AsyncIterableIterator<Drive> {
    let cursor = opts.cursor;
    for (;;) {
      const page = await this.listPage({ limit: opts.limit, cursor });
      yield* page.data;
      if (!page.meta.hasMore || page.meta.nextCursor === undefined) return;
      cursor = page.meta.nextCursor;
    }
  }

  /** `ref` is a name or an id. */
  async get(ref: string): Promise<Drive> {
    return this.client.request<Drive>("GET", `/v1/drives/${encodeURIComponent(ref)}`);
  }

  /**
   * Without `force`: 204, or `409 not_empty` when objects remain. With
   * `force`: 202 and the drive empties in the background; poll `get`
   * until it answers `404 not_found`.
   */
  async delete(ref: string, opts: { force?: boolean } = {}): Promise<Drive | undefined> {
    const qs = opts.force ? "?force=true" : "";
    return this.client.request<Drive | undefined>(
      "DELETE",
      `/v1/drives/${encodeURIComponent(ref)}${qs}`,
    );
  }

  /** Poll `get` until the drive is gone; false on timeout. */
  async waitDeleted(ref: string, timeoutMs = 120_000, intervalMs = 500): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      try {
        await this.get(ref);
      } catch (e) {
        if (e instanceof ApiError && e.code === "not_found") return true;
        throw e;
      }
      await new Promise((r) => setTimeout(r, intervalMs));
    }
    return false;
  }

  /** Configuration for `artfs-mount` inside a sandbox; no control-plane call. */
  mountConfig(driveName: string, opts: MountOptions): MountConfig {
    return mountConfig(this.client.options, driveName, opts);
  }
}
