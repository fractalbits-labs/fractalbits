# ARTFS Drive SDK

`@fractalbits/artfs` is the control-plane SDK for ARTFS drives. It
creates, lists, describes and deletes drives, and renders the
configuration that mounts a drive, or one directory of it, inside a
sandbox with `artfs-mount`. File content never goes through the SDK:
agents read and write through the POSIX mount, and orchestrators
collect results over S3.

The SDK lives in [`sdk/typescript`](../sdk/typescript) and wraps the
`/v1/drives` HTTP API served by the s3_gateway management port. This
document covers both: the TypeScript surface first, then the HTTP
API for other languages.

- [1. Model](#1-model)
- [2. Client](#2-client)
- [3. Drives](#3-drives)
- [4. Mounting a drive](#4-mounting-a-drive)
- [5. Errors](#5-errors)
- [6. HTTP API](#6-http-api)
- [7. Authentication](#7-authentication)
- [8. Development](#8-development)

## 1. Model

**A drive is a bucket**, one to one and under the same name. The
drive name is the S3 bucket name and the `bucket_name` of the mount
config, so nothing needs a lookup to go from one protocol to the
other. A drive adds control-plane metadata over the bucket: a stable
`id`, a `displayName`, `labels` and a `status`.

Because the drive is the bucket, S3 is a second door. An S3
`CreateBucket` makes a bucket that is not yet a drive; `create` by
the owning key adopts it and leaves its data alone. An S3
`DeleteBucket` removes the drive as well. A bucket deleted and
recreated under the same name is a new incarnation and never
inherits the old drive id.

Permissions are the bucket's. The key that creates a drive owns the
bucket. `list` returns the drives the key can read, `get` needs
read, `delete` needs owner. There is no drive-level permission
model, and a drive a key cannot read answers `404`, so names are
not enumerable.

## 2. Client

```ts
import { FractalbitsClient } from "@fractalbits/artfs";

const fb = new FractalbitsClient({
  mgmtUrl: "http://gateway:18080",
  gatewayAddrs: ["gateway:8180"],
  keyId: process.env.FB_KEY_ID!,
  secret: process.env.FB_SECRET!,
});
```

| Option | Type | Meaning |
|---|---|---|
| `mgmtUrl` | `string` | The s3_gateway management endpoint. Default port 18080. A trailing slash is stripped. |
| `gatewayAddrs` | `string[]` | `host:port` of the fs_gateway instances a mount may use. Only used by `mountConfig`. |
| `keyId` | `string` | API key id. |
| `secret` | `string` | API key secret. Used to sign requests locally and to render mount configs; it is never sent to the management API. |
| `fetch` | `typeof fetch` | Optional. Defaults to `globalThis.fetch`. Node 20 or newer. |

`fb.drives` is the `Drives` resource. `fb.request(method, path, body)`
is the signed JSON call every method uses and is available for
endpoints the SDK does not wrap yet.

## 3. Drives

### 3.1 The `Drive` object

```ts
type DriveStatus = "ready" | "deleting";

interface Drive {
  id: string;
  name: string;
  displayName: string;
  labels: Record<string, string>;
  status: DriveStatus;
  createdAt: string;
  deletingSince?: string;
  staleDeleting?: boolean;
}
```

| Field | Meaning |
|---|---|
| `id` | UUID minted at create. Stable across a later rename; the reference an orchestrator should store. |
| `name` | The bucket name. See [3.2](#32-create) for the rules. |
| `displayName` | Free text, empty when not set. |
| `labels` | String to string, at most 32 keys. |
| `status` | `ready`, or `deleting` while a force delete runs. |
| `createdAt` | RFC 3339. |
| `deletingSince` | RFC 3339, present only while `status` is `deleting`. |
| `staleDeleting` | Present and `true` when the drive is `deleting` but no sweeper has renewed its lease within 60 s (a gateway crashed mid-sweep). Send another `delete` with `force` to resume it. |

Every method that takes a `ref` accepts the drive `id` or its
`name`. The id is resolved first, since a UUID is also a valid
bucket name.

### 3.2 `create`

```ts
create(opts: {
  name: string;
  displayName?: string;
  labels?: Record<string, string>;
}): Promise<Drive>
```

`name` follows the S3 bucket rules: 3 to 63 characters, lowercase
letters, digits, dots and hyphens, starting and ending with a letter
or digit, not formatted as an IP address, not starting with `xn--`.
`labels` holds at most 32 keys.

Outcomes:

- A new drive, or adoption of a bucket this key already owns that
  has no drive record: the drive is returned.
- A drive this key owns with identical `displayName` and `labels`:
  the existing drive is returned. Retries are safe.
- A drive this key owns with different metadata, or a bucket owned
  by another key: `409 already_exists`.
- A drive whose force delete is still running: `409 deleting`.
- A key without permission to create buckets: `403 forbidden`.
- A bad name or too many labels: `400 invalid_name` or
  `400 invalid_labels`.

### 3.3 `createIfNotExists`

```ts
createIfNotExists(opts: CreateDriveOptions): Promise<Drive>
```

`create`, and on `409 already_exists` the existing drive from `get`.
Use it when the drive may already exist with other metadata and you
only need a handle. Any other error is rethrown, including
`409 deleting`.

### 3.4 `list` and `listPage`

```ts
listPage(opts?: { limit?: number; cursor?: string }): Promise<Page<Drive>>
list(opts?: { limit?: number; cursor?: string }): AsyncIterableIterator<Drive>

interface Page<T> {
  data: T[];
  meta: { hasMore: boolean; nextCursor?: string };
}
```

`listPage` returns one page of the drives the key can read, sorted
by name. `limit` defaults to 50 and is clamped to 1 through 200.
`cursor` is opaque; pass `meta.nextCursor` back to continue. A drive
whose bucket has been deleted or recreated through S3 is filtered
out.

`list` is an async iterator over `listPage` that follows cursors
until `hasMore` is false:

```ts
for await (const drive of fb.drives.list({ limit: 100 })) {
  console.log(drive.name, drive.labels);
}
```

### 3.5 `get`

```ts
get(ref: string): Promise<Drive>
```

The drive by id or name. `404 not_found` when it does not exist,
when the key cannot read it, or when its bucket has been deleted or
recreated through S3 since the drive was created. In the last case
the stale record is repaired as a side effect.

### 3.6 `delete`

```ts
delete(ref: string, opts?: { force?: boolean }): Promise<Drive | undefined>
```

Without `force`, the drive must be empty. The bucket and the record
are removed and the call resolves to `undefined` (HTTP 204). If
objects remain, `409 not_empty`.

With `force`, the call resolves to the drive with `status:
"deleting"` (HTTP 202) and the gateway empties the bucket in the
background: it pages the objects, deletes them in batches, then
removes the bucket and the record. Cost is proportional to the
object count. Poll `get` until it answers `404`, or use
`waitDeleted`. While `status` is `deleting`:

- `create` of the same name answers `409 deleting`.
- Mounts and S3 requests still resolve until the final bucket
  removal.
- A repeated `delete` with `force` answers 202 again without
  starting a second sweep, whichever gateway it reaches.

Only one sweeper runs per drive. If the gateway running it crashes,
`get` reports `staleDeleting: true` after 60 s and the next `delete`
with `force` resumes from the remaining objects.

`force` is destructive, so the request is authorized against the key
as stored in RSS rather than the gateway's key cache: a revoked key
or one that lost ownership gets `403 forbidden` even if the gateway
still has it cached. The `force` query parameter is covered by the
request signature ([7. Authentication](#7-authentication)), so a
signature over a plain delete cannot be replayed as a force delete.

`delete` of a missing drive is `404 not_found`. A reader that is not
an owner gets `403 forbidden`; a key that cannot read the drive gets
`404`.

**Mounted clients** see the bucket disappear as `ENOENT` on every
path, the same as an S3 `DeleteBucket` under a live mount.

### 3.7 `waitDeleted`

```ts
waitDeleted(ref: string, timeoutMs = 120_000, intervalMs = 500): Promise<boolean>
```

Polls `get` until it answers `404 not_found`. Resolves `true` when the
drive is gone and `false` on timeout. Any error other than
`not_found` is rethrown.

### 3.8 `mountConfig`

```ts
mountConfig(driveName: string, opts: {
  mountPath: string;
  prefix?: string;
  readOnly?: boolean;
}): { toml: string; env: Record<string, string> }
```

Renders the `artfs-mount` configuration for a drive or a subtree of
it. No control-plane call is made; see
[4. Mounting a drive](#4-mounting-a-drive).

## 4. Mounting a drive

The SDK does not mount. It produces the configuration for
`artfs-mount` (the binary built from `crates/fs_client`), and the
orchestrator runs that binary inside the sandbox however it runs
anything else. The mount authenticates against fs_gateway with the
same API key the client was built with.

```ts
const cfg = fb.drives.mountConfig("task-4711", {
  mountPath: "/mnt/data",
  prefix: "/agents/a/",
  readOnly: false,
});
```

| Option | Default | Meaning |
|---|---|---|
| `mountPath` | required | Where the drive appears inside the sandbox. |
| `prefix` | `/` | The directory of the drive to mount. Must start and end with `/`, with no empty, `.` or `..` segments. `""` and `/` both mean the whole drive. |
| `readOnly` | `true` | Read-only is the `artfs-mount` default. Pass `false` for a writable mount. |

`mountConfig` throws a plain `Error` on an invalid `prefix`.
`normalizePrefix(prefix)` is exported for callers that validate
earlier.

The result has two equivalent forms:

- `toml`: the contents of a file for `artfs-mount --config <file>`.
  Fields not set here take the binary's defaults.
- `env`: the same values as `FS_MOUNT_*` environment overrides, for
  images that ship a base config file.

```toml
gateway_addrs = ["gateway:8180"]
bucket_name = "task-4711"
mount_point = "/mnt/data"
api_key_id = "..."
api_key_secret = "..."
read_write = true
prefix = "/agents/a/"
```

| TOML key | Env var |
|---|---|
| `gateway_addrs` | `FS_MOUNT_GATEWAY_ADDRS` (comma separated) |
| `bucket_name` | `FS_MOUNT_BUCKET_NAME` |
| `mount_point` | `FS_MOUNT_MOUNT_POINT` |
| `api_key_id` | `FS_MOUNT_API_KEY_ID` |
| `api_key_secret` | `FS_MOUNT_API_KEY_SECRET` |
| `read_write` | `FS_MOUNT_READ_WRITE` (`true` or `false`) |
| `prefix` | `FS_MOUNT_PREFIX` |

Both forms carry the API key secret, so treat them like the secret
itself.

### 4.1 Subtree mounts

`prefix` mounts one directory of a drive as the root of the
filesystem the sandbox sees, the case of one drive holding many
project directories. The scope is enforced by fs_gateway: every
request is checked against the prefix in the session, listings are
clamped to it, and a rename across the boundary fails with `EXDEV`.
A read-write mount creates the prefix directory if it is missing; a
read-only mount of a missing prefix fails at mount time.

Scopes nest and overlap freely. `/repo/` and `/repo/tests/` can be
mounted at once, in the same or different sandboxes, and see the
same files. Coherence between mounts is by kernel cache TTL; there
is no shared dirty state.

At this step a scope is a narrowing the orchestrator chooses, not a
security boundary: the key still has whole-bucket permissions, so
the same key could mount `/`. The check protects against a tool
wandering out of its directory.

### 4.2 Read-only mounts

`readOnly` is enforced at fs_gateway, which refuses every mutation
on a read-only session, not only in the FUSE layer.

### 4.3 No listMounts, no unmount

fs_gateway holds no per-mount state, so nothing can enumerate
mounts. Unmount with `umount` inside the sandbox.

## 5. Errors

Every non-2xx answer from `/v1` carries one JSON shape, and the SDK
throws it as `ApiError`:

```ts
class ApiError extends Error {
  status: number;   // HTTP status
  code: string;     // machine-readable, see below
  message: string;
  details?: unknown;
}
```

```ts
try {
  await fb.drives.create({ name });
} catch (e) {
  if (e instanceof ApiError && e.code === "already_exists") { /* ... */ }
  else throw e;
}
```

| Status | `code` | When |
|---|---|---|
| 400 | `invalid_name` | `name` breaks the bucket naming rules. |
| 400 | `invalid_labels` | More than 32 labels. |
| 400 | `bad_body` | The request body could not be read. |
| 401 | `unauthorized` | Missing or malformed `FBSIG1` header, unknown or deleted key, bad signature, timestamp outside the 300 s window. |
| 403 | `forbidden` | Key may not create buckets, is not an owner on `delete`, or no longer owns the drive on a `force` delete. |
| 404 | `not_found` | No such drive, the key cannot read it, or its bucket is gone. |
| 409 | `already_exists` | Drive exists with different metadata, or the bucket belongs to another key. |
| 409 | `deleting` | `create` while a force delete is running. |
| 409 | `not_empty` | `delete` without `force` on a drive that holds objects. |
| 413 | `too_large` | Request body over 64 KiB. |
| 500 | `internal` | An RSS or gateway failure; the message names the step. |

A non-JSON error body maps to `code: "http_error"` with the HTTP
status text as the message.

## 6. HTTP API

For clients in other languages. Base URL is the management endpoint
(`mgmtUrl`). Every request needs the `Authorization` header of
[7. Authentication](#7-authentication). Request and response bodies
are JSON with camelCase fields.

| Method | Path | Success |
|---|---|---|
| `POST` | `/v1/drives` | 200 `Drive` |
| `GET` | `/v1/drives?limit=&cursor=` | 200 `Page<Drive>` |
| `GET` | `/v1/drives/{ref}` | 200 `Drive` |
| `DELETE` | `/v1/drives/{ref}` | 204, empty body |
| `DELETE` | `/v1/drives/{ref}?force=true` | 202 `Drive` with `status: "deleting"` |

`{ref}` is a drive id or name, URL-encoded.

### 6.1 Create

```
POST /v1/drives
Content-Type: application/json

{ "name": "task-4711", "displayName": "Task 4711", "labels": { "run": "r17" } }
```

`displayName` and `labels` are optional and default to `""` and `{}`.

```json
{
  "id": "0d1f6c8e-7a2b-4c3d-9e1f-2a3b4c5d6e7f",
  "name": "task-4711",
  "displayName": "Task 4711",
  "labels": { "run": "r17" },
  "status": "ready",
  "createdAt": "2026-09-12T23:03:28.417+00:00"
}
```

### 6.2 List

```
GET /v1/drives?limit=50&cursor=task-4710
```

```json
{
  "data": [ { "id": "...", "name": "task-4711", "...": "..." } ],
  "meta": { "hasMore": true, "nextCursor": "task-4711" }
}
```

`nextCursor` is omitted when `hasMore` is false. Results are sorted
by name; the cursor is the last name of the previous page.

### 6.3 Get

```
GET /v1/drives/task-4711
GET /v1/drives/0d1f6c8e-7a2b-4c3d-9e1f-2a3b4c5d6e7f
```

Answers the `Drive` object. During a force delete it also carries
`deletingSince`, and `staleDeleting: true` once the sweep lease has
lapsed.

### 6.4 Delete

```
DELETE /v1/drives/task-4711
DELETE /v1/drives/task-4711?force=true
```

The first answers 204 or `409 not_empty`. The second answers 202 with
the drive in `deleting`; poll `GET` until `404`.

### 6.5 Error body

```json
{ "code": "not_empty", "message": "drive has objects; use force" }
```

## 7. Authentication

`/v1` requests carry an HMAC signature under the API key's secret,
so the secret never travels and no AWS signer is needed:

```
Authorization: FBSIG1 key_id=<id>, ts=<unix ms>, nonce=<hex>, sig=<hex>
```

`sig` is hex of HMAC-SHA256 under the secret over these bytes, in
order:

```
key_id
"\n"
ts as u64 little-endian (8 bytes)
nonce (raw bytes, 16 random bytes in the SDK)
"\n"
METHOD            e.g. DELETE
"\n"
path              e.g. /v1/drives/task-4711, without the query
"\n"
query             raw query string without "?", empty when absent
"\n"
sha256 hex of the body, empty body included
```

The gateway rejects a `ts` more than 300 s from its clock, a deleted
or unknown key, and a signature mismatch, all as
`401 unauthorized`. The query string is signed so that `force=true`
cannot be appended to a request signed without it.

The SDK exports the signer for other callers:

```ts
import { fbSig1Signature } from "@fractalbits/artfs";

const { header } = await fbSig1Signature({
  keyId, secret,
  method: "GET",
  path: "/v1/drives",
  query: "limit=10",
  body: new Uint8Array(),
});
```

It uses WebCrypto, so it runs in Node 20 and newer and in browsers.
The same bytes are verified by `mgmt_signature` in
`crates/common/data_types/src/mgmt_sig.rs`, which is the reference
for implementations in other languages.

API keys are created with `rss_admin`; see
[API_KEY_MANAGEMENT.md](API_KEY_MANAGEMENT.md).

## 8. Development

```
cd sdk/typescript
npm install
npm run typecheck
npm run format:check
npm test
```

The tests run against a local cluster started with `just service
start` and the test key `just service init` creates. Point them
elsewhere with `FRACTALBITS_MGMT_URL`, `FRACTALBITS_KEY_ID` and
`FRACTALBITS_SECRET`.

The end-to-end drive and subtree cases on the Rust side run under
`just run-tests fs-server`, and the gateway's own drive suite under
`cargo test -p s3_gateway --test drives` with services up.
