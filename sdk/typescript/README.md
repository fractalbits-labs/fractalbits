# @fractalbits/agentfs

Control-plane SDK for fractalbits agentFS drives: create, list, get and
delete a drive, and render the `fractalbits-mount` configuration that
mounts it, or a subtree of it, inside a sandbox. Writes go through S3
or POSIX, never through this SDK.

```ts
import { FractalbitsClient } from "@fractalbits/agentfs";

const fb = new FractalbitsClient({
  mgmtUrl: "http://gateway:18080",
  gatewayAddrs: ["gateway:8180"],
  keyId: process.env.FB_KEY_ID!,
  secret: process.env.FB_SECRET!,
});

const drive = await fb.drives.create({ name: "task-4711", labels: { run: "r17" } });
const cfg = fb.drives.mountConfig(drive.name, {
  mountPath: "/mnt/data",
  subPath: "/agents/a/",
  readOnly: false,
});
// run `fractalbits-mount` in the sandbox with cfg.env or cfg.toml

await fb.drives.delete(drive.name, { force: true });
await fb.drives.waitDeleted(drive.name);
```

Requests carry an `FBSIG1` HMAC signature under the API key's secret, so
the secret never travels.

Development: `npm install`, `npm run typecheck`, `npm test` against a
local cluster started with `just service start`.
