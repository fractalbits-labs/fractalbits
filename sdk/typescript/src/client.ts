import { Drives } from "./drives.js";
import { ApiError } from "./errors.js";
import { fbSig1Signature } from "./sign.js";

export interface ClientOptions {
  /** The s3_gateway management endpoint, e.g. `http://127.0.0.1:18080`. */
  mgmtUrl: string;
  /** `host:port` of the fs_gateway instances a mount may use. */
  gatewayAddrs: string[];
  keyId: string;
  secret: string;
  fetch?: typeof fetch;
}

const encoder = new TextEncoder();

export class FractalbitsClient {
  readonly drives: Drives;
  readonly options: ClientOptions;
  private readonly fetchImpl: typeof fetch;

  constructor(options: ClientOptions) {
    this.options = { ...options, mgmtUrl: options.mgmtUrl.replace(/\/+$/, "") };
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    this.drives = new Drives(this);
  }

  /** One signed JSON call; a non-2xx answer becomes an `ApiError`. */
  async request<T>(method: string, pathAndQuery: string, body?: unknown): Promise<T> {
    const q = pathAndQuery.indexOf("?");
    const path = q < 0 ? pathAndQuery : pathAndQuery.slice(0, q);
    const query = q < 0 ? "" : pathAndQuery.slice(q + 1);
    const bytes = body === undefined ? new Uint8Array() : encoder.encode(JSON.stringify(body));
    const { header } = await fbSig1Signature({
      keyId: this.options.keyId,
      secret: this.options.secret,
      method,
      path,
      query,
      body: bytes,
    });
    const headers: Record<string, string> = { authorization: header };
    if (body !== undefined) headers["content-type"] = "application/json";
    const response = await this.fetchImpl(`${this.options.mgmtUrl}${pathAndQuery}`, {
      method,
      headers,
      body: body === undefined ? undefined : bytes,
    });
    const text = await response.text();
    const json = text.length > 0 ? JSON.parse(text) : undefined;
    if (!response.ok) {
      const code = typeof json?.code === "string" ? json.code : "http_error";
      const message = typeof json?.message === "string" ? json.message : response.statusText;
      throw new ApiError(response.status, code, message, json?.details);
    }
    return json as T;
  }
}
