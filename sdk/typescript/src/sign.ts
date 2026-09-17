/**
 * The `FBSIG1` request signature: HMAC-SHA256 under the API key's secret over
 * `key_id \n ts(u64 LE) nonce \n METHOD \n path \n query \n sha256hex(body)`, the
 * same bytes `data_types::mgmt_sig::mgmt_signature` verifies, so the
 * secret never travels.
 */

const encoder = new TextEncoder();

function hex(bytes: ArrayBuffer | Uint8Array): string {
  return Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, "0")).join("");
}

function u64le(value: bigint): Uint8Array {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, value, true);
  return out;
}

function concat(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

export interface FbSig1Parts {
  keyId: string;
  secret: string;
  method: string;
  /** Path without the query string. */
  path: string;
  /** Raw query string without the `?`, empty when absent; covered so `force` cannot be added later. */
  query: string;
  body: Uint8Array;
  timestampMs?: number;
  nonce?: Uint8Array;
}

export async function fbSig1Signature(parts: FbSig1Parts): Promise<{
  header: string;
  timestampMs: number;
  nonce: Uint8Array;
}> {
  const timestampMs = parts.timestampMs ?? Date.now();
  const nonce = parts.nonce ?? crypto.getRandomValues(new Uint8Array(16));
  const bodySha = hex(await crypto.subtle.digest("SHA-256", parts.body as BufferSource));
  const newline = encoder.encode("\n");
  const message = concat([
    encoder.encode(parts.keyId),
    newline,
    u64le(BigInt(timestampMs)),
    nonce,
    newline,
    encoder.encode(parts.method),
    newline,
    encoder.encode(parts.path),
    newline,
    encoder.encode(parts.query),
    newline,
    encoder.encode(bodySha),
  ]);
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(parts.secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = hex(await crypto.subtle.sign("HMAC", key, message as BufferSource));
  const header = `FBSIG1 key_id=${parts.keyId}, ts=${timestampMs}, nonce=${hex(nonce)}, sig=${sig}`;
  return { header, timestampMs, nonce };
}
