import { randomUUID } from "node:crypto";

export type DecodedMessage = {
  v: "v1";
  id: string;
  topic: string;
  ts: Date;
  contentType: string;
  payload: Buffer;
  meta: Record<string, string>;
};

export type OutgoingMessage = {
  id?: string;
  topic: string;
  ts?: Date;
  contentType?: string;
  payload: Buffer | Uint8Array;
  meta?: Record<string, string>;
  v?: "v1";
};

export type NormalizeOptions = {
  now?: () => Date;
  idGenerator?: () => string;
};

type EnvelopeJSON = {
  v: string;
  id: string;
  topic: string;
  ts: string;
  content_type: string;
  payload_b64: string;
  meta: Record<string, string>;
};

type NormalizedMessage = {
  id: string;
  topic: string;
  ts: Date;
  contentType: string;
  payload: Buffer;
  meta: Record<string, string>;
  v: "v1";
};

const DEFAULT_CONTENT_TYPE = "application/octet-stream";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function assertStringField(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`invalid ${field}`);
  }
  return value;
}

function assertMeta(value: unknown): Record<string, string> {
  if (!isRecord(value)) {
    throw new Error("invalid meta");
  }
  const meta: Record<string, string> = {};
  for (const [k, v] of Object.entries(value)) {
    if (typeof v !== "string") {
      throw new Error("invalid meta");
    }
    meta[k] = v;
  }
  return meta;
}

function normalizeMessage(message: OutgoingMessage, options?: NormalizeOptions): NormalizedMessage {
  if (!message) {
    throw new Error("message is required");
  }
  if (!message.topic) {
    throw new Error("topic is required");
  }
  if (message.payload === undefined || message.payload === null) {
    throw new Error("payload is required");
  }
  const payload = Buffer.isBuffer(message.payload)
    ? message.payload
    : Buffer.from(message.payload);

  const now = options?.now ?? (() => new Date());
  const idGenerator = options?.idGenerator ?? randomUUID;
  const id = message.id && message.id.length > 0 ? message.id : idGenerator();
  if (!id) {
    throw new Error("id is required");
  }
  const ts = message.ts ?? now();
  if (!(ts instanceof Date) || Number.isNaN(ts.getTime())) {
    throw new Error("invalid ts");
  }

  const meta: Record<string, string> = {};
  if (message.meta) {
    for (const [k, v] of Object.entries(message.meta)) {
      if (typeof v !== "string") {
        throw new Error("invalid meta");
      }
      meta[k] = v;
    }
  }

  const contentType = message.contentType && message.contentType.length > 0
    ? message.contentType
    : DEFAULT_CONTENT_TYPE;

  return {
    id,
    topic: message.topic,
    ts,
    contentType,
    payload,
    meta,
    v: "v1"
  };
}

function isValidBase64(value: string): boolean {
  if (value === "") {
    return true;
  }
  if (value.length % 4 !== 0) {
    return false;
  }
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(value)) {
    return false;
  }
  const decoded = Buffer.from(value, "base64");
  return decoded.toString("base64") === value;
}

export function decodeEnvelope(jsonBytes: Buffer | string): DecodedMessage {
  const raw = Buffer.isBuffer(jsonBytes) ? jsonBytes.toString("utf8") : jsonBytes;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new Error("invalid json");
  }
  if (!isRecord(parsed)) {
    throw new Error("invalid envelope");
  }

  const env = parsed as EnvelopeJSON;
  if (env.v !== "v1") {
    throw new Error("invalid v");
  }

  const id = assertStringField(env.id, "id");
  const topic = assertStringField(env.topic, "topic");
  const tsRaw = assertStringField(env.ts, "ts");
  const contentType = assertStringField(env.content_type, "content_type");

  if (typeof env.payload_b64 !== "string") {
    throw new Error("invalid payload_b64");
  }
  if (!isValidBase64(env.payload_b64)) {
    throw new Error("invalid payload_b64");
  }
  const payload = Buffer.from(env.payload_b64, "base64");

  const meta = assertMeta(env.meta);
  const ts = new Date(tsRaw);
  if (Number.isNaN(ts.getTime())) {
    throw new Error("invalid ts");
  }

  return {
    v: "v1",
    id,
    topic,
    ts,
    contentType,
    payload,
    meta
  };
}

export function encodeEnvelope(message: OutgoingMessage, options?: NormalizeOptions): Buffer {
  const normalized = normalizeMessage(message, options);
  const envelope: EnvelopeJSON = {
    v: normalized.v,
    id: normalized.id,
    topic: normalized.topic,
    ts: normalized.ts.toISOString(),
    content_type: normalized.contentType,
    payload_b64: normalized.payload.toString("base64"),
    meta: normalized.meta
  };

  return Buffer.from(JSON.stringify(envelope), "utf8");
}

export const defaults = {
  DEFAULT_CONTENT_TYPE
};
