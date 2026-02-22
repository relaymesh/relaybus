"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.defaults = void 0;
exports.decodeEnvelope = decodeEnvelope;
exports.encodeEnvelope = encodeEnvelope;
const node_crypto_1 = require("node:crypto");
const DEFAULT_CONTENT_TYPE = "application/octet-stream";
function isRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
function assertStringField(value, field) {
    if (typeof value !== "string" || value.length === 0) {
        throw new Error(`invalid ${field}`);
    }
    return value;
}
function assertMeta(value) {
    if (!isRecord(value)) {
        throw new Error("invalid meta");
    }
    const meta = {};
    for (const [k, v] of Object.entries(value)) {
        if (typeof v !== "string") {
            throw new Error("invalid meta");
        }
        meta[k] = v;
    }
    return meta;
}
function normalizeMessage(message, options) {
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
    const idGenerator = options?.idGenerator ?? node_crypto_1.randomUUID;
    const id = message.id && message.id.length > 0 ? message.id : idGenerator();
    if (!id) {
        throw new Error("id is required");
    }
    const ts = message.ts ?? now();
    if (!(ts instanceof Date) || Number.isNaN(ts.getTime())) {
        throw new Error("invalid ts");
    }
    const meta = {};
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
function isValidBase64(value) {
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
function decodeEnvelope(jsonBytes) {
    const raw = Buffer.isBuffer(jsonBytes) ? jsonBytes.toString("utf8") : jsonBytes;
    let parsed;
    try {
        parsed = JSON.parse(raw);
    }
    catch (err) {
        throw new Error("invalid json");
    }
    if (!isRecord(parsed)) {
        throw new Error("invalid envelope");
    }
    const env = parsed;
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
function encodeEnvelope(message, options) {
    const normalized = normalizeMessage(message, options);
    const envelope = {
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
exports.defaults = {
    DEFAULT_CONTENT_TYPE
};
