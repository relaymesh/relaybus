import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";
import { decodeEnvelope } from "../src/index";

const rootDir = path.resolve(__dirname, "../../../..", "spec", "corpus");
const samplesDir = path.join(rootDir, "samples");
const expectedDir = path.join(rootDir, "expected");

type Expected = {
  v: "v1";
  id: string;
  topic: string;
  ts: string;
  content_type: string;
  meta: Record<string, string>;
  payload_bytes_b64: string;
};

describe("decodeEnvelope corpus", () => {
  const entries = readdirSync(samplesDir).filter((name) => name.endsWith(".json"));
  for (const name of entries) {
    it(`decodes ${name}`, () => {
      const sample = readFileSync(path.join(samplesDir, name), "utf8");
      const expected = JSON.parse(readFileSync(path.join(expectedDir, name), "utf8")) as Expected;
      const decoded = decodeEnvelope(sample);

      expect(decoded.v).toBe(expected.v);
      expect(decoded.id).toBe(expected.id);
      expect(decoded.topic).toBe(expected.topic);
      const expectedDate = new Date(expected.ts);
      expect(decoded.ts.toISOString()).toBe(expectedDate.toISOString());
      expect(decoded.contentType).toBe(expected.content_type);
      expect(decoded.meta).toEqual(expected.meta);
      expect(decoded.payload.toString("base64")).toBe(expected.payload_bytes_b64);
    });
  }
});

describe("decodeEnvelope validation", () => {
  it("rejects invalid json", () => {
    expect(() => decodeEnvelope("{" as string)).toThrow(/invalid json/);
  });

  it("rejects missing fields", () => {
    const sample = JSON.stringify({ v: "v1" });
    expect(() => decodeEnvelope(sample)).toThrow(/invalid id/);
  });

  it("rejects invalid base64", () => {
    const sample = JSON.stringify({
      v: "v1",
      id: "id",
      topic: "t",
      ts: "2024-01-01T00:00:00Z",
      content_type: "text/plain",
      payload_b64: "???",
      meta: {}
    });
    expect(() => decodeEnvelope(sample)).toThrow(/invalid payload_b64/);
  });
});
