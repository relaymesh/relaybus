import { describe, expect, it } from "vitest";
import { encodeEnvelope } from "../src/index";

describe("encodeEnvelope", () => {
  it("sets defaults with injected clock/id", () => {
    const now = new Date("2024-01-01T00:00:00Z");
    const data = encodeEnvelope(
      {
        topic: "alpha",
        payload: Buffer.from("hi")
      },
      {
        now: () => now,
        idGenerator: () => "id-123"
      }
    );

    const parsed = JSON.parse(data.toString("utf8"));
    expect(parsed.v).toBe("v1");
    expect(parsed.id).toBe("id-123");
    expect(parsed.topic).toBe("alpha");
    expect(parsed.ts).toBe(now.toISOString());
    expect(parsed.content_type).toBe("application/octet-stream");
    expect(parsed.payload_b64).toBe(Buffer.from("hi").toString("base64"));
    expect(parsed.meta).toEqual({});
  });

  it("rejects missing topic", () => {
    expect(() =>
      encodeEnvelope({
        topic: "",
        payload: Buffer.from("hi")
      })
    ).toThrow(/topic is required/);
  });

  it("rejects missing payload", () => {
    expect(() =>
      encodeEnvelope({
        topic: "alpha",
        payload: undefined as unknown as Buffer
      })
    ).toThrow(/payload is required/);
  });

  it("rejects invalid meta", () => {
    expect(() =>
      encodeEnvelope({
        topic: "alpha",
        payload: Buffer.from("hi"),
        meta: { key: 123 as unknown as string }
      })
    ).toThrow(/invalid meta/);
  });
});
