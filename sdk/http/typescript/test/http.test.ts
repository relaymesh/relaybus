import { describe, expect, it } from "vitest";
import { HttpPublisher, HttpSubscriber } from "../src/index";
import { decodeEnvelope } from "../src/core";

describe("HttpPublisher", () => {
  it("posts encoded envelope", async () => {
    const calls: any[] = [];
    const publisher = new HttpPublisher({
      endpoint: "https://example.test/{topic}",
      doer: async (req) => {
        calls.push(req);
        return { status: 204 };
      }
    });

    await publisher.publish("alpha", {
      id: "id-1",
      topic: "alpha",
      payload: Buffer.from("hi")
    });

    expect(calls).toHaveLength(1);
    const call = calls[0];
    expect(call.url).toBe("https://example.test/alpha");
    expect(call.headers["Content-Type"]).toBe("application/json");
    expect(call.headers["Idempotency-Key"]).toBe("id-1");
    const decoded = decodeEnvelope(call.body);
    expect(decoded.id).toBe("id-1");
  });

  it("rejects non-2xx", async () => {
    const publisher = new HttpPublisher({
      endpoint: "https://example.test/{topic}",
      doer: async () => ({ status: 500 })
    });

    await expect(
      publisher.publish("alpha", { topic: "alpha", payload: Buffer.from("hi") })
    ).rejects.toThrow(/http status 500/);
  });

  it("includes response body in non-2xx errors", async () => {
    const publisher = new HttpPublisher({
      endpoint: "https://example.test/{topic}",
      doer: async () => ({ status: 500, body: '{"error":"bad gateway"}' })
    });

    await expect(
      publisher.publish("alpha", { topic: "alpha", payload: Buffer.from("hi") })
    ).rejects.toThrow(/bad gateway/);
  });

  it("rejects non-http endpoint in connect", () => {
    expect(() => HttpPublisher.connect({ endpoint: "ftp://example.test/topic" })).toThrow(
      /valid http\(s\) URL/
    );
  });
});

describe("HttpSubscriber", () => {
  it("decodes and forwards", async () => {
    let seen = "";
    const subscriber = new HttpSubscriber({
      onMessage: (msg) => {
        seen = msg.id;
      }
    });

    await subscriber.handle(
      Buffer.from(
        JSON.stringify({
          v: "v1",
          id: "id",
          topic: "alpha",
          ts: "2024-01-01T00:00:00Z",
          content_type: "text/plain",
          payload_b64: "aGVsbG8=",
          meta: {}
        })
      )
    );

    expect(seen).toBe("id");
  });

  it("rejects invalid json", async () => {
    const subscriber = new HttpSubscriber({
      onMessage: () => {}
    });
    await expect(subscriber.handle("{")).rejects.toThrow(/invalid json/);
  });
});
