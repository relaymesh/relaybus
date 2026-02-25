import { describe, expect, it } from "vitest";
import { NatsPublisher, NatsSubscriber } from "../src/index";
import { decodeEnvelope } from "../src/core";

describe("NatsPublisher", () => {
  it("publishes encoded envelope with subject prefix", async () => {
    const calls: any[] = [];
    const publisher = new NatsPublisher({
      client: {
        publish: async (subject, data) => {
          calls.push({ subject, data });
        }
      },
      subjectPrefix: "events"
    });

    await publisher.publish("alpha", {
      id: "id-1",
      topic: "alpha",
      payload: Buffer.from("hi")
    });

    expect(calls).toHaveLength(1);
    expect(calls[0].subject).toBe("events.alpha");
    const decoded = decodeEnvelope(calls[0].data);
    expect(decoded.id).toBe("id-1");
  });
});

describe("NatsSubscriber", () => {
  it("decodes messages", async () => {
    let seen = "";
    const subscriber = new NatsSubscriber({
      onMessage: (msg) => {
        seen = msg.topic;
      }
    });

    await subscriber.handleMessage(
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

    expect(seen).toBe("alpha");
  });

  it("rejects invalid base64", async () => {
    const subscriber = new NatsSubscriber({ onMessage: () => {} });
    await expect(
      subscriber.handleMessage(
        Buffer.from(
          JSON.stringify({
            v: "v1",
            id: "id",
            topic: "alpha",
            ts: "2024-01-01T00:00:00Z",
            content_type: "text/plain",
            payload_b64: "???",
            meta: {}
          })
        )
      )
    ).rejects.toThrow(/invalid payload_b64/);
  });
});
