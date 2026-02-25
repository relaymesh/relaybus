import { describe, expect, it } from "vitest";
import { AmqpPublisher } from "../src/index";
import { decodeEnvelope } from "../src/core";

type PublishCall = {
  exchange: string;
  routingKey: string;
  content: Buffer;
  options?: any;
};

describe("AmqpPublisher", () => {
  it("publishes encoded envelope with routing key", async () => {
    const calls: PublishCall[] = [];
    const channel = {
      publish: async (exchange: string, routingKey: string, content: Buffer, options?: any) => {
        calls.push({ exchange, routingKey, content, options });
      }
    };

    const publisher = new AmqpPublisher({
      channel,
      exchange: "ex",
      routingKeyTemplate: "events.{topic}"
    });

    await publisher.publish("alpha", {
      id: "id-1",
      topic: "alpha",
      payload: Buffer.from("hi"),
      meta: { source: "unit" }
    });

    expect(calls).toHaveLength(1);
    const call = calls[0];
    expect(call.exchange).toBe("ex");
    expect(call.routingKey).toBe("events.alpha");
    const decoded = decodeEnvelope(call.content);
    expect(decoded.id).toBe("id-1");
    expect(decoded.topic).toBe("alpha");
    expect(decoded.payload.toString()).toBe("hi");
  });

  it("rejects topic mismatch", async () => {
    const channel = {
      publish: async () => {}
    };
    const publisher = new AmqpPublisher({ channel });
    await expect(
      publisher.publish("alpha", { topic: "beta", payload: Buffer.from("hi") })
    ).rejects.toThrow(/topic mismatch/);
  });
});
