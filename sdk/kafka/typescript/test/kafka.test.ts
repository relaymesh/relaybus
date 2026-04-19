import { describe, expect, it } from "vitest";
import { KafkaPublisher, KafkaSubscriber } from "../src/index";
import { decodeEnvelope } from "../src/core";

describe("KafkaPublisher", () => {
  it("sends encoded envelope", async () => {
    const records: any[] = [];
    const publisher = new KafkaPublisher({
      producer: {
        send: async (record) => {
          records.push(record);
        }
      },
      topicPrefix: "rb-"
    });

    await publisher.publish("alpha", {
      id: "id-1",
      topic: "alpha",
      payload: Buffer.from("hi")
    });

    expect(records).toHaveLength(1);
    expect(records[0].topic).toBe("rb-alpha");
    const decoded = decodeEnvelope(records[0].value);
    expect(decoded.id).toBe("id-1");
  });
});

describe("KafkaSubscriber", () => {
  it("decodes envelope", async () => {
    let seen = "";
    const subscriber = new KafkaSubscriber({
      onMessage: (msg) => {
        seen = msg.id;
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

    expect(seen).toBe("id");
  });

  it("rejects missing fields", async () => {
    const subscriber = new KafkaSubscriber({ onMessage: () => {} });
    await expect(subscriber.handleMessage("{}"))
      .rejects.toThrow(/invalid v|invalid id/);
  });

  it("subscribes using configured topic prefix", async () => {
    const subscribedTopics: string[] = [];
    const fromBeginningValues: boolean[] = [];
    const subscriber = new KafkaSubscriber({ onMessage: () => {} });
    const fakeConsumer = {
      subscribe: async ({ topic, fromBeginning }: { topic: string; fromBeginning: boolean }) => {
        subscribedTopics.push(topic);
        fromBeginningValues.push(fromBeginning);
      },
      run: async ({ eachMessage }: { eachMessage: (args: { message: { value: Buffer } }) => Promise<void> }) => {
        await eachMessage({ message: { value: Buffer.from("{}") } });
      },
      stop: async () => {},
      disconnect: async () => {}
    };

    (subscriber as unknown as { consumer: typeof fakeConsumer; maxMessages: number; prefix: string }).consumer = fakeConsumer;
    (subscriber as unknown as { consumer: typeof fakeConsumer; maxMessages: number; prefix: string }).maxMessages = 1;
    (subscriber as unknown as { consumer: typeof fakeConsumer; maxMessages: number; prefix: string }).prefix = "rb-";

    await expect(subscriber.start("alpha")).rejects.toThrow(/invalid v|invalid id/);
    expect(subscribedTopics).toEqual(["rb-alpha"]);
    expect(fromBeginningValues).toEqual([false]);
  });

  it("joins prefix with dot when no delimiter", async () => {
    const records: any[] = [];
    const publisher = new KafkaPublisher({
      producer: {
        send: async (record) => {
          records.push(record);
        }
      },
      topicPrefix: "rb"
    });

    await publisher.publish("alpha", { topic: "alpha", payload: Buffer.from("hi") });
    expect(records[0].topic).toBe("rb.alpha");
  });
});
