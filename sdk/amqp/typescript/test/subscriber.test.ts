import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import path from "node:path";
import { AmqpSubscriber } from "../src/index";

const rootDir = path.resolve(__dirname, "../../../..", "spec", "corpus");
const samplePath = path.join(rootDir, "samples", "sample1.json");
const expectedPath = path.join(rootDir, "expected", "sample1.json");

type Expected = {
  v: "v1";
  id: string;
  topic: string;
  ts: string;
  content_type: string;
  meta: Record<string, string>;
  payload_bytes_b64: string;
};

describe("AmqpSubscriber", () => {
  it("invokes handler with decoded message", async () => {
    const sample = readFileSync(samplePath, "utf8");
    const expected = JSON.parse(readFileSync(expectedPath, "utf8")) as Expected;

    let received: any = null;
    const subscriber = new AmqpSubscriber({
      onMessage: (msg) => {
        received = msg;
      }
    });

    await subscriber.handleDelivery({ content: Buffer.from(sample) });

    expect(received).not.toBeNull();
    expect(received.id).toBe(expected.id);
    expect(received.topic).toBe(expected.topic);
    const expectedDate = new Date(expected.ts);
    expect(received.ts.toISOString()).toBe(expectedDate.toISOString());
    expect(received.contentType).toBe(expected.content_type);
    expect(received.meta).toEqual(expected.meta);
    expect(received.payload.toString("base64")).toBe(expected.payload_bytes_b64);
  });
});
