import { decodeEnvelope, encodeEnvelope, DecodedMessage, OutgoingMessage } from "./core";
import { Kafka, Consumer, Producer, Admin } from "kafkajs";

export type KafkaProducer = {
  send: (record: { topic: string; key?: Buffer; value: Buffer }) => Promise<void> | void;
};

export type KafkaPublisherConfig = {
  producer: KafkaProducer;
  topicPrefix?: string;
  admin?: Admin;
};

export type KafkaPublisherConnectConfig = {
  brokers: string[];
  topicPrefix?: string;
  clientId?: string;
};

export class KafkaPublisher {
  private readonly producer: KafkaProducer;
  private readonly prefix: string;
  private rawProducer?: Producer;
  private admin?: Admin;
  private ownsAdmin = false;
  private readonly ensuredTopics = new Set<string>();

  constructor(config: KafkaPublisherConfig) {
    if (!config.producer) {
      throw new Error("producer is required");
    }
    this.producer = config.producer;
    this.prefix = config.topicPrefix ?? "";
    this.admin = config.admin;
    this.ownsAdmin = false;
  }

  static async connect(config: KafkaPublisherConnectConfig): Promise<KafkaPublisher> {
    const kafka = new Kafka({
      clientId: config.clientId ?? "relaybus",
      brokers: config.brokers
    });
    const producer = kafka.producer();
    const admin = kafka.admin();
    await producer.connect();
    await admin.connect();
    const publisher = new KafkaPublisher({
      producer: {
        send: async (record) => {
          await producer.send({
            topic: record.topic,
            messages: [{ key: record.key, value: record.value }]
          });
        }
      },
      topicPrefix: config.topicPrefix
    });
    publisher.rawProducer = producer;
    publisher.admin = admin;
    publisher.ownsAdmin = true;
    return publisher;
  }

  async publish(topic: string, message: OutgoingMessage): Promise<void> {
    const resolved = resolveTopic(topic, message.topic);
    const payload = encodeEnvelope({ ...message, topic: resolved });
    const record = {
      topic: `${this.prefix}${resolved}`,
      key: message.id ? Buffer.from(message.id) : undefined,
      value: payload
    };
    await this.ensureTopic(record.topic);
    await Promise.resolve(this.producer.send(record));
  }

  async close(): Promise<void> {
    if (this.rawProducer) {
      await this.rawProducer.disconnect();
    }
    if (this.admin && this.ownsAdmin) {
      await this.admin.disconnect();
    }
  }

  private async ensureTopic(topic: string): Promise<void> {
    if (!this.admin) {
      return;
    }
    if (this.ensuredTopics.has(topic)) {
      return;
    }
    try {
      await this.admin.createTopics({
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
        waitForLeaders: true
      });
      this.ensuredTopics.add(topic);
    } catch (err) {
      if (isTopicAlreadyExists(err)) {
        this.ensuredTopics.add(topic);
        return;
      }
      // Best-effort provisioning: allow publish to proceed even if create fails.
    }
  }
}

export type KafkaSubscriberConfig = {
  onMessage: (msg: DecodedMessage) => void | Promise<void>;
};

export type KafkaSubscriberConnectConfig = {
  brokers: string[];
  onMessage: (msg: DecodedMessage) => void | Promise<void>;
  groupId?: string;
  clientId?: string;
  maxMessages?: number;
};

export class KafkaSubscriber {
  private readonly onMessage: (msg: DecodedMessage) => void | Promise<void>;
  private consumer?: Consumer;
  private maxMessages?: number;

  constructor(config: KafkaSubscriberConfig) {
    this.onMessage = config.onMessage;
  }

  static async connect(config: KafkaSubscriberConnectConfig): Promise<KafkaSubscriber> {
    const kafka = new Kafka({
      clientId: config.clientId ?? "relaybus",
      brokers: config.brokers
    });
    const consumer = kafka.consumer({ groupId: config.groupId ?? "relaybus" });
    await consumer.connect();
    const subscriber = new KafkaSubscriber({ onMessage: config.onMessage });
    subscriber.consumer = consumer;
    subscriber.maxMessages = config.maxMessages ?? 1;
    return subscriber;
  }

  async handleMessage(data: Buffer | string): Promise<void> {
    const decoded = decodeEnvelope(data);
    await this.onMessage(decoded);
  }

  async start(topic: string): Promise<void> {
    if (!this.consumer) {
      throw new Error("consumer is not initialized");
    }
    await this.consumer.subscribe({ topic, fromBeginning: true });
    const consumer = this.consumer;
    const max = this.maxMessages ?? 1;
    let count = 0;
    let finished = false;

    let finishError: Error | undefined;
    const finish = async (err?: Error) => {
      if (finished) {
        return;
      }
      finished = true;
      if (err) {
        finishError = err;
      }
      try {
        await consumer.stop();
      } catch {
        // Ignore stop failures.
      }
      try {
        await consumer.disconnect();
      } catch {
        // Ignore disconnect failures.
      }
    };

    try {
      await consumer.run({
        eachMessage: async ({ message }) => {
          const value = message.value ? Buffer.from(message.value) : Buffer.from("");
          await this.handleMessage(value);
          count++;
          if (max > 0 && count >= max) {
            await finish();
          }
        }
      });
    } catch (err) {
      await finish(err as Error);
    }
    if (finishError) {
      throw finishError;
    }
  }

  async close(): Promise<void> {
    if (this.consumer) {
      try {
        await this.consumer.disconnect();
      } catch {
        // Ignore double-disconnects.
      }
    }
  }
}

function resolveTopic(argumentTopic: string, messageTopic?: string): string {
  const topic = messageTopic && messageTopic.length > 0 ? messageTopic : argumentTopic;
  if (!topic) {
    throw new Error("topic is required");
  }
  if (argumentTopic && messageTopic && argumentTopic !== messageTopic) {
    throw new Error(`topic mismatch: ${messageTopic} vs ${argumentTopic}`);
  }
  return topic;
}

function isTopicAlreadyExists(err: unknown): boolean {
  if (!err || typeof err !== "object") {
    return false;
  }
  const record = err as {
    type?: string;
    name?: string;
    topicErrors?: Array<{ error?: { type?: string } }>;
  };
  if (record.type === "TOPIC_ALREADY_EXISTS") {
    return true;
  }
  if (record.name === "KafkaJSCreateTopicError" && Array.isArray(record.topicErrors)) {
    return record.topicErrors.every((topicError) => topicError?.error?.type === "TOPIC_ALREADY_EXISTS");
  }
  return false;
}
