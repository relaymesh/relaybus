import { decodeEnvelope, encodeEnvelope, DecodedMessage, OutgoingMessage } from "@relaymesh/relaybus-core";
import { connect, NatsConnection } from "nats";

export type NatsClient = {
  publish: (subject: string, data: Buffer) => Promise<void> | void;
};

export type NatsPublisherConfig = {
  client: NatsClient;
  subjectPrefix?: string;
};

export type NatsPublisherConnectConfig = {
  url: string;
  subjectPrefix?: string;
};

export class NatsPublisher {
  private readonly client: NatsClient;
  private readonly prefix: string;
  private connection?: NatsConnection;

  constructor(config: NatsPublisherConfig) {
    if (!config.client) {
      throw new Error("client is required");
    }
    this.client = config.client;
    this.prefix = config.subjectPrefix ?? "";
  }

  static async connect(config: NatsPublisherConnectConfig): Promise<NatsPublisher> {
    const connection = await connect({ servers: config.url });
    const publisher = new NatsPublisher({
      client: {
        publish: async (subject, data) => {
          connection.publish(subject, data);
        }
      },
      subjectPrefix: config.subjectPrefix
    });
    publisher.connection = connection;
    return publisher;
  }

  async publish(topic: string, message: OutgoingMessage): Promise<void> {
    const resolved = resolveTopic(topic, message.topic);
    const payload = encodeEnvelope({ ...message, topic: resolved });
    const subject = joinSubject(this.prefix, resolved);
    await Promise.resolve(this.client.publish(subject, payload));
  }

  async close(): Promise<void> {
    if (this.connection) {
      await this.connection.drain();
    }
  }
}

export type NatsSubscriberConfig = {
  onMessage: (msg: DecodedMessage) => void | Promise<void>;
};

export type NatsSubscriberConnectConfig = {
  url: string;
  onMessage: (msg: DecodedMessage) => void | Promise<void>;
  subjectPrefix?: string;
  maxMessages?: number;
};

export class NatsSubscriber {
  private readonly onMessage: (msg: DecodedMessage) => void | Promise<void>;
  private connection?: NatsConnection;
  private prefix?: string;
  private maxMessages?: number;

  constructor(config: NatsSubscriberConfig) {
    this.onMessage = config.onMessage;
  }

  static async connect(config: NatsSubscriberConnectConfig): Promise<NatsSubscriber> {
    const connection = await connect({ servers: config.url });
    const subscriber = new NatsSubscriber({ onMessage: config.onMessage });
    subscriber.connection = connection;
    subscriber.prefix = config.subjectPrefix ?? "";
    subscriber.maxMessages = config.maxMessages ?? 1;
    return subscriber;
  }

  async handleMessage(data: Buffer | string): Promise<void> {
    const decoded = decodeEnvelope(data);
    await this.onMessage(decoded);
  }

  async start(topic: string): Promise<void> {
    if (!this.connection) {
      throw new Error("connection is not initialized");
    }
    const subject = joinSubject(this.prefix ?? "", topic);
    const sub = this.connection.subscribe(subject);
    let count = 0;
    for await (const msg of sub) {
      const data = Buffer.from(msg.data);
      await this.handleMessage(data);
      count++;
      if (this.maxMessages && count >= this.maxMessages) {
        sub.unsubscribe();
        break;
      }
    }
  }

  async close(): Promise<void> {
    if (this.connection) {
      await this.connection.drain();
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

function joinSubject(prefix: string, topic: string): string {
  if (!prefix) {
    return topic;
  }
  if (prefix.endsWith(".")) {
    return `${prefix}${topic}`;
  }
  return `${prefix}.${topic}`;
}
