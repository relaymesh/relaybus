import { decodeEnvelope, encodeEnvelope, DecodedMessage, OutgoingMessage } from "@relaymesh/relaybus-core";
import { connect, Channel, ChannelModel, ConsumeMessage, ConfirmChannel } from "amqplib";

export type Delivery = {
  content: Buffer | string;
};

export type PublishOptions = {
  contentType?: string;
  messageId?: string;
  timestamp?: number;
  headers?: Record<string, string>;
};

export type AmqpChannel = {
  publish: (
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: PublishOptions
  ) => Promise<void> | void;
};

export type AmqpSubscriberConfig = {
  onMessage: (msg: DecodedMessage) => void | Promise<void>;
};

export type AmqpPublisherConfig = {
  channel: AmqpChannel;
  exchange?: string;
  routingKeyTemplate?: string;
};

export type AmqpPublisherConnectConfig = {
  url: string;
  exchange?: string;
  routingKeyTemplate?: string;
};

export type AmqpSubscriberConnectConfig = {
  url: string;
  onMessage: (msg: DecodedMessage) => void | Promise<void>;
  exchange?: string;
  routingKeyTemplate?: string;
  queue?: string;
};

export class AmqpSubscriber {
  private readonly onMessage: (msg: DecodedMessage) => void | Promise<void>;
  private channel?: Channel;
  private connection?: ChannelModel;
  private exchange?: string;
  private routingKeyTemplate?: string;
  private queue?: string;

  constructor(config: AmqpSubscriberConfig) {
    this.onMessage = config.onMessage;
  }

  static async connect(config: AmqpSubscriberConnectConfig): Promise<AmqpSubscriber> {
    const connection = await connect(config.url);
    const channel = await connection.createChannel();
    const subscriber = new AmqpSubscriber({ onMessage: config.onMessage });
    subscriber.channel = channel;
    subscriber.connection = connection;
    subscriber.exchange = config.exchange ?? "";
    subscriber.routingKeyTemplate = config.routingKeyTemplate ?? "{topic}";
    subscriber.queue = config.queue;
    return subscriber;
  }

  async handleDelivery(delivery: Delivery): Promise<void> {
    const decoded = decodeEnvelope(delivery.content);
    await this.onMessage(decoded);
  }

  async start(topic: string): Promise<void> {
    if (!this.channel) {
      throw new Error("channel is not initialized");
    }
    const queueName = this.queue ?? topic;
    await this.channel.assertQueue(queueName, { durable: false, autoDelete: true });
    if (this.exchange) {
      const key = buildRoutingKey(this.routingKeyTemplate ?? "{topic}", topic);
      await this.channel.bindQueue(queueName, this.exchange, key);
    }

    await new Promise<void>((resolve, reject) => {
      this.channel!.consume(queueName, async (msg: ConsumeMessage | null) => {
        if (!msg) {
          return;
        }
        try {
          await this.handleDelivery({ content: msg.content });
          this.channel!.ack(msg);
          resolve();
        } catch (err) {
          this.channel!.nack(msg, false, true);
          reject(err);
        }
      });
    });
  }

  async close(): Promise<void> {
    if (this.channel) {
      await this.channel.close();
    }
    if (this.connection) {
      await this.connection.close();
    }
  }
}

export class AmqpPublisher {
  private readonly channel: AmqpChannel;
  private readonly exchange: string;
  private readonly routingKeyTemplate: string;
  private connection?: ChannelModel;
  private confirmChannel?: ConfirmChannel;

  constructor(config: AmqpPublisherConfig) {
    this.channel = config.channel;
    this.exchange = config.exchange ?? "";
    this.routingKeyTemplate = config.routingKeyTemplate ?? "{topic}";
  }

  static async connect(config: AmqpPublisherConnectConfig): Promise<AmqpPublisher> {
    const connection = await connect(config.url);
    const channel = await connection.createConfirmChannel();
    const publisher = new AmqpPublisher({
      channel: {
        publish: (exchange, routingKey, content, options) => {
          return new Promise<void>((resolve, reject) => {
            channel.publish(exchange, routingKey, content, options, (err) => {
              if (err) {
                reject(err);
                return;
              }
              resolve();
            });
          });
        }
      },
      exchange: config.exchange,
      routingKeyTemplate: config.routingKeyTemplate
    });
    publisher.connection = connection;
    publisher.confirmChannel = channel;
    return publisher;
  }

  async publish(topic: string, message: OutgoingMessage): Promise<void> {
    const resolved = resolveTopic(topic, message.topic);
    const payload = encodeEnvelope({ ...message, topic: resolved });
    const routingKey = buildRoutingKey(this.routingKeyTemplate, resolved);
    const options: PublishOptions = {
      contentType: "application/json",
      messageId: message.id,
      timestamp: message.ts ? Math.floor(message.ts.getTime() / 1000) : undefined,
      headers: message.meta ?? undefined
    };
    await this.channel.publish(this.exchange, routingKey, payload, options);
  }

  async close(): Promise<void> {
    if (this.confirmChannel) {
      await this.confirmChannel.close();
    }
    if (this.connection) {
      await this.connection.close();
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

function buildRoutingKey(template: string, topic: string): string {
  if (!template) {
    return topic;
  }
  if (template.includes("{topic}")) {
    return template.split("{topic}").join(topic);
  }
  return template;
}
