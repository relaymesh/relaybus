import {
  decodeEnvelope,
  encodeEnvelope,
  DecodedMessage,
  OutgoingMessage,
  resolveTopicOrThrow
} from "./core";
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
  assertQueue?: (
    queue: string,
    options?: { durable?: boolean; autoDelete?: boolean; exclusive?: boolean }
  ) => Promise<unknown> | unknown;
  assertExchange?: (
    exchange: string,
    type: string,
    options?: { durable?: boolean; autoDelete?: boolean; internal?: boolean }
  ) => Promise<unknown> | unknown;
  bindQueue?: (
    queue: string,
    exchange: string,
    routingKey: string
  ) => Promise<unknown> | unknown;
};

export type AmqpSubscriberConfig = {
  onMessage: (msg: DecodedMessage) => void | Promise<void>;
};

export type AmqpPublisherConfig = {
  channel: AmqpChannel;
  exchange?: string;
  routingKeyTemplate?: string;
  exchangeType?: string;
  queue?: string;
  queueOptions?: { durable?: boolean; autoDelete?: boolean; exclusive?: boolean };
};

export type AmqpPublisherConnectConfig = {
  url: string;
  exchange?: string;
  routingKeyTemplate?: string;
  exchangeType?: string;
  queue?: string;
  queueOptions?: { durable?: boolean; autoDelete?: boolean; exclusive?: boolean };
};

export type AmqpSubscriberConnectConfig = {
  url: string;
  onMessage: (msg: DecodedMessage) => void | Promise<void>;
  exchange?: string;
  exchangeType?: string;
  routingKeyTemplate?: string;
  queue?: string;
  queueOptions?: { durable?: boolean; autoDelete?: boolean; exclusive?: boolean };
};

export class AmqpSubscriber {
  private readonly onMessage: (msg: DecodedMessage) => void | Promise<void>;
  private channel?: Channel;
  private connection?: ChannelModel;
  private exchange?: string;
  private exchangeType?: string;
  private routingKeyTemplate?: string;
  private queue?: string;
  private queueOptions?: { durable?: boolean; autoDelete?: boolean; exclusive?: boolean };

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
    subscriber.exchangeType = config.exchangeType ?? "topic";
    subscriber.routingKeyTemplate = config.routingKeyTemplate ?? "{topic}";
    subscriber.queue = config.queue;
    subscriber.queueOptions = config.queueOptions;
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
    if (this.exchange) {
      await this.channel.assertExchange(this.exchange, this.exchangeType ?? "topic", {
        durable: false,
        autoDelete: false
      });
    }
    const queueName = this.queue ?? topic;
    await this.channel.assertQueue(queueName, normalizeQueueOptions(this.queueOptions));
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
  private readonly exchangeType: string;
  private readonly queue?: string;
  private readonly queueOptions?: { durable?: boolean; autoDelete?: boolean; exclusive?: boolean };
  private readonly ensuredQueues = new Set<string>();
  private readonly ensuredExchanges = new Set<string>();
  private readonly ensuredBindings = new Set<string>();
  private connection?: ChannelModel;
  private confirmChannel?: ConfirmChannel;

  constructor(config: AmqpPublisherConfig) {
    this.channel = config.channel;
    this.exchange = config.exchange ?? "";
    this.routingKeyTemplate = config.routingKeyTemplate ?? "{topic}";
    this.exchangeType = config.exchangeType ?? "topic";
    this.queue = config.queue;
    this.queueOptions = config.queueOptions;
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
        },
        assertQueue: (queue, options) => channel.assertQueue(queue, options),
        assertExchange: (exchange, type, options) => channel.assertExchange(exchange, type, options),
        bindQueue: (queue, exchange, routingKey) => channel.bindQueue(queue, exchange, routingKey)
      },
      exchange: config.exchange,
      routingKeyTemplate: config.routingKeyTemplate,
      exchangeType: config.exchangeType,
      queue: config.queue,
      queueOptions: config.queueOptions
    });
    publisher.connection = connection;
    publisher.confirmChannel = channel;
    return publisher;
  }

  async publish(topic: string, message: OutgoingMessage): Promise<void> {
    const resolved = resolveTopicOrThrow(topic, message.topic);
    await this.ensureInfrastructure(resolved);
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

  private async ensureInfrastructure(topic: string): Promise<void> {
    if (this.exchange && this.channel.assertExchange) {
      if (!this.ensuredExchanges.has(this.exchange)) {
        await Promise.resolve(
          this.channel.assertExchange(this.exchange, this.exchangeType, {
            durable: false,
            autoDelete: false
          })
        );
        this.ensuredExchanges.add(this.exchange);
      }
    }

    const queueName = this.queue ?? topic;
    if (queueName && this.channel.assertQueue) {
      if (!this.ensuredQueues.has(queueName)) {
        await Promise.resolve(
          this.channel.assertQueue(queueName, normalizeQueueOptions(this.queueOptions))
        );
        this.ensuredQueues.add(queueName);
      }
    }

    if (this.exchange && queueName && this.channel.bindQueue) {
      const routingKey = buildRoutingKey(this.routingKeyTemplate, topic);
      const bindingKey = `${queueName}::${this.exchange}::${routingKey}`;
      if (!this.ensuredBindings.has(bindingKey)) {
        await Promise.resolve(this.channel.bindQueue(queueName, this.exchange, routingKey));
        this.ensuredBindings.add(bindingKey);
      }
    }
  }
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

function normalizeQueueOptions(
  options?: { durable?: boolean; autoDelete?: boolean; exclusive?: boolean }
): { durable: boolean; autoDelete: boolean; exclusive: boolean } {
  return {
    durable: options?.durable ?? false,
    autoDelete: options?.autoDelete ?? false,
    exclusive: options?.exclusive ?? false
  };
}
