import { decodeEnvelope, encodeEnvelope, DecodedMessage, OutgoingMessage } from "@relaymesh/relaybus-core";
import http from "node:http";

export type HttpRequest = {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: Buffer;
};

export type HttpResponse = {
  status: number;
};

export type HttpDoer = (req: HttpRequest) => Promise<HttpResponse> | HttpResponse;

export type HttpPublisherConfig = {
  endpoint: string;
  doer: HttpDoer;
  headers?: Record<string, string>;
  idempotencyHeader?: string;
};

export type HttpPublisherConnectConfig = {
  endpoint: string;
  headers?: Record<string, string>;
  idempotencyHeader?: string;
};

export class HttpPublisher {
  private readonly endpoint: string;
  private readonly doer: HttpDoer;
  private readonly headers: Record<string, string>;
  private readonly idempotencyHeader: string;

  constructor(config: HttpPublisherConfig) {
    if (!config.endpoint) {
      throw new Error("endpoint is required");
    }
    if (!config.doer) {
      throw new Error("doer is required");
    }
    this.endpoint = config.endpoint;
    this.doer = config.doer;
    this.headers = config.headers ?? {};
    this.idempotencyHeader = config.idempotencyHeader ?? "Idempotency-Key";
  }

  static connect(config: HttpPublisherConnectConfig): HttpPublisher {
    if (!globalThis.fetch) {
      throw new Error("fetch is not available");
    }
    return new HttpPublisher({
      endpoint: config.endpoint,
      headers: config.headers,
      idempotencyHeader: config.idempotencyHeader,
      doer: async (req) => {
        const response = await fetch(req.url, {
          method: req.method,
          headers: req.headers,
          body: new Uint8Array(req.body)
        });
        return { status: response.status };
      }
    });
  }

  async publish(topic: string, message: OutgoingMessage): Promise<void> {
    const resolved = resolveTopic(topic, message.topic);
    const payload = encodeEnvelope({ ...message, topic: resolved });

    const req: HttpRequest = {
      url: buildEndpoint(this.endpoint, resolved),
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...this.headers
      },
      body: payload
    };

    if (message.id) {
      req.headers[this.idempotencyHeader] = message.id;
    }

    const response = await Promise.resolve(this.doer(req));
    if (response.status < 200 || response.status >= 300) {
      throw new Error(`http status ${response.status}`);
    }
  }
}

export type HttpSubscriberConfig = {
  onMessage: (msg: DecodedMessage) => void | Promise<void>;
};

export type HttpSubscriberListenConfig = {
  port: number;
  host?: string;
};

export class HttpSubscriber {
  private readonly onMessage: (msg: DecodedMessage) => void | Promise<void>;

  constructor(config: HttpSubscriberConfig) {
    this.onMessage = config.onMessage;
  }

  async handle(body: Buffer | string): Promise<void> {
    const decoded = decodeEnvelope(body);
    await this.onMessage(decoded);
  }

  async listen(config: HttpSubscriberListenConfig): Promise<http.Server> {
    const server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
      req.on("end", async () => {
        try {
          await this.handle(Buffer.concat(chunks));
          res.statusCode = 204;
        } catch {
          res.statusCode = 400;
        }
        res.end();
      });
    });

    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(config.port, config.host, () => resolve());
    });
    return server;
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

function buildEndpoint(endpoint: string, topic: string): string {
  if (endpoint.includes("{topic}")) {
    return endpoint.split("{topic}").join(encodeURIComponent(topic));
  }
  if (!topic) {
    return endpoint;
  }
  return endpoint.replace(/\/$/, "") + "/" + encodeURIComponent(topic);
}
