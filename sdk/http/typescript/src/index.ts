import {
  decodeEnvelope,
  encodeEnvelope,
  DecodedMessage,
  OutgoingMessage,
  resolveTopicOrThrow
} from "./core";
import http from "node:http";
import { timingSafeEqual } from "node:crypto";

export type HttpRequest = {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: Buffer;
};

export type HttpResponse = {
  status: number;
  body?: string;
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
  timeoutMs?: number;
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
    const endpointUrl = new URL(config.endpoint);
    if (endpointUrl.protocol !== "http:" && endpointUrl.protocol !== "https:") {
      throw new Error("endpoint must be a valid http(s) URL with host");
    }
    const timeoutMs = config.timeoutMs ?? 10000;
    return new HttpPublisher({
      endpoint: config.endpoint,
      headers: config.headers,
      idempotencyHeader: config.idempotencyHeader,
      doer: async (req) => {
        const signal = typeof AbortSignal.timeout === "function"
          ? AbortSignal.timeout(timeoutMs)
          : undefined;
        const response = await fetch(req.url, {
          method: req.method,
          headers: req.headers,
          body: new Uint8Array(req.body),
          signal
        });
        return {
          status: response.status,
          body: response.status >= 200 && response.status < 300 ? undefined : await response.text()
        };
      }
    });
  }

  async publish(topic: string, message: OutgoingMessage): Promise<void> {
    const resolved = resolveTopicOrThrow(topic, message.topic);
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
      const detail = response.body && response.body.trim().length > 0
        ? `: ${response.body.trim().slice(0, 200)}`
        : "";
      throw new Error(`http status ${response.status}${detail}`);
    }
  }
}

export type HttpSubscriberConfig = {
  onMessage: (msg: DecodedMessage) => void | Promise<void>;
};

export type HttpSubscriberListenConfig = {
  port: number;
  host?: string;
  maxBodyBytes?: number;
  authHeader?: string;
  authToken?: string;
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
    const maxBodyBytes = config.maxBodyBytes ?? 1_048_576;
    const authHeader = config.authHeader ?? "authorization";
    const authToken = config.authToken;
    const server = http.createServer((req, res) => {
      if (req.method !== "POST") {
        res.statusCode = 405;
        res.end();
        return;
      }
      if (authToken !== undefined) {
        const provided = (req.headers[authHeader] as string | undefined)?.trim() ?? "";
        if (!constantTimeEquals(provided, authToken.trim())) {
          res.statusCode = 401;
          res.end();
          return;
        }
      }
      const contentType = req.headers["content-type"];
      if (typeof contentType === "string" && !contentType.startsWith("application/json")) {
        res.statusCode = 415;
        res.end();
        return;
      }
      const lengthHeader = req.headers["content-length"];
      if (typeof lengthHeader !== "string") {
        res.statusCode = 411;
        res.end();
        return;
      }
      const expected = Number(lengthHeader);
      if (!Number.isInteger(expected) || expected < 0) {
        res.statusCode = 400;
        res.end();
        return;
      }
      if (expected > maxBodyBytes) {
        res.statusCode = 413;
        res.end();
        return;
      }
      const chunks: Buffer[] = [];
      let total = 0;
      req.on("data", (chunk) => {
        const data = Buffer.from(chunk);
        chunks.push(data);
        total += chunk.length;
        if (total > maxBodyBytes) {
          res.statusCode = 413;
          res.end();
          req.destroy();
        }
      });
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

function buildEndpoint(endpoint: string, topic: string): string {
  if (endpoint.includes("{topic}")) {
    return endpoint.split("{topic}").join(encodeURIComponent(topic));
  }
  if (!topic) {
    return endpoint;
  }
  return endpoint.replace(/\/$/, "") + "/" + encodeURIComponent(topic);
}

function constantTimeEquals(left: string, right: string): boolean {
  const leftBuf = Buffer.from(left, "utf8");
  const rightBuf = Buffer.from(right, "utf8");
  if (leftBuf.length !== rightBuf.length) {
    return false;
  }
  return timingSafeEqual(leftBuf, rightBuf);
}
