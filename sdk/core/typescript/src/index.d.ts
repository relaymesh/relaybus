export type DecodedMessage = {
    v: "v1";
    id: string;
    topic: string;
    ts: Date;
    contentType: string;
    payload: Buffer;
    meta: Record<string, string>;
};
export type OutgoingMessage = {
    id?: string;
    topic: string;
    ts?: Date;
    contentType?: string;
    payload: Buffer | Uint8Array;
    meta?: Record<string, string>;
    v?: "v1";
};
export type NormalizeOptions = {
    now?: () => Date;
    idGenerator?: () => string;
};
export declare function decodeEnvelope(jsonBytes: Buffer | string): DecodedMessage;
export declare function encodeEnvelope(message: OutgoingMessage, options?: NormalizeOptions): Buffer;
export declare const defaults: {
    DEFAULT_CONTENT_TYPE: string;
};
