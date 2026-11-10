import { KynetError } from "./errors.js";

export enum PacketType { Request, Response, Push, Ping, Pong }

export interface GatewayPacket {
  type: PacketType;
  oneWay: boolean;
  requestId: bigint;
  cmd: number;
  act: number;
  timeoutMs: bigint;
  sessionId: bigint;
  userId: bigint;
  errorCode: string | null;
  errorMessage: string | null;
  payload: Uint8Array | null;
}

export interface PacketLimits {
  maxFrameLength: number;
  maxPayloadLength: number;
  maxStringLength: number;
}

export const defaultLimits: Readonly<PacketLimits> = Object.freeze({
  maxFrameLength: 1024 * 1024, maxPayloadLength: 512 * 1024, maxStringLength: 8 * 1024,
});

export function packet(values: Partial<GatewayPacket> = {}): GatewayPacket {
  return { type: PacketType.Request, oneWay: false, requestId: 0n, cmd: 0, act: 0,
    timeoutMs: 0n, sessionId: 0n, userId: 0n, errorCode: null, errorMessage: null,
    payload: null, ...values };
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const protocolError = (message: string): never => { throw new KynetError("PROTOCOL_ERROR", message); };

export class GatewayPacketCodec {
  readonly limits: Readonly<PacketLimits>;

  constructor(limits: Partial<PacketLimits> = {}) {
    this.limits = Object.freeze({
      maxFrameLength: limits.maxFrameLength ?? defaultLimits.maxFrameLength,
      maxPayloadLength: limits.maxPayloadLength ?? defaultLimits.maxPayloadLength,
      maxStringLength: limits.maxStringLength ?? defaultLimits.maxStringLength,
    });
    for (const [name, value] of Object.entries(this.limits)) {
      if (!Number.isInteger(value) || value < (name === "maxFrameLength" ? 64 : 0) || value > 0x7fffffff) {
        throw new RangeError(`Invalid ${name}.`);
      }
    }
  }

  validateLength(length: number): void {
    if (!Number.isInteger(length) || length < 60 || length > this.limits.maxFrameLength - 4) {
      protocolError(`Invalid gateway frame length: ${length}.`);
    }
  }

  encode(input: GatewayPacket): Uint8Array {
    if (!Number.isInteger(input.type) || input.type < 0 || input.type > PacketType.Pong) protocolError("Unknown packet type.");
    for (const name of ["requestId", "timeoutMs", "sessionId", "userId"] as const) {
      if (typeof input[name] !== "bigint" || BigInt.asIntN(64, input[name]) !== input[name]) {
        protocolError(`${name} must be a signed int64 bigint.`);
      }
    }
    for (const value of [input.cmd, input.act]) {
      if (!Number.isInteger(value) || value < -0x80000000 || value > 0x7fffffff) protocolError("Invalid cmd/act.");
    }
    const fields = [input.errorCode === null ? null : encoder.encode(input.errorCode),
      input.errorMessage === null ? null : encoder.encode(input.errorMessage), input.payload];
    if (input.payload !== null && !(input.payload instanceof Uint8Array)) protocolError("Payload must be Uint8Array or null.");
    fields.forEach((field, index) => {
      if (field !== null && field.length > (index === 2 ? this.limits.maxPayloadLength : this.limits.maxStringLength)) {
        protocolError("Variable-length field exceeds the configured limit.");
      }
    });
    const length = 60 + fields.reduce((sum, field) => sum + (field?.length ?? 0), 0);
    this.validateLength(length);
    const data = new Uint8Array(length);
    const view = new DataView(data.buffer);
    view.setUint32(0, 0x4b594e54);
    view.setUint16(4, 1);
    view.setUint8(6, input.type);
    view.setUint8(7, input.oneWay ? 1 : 0);
    view.setBigInt64(8, input.requestId);
    view.setInt32(16, input.cmd);
    view.setInt32(20, input.act);
    view.setBigInt64(24, input.timeoutMs);
    view.setBigInt64(32, input.sessionId);
    view.setBigInt64(40, input.userId);
    let offset = 48;
    for (const field of fields) {
      view.setInt32(offset, field === null ? -1 : field.length);
      offset += 4;
      if (field !== null) { data.set(field, offset); offset += field.length; }
    }
    return data;
  }

  decode(data: Uint8Array): GatewayPacket {
    this.validateLength(data.length);
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    if (view.getUint32(0) !== 0x4b594e54 || view.getUint16(4) !== 1) protocolError("Invalid magic or unsupported gateway version.");
    const type = view.getUint8(6);
    if (type > PacketType.Pong) protocolError("Unknown packet type.");
    let offset = 48;
    const readField = (limit: number): Uint8Array | null => {
      if (offset + 4 > data.length) return protocolError("Truncated variable-length field.");
      const length = view.getInt32(offset);
      offset += 4;
      if (length === -1) return null;
      if (length < 0 || length > limit || length > data.length - offset) return protocolError("Invalid variable-length field size.");
      const bytes = new Uint8Array(data.subarray(offset, offset + length));
      offset += length;
      return bytes;
    };
    const readString = (): string | null => {
      const bytes = readField(this.limits.maxStringLength);
      try { return bytes === null ? null : decoder.decode(bytes); }
      catch { return protocolError("Invalid UTF-8 error field."); }
    };
    const result = packet({ type, oneWay: view.getUint8(7) !== 0, requestId: view.getBigInt64(8),
      cmd: view.getInt32(16), act: view.getInt32(20), timeoutMs: view.getBigInt64(24),
      sessionId: view.getBigInt64(32), userId: view.getBigInt64(40),
      errorCode: readString(), errorMessage: readString(), payload: readField(this.limits.maxPayloadLength) });
    if (offset !== data.length) protocolError("Unexpected trailing packet data.");
    return result;
  }
}

export function actionKey(cmd: number, act: number): number {
  if (![cmd, act].every(value => Number.isInteger(value) && value >= 0 && value <= 65535)) {
    throw new RangeError("cmd/act must be between 0 and 65535.");
  }
  return ((cmd << 16) | act) >>> 0;
}

export function splitMessageId(messageId: number): [number, number] {
  if (!Number.isInteger(messageId) || messageId < -0x80000000 || messageId > 0xffffffff) throw new RangeError("Invalid messageId.");
  return [messageId >>> 16, messageId & 65535];
}
