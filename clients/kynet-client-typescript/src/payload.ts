import protobuf from "protobufjs/minimal.js";
import Long from "long";

const { Reader, Writer } = protobuf;
// protobufjs uses Long for exact int64 decoding; the public SDK exposes bigint.
protobuf.util.Long = Long;
protobuf.configure();

export const utf8 = {
  encode(value: string): Uint8Array { return new TextEncoder().encode(value); },
  decode(value: Uint8Array): string { return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(value); },
};

export interface LoginMessage { username: string; password: string; token?: string }

export const loginMessage = {
  encode(message: LoginMessage): Uint8Array {
    const writer = Writer.create().uint32(10).string(message.username).uint32(18).string(message.password);
    if (message.token !== undefined) writer.uint32(26).string(message.token);
    return writer.finish();
  },
  decode(data: Uint8Array): LoginMessage {
    const result: LoginMessage = { username: "", password: "", token: "" };
    const reader = Reader.create(data);
    while (reader.pos < reader.len) {
      const tag = reader.uint32();
      if (tag === 10) result.username = reader.string();
      else if (tag === 18) result.password = reader.string();
      else if (tag === 26) result.token = reader.string();
      else skip(reader, tag);
    }
    return result;
  },
};

/** Google StringValue, used by broadcasts rather than ordinary String actions. */
export const stringValue = {
  encode(value: string): Uint8Array { return Writer.create().uint32(10).string(value).finish(); },
  decode(data: Uint8Array): string {
    const reader = Reader.create(data);
    let value = "";
    while (reader.pos < reader.len) {
      const tag = reader.uint32();
      if (tag === 10) value = reader.string(); else skip(reader, tag);
    }
    return value;
  },
};

export enum PrimitiveKind { Int, Long, Bool, Double, Float }
export type Primitive = number | bigint | boolean;

/** Java PrimitiveValue, distinct from Google's scalar wrapper messages. */
export const primitiveValue = {
  encode(kind: PrimitiveKind, value: Primitive): Uint8Array {
    const writer = Writer.create().uint32(8).int32(kind);
    switch (kind) {
      case PrimitiveKind.Int:
        if (typeof value !== "number" || !Number.isInteger(value) || value < -2147483648 || value > 2147483647) throw new RangeError("Expected int32.");
        writer.uint32(16).int32(value); break;
      case PrimitiveKind.Long:
        if (typeof value !== "bigint" || BigInt.asIntN(64, value) !== value) throw new RangeError("Expected int64 bigint.");
        writer.uint32(24).int64(value.toString()); break;
      case PrimitiveKind.Bool:
        if (typeof value !== "boolean") throw new TypeError("Expected boolean.");
        writer.uint32(32).bool(value); break;
      case PrimitiveKind.Double: case PrimitiveKind.Float:
        if (typeof value !== "number") throw new TypeError("Expected number.");
        if (kind === PrimitiveKind.Double) writer.uint32(41).double(value); else writer.uint32(53).float(value);
        break;
      default: throw new RangeError("Invalid PrimitiveValue kind.");
    }
    return writer.finish();
  },
  decode(data: Uint8Array, expectedKind: PrimitiveKind): Primitive {
    const reader = Reader.create(data);
    let kind = -1;
    const values = new Map<PrimitiveKind, Primitive>();
    while (reader.pos < reader.len) {
      const tag = reader.uint32();
      switch (tag) {
        case 8: kind = reader.int32(); break;
        case 16: values.set(PrimitiveKind.Int, reader.int32()); break;
        case 24: values.set(PrimitiveKind.Long, BigInt(reader.int64().toString())); break;
        case 32: values.set(PrimitiveKind.Bool, reader.bool()); break;
        case 41: values.set(PrimitiveKind.Double, reader.double()); break;
        case 53: values.set(PrimitiveKind.Float, reader.float()); break;
        default: skip(reader, tag);
      }
    }
    if (kind !== expectedKind || kind < 0 || kind > 4) throw new TypeError("PrimitiveValue kind mismatch.");
    return values.get(kind) ?? (kind === PrimitiveKind.Long ? 0n : kind === PrimitiveKind.Bool ? false : 0);
  },
};

function skip(reader: protobuf.Reader, tag: number): void {
  if ((tag >>> 3) === 0 || (tag & 7) === 4) throw new TypeError("Invalid Protobuf field tag.");
  reader.skipType(tag & 7);
}
