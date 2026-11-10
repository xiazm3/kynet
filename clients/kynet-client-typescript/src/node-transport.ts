import net from "node:net";
import tls from "node:tls";
import dgram from "node:dgram";
import { lookup } from "node:dns/promises";
import WebSocket from "ws";
import { aborted, asError, KynetError } from "./errors.js";
import type { GatewayPacketCodec } from "./protocol.js";
import type { GatewayTransport, TransportFactory, TransportHandlers } from "./transport.js";

export interface NodeTransportOptions {
  tls?: Pick<tls.ConnectionOptions, "ca" | "cert" | "key" | "servername">;
}

/** Incremental framing allocates a body only after its prefix passes the limit. */
export class TcpFrameDecoder {
  private buffer = new Uint8Array(4);
  private offset = 0;
  private readingPrefix = true;
  constructor(private readonly codec: GatewayPacketCodec) {}
  feed(chunk: Uint8Array, onPacket: (data: Uint8Array) => void): void {
    let position = 0;
    while (position < chunk.length) {
      const count = Math.min(this.buffer.length - this.offset, chunk.length - position);
      this.buffer.set(chunk.subarray(position, position + count), this.offset);
      this.offset += count;
      position += count;
      if (this.offset !== this.buffer.length) continue;
      this.offset = 0;
      if (this.readingPrefix) {
        const length = new DataView(this.buffer.buffer).getInt32(0);
        this.codec.validateLength(length);
        this.buffer = new Uint8Array(length);
        this.readingPrefix = false;
      } else {
        const complete = this.buffer;
        this.buffer = new Uint8Array(4);
        this.readingPrefix = true;
        onPacket(complete);
      }
    }
  }
}

class NodeTransport implements GatewayTransport {
  private stream?: net.Socket;
  private udp?: dgram.Socket;
  private websocket?: WebSocket;
  private readonly stop = new AbortController();
  private cancelConnect?: () => void;

  constructor(private readonly endpoint: URL, private readonly codec: GatewayPacketCodec,
    private readonly options: NodeTransportOptions) {}

  async connect(signal: AbortSignal, handlers: TransportHandlers): Promise<void> {
    const host = this.endpoint.hostname.replace(/^\[|\]$/g, "");
    const protocol = this.endpoint.protocol;
    // WHATWG URL omits an explicit default ws/wss port after parsing.
    const port = Number(this.endpoint.port || (protocol === "wss:" ? 443 : 80));
    const combined = AbortSignal.any([signal, this.stop.signal]);
    const fail = (error: unknown) => { if (!this.stop.signal.aborted) { handlers.close(asError(error)); this.close(); } };
    if (protocol === "udp:") {
      const address = await lookup(host);
      if (combined.aborted) throw aborted();
      const socket = this.udp = dgram.createSocket(address.family === 6 ? "udp6" : "udp4");
      socket.on("message", bytes => { if (!this.stop.signal.aborted) handlers.packet(bytes); });
      socket.on("error", fail);
      socket.on("close", () => fail(new KynetError("DISCONNECTED", "Gateway UDP socket closed.")));
      await this.open(combined, ready => socket.connect(port, address.address, ready));
    } else if (protocol === "tcp:" || protocol === "tcps:") {
      const socket = this.stream = protocol === "tcps:"
        ? tls.connect({ ...this.options.tls, host, port,
          servername: this.options.tls?.servername ?? (net.isIP(host) ? undefined : host), rejectUnauthorized: true })
        : net.connect({ host, port });
      socket.setNoDelay(true);
      const decoder = new TcpFrameDecoder(this.codec);
      socket.on("data", data => {
        if (this.stop.signal.aborted) return;
        try { decoder.feed(data, bytes => { if (!this.stop.signal.aborted) handlers.packet(bytes); }); }
        catch (error) { fail(error); }
      });
      socket.on("error", fail);
      socket.on("close", () => fail(new KynetError("DISCONNECTED", "Gateway TCP connection closed.")));
      await this.open(combined, ready => socket.once(protocol === "tcps:" ? "secureConnect" : "connect", ready));
    } else {
      const socket = this.websocket = new WebSocket(this.endpoint, {
        ...this.options.tls, rejectUnauthorized: true, maxPayload: this.codec.limits.maxFrameLength - 4,
        perMessageDeflate: false,
      });
      socket.on("message", (data, binary) => {
        if (this.stop.signal.aborted) return;
        if (!binary) { fail(new KynetError("PROTOCOL_ERROR", "Expected a binary WebSocket message.")); return; }
        const bytes = data instanceof ArrayBuffer ? new Uint8Array(data) : Buffer.isBuffer(data) ? data : Buffer.concat(data);
        handlers.packet(bytes);
      });
      socket.on("error", fail);
      socket.on("close", () => fail(new KynetError("DISCONNECTED", "Gateway WebSocket closed.")));
      await this.open(combined, ready => socket.once("open", ready));
    }
  }

  private open(signal: AbortSignal, start: (ready: () => void) => void): Promise<void> {
    return new Promise((resolve, reject) => {
      const cleanup = () => { signal.removeEventListener("abort", onAbort); this.cancelConnect = undefined; };
      const onAbort = () => { cleanup(); reject(aborted()); };
      this.cancelConnect = () => { cleanup(); reject(new KynetError("DISCONNECTED", "Gateway transport closed.")); };
      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) { onAbort(); return; }
      start(() => { cleanup(); resolve(); });
    });
  }

  send(data: Uint8Array, signal: AbortSignal): Promise<void> {
    if (signal.aborted || this.stop.signal.aborted) return Promise.reject(aborted());
    if (this.udp && data.length > 65507) return Promise.reject(new KynetError("PAYLOAD_TOO_LARGE", "UDP packet exceeds 65507 bytes."));
    const combined = AbortSignal.any([signal, this.stop.signal]);
    return new Promise((resolve, reject) => {
      let finished = false;
      const complete = (error?: Error | null) => {
        if (finished) return;
        finished = true;
        combined.removeEventListener("abort", onAbort);
        if (error) reject(error); else resolve();
      };
      const onAbort = () => { complete(aborted()); this.close(); };
      combined.addEventListener("abort", onAbort, { once: true });
      try {
        if (this.websocket) this.websocket.send(data, { binary: true }, complete);
        else if (this.udp) this.udp.send(data, complete);
        else if (this.stream) {
          const frame = Buffer.allocUnsafe(data.length + 4);
          frame.writeInt32BE(data.length);
          frame.set(data, 4);
          this.stream.write(frame, complete);
        } else complete(new KynetError("DISCONNECTED", "Transport is not open."));
      } catch (error) { complete(asError(error)); }
    });
  }

  close(): void {
    if (this.stop.signal.aborted) return;
    this.stop.abort();
    this.cancelConnect?.();
    this.stream?.destroy();
    if (this.udp) { try { this.udp.close(); } catch { /* An unbound UDP socket is already closed. */ } }
    this.websocket?.terminate();
  }
}

export function createNodeTransportFactory(options: NodeTransportOptions = {}): TransportFactory {
  return (endpoint, codec) => new NodeTransport(endpoint, codec, options);
}
