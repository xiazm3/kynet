export * from "./index.js";
export { createNodeTransportFactory } from "./node-transport.js";
export type { NodeTransportOptions } from "./node-transport.js";

import { KynetClient } from "./client.js";
import type { ClientOptions } from "./client.js";
import { createNodeTransportFactory } from "./node-transport.js";
import type { NodeTransportOptions } from "./node-transport.js";

export class NodeKynetClient extends KynetClient {
  constructor(options: ClientOptions & NodeTransportOptions = {}) {
    super({ ...options, endpoint: options.endpoint ?? "tcp://127.0.0.1:30001",
      transportFactory: options.transportFactory ?? createNodeTransportFactory(options) });
  }
}
