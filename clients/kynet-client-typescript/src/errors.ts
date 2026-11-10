export class KynetError extends Error {
  constructor(public readonly code: string, message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "KynetError";
  }
}

export class GatewayRequestError extends KynetError {
  constructor(code: string, message: string | null) {
    super(code, message ?? code);
    this.name = "GatewayRequestError";
  }
}

export function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export function aborted(): KynetError {
  return new KynetError("CANCELLED", "Operation cancelled.");
}
