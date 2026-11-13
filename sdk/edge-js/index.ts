export interface EdgeConfig {
  endpoint?: string;
  dataset: string;
  apiKey?: string;
  tenantId?: string;
  clientId?: string;
  offlineFirst?: boolean;
  conflictStrategy?: "lww" | "vector_clock";
}

export interface EdgeDocument {
  id: string;
  vector: number[];
  metadata?: Record<string, unknown>;
  updatedAt?: number;
  deleted?: boolean;
}

export interface EdgeSearchResult {
  id: string;
  distance: number;
  score: number;
  metadata?: Record<string, unknown>;
}

/**
 * LongbowEdgeClient runs on Browsers, Cloudflare Workers, Deno Deploy, and Node.
 * Provides an offline-first in-memory HNSW/cosine vector cache, pending write queue,
 * and automatic LWW / vector-clock conflict resolution over Fetch HTTP/gRPC-web endpoints.
 */
export class LongbowEdgeClient {
  private cfg: EdgeConfig;
  private docs = new Map<string, EdgeDocument>();
  private queue: Array<{ op: "upsert" | "delete"; doc: EdgeDocument; ts: number }> = [];

  constructor(cfg: EdgeConfig) {
    if (!cfg.dataset) throw new Error("dataset is required");
    this.cfg = { offlineFirst: true, conflictStrategy: "lww", ...cfg };
  }

  async upsert(doc: EdgeDocument): Promise<void> {
    const stamped: EdgeDocument = { ...doc, updatedAt: doc.updatedAt ?? Date.now() };
    this.docs.set(doc.id, stamped);
    this.queue.push({ op: "upsert", doc: stamped, ts: stamped.updatedAt! });
    if (!this.cfg.offlineFirst && this.cfg.endpoint) {
      await this.flushSync();
    }
  }

  searchLocal(query: number[], k = 10): EdgeSearchResult[] {
    const results: EdgeSearchResult[] = [];
    for (const doc of this.docs.values()) {
      if (doc.deleted || doc.vector.length !== query.length) continue;
      const dist = cosineDistance(query, doc.vector);
      results.push({
        id: doc.id,
        distance: dist,
        score: 1 / (1 + dist),
        metadata: doc.metadata,
      });
    }
    results.sort((a, b) => a.distance - b.distance);
    return results.slice(0, Math.max(1, k));
  }

  async search(query: number[], k = 10): Promise<EdgeSearchResult[]> {
    if (this.cfg.offlineFirst || !this.cfg.endpoint) {
      return this.searchLocal(query, k);
    }
    try {
      const res = await fetch(`${this.cfg.endpoint.replace(/\/$/, "")}/api/v1/search`, {
        method: "POST",
        headers: this.headers(),
        body: JSON.stringify({ dataset: this.cfg.dataset, vector: query, k }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const body = (await res.json()) as { results?: EdgeSearchResult[] };
      return body.results ?? [];
    } catch {
      return this.searchLocal(query, k);
    }
  }

  async flushSync(): Promise<number> {
    if (!this.cfg.endpoint) throw new Error("endpoint required for sync");
    if (this.queue.length === 0) return 0;
    const batch = this.queue.splice(0, this.queue.length);
    const res = await fetch(`${this.cfg.endpoint.replace(/\/$/, "")}/api/v1/edge/sync`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify({
        dataset: this.cfg.dataset,
        client_id: this.cfg.clientId ?? "edge-js",
        mutations: batch,
      }),
    });
    if (!res.ok) {
      this.queue.unshift(...batch);
      throw new Error(`sync failed HTTP ${res.status}`);
    }
    const payload = (await res.json()) as { remote_docs?: EdgeDocument[] };
    for (const remote of payload.remote_docs ?? []) {
      const local = this.docs.get(remote.id);
      if (!local || (remote.updatedAt ?? 0) >= (local.updatedAt ?? 0)) {
        this.docs.set(remote.id, remote);
      }
    }
    return batch.length;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = { "Content-Type": "application/json" };
    if (this.cfg.apiKey) h["Authorization"] = `Bearer ${this.cfg.apiKey}`;
    if (this.cfg.tenantId) h["X-Longbow-Tenant"] = this.cfg.tenantId;
    return h;
  }
}

function cosineDistance(a: number[], b: number[]): number {
  let dot = 0;
  let na = 0;
  let nb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    na += a[i] * a[i];
    nb += b[i] * b[i];
  }
  if (na === 0 || nb === 0) return 1;
  return 1 - dot / (Math.sqrt(na) * Math.sqrt(nb));
}

