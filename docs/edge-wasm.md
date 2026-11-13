# Serverless, Edge SDKs & WASM Client

Longbow provides a lightweight pure-Go WASM/edge vector engine (`pkg/wasmclient`), a browser/WASM compilation entrypoint (`cmd/longbow-wasm`), a TypeScript SDK (`sdk/edge-js`), and HTTP sync/search endpoints (`internal/store/edge_http.go`) for Cloudflare Workers, Deno Deploy, and browser runtimes.

---

## 1. Architecture Overview

- **Pure-Go Embedded HNSW (`pkg/wasmclient/hnsw_edge.go`)**: Zero-CGO `EdgeIndex` supporting `cosine`, `l2`, and `dot` metrics for client-side/offline low-latency ANN queries inside WASM or stateless workers.
- **Offline-First Sync Engine (`pkg/wasmclient/sync_engine.go`)**: Buffers `upsert` and `delete` mutations while disconnected and resolves divergent concurrent writes using `ConflictLWW` (Last-Write-Wins) or `ConflictVectorClock` conflict strategies.
- **HTTP Transport Fallback (`internal/store/edge_http.go`)**:
  - `POST /api/v1/search` (`EdgeSearchHTTPHandler`): Executes authenticated vector search over HTTPS/JSON without native gRPC/HTTP2 trailers.
  - `POST /api/v1/edge/sync` (`EdgeSyncHTTPHandler`): Applies batched offline edge mutations into the store's streaming ingestion pipeline.

---

## 2. Building & Testing the WASM Module

Build and test the `js/wasm` binary using the `Makefile` targets:

```bash
# Build dist/longbow.wasm (GOOS=js GOARCH=wasm)
make build-wasm-client

# Run WASM/Edge unit and fuzz tests plus wasm build check
make test-wasm
```

Browser integration registers global helpers (`longbowInit`, `longbowUpsert`, `longbowSearch`, `longbowSync`) from `cmd/longbow-wasm/main.go`. Copy Go's `wasm_exec.js` alongside `dist/longbow.wasm`:

```javascript
const go = new Go();
const { instance } = await WebAssembly.instantiateStreaming(fetch("/dist/longbow.wasm"), go.importObject);
go.run(instance);

await longbowInit({ endpoint: "https://longbow.example.com", dataset: "docs", offlineFirst: true });
await longbowUpsert("doc-1", [0.9, 0.1, 0.0, 0.0], { topic: "wasm" });
const hits = await longbowSearch([1, 0, 0, 0], 5);
```

---

## 3. TypeScript Edge SDK (`sdk/edge-js`)

`sdk/edge-js/index.ts` provides `LongbowEdgeClient` for Cloudflare Workers, Deno, Node, and browser runtimes with in-memory local HNSW search, queued offline writes, and `flushSync()` against `/api/v1/edge/sync`.

See `sdk/edge-js/examples/cloudflare_worker.ts` for a complete Cloudflare Worker router handling `/search` and `/sync`.

```typescript
import { LongbowEdgeClient } from "./sdk/edge-js";

const client = new LongbowEdgeClient({
  endpoint: "https://longbow.example.com",
  dataset: "products",
  apiKey: process.env.LONGBOW_API_KEY,
  offlineFirst: true,
});

await client.upsert("sku-1", [0.8, 0.2, 0.0, 0.0], { category: "edge" });
const results = await client.search([1, 0, 0, 0], 5);
await client.flushSync();
```
