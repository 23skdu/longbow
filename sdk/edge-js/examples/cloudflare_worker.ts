import { LongbowEdgeClient } from "../index";

export default {
  async fetch(request: Request, env: { LONGBOW_URL: string; LONGBOW_API_KEY: string }): Promise<Response> {
    const client = new LongbowEdgeClient({
      endpoint: env.LONGBOW_URL,
      dataset: "edge-catalog",
      apiKey: env.LONGBOW_API_KEY,
      offlineFirst: false,
    });
    const body = (await request.json()) as { vector: number[]; k?: number };
    const results = await client.search(body.vector, body.k ?? 5);
    return new Response(JSON.stringify({ results }), {
      headers: { "Content-Type": "application/json" },
    });
  },
};

