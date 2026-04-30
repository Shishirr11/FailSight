const BASE = `${import.meta.env.VITE_API_BASE || ""}/api/search`;

export async function semanticSearch({
  query,
  sources = [],
  limit = 20,
  offset = 0,
} = {}) {
  const res = await fetch(BASE, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query,
      sources,
      limit,
      offset,
      use_embeddings: false,
    }),
  });
  if (!res.ok) throw new Error(`Search failed: ${res.status}`);
  return res.json();
}

export async function fetchSuggestions(q = "") {
  if (q.length < 2) return { sectors: [], agencies: [] };
  const res = await fetch(`${BASE}/suggest?q=${encodeURIComponent(q)}`);
  if (!res.ok) return { sectors: [], agencies: [] };
  return res.json();
}
