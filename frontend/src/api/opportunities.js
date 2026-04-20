const BASE = `${import.meta.env.VITE_API_BASE || ""}/api/search`;

export async function fetchOpportunities({
  q = "",
  source = [],
  sector = "",
  minFunding = null,
  maxFunding = null,
  openOnly = true,
  limit = 20,
  offset = 0,
} = {}) {
  const params = new URLSearchParams();

  if (q) params.set("q", q);
  if (sector) params.set("sector", sector);
  if (minFunding) params.set("min_funding", minFunding);
  if (maxFunding) params.set("max_funding", maxFunding);
  params.set("open_only", openOnly);
  params.set("limit", limit);
  params.set("offset", offset);

  source.forEach((s) => params.append("source", s));

  const res = await fetch(`${BASE}?${params}`);
  if (!res.ok) throw new Error(`Failed to fetch opportunities: ${res.status}`);
  return res.json();
}

export async function fetchOpportunity(oppId) {
  const res = await fetch(`${BASE}/${oppId}`);
  if (!res.ok) throw new Error(`Opportunity not found: ${oppId}`);
  return res.json();
}

export async function fetchStats() {
  const res = await fetch(`${BASE}/stats`);
  if (!res.ok) throw new Error(`Failed to fetch stats: ${res.status}`);
  return res.json();
}
