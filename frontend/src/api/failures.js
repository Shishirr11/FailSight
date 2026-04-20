const BASE = `${import.meta.env.VITE_API_BASE || ""}/api/failures`;
export async function fetchFailureStats() {
  const res = await fetch(`${BASE}/stats`);
  if (!res.ok) throw new Error(`Failed to fetch failure stats: ${res.status}`);
  return res.json();
}

export async function fetchFailures({
  q = "",
  sector = "",
  reason = "",
  minYear = null,
  maxYear = null,
  source = "",
  limit = 20,
  offset = 0,
} = {}) {
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (sector) params.set("sector", sector);
  if (reason) params.set("reason", reason);
  if (minYear) params.set("min_year", minYear);
  if (maxYear) params.set("max_year", maxYear);
  if (source) params.set("source", source);
  params.set("limit", limit);
  params.set("offset", offset);

  const res = await fetch(`${BASE}?${params}`);
  if (!res.ok) throw new Error(`Failed to fetch failures: ${res.status}`);
  return res.json();
}

export async function fetchFailuresBySector(sectorName) {
  const res = await fetch(`${BASE}/sector/${encodeURIComponent(sectorName)}`);
  if (!res.ok)
    throw new Error(`Failed to fetch sector failures: ${res.status}`);
  return res.json();
}

export async function fetchFailure(failureId) {
  const res = await fetch(`${BASE}/${failureId}`);
  if (!res.ok) throw new Error(`Failure not found: ${failureId}`);
  return res.json();
}
