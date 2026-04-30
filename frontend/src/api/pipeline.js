// BASE now points to /api/storage — the R2-aware router that
// exports parquet and uploads to R2 after every pipeline run.
// The old /api/pipeline router does NOT upload to R2 and can be removed.
const BASE = `${import.meta.env.VITE_API_BASE || ""}/api/storage`;

export async function triggerPipelineRun(source = null, rebuildIndex = true) {
  const res = await fetch(`${BASE}/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source, rebuild_index: rebuildIndex }),
  });
  if (!res.ok) throw new Error(`Pipeline trigger failed: ${res.status}`);
  return res.json();
}

export async function fetchPipelineStatus() {
  const res = await fetch(`${BASE}/status`);
  if (!res.ok) throw new Error(`Pipeline status failed: ${res.status}`);
  return res.json();
}
