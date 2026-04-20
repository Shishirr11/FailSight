const BASE = `${import.meta.env.VITE_API_BASE || ""}/api/pipeline`;
export async function triggerPipelineRun(source = null) {
  const res = await fetch(`${BASE}/run`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source }),
  });
  if (!res.ok) throw new Error(`Pipeline trigger failed: ${res.status}`);
  return res.json();
}

export async function fetchPipelineStatus() {
  const res = await fetch(`${BASE}/status`);
  if (!res.ok) throw new Error(`Pipeline status failed: ${res.status}`);
  return res.json();
}
