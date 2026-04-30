const BASE = `${import.meta.env.VITE_API_BASE || ""}/api/groq`;
export async function whyCare(oppId, userQuery) {
  const res = await fetch(`${BASE}/why-care`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ opp_id: oppId, user_query: userQuery }),
  });
  if (!res.ok) {
    const e = await res.json().catch(() => ({}));
    throw new Error(e.detail || `why-care failed: ${res.status}`);
  }
  return res.json();
}

export async function validateIdea(idea, sector = null) {
  const res = await fetch(`${BASE}/validate-idea`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ idea, sector }),
  });
  if (!res.ok) {
    const e = await res.json().catch(() => ({}));
    throw new Error(e.detail || `failed: ${res.status}`);
  }
  return res.json();
}

export async function gapFinder(sector) {
  const res = await fetch(`${BASE}/gap-finder`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sector }),
  });
  if (!res.ok) {
    const e = await res.json().catch(() => ({}));
    throw new Error(e.detail || `failed: ${res.status}`);
  }
  return res.json();
}

export async function grantMatch(projectDescription, sector = null) {
  const res = await fetch(`${BASE}/grant-match`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ project_description: projectDescription, sector }),
  });
  if (!res.ok) {
    const e = await res.json().catch(() => ({}));
    throw new Error(e.detail || `failed: ${res.status}`);
  }
  return res.json();
}
