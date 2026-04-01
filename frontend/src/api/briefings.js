const BASE = "/api/briefings";

export async function fetchSectorBriefing(sector) {
  const res = await fetch(`${BASE}/sector`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sector }),
  });
  if (!res.ok) throw new Error(`Briefing failed: ${res.status}`);
  return res.json(); // { sector, briefing, data }
}

export async function explainOpportunity(opp_id, user_context = "") {
  const res = await fetch(`${BASE}/opportunity`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ opp_id, user_context }),
  });
  if (!res.ok) throw new Error(`Explainer failed: ${res.status}`);
  return res.json(); // { opp_id, title, explanation }
}
