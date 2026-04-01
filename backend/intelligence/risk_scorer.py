from collections import Counter
from loguru import logger


def score_sector_risk(sector: str, con) -> dict:
    rows = con.execute("""
        SELECT
            failure_reasons,
            funding_raised_usd,
            company_name,
            year_failed
        FROM failures_unified
        WHERE sector ILIKE ?
    """, [f"%{sector}%"]).fetchdf()

    n = len(rows)
    if n == 0:
        return {
            "risk_level":        "UNKNOWN",
            "total_failures":    0,
            "top_reasons":       [],
            "avg_funding_burned": 0,
            "recommendation":    "No failure data found for this sector.",
        }
    all_reasons = []
    for reasons in rows["failure_reasons"].tolist():
        if isinstance(reasons, list):
            all_reasons.extend(reasons)
        elif isinstance(reasons, str) and reasons:
            all_reasons.append(reasons)

    top_reasons = [
        {"reason": r, "count": c}
        for r, c in Counter(all_reasons).most_common(3)
        if r != "unknown"
    ]

    funded = rows["funding_raised_usd"].dropna()
    funded = funded[funded > 0]
    avg_funding = float(funded.mean()) if len(funded) > 0 else 0

    risk_level = "LOW" if n < 3 else "MEDIUM" if n < 10 else "HIGH"

    top_reason_label = top_reasons[0]["reason"].replace("_", " ").title() if top_reasons else "unknown"

    recommendations = {
        "LOW":    f"Few known failures in this sector. Primary risk: {top_reason_label}. Validate early.",
        "MEDIUM": f"{n} known failures. Watch out for {top_reason_label}. Do deep market validation first.",
        "HIGH":   f"{n} known failures — high-risk sector. {top_reason_label} is the leading killer here. "
                  f"Average ${avg_funding/1e6:.1f}M burned before shutdown. Proceed with strong differentiation.",
    }

    return {
        "risk_level":         risk_level,
        "total_failures":     n,
        "top_reasons":        top_reasons,
        "avg_funding_burned": round(avg_funding, 2),
        "recommendation":     recommendations[risk_level],
    }