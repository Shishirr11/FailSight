from loguru import logger

def detect_white_spaces(con, min_innovation: int = 5) -> list[dict]:
    rows = con.execute("""
        WITH signals AS (
            SELECT
                sector,
                SUM(CASE WHEN source IN ('patents', 'research') THEN 1 ELSE 0 END)
                    AS innovation_signal,
                SUM(CASE WHEN source IN ('sam', 'grants') THEN 1 ELSE 0 END)
                    AS market_signal,
                COUNT(DISTINCT CASE WHEN source = 'patents'
                    THEN agency END)                                    AS unique_ip_holders,
                COUNT(DISTINCT CASE WHEN source = 'sam'
                    THEN agency END)                                    AS unique_buyers
            FROM unified_opportunities
            WHERE sector IS NOT NULL AND sector != 'Other'
            GROUP BY sector
        )
        SELECT
            sector,
            innovation_signal,
            market_signal,
            unique_ip_holders,
            unique_buyers,
            (innovation_signal - market_signal)     AS white_space_score,
            CASE
                WHEN (innovation_signal - market_signal) > 50
                    THEN 'HIGH'
                WHEN (innovation_signal - market_signal) > 20
                    THEN 'MODERATE'
                ELSE 'LOW'
            END                                     AS opportunity_level
        FROM signals
        WHERE innovation_signal >= ?
        ORDER BY white_space_score DESC
    """, [min_innovation]).fetchdf()

    return rows.fillna(0).to_dict(orient="records")


def get_sector_white_space(sector: str, con) -> dict:

    innovation = con.execute("""
        SELECT COUNT(*) FROM unified_opportunities
        WHERE source IN ('patents', 'research') AND sector ILIKE ?
    """, [f"%{sector}%"]).fetchone()[0]

    market = con.execute("""
        SELECT COUNT(*) FROM unified_opportunities
        WHERE source IN ('sam', 'grants') AND sector ILIKE ?
    """, [f"%{sector}%"]).fetchone()[0]

    score = int(innovation) - int(market)

    opportunity_level = (
        "HIGH"     if score > 50 else
        "MODERATE" if score > 20 else
        "LOW"
    )

    interpretation = {
        "HIGH":     "Strong R&D activity with limited market contracts — classic first-mover opportunity.",
        "MODERATE": "Some R&D activity ahead of market demand — worth monitoring closely.",
        "LOW":      "Market and innovation signals are balanced — competitive but established space.",
    }

    return {
        "sector":            sector,
        "innovation_signal": int(innovation),
        "market_signal":     int(market),
        "white_space_score": score,
        "opportunity_level": opportunity_level,
        "interpretation":    interpretation[opportunity_level],
    }