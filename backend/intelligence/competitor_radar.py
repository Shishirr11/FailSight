from loguru import logger

def get_competitor_radar(sector: str, con) -> dict:

    patent_holders = con.execute("""
        SELECT
            agency                          AS org,
            COUNT(*)                        AS patent_count
        FROM unified_opportunities
        WHERE source = 'patents'
          AND sector ILIKE ?
          AND agency != ''
          AND agency IS NOT NULL
        GROUP BY agency
        ORDER BY patent_count DESC
        LIMIT 10
    """, [f"%{sector}%"]).fetchdf().to_dict(orient="records")

    research_institutions = con.execute("""
        SELECT
            agency                          AS institution,
            COUNT(*)                        AS paper_count
        FROM unified_opportunities
        WHERE source = 'research'
          AND sector ILIKE ?
          AND agency != ''
          AND agency IS NOT NULL
        GROUP BY agency
        ORDER BY paper_count DESC
        LIMIT 10
    """, [f"%{sector}%"]).fetchdf().to_dict(orient="records")

    top_buyers = con.execute("""
        SELECT
            agency,
            COUNT(*)                                                    AS contract_count,
            COALESCE(AVG(funding_max)
                FILTER (WHERE funding_max > 0), 0)                     AS avg_contract_value
        FROM unified_opportunities
        WHERE source = 'sam'
          AND sector ILIKE ?
          AND agency != ''
          AND agency IS NOT NULL
        GROUP BY agency
        ORDER BY contract_count DESC
        LIMIT 10
    """, [f"%{sector}%"]).fetchdf().fillna(0).to_dict(orient="records")


    unique_holders = con.execute("""
        SELECT COUNT(DISTINCT agency)
        FROM unified_opportunities
        WHERE source = 'patents' AND sector ILIKE ?
          AND agency != '' AND agency IS NOT NULL
    """, [f"%{sector}%"]).fetchone()[0]

    ip_density = (
        "HIGH"   if int(unique_holders) > 20 else
        "MEDIUM" if int(unique_holders) > 5  else
        "LOW"
    )

    return {
        "ip_landscape": {
            "density":      ip_density,
            "unique_holders": int(unique_holders),
            "top_holders":  patent_holders,
        },
        "research_leaders":  research_institutions,
        "top_buyers":        top_buyers,
    }