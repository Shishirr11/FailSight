from loguru import logger

def validate_market(sector: str, con) -> dict:


    contracts = con.execute("""
        SELECT COUNT(*) FROM unified_opportunities
        WHERE source = 'sam' AND sector ILIKE ?
        AND (close_date IS NULL OR close_date >= CURRENT_DATE)
    """, [f"%{sector}%"]).fetchone()[0]


    grants = con.execute("""
        SELECT COUNT(*) FROM unified_opportunities
        WHERE source = 'grants' AND sector ILIKE ?
        AND (close_date IS NULL OR close_date >= CURRENT_DATE)
    """, [f"%{sector}%"]).fetchone()[0]


    patents = con.execute("""
        SELECT COUNT(*) FROM unified_opportunities
        WHERE source = 'patents' AND sector ILIKE ?
        AND posted_date >= CURRENT_DATE - INTERVAL '2 YEARS'
    """, [f"%{sector}%"]).fetchone()[0]


    research = con.execute("""
        SELECT COUNT(*) FROM unified_opportunities
        WHERE source = 'research' AND sector ILIKE ?
        AND posted_date >= CURRENT_DATE - INTERVAL '2 YEARS'
    """, [f"%{sector}%"]).fetchone()[0]


    failures = con.execute("""
        SELECT COUNT(*) FROM failures_unified
        WHERE sector ILIKE ?
    """, [f"%{sector}%"]).fetchone()[0]


    contract_score  = min(25, int(contracts) * 2)
    grant_score     = min(25, int(grants) * 3)
    patent_score    = min(20, int(patents) // 5)
    research_score  = min(15, int(research) // 5)
    failure_penalty = min(15, int(failures) * 1)

    raw_score = contract_score + grant_score + patent_score + research_score - failure_penalty
    score     = max(0, min(100, raw_score))

    grade = "A" if score >= 70 else "B" if score >= 50 else "C" if score >= 30 else "D"

    grade_labels = {
        "A": "Strong market signals — good time to enter",
        "B": "Moderate signals — validate before committing",
        "C": "Weak signals — high risk, limited public demand",
        "D": "Very weak signals — reconsider this sector",
    }

    return {
        "score":    score,
        "grade":    grade,
        "label":    grade_labels[grade],
        "signals": {
            "contracts":      int(contracts),
            "grants":         int(grants),
            "patents":        int(patents),
            "research":       int(research),
            "known_failures": int(failures),
        },
        "score_breakdown": {
            "contract_score":  contract_score,
            "grant_score":     grant_score,
            "patent_score":    patent_score,
            "research_score":  research_score,
            "failure_penalty": -failure_penalty,
        },
    }