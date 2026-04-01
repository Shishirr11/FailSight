# tests/test_sam_days_back.py

import os
import time
import httpx
from datetime import date, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://api.sam.gov/opportunities/v2/search"
API_KEY = os.getenv("SAM_API_KEY", "").strip()

TIMEOUT = httpx.Timeout(connect=10.0, read=25.0, write=10.0, pool=10.0)

TEST_DAY_RANGES = [1, 3, 7, 14, 30, 60, 90]
TEST_NAICS = "541511"
LIMIT = 10


def build_params(days_back: int, offset: int = 0) -> dict:
    posted_to = date.today()
    posted_from = posted_to - timedelta(days=days_back)

    return {
        "api_key": API_KEY,
        "limit": LIMIT,
        "offset": offset,
        "ncode": TEST_NAICS,
        "postedFrom": posted_from.strftime("%m/%d/%Y"),
        "postedTo": posted_to.strftime("%m/%d/%Y"),
    }


def run_single_test(days_back: int) -> dict:
    params = build_params(days_back)
    started = time.perf_counter()

    try:
        response = httpx.get(API_URL, params=params, timeout=TIMEOUT)
        elapsed = time.perf_counter() - started

        result = {
            "days_back": days_back,
            "status_code": response.status_code,
            "elapsed_seconds": round(elapsed, 2),
            "success": False,
            "timed_out": False,
            "record_count": None,
            "error": None,
        }

        response.raise_for_status()

        data = response.json()

        opportunities = []
        if isinstance(data, dict):
            if "opportunitiesData" in data and isinstance(data["opportunitiesData"], list):
                opportunities = data["opportunitiesData"]
            elif "data" in data and isinstance(data["data"], list):
                opportunities = data["data"]

        result["record_count"] = len(opportunities)
        result["success"] = True
        return result

    except httpx.ConnectTimeout:
        elapsed = time.perf_counter() - started
        return {
            "days_back": days_back,
            "status_code": None,
            "elapsed_seconds": round(elapsed, 2),
            "success": False,
            "timed_out": True,
            "record_count": None,
            "error": "ConnectTimeout",
        }
    except httpx.ReadTimeout:
        elapsed = time.perf_counter() - started
        return {
            "days_back": days_back,
            "status_code": None,
            "elapsed_seconds": round(elapsed, 2),
            "success": False,
            "timed_out": True,
            "record_count": None,
            "error": "ReadTimeout",
        }
    except Exception as e:
        elapsed = time.perf_counter() - started
        return {
            "days_back": days_back,
            "status_code": None,
            "elapsed_seconds": round(elapsed, 2),
            "success": False,
            "timed_out": False,
            "record_count": None,
            "error": str(e),
        }


def choose_best_range(results: list[dict], max_seconds: float = 8.0) -> Optional[dict]:
    stable = [
        r for r in results
        if r["success"] and r["elapsed_seconds"] <= max_seconds
    ]
    if not stable:
        return None
    return max(stable, key=lambda x: x["days_back"])


def main():
    if not API_KEY:
        raise ValueError("SAM_API_KEY is not set in environment variables.")

    print("\nTesting SAM.gov day ranges...\n")

    results = []
    for days in TEST_DAY_RANGES:
        result = run_single_test(days)
        results.append(result)
        print(
            f"days_back={result['days_back']:>3} | "
            f"success={str(result['success']):<5} | "
            f"timeout={str(result['timed_out']):<5} | "
            f"status={str(result['status_code']):<4} | "
            f"time={result['elapsed_seconds']:<5}s | "
            f"records={str(result['record_count']):<4} | "
            f"error={result['error']}"
        )

    best = choose_best_range(results, max_seconds=8.0)

    print("\nRecommended range:")
    if best:
        print(
            f"Use days_back={best['days_back']} "
            f"(response time: {best['elapsed_seconds']}s, records: {best['record_count']})"
        )
    else:
        print("No stable range found under the selected timeout threshold.")


if __name__ == "__main__":
    print(os.getenv("SAM_API_KEY", "").strip())
    main()