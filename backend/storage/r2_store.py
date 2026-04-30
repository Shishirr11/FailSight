import json
import os
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from loguru import logger

_s3 = None
_s3_lock = threading.Lock()


def _get_client():
    global _s3
    if _s3 is not None:
        return _s3
    with _s3_lock:
        if _s3 is not None:
            return _s3
        try:
            import boto3
            from botocore.config import Config

            account_id = os.environ["R2_ACCOUNT_ID"]
            _s3 = boto3.client(
                "s3",
                endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
                aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
                config=Config(
                    retries={"max_attempts": 3, "mode": "adaptive"},
                    connect_timeout=10,
                    read_timeout=120,
                ),
                region_name="auto",
            )
            logger.info("R2 client initialised.")
            return _s3
        except KeyError as e:
            raise RuntimeError(
                f"Missing R2 env var: {e}. "
                "Set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY."
            ) from e


BUCKET = os.environ.get("R2_BUCKET_NAME", "failsight-data")

STORAGE_CAP_BYTES  = 8  * 1024 ** 3
READ_CAP_MONTHLY   = 8_000_000
WRITE_CAP_MONTHLY  = 800_000

STORAGE_WARN_BYTES = int(STORAGE_CAP_BYTES * 0.70)
READ_WARN_MONTHLY  = int(READ_CAP_MONTHLY  * 0.70)
WRITE_WARN_MONTHLY = int(WRITE_CAP_MONTHLY * 0.70)

USAGE_KEY = "_meta/usage.json"

_usage_cache: dict = {}
_usage_lock  = threading.RLock()
_last_fetched: float = 0.0
_CACHE_TTL = 300


class R2CapExceeded(Exception):
    def __init__(self, resource: str, current, cap, unit: str = ""):
        self.resource = resource
        self.current  = current
        self.cap      = cap
        self.unit     = unit
        super().__init__(
            f"R2 {resource} cap reached: {current:,}{unit} / {cap:,}{unit}. "
            f"Operation blocked to prevent charges."
        )


class R2Unavailable(Exception):
    pass


def _blank_usage() -> dict:
    now = datetime.now(timezone.utc)
    return {
        "month":         now.strftime("%Y-%m"),
        "reads":         0,
        "writes":        0,
        "storage_bytes": 0,
        "last_updated":  now.isoformat(),
    }


def _load_usage(force: bool = False) -> dict:
    global _usage_cache, _last_fetched
    with _usage_lock:
        now = time.monotonic()
        if not force and _usage_cache and (now - _last_fetched) < _CACHE_TTL:
            return dict(_usage_cache)

        try:
            client = _get_client()
            obj    = client.get_object(Bucket=BUCKET, Key=USAGE_KEY)
            data   = json.loads(obj["Body"].read())

            current_month = datetime.now(timezone.utc).strftime("%Y-%m")
            if data.get("month") != current_month:
                logger.info(f"New billing month ({current_month}) — resetting R2 counters.")
                data = _blank_usage()
                _save_usage_unlocked(data)

            _usage_cache  = data
            _last_fetched = now
            return dict(data)

        except Exception as e:
            err_str = str(e)
            if "NoSuchKey" in err_str or "does not exist" in err_str:
                # First run — usage file doesn't exist yet, create it
                logger.info("No usage file in R2 yet — creating fresh one.")
                data = _blank_usage()
                try:
                    _save_usage_unlocked(data)
                except Exception:
                    pass
                _usage_cache  = data
                _last_fetched = time.monotonic()
                return dict(data)
            logger.warning(f"Could not load R2 usage: {e} — using cached/blank.")
            if _usage_cache:
                return dict(_usage_cache)
            return _blank_usage()


def _save_usage_unlocked(data: dict):
    for attempt in range(3):
        try:
            data["last_updated"] = datetime.now(timezone.utc).isoformat()
            _get_client().put_object(
                Bucket=BUCKET,
                Key=USAGE_KEY,
                Body=json.dumps(data).encode(),
                ContentType="application/json",
            )
            return
        except Exception as e:
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
            else:
                logger.warning(f"Could not save R2 usage: {e}")


def _record_operation(op: str, bytes_delta: int = 0):
    with _usage_lock:
        data = _load_usage()
        if op == "read":
            data["reads"] += 1
        elif op in ("write", "multipart"):
            data["writes"] += 1
        if bytes_delta:
            data["storage_bytes"] = max(0, data.get("storage_bytes", 0) + bytes_delta)
        _usage_cache.update(data)
        threading.Thread(target=_save_usage_unlocked, args=(dict(data),), daemon=True).start()


def _check_write_cap(file_size_bytes: int = 0):
    usage = _load_usage()

    projected_storage = usage.get("storage_bytes", 0) + file_size_bytes
    if projected_storage > STORAGE_CAP_BYTES:
        raise R2CapExceeded(
            "storage",
            projected_storage // (1024 ** 3),
            STORAGE_CAP_BYTES // (1024 ** 3),
            " GB",
        )
    if projected_storage > STORAGE_WARN_BYTES:
        pct = projected_storage / STORAGE_CAP_BYTES * 100
        logger.warning(f"R2 storage at {pct:.0f}% of cap")

    writes = usage.get("writes", 0) + 1
    if writes > WRITE_CAP_MONTHLY:
        raise R2CapExceeded("writes", writes, WRITE_CAP_MONTHLY, " ops/month")
    if writes > WRITE_WARN_MONTHLY:
        logger.warning(f"R2 writes at {writes/WRITE_CAP_MONTHLY*100:.0f}% of monthly cap")


def _check_read_cap():
    usage = _load_usage()
    reads = usage.get("reads", 0) + 1
    if reads > READ_CAP_MONTHLY:
        raise R2CapExceeded("reads", reads, READ_CAP_MONTHLY, " ops/month")
    if reads > READ_WARN_MONTHLY:
        logger.warning(f"R2 reads at {reads/READ_CAP_MONTHLY*100:.0f}% of monthly cap")


def upload_file(local_path: Path, r2_key: str, content_type: str = "application/octet-stream") -> str:
    if not local_path.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    file_size = local_path.stat().st_size
    _check_write_cap(file_size)

    try:
        client = _get_client()
        logger.info(f"Uploading {local_path.name} ({file_size/1024/1024:.1f} MB) → R2:{r2_key}")
        with open(local_path, "rb") as fh:
            client.put_object(
                Bucket=BUCKET,
                Key=r2_key,
                Body=fh,
                ContentType=content_type,
            )
        _record_operation("write", 0)
        logger.success(f"Uploaded → R2:{r2_key}")
        return r2_key
    except R2CapExceeded:
        raise
    except Exception as e:
        raise R2Unavailable(f"R2 upload failed for {r2_key}: {e}") from e


def download_file(r2_key: str, local_path: Path) -> Path:
    _check_read_cap()
    try:
        client = _get_client()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloading R2:{r2_key} → {local_path}")
        obj       = client.get_object(Bucket=BUCKET, Key=r2_key)
        file_size = obj.get("ContentLength", 0)
        with open(local_path, "wb") as fh:
            fh.write(obj["Body"].read())
        _record_operation("read")
        logger.success(f"Downloaded R2:{r2_key} ({file_size/1024/1024:.1f} MB)")
        return local_path
    except R2CapExceeded:
        raise
    except Exception as e:
        if "NoSuchKey" in str(e) or "404" in str(e):
            raise FileNotFoundError(f"R2 key not found: {r2_key}")
        raise R2Unavailable(f"R2 download failed for {r2_key}: {e}") from e


def key_exists(r2_key: str) -> bool:
    _check_read_cap()
    try:
        _get_client().head_object(Bucket=BUCKET, Key=r2_key)
        _record_operation("read")
        return True
    except Exception:
        return False


def delete_file(r2_key: str, file_size_bytes: int = 0):
    try:
        _get_client().delete_object(Bucket=BUCKET, Key=r2_key)
        _record_operation("delete", -abs(file_size_bytes))
        logger.info(f"Deleted R2:{r2_key}")
    except Exception as e:
        logger.warning(f"Could not delete R2:{r2_key}: {e}")


KEYS = {
    "opportunities_parquet": "data/unified_opportunities.parquet",
    "failures_parquet":      "data/failures_unified.parquet",
    "enriched_parquet":      "data/enriched_details.parquet",
    "tfidf_matrix":          "index/tfidf_matrix.npz",
    "tfidf_vectorizer":      "index/tfidf_vectorizer.pkl",
    "tfidf_ids":             "index/tfidf_record_ids.json",
    "embed_matrix":          "index/embeddings_matrix.npy",
    "embed_ids":             "index/embedding_record_ids.json",
    "usage":                 USAGE_KEY,
}


def upload_data_assets(data_dir: Path, index_dir: Path):
    assets = [
        (data_dir  / "unified_opportunities.parquet", KEYS["opportunities_parquet"], "application/octet-stream"),
        (data_dir  / "failures_unified.parquet",      KEYS["failures_parquet"],      "application/octet-stream"),
        (data_dir  / "enriched_details.parquet",      KEYS["enriched_parquet"],      "application/octet-stream"),
        (index_dir / "tfidf_matrix.npz",              KEYS["tfidf_matrix"],          "application/octet-stream"),
        (index_dir / "tfidf_vectorizer.pkl",          KEYS["tfidf_vectorizer"],      "application/octet-stream"),
        (index_dir / "tfidf_record_ids.json",         KEYS["tfidf_ids"],             "application/json"),
        (index_dir / "embeddings_matrix.npy",         KEYS["embed_matrix"],          "application/octet-stream"),
        (index_dir / "embedding_record_ids.json",     KEYS["embed_ids"],             "application/json"),
    ]
    uploaded, skipped = [], []
    for local, key, ctype in assets:
        if local.exists():
            try:
                upload_file(local, key, ctype)
                uploaded.append(key)
            except R2CapExceeded:
                raise
            except R2Unavailable as e:
                logger.warning(f"Skipped {key}: {e}")
                skipped.append(key)
        else:
            skipped.append(key)
    return {"uploaded": uploaded, "skipped": skipped}


def download_data_assets(data_dir: Path, index_dir: Path, skip_embeddings: bool = False):
    assets = [
        (KEYS["opportunities_parquet"], data_dir  / "unified_opportunities.parquet"),
        (KEYS["failures_parquet"],      data_dir  / "failures_unified.parquet"),
        (KEYS["enriched_parquet"],      data_dir  / "enriched_details.parquet"),
        (KEYS["tfidf_matrix"],          index_dir / "tfidf_matrix.npz"),
        (KEYS["tfidf_vectorizer"],      index_dir / "tfidf_vectorizer.pkl"),
        (KEYS["tfidf_ids"],             index_dir / "tfidf_record_ids.json"),
    ]
    if not skip_embeddings:
        assets += [
            (KEYS["embed_matrix"], index_dir / "embeddings_matrix.npy"),
            (KEYS["embed_ids"],    index_dir / "embedding_record_ids.json"),
        ]
    downloaded, missing = [], []
    for key, local in assets:
        try:
            download_file(key, local)
            downloaded.append(key)
        except FileNotFoundError:
            logger.warning(f"R2 asset not found (first deploy?): {key}")
            missing.append(key)
        except R2CapExceeded:
            raise
        except R2Unavailable as e:
            logger.warning(f"Could not download {key}: {e}")
            missing.append(key)
    return {"downloaded": downloaded, "missing": missing}


def get_usage_report() -> dict:
    usage = _load_usage(force=True)

    def pct(val, cap):
        return round(val / cap * 100, 1)

    return {
        "month": usage.get("month"),
        "storage": {
            "used_bytes": usage.get("storage_bytes", 0),
            "used_gb":    round(usage.get("storage_bytes", 0) / 1024**3, 3),
            "cap_gb":     round(STORAGE_CAP_BYTES / 1024**3, 1),
            "pct":        pct(usage.get("storage_bytes", 0), STORAGE_CAP_BYTES),
            "warn":       usage.get("storage_bytes", 0) > STORAGE_WARN_BYTES,
            "exceeded":   usage.get("storage_bytes", 0) > STORAGE_CAP_BYTES,
        },
        "reads": {
            "used":     usage.get("reads", 0),
            "cap":      READ_CAP_MONTHLY,
            "pct":      pct(usage.get("reads", 0), READ_CAP_MONTHLY),
            "warn":     usage.get("reads", 0) > READ_WARN_MONTHLY,
            "exceeded": usage.get("reads", 0) > READ_CAP_MONTHLY,
        },
        "writes": {
            "used":     usage.get("writes", 0),
            "cap":      WRITE_CAP_MONTHLY,
            "pct":      pct(usage.get("writes", 0), WRITE_CAP_MONTHLY),
            "warn":     usage.get("writes", 0) > WRITE_WARN_MONTHLY,
            "exceeded": usage.get("writes", 0) > WRITE_CAP_MONTHLY,
        },
        "last_updated": usage.get("last_updated"),
    }