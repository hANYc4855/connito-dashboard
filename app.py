import asyncio
import fcntl
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import cloudscraper
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles

# ── logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("connito-api")

for _noisy in ("httpx", "httpcore", "hpack", "urllib3", "cloudscraper"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.INFO)

# ── config ─────────────────────────────────────────────────────────────────────
CYCLE_API_URL = "https://cycle-api.connito.ai"
LB_API_URL    = "https://dashboard-api.connito.ai"
TIMEOUT       = 10.0
CACHE_TTL     = 2.0   # cycle endpoints — data changes every block
LB_CACHE_TTL  = 5.0   # leaderboard — heavier payload

HISTORY_DIR              = Path(__file__).resolve().parent / "data"
HISTORY_PATH             = HISTORY_DIR / "miner_history.json"
HISTORY_LOCK_PATH        = HISTORY_DIR / "miner_history.lock"
MAX_REVISIONS_PER_MINER  = 100
MAX_SAMPLES_PER_REVISION = 100  # val_loss/score readings within one hf_revision
METRIC_DECIMALS          = 7


def _round_metric(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(value), METRIC_DECIMALS)

CYCLE_ENDPOINTS = {
    "get_phase":               "/get_phase",
    "blocks_until_next_phase": "/blocks_until_next_phase",
}

# ── shared state ───────────────────────────────────────────────────────────────
_scraper:   cloudscraper.CloudScraper | None = None
_lb_client: httpx.AsyncClient | None         = None
_cache: dict[str, tuple[float, Any]]         = {}
_history_lock = asyncio.Lock()


def _read_miner_history_unlocked() -> dict[str, dict[str, dict[str, Any]]]:
    if not HISTORY_PATH.exists():
        return {}
    try:
        with HISTORY_PATH.open(encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except Exception as exc:
        logger.warning("miner history read failed: %s", exc)
        return {}


def _write_miner_history_unlocked(history: dict[str, dict[str, dict[str, Any]]]) -> None:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    tmp = HISTORY_PATH.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(history, f, separators=(",", ":"))
    tmp.replace(HISTORY_PATH)


def _history_snapshot(history: dict[str, dict[str, dict[str, Any]]]) -> dict[str, dict[str, dict[str, Any]]]:
    return {uid: dict(revs) for uid, revs in history.items()}


def _rev_samples(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [p for p in raw if isinstance(p, dict) and "v" in p]
    if isinstance(raw, dict) and "v" in raw:
        return [raw]
    return []


def _rev_latest_t(raw: Any) -> int:
    samples = _rev_samples(raw)
    return samples[-1].get("t", 0) if samples else 0


def _trim_revision_samples(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(samples) <= MAX_SAMPLES_PER_REVISION:
        return samples
    return samples[-MAX_SAMPLES_PER_REVISION:]


def _trim_miner_revisions(history: dict[str, dict[str, Any]], uid_key: str) -> None:
    revs = history.get(uid_key)
    if not revs or len(revs) <= MAX_REVISIONS_PER_MINER:
        return
    ordered = sorted(revs.items(), key=lambda item: _rev_latest_t(item[1]))
    for rev_key, _ in ordered[: len(ordered) - MAX_REVISIONS_PER_MINER]:
        del revs[rev_key]


def _merge_leaderboard_into_history(
    history: dict[str, dict[str, Any]],
    entries: list[Any],
) -> None:
    now_ms = int(time.time() * 1000)
    for row in entries:
        if not isinstance(row, dict):
            continue
        uid = row.get("uid")
        val = _round_metric(row.get("val_loss"))
        if uid is None or val is None:
            continue
        uid_key = str(uid)
        rev = row.get("hf_revision") or None
        rev_key = rev or "__none__"
        score = _round_metric(row.get("score"))
        revs = history.setdefault(uid_key, {})
        samples = _rev_samples(revs.get(rev_key))
        last = samples[-1] if samples else None
        if last is not None and last.get("v") == val and last.get("s") == score:
            continue
        samples.append({"v": val, "s": score, "t": now_ms, "rev": rev})
        revs[rev_key] = _trim_revision_samples(samples)
        _trim_miner_revisions(history, uid_key)


def _with_history_file(fn):
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    with HISTORY_LOCK_PATH.open("w") as lockf:
        fcntl.flock(lockf.fileno(), fcntl.LOCK_EX)
        return fn()


def _read_miner_history_sync() -> dict[str, dict[str, dict[str, Any]]]:
    def work():
        return _history_snapshot(_read_miner_history_unlocked())

    return _with_history_file(work)


def _update_miner_history_sync(entries: list[Any] | None) -> dict[str, dict[str, dict[str, Any]]]:
    def work():
        history = _read_miner_history_unlocked()
        if entries:
            _merge_leaderboard_into_history(history, entries)
            _write_miner_history_unlocked(history)
        return _history_snapshot(history)

    return _with_history_file(work)


async def _read_miner_history() -> dict[str, dict[str, dict[str, Any]]]:
    async with _history_lock:
        return await asyncio.to_thread(_read_miner_history_sync)


async def _update_miner_history(entries: list[Any] | None) -> dict[str, dict[str, dict[str, Any]]]:
    async with _history_lock:
        return await asyncio.to_thread(_update_miner_history_sync, entries)


def _leaderboard_rows(data: Any) -> list[Any]:
    if not isinstance(data, dict):
        return []
    inner = data.get("data")
    if isinstance(inner, dict) and isinstance(inner.get("leaderboard"), list):
        return inner["leaderboard"]
    if isinstance(data.get("leaderboard"), list):
        return data["leaderboard"]
    return []


@asynccontextmanager
async def lifespan(app):
    global _scraper, _lb_client

    if HISTORY_PATH.exists():
        logger.info("Miner history file present at %s", HISTORY_PATH)
    else:
        logger.info("Miner history will be stored at %s", HISTORY_PATH)

    _scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "linux", "mobile": False}
    )
    _scraper.headers.update({"Accept": "application/json, */*"})

    _lb_client = httpx.AsyncClient(
        base_url=LB_API_URL,
        timeout=TIMEOUT,
        limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        http2=True,
    )

    logger.info("Clients initialised (CloudScraper + httpx/HTTP2)")
    yield
    _scraper.close()
    await _lb_client.aclose()
    logger.info("Clients closed")


# ── app ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Connito Monitor", version="2.0.0", lifespan=lifespan)


# ── middleware ─────────────────────────────────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    logger.debug("→ %s %s  client=%s", request.method, request.url.path,
                 request.client.host if request.client else "?")
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    level = logging.WARNING if response.status_code >= 400 else logging.DEBUG
    logger.log(level, "← %s %s  %d  %.1fms",
               request.method, request.url.path, response.status_code, elapsed_ms)
    return response


# ── cycle-api fetch (cloudscraper via thread pool) ─────────────────────────────
def _cycle_fetch_sync(path: str) -> Any | None:
    url = f"{CYCLE_API_URL}{path}"
    start = time.perf_counter()
    try:
        resp = _scraper.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        logger.debug("cycle OK  %s  %.1fms", path, (time.perf_counter() - start) * 1000)
        return data
    except Exception as exc:
        logger.warning("cycle ERR  %s  %s: %s", path, type(exc).__name__, exc)
        return None


async def _cycle_fetch(path: str) -> Any | None:
    now = time.monotonic()
    cached_at, cached_data = _cache.get(path, (0.0, None))
    if cached_data is not None and (now - cached_at) < CACHE_TTL:
        return cached_data
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, _cycle_fetch_sync, path)
    if data is not None:
        _cache[path] = (time.monotonic(), data)
    return data


# ── leaderboard fetch (httpx async) ───────────────────────────────────────────
async def _lb_fetch() -> Any | None:
    now = time.monotonic()
    cached_at, cached_data = _cache.get("leaderboard", (0.0, None))
    if cached_data is not None and (now - cached_at) < LB_CACHE_TTL:
        return cached_data
    start = time.perf_counter()
    try:
        resp = await _lb_client.get("/api/v1/leaderboard")
        resp.raise_for_status()
        data = resp.json()
        logger.debug("leaderboard OK  %.1fms", (time.perf_counter() - start) * 1000)
        _cache["leaderboard"] = (time.monotonic(), data)
        return data
    except Exception as exc:
        logger.warning("leaderboard ERR  %s: %s", type(exc).__name__, exc)
        return None


# ── endpoints ──────────────────────────────────────────────────────────────────
@app.get("/api/get_phase")
async def get_phase():
    data = await _cycle_fetch("/get_phase")
    if data is None:
        raise HTTPException(502, "Upstream API unavailable")
    return data


@app.get("/api/blocks_until_next_phase")
async def blocks_until_next_phase():
    data = await _cycle_fetch("/blocks_until_next_phase")
    if data is None:
        raise HTTPException(502, "Upstream API unavailable")
    return data


@app.get("/api/miner-history")
async def get_miner_history():
    return await _read_miner_history()


@app.get("/api/leaderboard")
async def get_leaderboard():
    data = await _lb_fetch()
    if data is None:
        raise HTTPException(502, "Leaderboard API unavailable")
    history = await _update_miner_history(_leaderboard_rows(data))
    return {**data, "miner_history": history}


@app.get("/api/all")
async def get_all():
    results = await asyncio.gather(
        _cycle_fetch("/get_phase"),
        _cycle_fetch("/blocks_until_next_phase"),
    )
    return {
        "get_phase":               results[0],
        "blocks_until_next_phase": results[1],
    }


app.mount("/", StaticFiles(directory="static", html=True), name="static")
