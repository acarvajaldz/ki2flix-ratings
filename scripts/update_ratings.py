#!/usr/bin/env python3
"""Daily updater for KI²flix shared combined ratings cache (IMDb + RT + TMDB via MDBList).

Zero external dependencies (Python 3 stdlib only). Reads TMDB_API_KEY and MDBLIST_API_KEY
from environment variables, discovers popular/trending/top-rated/streaming-channel titles
on TMDB, fetches missing or stale ratings from MDBList in large batches while preserving a
daily safety reserve for live TV Box fallbacks, and writes compact gzipped ratings.json.gz.
"""

from __future__ import annotations

import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
RATINGS_PATH = ROOT / "ratings.json.gz"
META_PATH = ROOT / "meta.json.gz"

TMDB_BASE = "https://api.themoviedb.org/3"
MDBLIST_BASE = "https://api.mdblist.com"

# Spanish + major global streaming providers covered in KI²flix Channels
PROVIDERS_ES = [8, 119, 337, 1899, 350, 149, 1773, 63, 62, 393]

# Genres matching KI²flix Categories so ★ Nota (vote_count.gte 300/150) hits the shared cache immediately
MOVIE_GENRES_HIGH_VOTES = [28, 12, 16, 35, 80, 18, 10751, 14, 27, 9648, 10749, 878, 53, 10752]
MOVIE_GENRES_NICHE_VOTES = [99, 10402, 37, 10770]  # Documentary, Music, Western, TV Movie
TV_GENRES_HIGH_VOTES = [10759, 16, 35, 80, 99, 18, 10751, 9648, 10765, 10768, 37]

BATCH_SIZE = 150
RESERVE_DAILY_REQUESTS = 200  # Always leave >=200/day for live family TV Box fallbacks
MAX_BATCHES_PER_RUN = 60      # Upper bound per run even if batch counts as 1 request
FRESH_TTL_RECENT_SEC = 14 * 86400   # Releases < 6 months old: refresh every 14 days
FRESH_TTL_MID_SEC = 180 * 86400     # Releases 6–24 months old: refresh every 6 months
FRESH_TTL_CLASSIC_SEC = 365 * 86400 # Releases > 2 years old: refresh once a year
SIX_MONTHS_SEC = 180 * 86400
TWO_YEARS_SEC = 730 * 86400
MISS_TTL_SEC = 30 * 86400


def http_json(url: str, data: dict[str, Any] | None = None, timeout: int = 20) -> tuple[int, dict[str, str], Any]:
    headers = {"Accept": "application/json", "User-Agent": "KI2flix-RatingsBot/1.0"}
    body = None
    method = "GET"
    if data is not None:
        body = json.dumps(data, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
        method = "POST"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            resp_headers = {k.lower(): v for k, v in resp.headers.items()}
            return resp.status, resp_headers, json.loads(raw) if raw.strip() else None
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        resp_headers = {k.lower(): v for k, v in e.headers.items()}
        try:
            parsed = json.loads(raw) if raw.strip() else None
        except json.JSONDecodeError:
            parsed = raw[:200]
        return e.code, resp_headers, parsed
    except Exception as e:
        print(f"WARN network error: {type(e).__name__}: {e}", file=sys.stderr)
        return 0, {}, None


def load_gz_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def save_gz_json(path: Path, payload: dict[str, Any]) -> None:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    with path.open("wb") as raw_f:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw_f, compresslevel=9, mtime=0) as f:
            f.write(raw)


def tmdb_get(api_key: str, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    q = {"api_key": api_key, "language": "es-ES"}
    if params:
        q.update(params)
    url = f"{TMDB_BASE}/{endpoint.lstrip('/')}?{urllib.parse.urlencode(q)}"
    status, _, data = http_json(url, timeout=15)
    if status == 200 and isinstance(data, dict):
        return data
    return None


def parse_release_ts(date_str: str) -> int:
    if len(date_str) >= 10:
        try:
            return int(time.mktime(time.strptime(date_str[:10], "%Y-%m-%d")))
        except ValueError:
            pass
    if len(date_str) >= 4 and date_str[:4].isdigit():
        try:
            return int(time.mktime(time.strptime(f"{date_str[:4]}-06-15", "%Y-%m-%d")))
        except ValueError:
            pass
    return 0


def discover_tmdb_ids(tmdb_key: str) -> tuple[dict[str, int], dict[str, int]]:
    """Returns (movie_release_ts, tv_release_ts) keyed by TMDB ID string -> release epoch (or 0)."""
    movies: dict[str, int] = {}
    shows: dict[str, int] = {}

    def record(target: dict[str, int], results: list[dict[str, Any]], date_field: str) -> None:
        for item in results:
            tid = item.get("id")
            if not isinstance(tid, int) or tid <= 0:
                continue
            sid = str(tid)
            if sid in target:
                continue
            target[sid] = parse_release_ts(str(item.get(date_field) or ""))

    def collect(endpoint: str, target: dict[str, int], date_field: str, pages: int, extra: dict[str, Any] | None = None) -> None:
        for page in range(1, pages + 1):
            p = dict(extra or {})
            p["page"] = page
            data = tmdb_get(tmdb_key, endpoint, p)
            if not data:
                break
            results = data.get("results") or []
            if not results:
                break
            record(target, results, date_field)
            if page >= int(data.get("total_pages") or 1):
                break

    # Core feeds (trending, popular, top rated, now playing / on the air)
    collect("trending/movie/week", movies, "release_date", pages=8)
    collect("trending/tv/week", shows, "first_air_date", pages=8)
    collect("movie/popular", movies, "release_date", pages=30)
    collect("tv/popular", shows, "first_air_date", pages=25)
    collect("movie/top_rated", movies, "release_date", pages=30)
    collect("tv/top_rated", shows, "first_air_date", pages=25)
    collect("movie/now_playing", movies, "release_date", pages=5)
    collect("tv/on_the_air", shows, "first_air_date", pages=5)

    # Major streaming platforms in Spain (popular + top rated)
    for pid in PROVIDERS_ES:
        base_params = {
            "watch_region": "ES",
            "with_watch_providers": str(pid),
            "with_watch_monetization_types": "flatrate",
        }
        collect("discover/movie", movies, "release_date", pages=6, extra={**base_params, "sort_by": "popularity.desc"})
        collect("discover/movie", movies, "release_date", pages=4, extra={**base_params, "sort_by": "vote_average.desc", "vote_count.gte": "100"})
        collect("discover/tv", shows, "first_air_date", pages=6, extra={**base_params, "sort_by": "popularity.desc"})
        collect("discover/tv", shows, "first_air_date", pages=4, extra={**base_params, "sort_by": "vote_average.desc", "vote_count.gte": "50"})

    # Per-genre top-rated discovery matching KI²flix adaptive vote floors (300/150 movies/TV, 50 niche)
    for gid in MOVIE_GENRES_HIGH_VOTES:
        collect("discover/movie", movies, "release_date", pages=3, extra={"with_genres": str(gid), "sort_by": "vote_average.desc", "vote_count.gte": "300"})
    for gid in MOVIE_GENRES_NICHE_VOTES:
        collect("discover/movie", movies, "release_date", pages=2, extra={"with_genres": str(gid), "sort_by": "vote_average.desc", "vote_count.gte": "50"})
    for gid in TV_GENRES_HIGH_VOTES:
        collect("discover/tv", shows, "first_air_date", pages=2, extra={"with_genres": str(gid), "sort_by": "vote_average.desc", "vote_count.gte": "150"})

    return movies, shows


def mdblist_user_quota(api_key: str) -> tuple[int, int] | None:
    """Returns (used, limit) from MDBList /user endpoint, or None on error."""
    status, _, data = http_json(f"{MDBLIST_BASE}/user?apikey={urllib.parse.quote(api_key)}")
    if status != 200 or not isinstance(data, dict):
        return None
    limit = int(data.get("api_requests") or 1000)
    used = int(data.get("api_requests_count") or 0)
    return used, limit


def compute_entry(obj: dict[str, Any]) -> list[int | None] | None:
    ratings = obj.get("ratings")
    if not isinstance(ratings, list):
        return None
    imdb: float | None = None
    tomatoes: float | None = None
    popcorn: float | None = None
    tmdb: float | None = None

    for r in ratings:
        if not isinstance(r, dict):
            continue
        source = str(r.get("source") or "").lower()
        score = r.get("score")
        value = r.get("value")
        score_f = float(score) if isinstance(score, (int, float)) else None
        value_f = float(value) if isinstance(value, (int, float)) else None
        if source == "imdb":
            imdb = score_f if score_f is not None else (value_f * 10.0 if value_f is not None else None)
        elif source == "tomatoes":
            tomatoes = score_f if score_f is not None else value_f
        elif source == "popcorn":
            popcorn = score_f if score_f is not None else value_f
        elif source == "tmdb":
            tmdb = score_f if score_f is not None else value_f

    imdb_norm = imdb if imdb is not None and 0.0 < imdb <= 100.0 else None
    rt_norm = tomatoes if tomatoes is not None and 0.0 < tomatoes <= 100.0 else (
        popcorn if popcorn is not None and 0.0 < popcorn <= 100.0 else None
    )
    tmdb_norm = tmdb if tmdb is not None and 0.0 < tmdb <= 100.0 else None

    available = [x for x in (imdb_norm, rt_norm, tmdb_norm) if x is not None]
    if not available:
        return None

    avg10 = int(round(sum(available) / len(available)))
    imdb10 = int(round(imdb_norm)) if imdb_norm is not None else None
    rt_int = int(round(rt_norm)) if rt_norm is not None else None
    tmdb10 = int(round(tmdb_norm)) if tmdb_norm is not None else None
    return [avg10, imdb10, rt_int, tmdb10]


def is_stale(sid: str, release_ts: int, meta_bucket: dict[str, int], miss_bucket: dict[str, int], now: int) -> bool:
    last_miss = miss_bucket.get(sid)
    if last_miss and now - last_miss < MISS_TTL_SEC:
        return False
    last_ok = meta_bucket.get(sid)
    if not last_ok:
        return True
    age_since_release = (now - release_ts) if release_ts > 0 else TWO_YEARS_SEC + 1
    if age_since_release < SIX_MONTHS_SEC:
        ttl = FRESH_TTL_RECENT_SEC
    elif age_since_release < TWO_YEARS_SEC:
        ttl = FRESH_TTL_MID_SEC
    else:
        ttl = FRESH_TTL_CLASSIC_SEC
    return (now - last_ok) >= ttl


def main() -> int:
    tmdb_key = os.environ.get("TMDB_API_KEY", "").strip()
    mdblist_key = os.environ.get("MDBLIST_API_KEY", "").strip()
    if not tmdb_key or not mdblist_key:
        print("ERROR: TMDB_API_KEY and MDBLIST_API_KEY must be set.", file=sys.stderr)
        return 1

    now = int(time.time())
    ratings_doc = load_gz_json(RATINGS_PATH, {"v": 1, "updated": 0, "m": {}, "t": {}})
    meta_doc = load_gz_json(META_PATH, {"m": {}, "t": {}, "miss_m": {}, "miss_t": {}})

    ratings_m: dict[str, list[int | None]] = ratings_doc.setdefault("m", {})
    ratings_t: dict[str, list[int | None]] = ratings_doc.setdefault("t", {})
    meta_m: dict[str, int] = meta_doc.setdefault("m", {})
    meta_t: dict[str, int] = meta_doc.setdefault("t", {})
    miss_m: dict[str, int] = meta_doc.setdefault("miss_m", {})
    miss_t: dict[str, int] = meta_doc.setdefault("miss_t", {})

    quota = mdblist_user_quota(mdblist_key)
    if quota:
        used, limit = quota
        remaining = max(0, limit - used)
        print(f"MDBList quota before run: used={used}/{limit}, remaining={remaining}")
    else:
        used, limit, remaining = 0, 1000, 800
        print("MDBList quota endpoint unavailable; proceeding conservatively.")

    budget = max(0, remaining - RESERVE_DAILY_REQUESTS)
    if budget <= 0:
        print("Daily reserve reached; skipping MDBList fetches today.")
        return 0

    print("Discovering TMDB titles...")
    discovered_m, discovered_t = discover_tmdb_ids(tmdb_key)
    print(f"Discovered {len(discovered_m)} unique movies and {len(discovered_t)} unique TV shows.")

    # Prioritize missing titles first, then stale titles
    todo_m = sorted(
        [sid for sid, yr in discovered_m.items() if is_stale(sid, yr, meta_m, miss_m, now)],
        key=lambda sid: (0 if sid not in ratings_m else 1, -discovered_m[sid]),
    )
    todo_t = sorted(
        [sid for sid, yr in discovered_t.items() if is_stale(sid, yr, meta_t, miss_t, now)],
        key=lambda sid: (0 if sid not in ratings_t else 1, -discovered_t[sid]),
    )
    print(f"Pending refresh: {len(todo_m)} movies, {len(todo_t)} TV shows.")

    # Interleave movie & TV batches so both categories populate from day 1
    movie_batches = [todo_m[i : i + BATCH_SIZE] for i in range(0, len(todo_m), BATCH_SIZE)]
    tv_batches = [todo_t[i : i + BATCH_SIZE] for i in range(0, len(todo_t), BATCH_SIZE)]
    plan: list[tuple[str, list[str]]] = []
    for i in range(max(len(movie_batches), len(tv_batches))):
        if i < len(movie_batches):
            plan.append(("movie", movie_batches[i]))
        if i < len(tv_batches):
            plan.append(("show", tv_batches[i]))

    updated_count = 0
    batches_done = 0
    cost_per_item = False

    for kind, batch_ids in plan:
        if batches_done >= MAX_BATCHES_PER_RUN or budget <= 0:
            break
        # If MDBList counts 1 request per item, trim batch to remaining budget
        if cost_per_item and len(batch_ids) > budget:
            batch_ids = batch_ids[:budget]
        if not batch_ids:
            break

        int_ids = [int(x) for x in batch_ids]
        status, headers, data = http_json(
            f"{MDBLIST_BASE}/tmdb/{kind}?apikey={urllib.parse.quote(mdblist_key)}",
            data={"ids": int_ids},
            timeout=30,
        )
        if status == 429:
            print("Hit HTTP 429 rate limit; stopping cleanly for today.")
            break
        if status != 200 or not isinstance(data, list):
            print(f"WARN: MDBList {kind} batch returned HTTP {status}; skipping.")
            continue

        target_ratings = ratings_m if kind == "movie" else ratings_t
        target_meta = meta_m if kind == "movie" else meta_t
        target_miss = miss_m if kind == "movie" else miss_t

        resolved: set[str] = set()
        for obj in data:
            if not isinstance(obj, dict):
                continue
            ids = obj.get("ids")
            if not isinstance(ids, dict):
                continue
            tid = ids.get("tmdb")
            if not isinstance(tid, int):
                continue
            sid = str(tid)
            entry = compute_entry(obj)
            if entry is not None:
                target_ratings[sid] = entry
                target_meta[sid] = now
                target_miss.pop(sid, None)
                resolved.add(sid)
                updated_count += 1

        for sid in batch_ids:
            if sid not in resolved:
                target_miss[sid] = now

        batches_done += 1

        # Measure real quota delta after the very first batch to self-adapt!
        if batches_done == 1 and quota is not None:
            after = mdblist_user_quota(mdblist_key)
            if after is not None:
                delta = after[0] - used
                print(f"First batch ({len(batch_ids)} items) consumed {delta} MDBList request(s).")
                if delta >= max(2, len(batch_ids) // 2):
                    cost_per_item = True
                remaining = max(0, after[1] - after[0])
                budget = max(0, remaining - RESERVE_DAILY_REQUESTS)
        else:
            hdr_rem = headers.get("x-ratelimit-remaining")
            if hdr_rem and hdr_rem.isdigit():
                budget = max(0, int(hdr_rem) - RESERVE_DAILY_REQUESTS)
            else:
                budget -= len(batch_ids) if cost_per_item else 1

        time.sleep(0.35)

    print(
        f"Run complete: {batches_done} batches, {updated_count} titles updated. "
        f"Cache totals: {len(ratings_m)} movies + {len(ratings_t)} shows = {len(ratings_m) + len(ratings_t)} total."
    )

    if updated_count > 0 or not RATINGS_PATH.exists():
        ratings_doc["v"] = 1
        ratings_doc["updated"] = now
        save_gz_json(RATINGS_PATH, ratings_doc)
        save_gz_json(META_PATH, meta_doc)
        print(f"Wrote {RATINGS_PATH.name} ({RATINGS_PATH.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
