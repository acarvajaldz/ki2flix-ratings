# KI²flix Ratings Cache

Shared, pre-computed combined ratings cache (**IMDb + Rotten Tomatoes + TMDB** average on a 0.0–10.0 scale) updated daily via GitHub Actions for KI²flix TV clients.

## Format (`ratings.json.gz`)

```json
{
  "v": 1,
  "updated": 1764010000,
  "m": { "550": [85, 88, 81, 84] },
  "t": { "1399": [87, 92, 89, 84] }
}
```

- `m`: Movies keyed by TMDB numeric ID.
- `t`: TV shows keyed by TMDB numeric ID.
- Array values `[avg10, imdb10, rt, tmdb10]`:
  - `avg10`: Combined average × 10 (integer, e.g. `85` = `8.5`).
  - `imdb10`: IMDb score × 10 (integer or `null`).
  - `rt`: Rotten Tomatoes score `0..100` (critics preferred, audience fallback, or `null`).
  - `tmdb10`: TMDB score × 10 (integer or `null`).
