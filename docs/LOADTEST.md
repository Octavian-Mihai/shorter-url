# Load test results

Workload (`loadtest/mixed.js`, [k6](https://k6.io)): constant-arrival-rate, **100 redirects : 1 create**,
plus ~2% unknown-slug lookups (negative cache). Slug popularity is skewed (u³ → ~50% of traffic on the
hottest ~13% of 500 links). Redirects are *not* followed; we measure our own 302.

Run it: `make loadtest RATE=1500 DURATION=40s` (watch Grafana at http://localhost:3000 meanwhile).
`make loadtest` raises the per-key creation rate limit for the run, then restores it.

## Result: 1,500 redirects/s for 40 s (+15 creates/s, +30 unknown/s)

| Metric | Result | Threshold |
|---|---|---|
| Redirect latency p50 / p95 | 0.4 ms / 1.6 ms | p95 < 50 ms, p99 < 100 ms |
| Redirect failures | 0 of 59,537 | < 0.1% |
| Create latency p95 | 5.7 ms | < 250 ms |
| Checks passed | 100% (61,339) | > 99.9% |

## Result: analytics are lossless at that rate

Controlled run, 1,000 redirects/s for 20 s:

| | |
|---|---|
| Click events published by the API | 20,002 |
| Rows added to `clicks` | 20,002 |
| Events dropped / failed | 0 / 0 |
| Consumer lag afterwards | 0 |

## Read these numbers with care

- Everything ran on one laptop under Docker Desktop: k6, nginx, 2 API replicas, Postgres, Redis, Kafka, the
  consumer, Prometheus and Grafana share the same CPUs. It shows the design is sound and where it stands;
  it is **not** a capacity benchmark. Absolute figures will differ on dedicated hardware.
- The hot set (500 links) fits entirely in Redis, so almost all redirects are cache hits. A cold-cache or
  much larger keyspace run would show the Postgres-bound path.
- 1,500/s was chosen to be comfortably sustainable here, not to find the limit. To find it, raise `RATE`
  until `http_req_duration` p99 or `shortener_click_events_dropped_total` starts moving.
