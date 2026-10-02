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

## Run-to-run variability (added after re-running)

The 1.6 ms p95 above is the best case, not a guarantee. Re-running the same workload (1,200/s for 60 s) three more
times on the same laptop gave:

| Run | Redirect p50 | p95 | max | Failures |
|---|---|---|---|---|
| 1 (Grafana open in a browser) | 0.54 ms | 44 ms | 926 ms | 7 of 713 creates: `connection reset` |
| 2 | 0.50 ms | 36 ms | 813 ms | 0 |
| 3 | 0.42 ms | 1.6 ms | 48 ms | 0 |

The **median is stable (~0.4–0.5 ms)**; the **tail is not** (p95 from 1.6 ms to 44 ms between identical runs). No container
restarted, none was OOM-killed, and no component logged an error. The resets in run 1 happened in a roughly one-second burst
and did not recur in the next two runs. The likeliest explanation is CPU contention on a laptop running ~10 containers plus
a browser, but that is a hypothesis: I did not profile it. Treat tail latency here as "noisy", and do not read the best run as a
guarantee. A dedicated load generator and host would be needed to say anything firm about p99.

## Read these numbers with care

- Everything ran on one laptop under Docker Desktop: k6, nginx, 2 API replicas, Postgres, Redis, Kafka, the
  consumer, Prometheus and Grafana share the same CPUs. It shows the design is sound and where it stands;
  it is **not** a capacity benchmark. Absolute figures will differ on dedicated hardware.
- The hot set (500 links) fits entirely in Redis, so almost all redirects are cache hits. A cold-cache or
  much larger keyspace run would show the Postgres-bound path.
- 1,500/s was chosen to be comfortably sustainable here, not to find the limit. To find it, raise `RATE`
  until `http_req_duration` p99 or `shortener_click_events_dropped_total` starts moving.
