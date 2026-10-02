# Architecture

```mermaid
flowchart LR
    Client([Client]) --> LB[nginx<br/>round-robin]
    LB --> API1[api #1]
    LB --> API2[api #2]

    subgraph API["api (stateless, N replicas)"]
        direction TB
        H[Handlers<br/>create / redirect / stats]
        SVC[link.Service<br/>cache-aside + singleflight]
        IDG["idgen: claim block → scramble → base62"]
        ASYNC["events.Async<br/>bounded queue, drops on overflow"]
        H --> SVC
        SVC --> IDG
        H -->|Emit click| ASYNC
    end
    API1 -.-> API
    API2 -.-> API

    Redis[(Redis<br/>slug cache · negative cache · token buckets)]
    PG[(PostgreSQL<br/>links · api_keys · id_sequences · clicks)]
    Kafka[[Kafka: click-events<br/>3 partitions, key = slug]]
    subgraph C["consumer (consumer group)"]
        B[batch by size / time]
    end

    SVC <-->|GET / SET| Redis
    SVC -->|miss| PG
    IDG -->|claim 1000 IDs per round trip| PG
    H -->|rate limit: Lua token bucket| Redis
    ASYNC -->|batched produce| Kafka
    Kafka --> B -->|"INSERT … ON CONFLICT DO NOTHING<br/>then commit offsets"| PG
    H -->|stats query| PG
```

## Request paths

**Redirect `GET /{slug}`**: Redis hit → 302. Miss → one coalesced Postgres read → fill Redis → 302.
The click is handed to an in-memory queue and the response is sent; Kafka is never on the critical path.

**Create `POST /v1/links`**: authenticate (hashed key lookup) → token bucket → validate →
slug from local ID block (or custom alias) → insert into Postgres.

**Analytics**: API → Kafka → consumer → `clicks` table (idempotent on `event_id`) → `GET /v1/links/{slug}/stats`.

| Package | Role |
|---|---|
| `internal/idgen` | Base62, Feistel scrambler, block allocator |
| `internal/link` | Domain model, service (create/resolve), cache interface |
| `internal/cache` | Redis cache-aside implementation |
| `internal/ratelimit` | Redis Lua token bucket |
| `internal/events` | `Publisher` interface, non-blocking `Async` wrapper; `events/kafka` backend |
| `internal/consumer` | `Source`/`Sink` interfaces, batching/commit loop, Kafka source |
| `internal/store/postgres` | Pool, block claiming, repos, batch click insert, stats |
| `internal/httpapi` | Handlers, auth middleware, docs endpoints |
| `api/` | Embedded OpenAPI spec |
