# Architecture

> The repo currently contains the ID-generation package and configuration; the API and consumer entrypoints are stubs. This diagram shows the intended design, as defined by `internal/config` and `internal/idgen`.

```mermaid
flowchart LR
    Client([Client])
    subgraph API["cmd/api (HTTP :8080)"]
        RL[Rate limiter]
        H[Create / Redirect handlers]
        IDG["idgen: block allocator → scramble → base62"]
    end
    Redis[(Redis<br/>URL cache + negative cache)]
    PG[(PostgreSQL<br/>urls, api keys)]
    Kafka[[Kafka topic: click-events]]
    subgraph Consumer["cmd/consumer"]
        B[Batch + flush]
    end

    Client -->|POST /shorten| RL --> H
    H --> IDG
    IDG -->|reserve ID block| PG
    H -->|persist mapping| PG
    Client -->|GET /:code| H
    H -->|lookup| Redis
    Redis -.miss.-> PG
    H -->|publish click| Kafka
    Kafka --> B -->|batched writes| PG
```

| Component | Role |
|---|---|
| `internal/idgen` | Allocates ID blocks (`BLOCK_SIZE`), scrambles with a secret, encodes as base62 |
| `internal/config` | Env-based config (DB, Redis, Kafka, TTLs, rate limits) |
| `cmd/api` | Public HTTP API (stub) |
| `cmd/consumer` | Click-event consumer with batch flushing (stub) |
