.PHONY: test test-integration up down logs smoke loadtest loadtest-smoke

test:
	go test -race ./...

# Needs: docker run -d --name pg -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=t -p 55432:5432 postgres:16-alpine
#        docker run -d --name kafka -p 9092:9092 apache/kafka:3.8.0
test-integration:
	TEST_DATABASE_URL='postgres://postgres:pw@localhost:55432/t?sslmode=disable' \
	TEST_KAFKA_BROKERS=localhost:9092 go test -tags integration -race -count=1 ./...

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f api consumer

smoke:
	./scripts/smoke.sh

RATE ?= 1500
DURATION ?= 60s
K6 = docker run --rm --network shortener_default -v $(PWD)/loadtest:/loadtest -e BASE=http://nginx:8080 grafana/k6:0.54.0

# Raises the per-key creation limit (it exists to protect production, not benchmarks),
# then runs the mixed read-heavy workload. Watch it live in Grafana: http://localhost:3000
loadtest:
	RATE_LIMIT_BURST=1000000 RATE_LIMIT_PER_MINUTE=100000000 docker compose up -d --force-recreate api
	@sleep 8
	$(K6) run -e RATE=$(RATE) -e DURATION=$(DURATION) /loadtest/mixed.js
	docker compose up -d --force-recreate api

loadtest-smoke:
	$(K6) run /loadtest/smoke.js
