.PHONY: test test-integration up down logs smoke

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
