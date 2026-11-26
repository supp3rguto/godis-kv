.PHONY: build run test bench clean docker-up docker-down

BINARY_SERVER=bin/godis-server
BINARY_BENCH=bin/godis-bench

build:
	@mkdir -p bin
	go build -ldflags="-s -w" -o $(BINARY_SERVER) ./cmd/server
	go build -ldflags="-s -w" -o $(BINARY_BENCH) ./cmd/bench

run:
	go run ./cmd/server -addr :6379 -aof data/database.log

test:
	go test -race -v -cover ./...

bench:
	go run ./cmd/bench -addr localhost:6379 -workers 50 -requests 10000 -mode mixed

clean:
	rm -rf bin/

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down