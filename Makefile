.PHONY: up down test

up:
	docker-compose up -d

down:
	docker-compose down

test:
	go test -v ./test/...

test-race:
	go test -v -race ./test/...

bench:
	go test -bench=. -benchmem ./benchmark/