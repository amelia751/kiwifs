BINARY := kiwifs
ROOT   := ./knowledge
PORT   := 3333

.PHONY: build run test clean tidy

build:
	go build -o $(BINARY) .

run: build
	./$(BINARY) serve --root $(ROOT) --port $(PORT)

dev:
	go run . serve --root $(ROOT) --port $(PORT)

test:
	go test ./...

tidy:
	go mod tidy

clean:
	rm -f $(BINARY)
