BINARY := kiwifs
ROOT   := ./knowledge
PORT   := 3333

.PHONY: build run test clean tidy ui ui-install dev-ui

# Full build: bundle the UI first, then compile the Go binary so
# `//go:embed ui/dist` picks up the latest assets.
build: ui
	go build -o $(BINARY) .

# Build only the Go binary. Use this when the UI hasn't changed — the
# previously-built ui/dist is still embedded.
go-build:
	go build -o $(BINARY) .

# Build just the UI. Runs `npm install` lazily on first use.
ui:
	@if [ ! -d ui/node_modules ]; then $(MAKE) ui-install; fi
	cd ui && npm run build

ui-install:
	cd ui && npm install

run: build
	./$(BINARY) serve --root $(ROOT) --port $(PORT)

dev:
	go run . serve --root $(ROOT) --port $(PORT)

# Run the UI dev server against a running kiwifs on :3333. Useful when
# iterating on the UI without rebuilding the Go binary between edits.
dev-ui:
	cd ui && npm run dev

test:
	go test ./...

tidy:
	go mod tidy

clean:
	rm -f $(BINARY)
	rm -rf ui/dist/assets ui/dist/*.html ui/dist/favicon.svg 2>/dev/null || true
