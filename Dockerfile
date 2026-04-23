# Stage 1: Build the web UI with Node.
FROM node:22-alpine AS ui

WORKDIR /ui
COPY ui/package.json ui/package-lock.json* ./
RUN npm install --no-audit --no-fund --loglevel=error
COPY ui ./
RUN npm run build

# Stage 2: Build the Go binary with the UI assets embedded.
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Drop whatever dist shipped in the repo and copy in the freshly-built UI.
RUN rm -rf ui/dist
COPY --from=ui /ui/dist ./ui/dist
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /kiwifs .

# Stage 3: Minimal runtime.
FROM alpine:3.20

RUN apk add --no-cache git ca-certificates

COPY --from=builder /kiwifs /usr/local/bin/kiwifs

EXPOSE 3333

VOLUME ["/data"]

ENTRYPOINT ["kiwifs"]
CMD ["serve", "--root", "/data", "--port", "3333", "--host", "0.0.0.0", "--search", "sqlite"]
