FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /kiwifs .

FROM alpine:3.20

RUN apk add --no-cache git ca-certificates

COPY --from=builder /kiwifs /usr/local/bin/kiwifs

EXPOSE 3333

VOLUME ["/data"]

ENTRYPOINT ["kiwifs"]
CMD ["serve", "--root", "/data", "--port", "3333", "--host", "0.0.0.0"]
