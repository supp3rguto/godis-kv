#1- build do binario estatico
FROM golang:alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/godis-server ./cmd/server

# 2- imagem final enxuta e sem root
FROM alpine:3.21

RUN adduser -D -H -s /sbin/nologin godis && \
    mkdir -p /app/data && \
    chown -R godis:godis /app

WORKDIR /app

COPY --from=builder /out/godis-server /app/godis-server

USER godis

EXPOSE 6379

ENTRYPOINT ["/app/godis-server"]
CMD ["-addr", ":6379", "-aof", "/app/data/database.log"]