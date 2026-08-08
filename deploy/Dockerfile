# Build stage.
FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies are copied and downloaded first so a source-only change does not
# invalidate the module cache — the difference between a 3-second rebuild and a
# 60-second one during the chaos/load iteration loop.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

# CGO off so the binary runs on a scratch-like base with no libc to match.
# Symbols stripped: nothing here is debugged by attaching to the container.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway

# Runtime stage.
FROM alpine:3.20

# wget is used by the Compose healthcheck; ca-certificates is habit rather than
# necessity here, since nothing leaves localhost.
RUN apk add --no-cache ca-certificates wget

# Run unprivileged. A gateway that only needs a socket has no business as root.
RUN addgroup -S beacon && adduser -S -G beacon beacon

WORKDIR /app
COPY --from=build /out/gateway /app/gateway
COPY web/ /app/web/

USER beacon
EXPOSE 8080

ENV BEACON_HTTP_ADDR=":8080" \
    BEACON_WEB_DIR="/app/web"

ENTRYPOINT ["/app/gateway"]
