# Build Stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependency manifests
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build API binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /api-server ./cmd/api

# Final Stage
FROM alpine:3.20

RUN apk add --no-ca-certificates ca-certificates tzdata

WORKDIR /app

COPY --from=builder /api-server /app/api-server

EXPOSE 8080

CMD ["/app/api-server"]
