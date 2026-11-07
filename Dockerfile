# Build Stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependency manifests
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build API and Worker binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /api-server ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /worker-service ./cmd/worker

# Final Stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /api-server /app/api-server
COPY --from=builder /worker-service /app/worker-service

EXPOSE 8080

CMD ["/app/api-server"]
