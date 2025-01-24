FROM golang:1.22 as builder

WORKDIR /app
COPY . .
WORKDIR /app/test/utils/cmd/tstore

RUN go build -o tstore main.go

# Copy built binary to the deployer image
FROM bitnami/minideb
WORKDIR /app
COPY --from=builder /app/test/utils/cmd/tstore/tstore .

EXPOSE 50051

CMD ["./tstore"]
