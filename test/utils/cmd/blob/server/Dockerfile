FROM golang:1.22 as builder

WORKDIR /app
COPY . .
WORKDIR /app/test/utils/cmd/blob/server

RUN go build -o blobserver main.go

# Copy built binary to the deployer image
FROM bitnami/minideb
WORKDIR /app
COPY --from=builder /app/test/utils/cmd/blob/server/blobserver .

EXPOSE 8080

CMD ["./blobserver"]
