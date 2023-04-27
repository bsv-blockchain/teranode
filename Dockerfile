# Set the base image
FROM golang:alpine
ARG GITHUB_SHA

RUN apk update && apk add build-base

RUN mkdir /app
# Copy the source code from the current directory to the working directory inside the container
COPY . /app

# Set the working directory inside the container
WORKDIR /app

ENV CGO_ENABLED=1
RUN echo "${GITHUB_SHA}"

RUN go get -u github.com/apple/foundationdb/bindings/go/src/fdb@release-7.2

# Build the Go library
RUN go build -tags aerospike --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o ubsv.run main.go

# Build TX Blaster
RUN go build --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o blaster.run ./cmd/txblaster/

# Install Delve debugger
RUN go install -ldflags "-s -w -extldflags ' -static'" github.com/go-delve/delve/cmd/dlv@latest


FROM alpine:latest

RUN apk update && apk add vim htop curl wget lsof netcat-openbsd iputils bind-tools

WORKDIR /app

COPY --from=0 /app/ubsv.run .
COPY --from=0 /app/settings_local.conf .
COPY --from=0 /app/settings.conf .
COPY --from=0 /app/blaster.run .
COPY --from=0 /go/bin/dlv .

# Set the entrypoint to the library
# ENTRYPOINT ["./ubsv.run"]

# Wrap the ubsv.run in the Delve debugger
ENTRYPOINT ["./dlv", "--listen=:4040", "--continue", "--accept-multiclient", "--headless=true", "--api-version=2", "exec", "./ubsv.run", "--"]
