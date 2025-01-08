# These values should be overwritten by buildx --build-args and replaced with cluster base/run ID from repo
ARG BASE_IMG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode:base-build-db1a6f0
ARG RUN_IMG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode:base-run-db1a6f0
ARG PLATFORM_ARCH=linux/arm64

# Build stage
FROM ${BASE_IMG}
ARG GITHUB_SHA
ARG TARGETOS
ARG TARGETARCH
ARG PLATFORM_ARCH

# Download all node dependencies for the dashboard, so Docker can cache them if the package.json and package-lock.json files are not changed
WORKDIR /app/ui/dashboard
COPY package.json package-lock.json ./
RUN npm install && npx node-prune

# Download all the go dependencies so Docker can cache them if the go.mod and go.sum files are not changed
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code from the current directory to the working directory inside the container
COPY . /app

ENV CGO_ENABLED=1
RUN echo "Building git sha: ${GITHUB_SHA}"

# Add these lines to verify Go module setup
RUN go mod verify
RUN go mod tidy

# Build main binaries
RUN RACE=true TXMETA_SMALL_TAG=true make build-teranode-ci -j 32

RUN RACE=true TXMETA_SMALL_TAG=true make build-tx-blaster -j 32

# Build test binaries
RUN RACE=true TXMETA_SMALL_TAG=true make buildtest -j 32

ENV GOPATH=/go
RUN go install github.com/go-delve/delve/cmd/dlv@latest

# AMD64 final image
FROM --platform=linux/amd64 ${RUN_IMG} AS linux-amd64
WORKDIR /app
COPY --from=0 /app/teranode.run ./teranode.run
COPY --from=0 /app/blaster.run ./blaster.run
COPY --from=0 /app/wait.sh /app/wait.sh
COPY --from=0 /app/test/build/tne.test ./test/tne/tne.test
COPY --from=0 /app/test/build/tna.test ./test/tna/tna.test
COPY --from=0 /app/test/build/smoke.test ./test/smoke/smoke.test

# ARM64 final image
FROM --platform=linux/arm64 ${RUN_IMG} AS linux-arm64
WORKDIR /app
COPY --from=0 /app/teranode.run ./teranode.run
COPY --from=0 /app/blaster.run ./blaster.run
COPY --from=0 /app/wait.sh /app/wait.sh
COPY --from=0 /app/test/build/tne.test ./test/tne/tne.test
COPY --from=0 /app/test/build/tna.test ./test/tna/tna.test
COPY --from=0 /app/test/build/smoke.test ./test/smoke/smoke.test

ENV TARGETARCH=${TARGETARCH}
ENV TARGETOS=${TARGETOS}

FROM ${TARGETOS}-${TARGETARCH}

RUN apt update && \
  apt install -y vim htop curl lsof iputils-ping net-tools netcat-openbsd dnsutils postgresql telnet && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=0 /go/bin/dlv .

COPY --from=0 /app/settings_local.conf .
COPY --from=0 /app/certs /app/certs
COPY --from=0 /app/settings.conf .

RUN ln -s teranode.run miner.run

ENV LD_LIBRARY_PATH=/app:$LD_LIBRARY_PATH

# Set the entrypoint to the library
#ENTRYPOINT ["./dlv", "--listen=:4040", "--continue", "--accept-multiclient", "--headless=true", "--api-version=2", "exec", "./teranode.run", "--"]
