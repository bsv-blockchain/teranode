# These values should be overwritten by buildx --build-args and replaced with cluster base/run ID from repo
ARG BASE_IMG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode:base-build-866edae
ARG RUN_IMG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode:base-run-866edae
ARG PLATFORM_ARCH=linux/arm64
FROM ${BASE_IMG}
ARG GITHUB_SHA
ARG TARGETOS
ARG TARGETARCH
ARG PLATFORM_ARCH

# Download all node dependencies for the dashboard, so Docker can cache them if the package.json and package-lock.json files are not changed
WORKDIR /app/ui/dashboard

COPY package.json package-lock.json ./
RUN npm install && npx node-prune

# Download all the go dependecies so Docker can cache them if the go.mod and go.sum files are not changed
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code from the current directory to the working directory inside the container
COPY . /app

ENV CGO_ENABLED=1
RUN echo "Building git sha: ${GITHUB_SHA}"

RUN RACE=true TXMETA_SMALL_TAG=true make build -j 32
RUN RACE=true TXMETA_SMALL_TAG=true make build-tx-blaster -j 32

# RUN_IMG should be overritten by --build-args
FROM --platform=linux/amd64 ${RUN_IMG} AS linux-amd64
WORKDIR /app
COPY --from=0 /app/teranode.run ./teranode.run
COPY --from=0 /app/blaster.run ./blaster.run
COPY --from=0 /app/wait.sh /app/wait.sh

# Don't do anything different for ARM64 (for now)
FROM --platform=linux/arm64 ${RUN_IMG} AS linux-arm64
WORKDIR /app
COPY --from=0 /app/teranode.run ./teranode.run
COPY --from=0 /app/blaster.run ./blaster.run
COPY --from=0 /app/wait.sh /app/wait.sh

ENV TARGETARCH=${TARGETARCH}
ENV TARGETOS=${TARGETOS}
FROM ${TARGETOS}-${TARGETARCH}

# Add a build argument to control installation of debugging tools
ARG INSTALL_DEBUG_TOOLS=true

# Install necessary tools for debugging and troubleshooting if INSTALL_DEBUG_TOOLS is true
RUN if [ "$INSTALL_DEBUG_TOOLS" = "true" ]; then \
  apt-get update && \
  apt-get install -y vim htop curl lsof iputils-ping net-tools netcat-openbsd dnsutils postgresql telnet && \
  rm -rf /var/lib/apt/lists/*; \
  else \
  echo "Skipping installation of debugging tools"; \
  fi

WORKDIR /app

# COPY --from=0 /go/bin/dlv .

COPY --from=0 /app/settings_local.conf .
# COPY --from=0 /app/certs /app/certs
COPY --from=0 /app/settings.conf .

# RUN ln -s teranode.run chainintegrity.run
# RUN ln -s teranode.run blaster.run
# RUN ln -s teranode.run propagationblaster.run
# RUN ln -s teranode.run blockassemblyblaster.run
# RUN ln -s teranode.run utxostoreblaster.run
# RUN ln -s teranode.run aerospiketest.run
# RUN ln -s teranode.run s3blaster.run
RUN ln -s teranode.run miner.run

ENV LD_LIBRARY_PATH=/app:$LD_LIBRARY_PATH

# Set the entrypoint to the library
#ENTRYPOINT ["./dlv", "--listen=:4040", "--continue", "--accept-multiclient", "--headless=true", "--api-version=2", "exec", "./teranode.run", "--"]
