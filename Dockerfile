# These base images are able to be customized via build-args override
ARG BASE_IMG=ghcr.io/bsv-blockchain/teranode-base:build-latest
ARG RUN_IMG=ghcr.io/bsv-blockchain/teranode-base:run-latest

# Enter the build environment
FROM ${BASE_IMG}
ARG GIT_VERSION
ARG GIT_COMMIT
ARG GIT_SHA
ARG TARGETOS
ARG TARGETARCH
ARG BUILD_JOBS=32
ARG TXMETA_SMALL_TAG=false

# Download all the go dependecies so Docker can cache them if the go.mod and go.sum files are not changed
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code from the current directory to the working directory inside the container
# This is safe as we have a comprehensive .dockerignore file that excludes sensitive data
# Only source code, configuration files, and necessary build files are included
COPY . /app

# CGO is required for BDK
ENV CGO_ENABLED=1

# Display the Git SHA for the build (if any)
RUN echo "Building Git SHA: ${GIT_SHA}"

# Build with $BUILD_JOBS parallel jobs
RUN if [ "$TXMETA_SMALL_TAG" = "true" ]; then \
      CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} TXMETA_SMALL_TAG=true GIT_VERSION="${GIT_VERSION}" GIT_COMMIT="${GIT_COMMIT}" GIT_SHA="${GIT_SHA}" make build -j ${BUILD_JOBS}; \
    else \
      CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} GIT_VERSION="${GIT_VERSION}" GIT_COMMIT="${GIT_COMMIT}" GIT_SHA="${GIT_SHA}" make build -j ${BUILD_JOBS}; \
    fi

# Build teranode-cli
RUN CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} GIT_VERSION="${GIT_VERSION}" GIT_COMMIT="${GIT_COMMIT}" GIT_SHA="${GIT_SHA}" make build-teranode-cli

# This could be run in the ${BASE_IMG} so we don't have to do it on every build, but it's not a big deal and this is pretty quick
ENV GOPATH=/go
RUN go install github.com/go-delve/delve/cmd/dlv@latest

# RUN_IMG should be overritten by --build-args
FROM ${RUN_IMG}

WORKDIR /app

COPY --from=0 /app/teranode.run ./teranode.run
COPY --from=0 /app/teranode-cli ./teranode-cli
COPY --from=0 /app/compose/wait.sh /app/wait.sh
COPY --from=0 /go/bin/dlv .
COPY --from=0 /app/settings.conf .

RUN chmod +x ./wait.sh

ENV LD_LIBRARY_PATH=/app:${LD_LIBRARY_PATH}
ENV PATH=/app:$PATH

# Set GOGC=200 to reduce GC aggressiveness (default is 100)
# Higher values trade memory for less frequent GC pauses
# Can be overridden at deployment time via docker-compose or Kubernetes
ENV GOGC=200

# Set the entrypoint to the library
ENTRYPOINT ["./teranode.run"]
#ENTRYPOINT ["./dlv", "--listen=:4040", "--continue", "--accept-multiclient", "--headless=true", "--api-version=2", "exec", "./teranode.run", "--"]
