# These base images are able to be customized via build-args override
ARG BASE_IMG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-base:build-latest
ARG RUN_IMG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-base:run-latest

# Enter the build environment
FROM ${BASE_IMG}
ARG GITHUB_SHA
ARG TARGETOS
ARG TARGETARCH
ARG BUILD_JOBS=32
ARG TXMETA_SMALL_TAG=false

# Download all node dependencies for the dashboard, so Docker can cache them if the package.json and package-lock.json files are not changed
WORKDIR /app/ui/dashboard

COPY package.json package-lock.json ./
# Run npm install with --ignore-scripts to prevent execution of potentially malicious scripts
RUN npm install --ignore-scripts && npx node-prune

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
RUN echo "Building Git SHA: ${GITHUB_SHA}"

# Build with $BUILD_JOBS parallel jobs
RUN if [ "$TXMETA_SMALL_TAG" = "true" ]; then \
      TXMETA_SMALL_TAG=true make build -j ${BUILD_JOBS}; \
    else \
      make build -j ${BUILD_JOBS}; \
    fi

# Build teranode-cli
RUN make build-teranode-cli

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
COPY --from=0 /app/settings_local.conf .
COPY --from=0 /app/settings.conf .

RUN chmod +x ./wait.sh

ENV LD_LIBRARY_PATH=/app:${LD_LIBRARY_PATH}
ENV PATH=/app:$PATH

# Set the entrypoint to the library
ENTRYPOINT ["./teranode.run"]
#ENTRYPOINT ["./dlv", "--listen=:4040", "--continue", "--accept-multiclient", "--headless=true", "--api-version=2", "exec", "./teranode.run", "--"]
