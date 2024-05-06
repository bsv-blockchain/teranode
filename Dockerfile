# These values should be overwritten by buildx --build-args and replaced with cluster base/run ID from repo
ARG BASE_IMG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:base-build-v2
ARG RUN_IMG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:base-run-v2
ARG PLATFORM_ARCH=linux/amd64
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

RUN make build -j 32

# RUN_IMG should be overritten by --build-args
FROM --platform=linux/amd64 ${RUN_IMG} as linux-amd64
WORKDIR /app
COPY --from=0 /app/ubsv.run ./ubsv.run

# Don't do anything different for ARM64 (for now)
FROM --platform=linux/arm64 ${RUN_IMG} as linux-arm64
WORKDIR /app
COPY --from=0 /app/ubsv.run ./ubsv.run

ENV TARGETARCH=${TARGETARCH}
ENV TARGETOS=${TARGETOS}
FROM ${TARGETOS}-${TARGETARCH}

WORKDIR /app

COPY --from=0 /go/bin/dlv .
COPY --from=0 /usr/lib/*-linux-gnu/libsecp256k1.so.0.0.0 .

COPY --from=0 /app/settings_local.conf .
COPY --from=0 /app/certs /app/certs
COPY --from=0 /app/settings.conf .

RUN ln -s ubsv.run chainintegrity.run
RUN ln -s ubsv.run blaster.run
RUN ln -s ubsv.run propagationblaster.run
RUN ln -s ubsv.run blockassemblyblaster.run
RUN ln -s ubsv.run utxostoreblaster.run
RUN ln -s ubsv.run aerospiketest.run
RUN ln -s ubsv.run s3blaster.run
RUN ln -s ubsv.run filereader.run

RUN ln -s libsecp256k1.so.0.0.0 libsecp256k1.so.0 && \
  ln -s libsecp256k1.so.0.0.0 libsecp256k1.so

ENV LD_LIBRARY_PATH=.

# Set the entrypoint to the library
ENTRYPOINT ["./ubsv.run"]
#ENTRYPOINT ["./dlv", "--listen=:4040", "--continue", "--accept-multiclient", "--headless=true", "--api-version=2", "exec", "./ubsv.run", "--"]
