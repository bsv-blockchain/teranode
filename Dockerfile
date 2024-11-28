# These values should be overwritten by buildx --build-args and replaced with cluster base/run ID from repo
ARG BASE_IMG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:base-build-latest
ARG RUN_IMG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:base-run-latest
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

# This could be run in the ${BASE_IMG} so we don't have to do it on every build, but it's not a big deal and this is pretty quick
ENV GOPATH=/go
RUN go install github.com/go-delve/delve/cmd/dlv@latest

# RUN_IMG should be overritten by --build-args
FROM --platform=linux/amd64 ${RUN_IMG} AS linux-amd64
WORKDIR /app
COPY --from=0 /app/ubsv.run ./ubsv.run

# Don't do anything different for ARM64 (for now)
FROM --platform=linux/arm64 ${RUN_IMG} AS linux-arm64
WORKDIR /app
COPY --from=0 /app/ubsv.run ./ubsv.run

ENV TARGETARCH=${TARGETARCH}
ENV TARGETOS=${TARGETOS}
FROM ${TARGETOS}-${TARGETARCH}

WORKDIR /app

COPY --from=0 /go/bin/dlv .

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
RUN ln -s ubsv.run s3inventoryintegrity.run
RUN ln -s ubsv.run txblockidcheck.run
RUN ln -s ubsv.run aerospike_reader.run
RUN ln -s ubsv.run utxopersister.run
RUN ln -s ubsv.run seeder.run
RUN ln -s ubsv.run bitcoin2utxoset.run
RUN ln -s ubsv.run settings.run
RUN ln -s ubsv.run unspend.run
RUN ln -s ubsv.run recovertx.run

ENV LD_LIBRARY_PATH=/app:$LD_LIBRARY_PATH
ENV PATH=/app:$PATH

# Set the entrypoint to the library
ENTRYPOINT ["./ubsv.run"]
#ENTRYPOINT ["./dlv", "--listen=:4040", "--continue", "--accept-multiclient", "--headless=true", "--api-version=2", "exec", "./ubsv.run", "--"]
