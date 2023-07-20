# Set the base image
#FROM --platform=linux/amd64 ubuntu:focal
FROM --platform=linux/amd64 ubuntu:latest
ARG GITHUB_SHA

RUN apt update && apt install -y wget curl build-essential libsecp256k1-dev

#RUN wget -q https://github.com/apple/foundationdb/releases/download/7.2.5/foundationdb-clients_7.2.5-1_amd64.deb && \
#  dpkg -i foundationdb-clients_7.2.5-1_amd64.deb

RUN wget -q https://go.dev/dl/go1.20.5.linux-amd64.tar.gz && \
  tar -C /usr/local -xzf go1.20.5.linux-amd64.tar.gz

ENV PATH=${PATH}:/usr/local/go/bin
ENV GOPATH=/root/go

RUN mkdir /app
# Copy the source code from the current directory to the working directory inside the container
COPY . /app

# Set the working directory inside the container
WORKDIR /app

ENV CGO_ENABLED=1
RUN echo "${GITHUB_SHA}"

RUN go test -race -count=1 $(go list ./... | grep -v playground | grep -v poc)

RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.53.3
RUN $(go env GOPATH)/bin/golangci-lint run --skip-dirs p2p/wire

RUN go install honnef.co/go/tools/cmd/staticcheck@latest
RUN $(go env GOPATH)/bin/staticcheck ./...

# Build the Go library
#RUN go build -tags aerospike,foundationdb,native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o ubsv.run .
RUN go build -tags aerospike,native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o ubsv.run .

# Build TX Blaster
RUN go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o blaster.run ./cmd/txblaster/

# Install Delve debugger
RUN go install github.com/go-delve/delve/cmd/dlv@latest


FROM --platform=linux/amd64 ubuntu:latest

RUN apt update && apt install -y vim htop curl wget lsof iputils-ping net-tools dnsutils

WORKDIR /app

COPY --from=0 /app/ubsv.run .
COPY --from=0 /app/settings_local.conf .
COPY --from=0 /app/certs .
COPY --from=0 /app/settings.conf .
COPY --from=0 /app/blaster.run .
COPY --from=0 /root/go/bin/dlv .
#COPY --from=0 /usr/lib/libfdb_c.so .
COPY --from=0 /usr/lib/x86_64-linux-gnu/libsecp256k1.so.0.0.0 .

RUN ln -s libsecp256k1.so.0.0.0 libsecp256k1.so.0 && \
  ln -s libsecp256k1.so.0.0.0 libsecp256k1.so

ENV LD_LIBRARY_PATH=.

# Set the entrypoint to the library
# ENTRYPOINT ["./ubsv.run"]

# Wrap the ubsv.run in the Delve debugger
ENTRYPOINT ["./ubsv.run"]
