# Set the base image
FROM --platform=linux/amd64 ubuntu:latest
ARG GITHUB_SHA

RUN apt update && apt install wget build-essential -y
RUN wget -q https://github.com/apple/foundationdb/releases/download/7.2.5/foundationdb-clients_7.2.5-1_amd64.deb
RUN dpkg -i foundationdb-clients_7.2.5-1_amd64.deb

RUN wget -q https://go.dev/dl/go1.20.3.linux-amd64.tar.gz
RUN rm -rf /usr/local/go && tar -C /usr/local -xzf go1.20.3.linux-amd64.tar.gz
ENV PATH=${PATH}:/usr/local/go/bin

RUN mkdir /app
# Copy the source code from the current directory to the working directory inside the container
COPY . /app

# Set the working directory inside the container
WORKDIR /app

ENV CGO_ENABLED=1
RUN echo "${GITHUB_SHA}"

RUN go get -u github.com/apple/foundationdb/bindings/go/src/fdb@release-7.2

# Build the Go library
RUN go build -tags aerospike,foundationdb --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o ubsv.run main.go

# Build TX Blaster
RUN go build --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o blaster.run ./cmd/txblaster/

# Install Delve debugger
RUN go install -ldflags "-s -w -extldflags ' -static'" github.com/go-delve/delve/cmd/dlv@latest


FROM --platform=linux/amd64 ubuntu:latest

RUN apt update && apt install -y vim htop curl wget lsof iputils-ping net-tools dnsutils

WORKDIR /app

COPY --from=0 /app/ubsv.run .
COPY --from=0 /app/settings_local.conf .
COPY --from=0 /app/settings.conf .
COPY --from=0 /app/blaster.run .
COPY --from=0 /root/go/bin/dlv .
COPY --from=0 /usr/lib/libfdb_c.so .

ENV LD_LIBRARY_PATH=.

# Set the entrypoint to the library
# ENTRYPOINT ["./ubsv.run"]

# Wrap the ubsv.run in the Delve debugger
ENTRYPOINT ["./dlv", "--listen=:4040", "--continue", "--accept-multiclient", "--headless=true", "--api-version=2", "exec", "./ubsv.run", "--"]
