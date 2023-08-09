# Set the base image
FROM --platform=linux/amd64 golang:1.20.7-bullseye
ARG GITHUB_SHA

RUN apt update && apt install -y wget curl build-essential libsecp256k1-dev

# Set the working directory inside the container
WORKDIR /app

# Copy the source code from the current directory to the working directory inside the container
COPY . /app

ENV CGO_ENABLED=1
RUN echo "Building git sha: ${GITHUB_SHA}"

# Build the Go libraries of the project
RUN make build

# Install Delve debugger
RUN go install github.com/go-delve/delve/cmd/dlv@latest


FROM --platform=linux/amd64 ubuntu:latest

RUN apt update && apt install -y vim htop curl wget lsof iputils-ping net-tools dnsutils postgresql telnet

WORKDIR /app

COPY --from=0 /app/ubsv.run .
COPY --from=0 /app/settings_local.conf .
COPY --from=0 /app/certs .
COPY --from=0 /app/settings.conf .
COPY --from=0 /app/blaster.run .
COPY --from=0 /app/status.run .
COPY --from=0 /go/bin/dlv .
COPY --from=0 /usr/lib/x86_64-linux-gnu/libsecp256k1.so.0.0.0 .

RUN ln -s libsecp256k1.so.0.0.0 libsecp256k1.so.0 && \
  ln -s libsecp256k1.so.0.0.0 libsecp256k1.so

ENV LD_LIBRARY_PATH=.

# Set the entrypoint to the library
ENTRYPOINT ["./ubsv.run"]
#ENTRYPOINT ["./dlv", "--listen=:4040", "--continue", "--accept-multiclient", "--headless=true", "--api-version=2", "exec", "./ubsv.run", "--"]
