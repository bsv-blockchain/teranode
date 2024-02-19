# Set the base image
FROM --platform=linux/amd64 golang:1.21.0-bullseye
ARG GITHUB_SHA

RUN apt update && apt install -y ca-certificates curl gnupg wget build-essential libsecp256k1-dev

# Add nodesource to apt sources
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg

# 18 is the latest LTS version of NodeJS
ENV NODE_MAJOR=18
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_$NODE_MAJOR.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list

RUN apt-get update && apt-get install -y nodejs


# Download all node dependencies for the dashboard, so Docker can cache them if the package.json and package-lock.json files are not changed
WORKDIR /app/ui/dashboard

COPY package.json package-lock.json ./
RUN npm install


# Download all the go dependecies so Docker can cache them if the go.mod and go.sum files are not changed
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Install Delve debugger
RUN go install github.com/go-delve/delve/cmd/dlv@latest


# Copy the source code from the current directory to the working directory inside the container
COPY . /app

ENV CGO_ENABLED=1
RUN echo "Building git sha: ${GITHUB_SHA}"


# Build the Go libraries of the project
# todo change to make build
RUN RACE=true make build-tx-blaster

FROM --platform=linux/amd64 debian:latest

RUN apt update && \
  apt install -y vim htop curl lsof iputils-ping net-tools dnsutils postgresql telnet && \
  rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=0 /go/bin/dlv .
COPY --from=0 /usr/lib/x86_64-linux-gnu/libsecp256k1.so.0.0.0 .

COPY --from=0 /app/settings_local.conf .
COPY --from=0 /app/certs /app/certs
COPY --from=0 /app/settings.conf .
COPY --from=0 /app/blaster.run .

RUN ln -s libsecp256k1.so.0.0.0 libsecp256k1.so.0 && \
  ln -s libsecp256k1.so.0.0.0 libsecp256k1.so

ENV LD_LIBRARY_PATH=.

# Set the entrypoint to the library
# ENTRYPOINT [ "tail", "-f", "/dev/null" ]
# ENTRYPOINT [ "./blaster.run", "-workers=1", "-print=1", "-profile=:9092", "-log=1", "-limit=100", "-e2e", "-iterations=10"]
#ENTRYPOINT ["./dlv", "--listen=:4040", "--continue", "--accept-multiclient", "--headless=true", "--api-version=2", "exec", "./ubsv.run", "--"]
# ENTRYPOINT [ "./blaster.run", "--quic=true", "-workers=10"]
