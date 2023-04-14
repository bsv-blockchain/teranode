# Set the base image
FROM golang:1.20.3-bullseye
ARG GITHUB_SHA

RUN apt-get update && apt-get install -y vim htop curl wget lsof iputils-ping telnet net-tools iptables

# Set the working directory inside the container
WORKDIR /app

# Copy the source code from the current directory to the working directory inside the container
COPY . .

ENV CGO_ENABLED=1
RUN echo "${GITHUB_SHA}"

# Build the Go library
RUN go build -o ubsv.run main.go --trimpath -ldflags="-s -w -X main.commit=${GITHUB_SHA} -X main.version=MANUAL"

# Build TX Blaster
RUN go build -o blaster.run ./cmd/txblaster/ --trimpath -ldflags="-s -w -X main.commit=${GITHUB_SHA} -X main.version=MANUAL"

# Set the entrypoint to the library
ENTRYPOINT ["./ubsv.run"]
