# Set the base image
FROM golang:1.20.3-bullseye

RUN apt-get update && apt-get install -y vim htop curl wget lsof iputils-ping telnet net-tools iptables

# Set the working directory inside the container
WORKDIR /app

# Copy the source code from the current directory to the working directory inside the container
COPY . .


# Build the Go library
RUN CGO_ENABLED=1 go build -o ubsv.run main.go --trimpath -ldflags="-s -w -X main.commit=${GITHUB_SHA} -X main.version=MANUAL"

# Build TX Blaster
RUN CGO_ENABLED=1 go build -o blaster.run ./cmd/txblaster/ --trimpath -ldflags="-s -w -X main.commit=${GITHUB_SHA} -X main.version=MANUAL"

# Set the entrypoint to the library
ENTRYPOINT ["./ubsv.run"]
