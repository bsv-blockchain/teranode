# Set the base image
FROM golang:1.20.3-bullseye

RUN apt-get update && apt-get install -y vim htop curl wget lsof iputils-ping telnet net-tools iptables

# Set the working directory inside the container
WORKDIR /app

# Copy the source code from the current directory to the working directory inside the container
COPY . .


# Build the Go library
RUN go build -o ubsv.run main.go

# Build TX Blaster
RUN go build -o blaster.run ./cmd/txblaster/

# Set the entrypoint to the library
ENTRYPOINT ["./ubsv.run"]
