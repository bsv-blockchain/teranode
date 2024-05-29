# Name of the binary to build
BINARY_NAME=kafkapartitions

# Default target executed when no arguments are given to make.
default: build

# This target builds your binary if main.go has changed.
build: $(BINARY_NAME)

$(BINARY_NAME): main.go
	@echo "Building..."
	@go build -o $(BINARY_NAME) main.go

# Target to clean up the built binary
clean:
	@echo "Cleaning..."
	@rm $(BINARY_NAME)

.PHONY: default build clean