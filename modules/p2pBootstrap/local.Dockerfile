FROM 434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:base-build-v2
ARG GITHUB_SHA


# Download all the go dependecies so Docker can cache them if the go.mod and go.sum files are not changed
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code from the current directory to the working directory inside the container
COPY . /app

ENV CGO_ENABLED=1
RUN echo "Building git sha: ${GITHUB_SHA}"

# Build the Go libraries of the project
RUN go build .

FROM 434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:base-run-v2

RUN adduser --disabled-password --gecos '' appuser
USER appuser

WORKDIR /app

COPY --from=0 /go/bin/dlv .

COPY --from=0 /app/settings.conf .
COPY --from=0 /app/settings_local.conf .
COPY --from=0 /app/p2pBootstrap .

# Set the entrypoint to the library
ENTRYPOINT ["./p2pBootstrap"]
