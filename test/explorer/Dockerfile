# Use a base Linux image
FROM ubuntu:latest

# Install required dependencies
RUN apt-get update && apt-get install -y \
    git \
    golang-go \
    postgresql \
    postgresql-contrib \
    python3 \
    python3-pip \
    python3-venv \
    && ln -s /usr/bin/python3.9 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

USER root
# Install Jupyter notebook
RUN python3 -m venv /venv
RUN /venv/bin/pip install --upgrade pip
RUN /venv/bin/pip install jupyter

# Install Gophernotes
RUN go install github.com/gopherdata/gophernotes@v0.7.5 && \
    mkdir -p /root/.local/share/jupyter/kernels/gophernotes && \
    cd /root/.local/share/jupyter/kernels/gophernotes && \
    cp "$(go env GOPATH)"/pkg/mod/github.com/gopherdata/gophernotes@v0.7.5/kernel/* . && \
    chmod +w ./kernel.json && \
    sed "s|gophernotes|$(go env GOPATH)/bin/gophernotes|" < kernel.json.in > kernel.json

ENV GOPATH /go

# Expose port for Jupyter notebook
EXPOSE 8888

WORKDIR /tests
COPY ./scripts /tests
COPY ./entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Start Jupyter notebook
CMD ["/usr/local/bin/entrypoint.sh"]
