## Quickstart

1. **Generate the compose**
    ```bash
    go run main.go generate --numNodes=4 # (default 3)
    ```
   This command generates the compose.

2. **Run the compose**
    ```bash
    go run main.go run --up
    ```
   This command runs the compose.

3. **Stop and Clean**
    ```bash
    go run main.go run --clean
    ```
   This command stops and cleans the compose.
