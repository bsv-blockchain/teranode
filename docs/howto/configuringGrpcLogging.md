# How to Enable Additional gRPC Logs

By setting specific environment variables, you can obtain more detailed logs that provide insight into gRPC's internal workings, such as the client channel and round-robin load balancing. These logs can help in debugging and troubleshooting gRPC-related issues.

## Step-by-Step: Enabling gRPC Logs

## Step 1: Understanding gRPC Log Levels

By default, gRPC provides minimal logging. However, for debugging purposes, you can increase the verbosity of the logs by setting the environment variable `GRPC_VERBOSITY`. The available levels for `GRPC_VERBOSITY` are:

- `ERROR`: Logs only error messages.
- `INFO`: Logs informational messages (this is the default level).
- `DEBUG`: Logs detailed debug messages, useful for tracking detailed behaviors.

In addition to setting the verbosity, you can specify which gRPC components you want to trace by setting the `GRPC_TRACE` environment variable. Some of the commonly traced components include:

- `client_channel`: Provides logs related to the client-side gRPC channel.
- `round_robin`: Provides logs related to the round-robin load balancing mechanism.

## Step 2: Setting the Environment Variables

To enable additional logs when running the node, set the following environment variables:

```bash
export GRPC_VERBOSITY=debug
export GRPC_TRACE=client_channel,round_robin
```

Here’s what each of these does:

- `GRPC_VERBOSITY=debug`: This sets the log verbosity to `debug`, which provides detailed logging of gRPC operations.
- `GRPC_TRACE=client_channel,round_robin`: This specifies that logs should be generated for the `client_channel` and `round_robin` components of gRPC, which are useful for debugging connection handling and load balancing behaviors.

## Step 3: Running the Node with gRPC Logging Enabled

Once the environment variables are set, you can run the node as usual. The additional gRPC logs will now be printed to the console or log file, depending on your node’s logging configuration.

Here’s an example of how to run the node with the environment variables enabled:

```bash
GRPC_VERBOSITY=debug GRPC_TRACE=client_channel,round_robin go run .
```


## Step 4: Interpreting the Logs

After enabling gRPC logs, you will see more detailed output related to the gRPC client channels and round-robin load balancing. Some of the key log entries to watch for include:

- **Connection and Reconnection Events**: Logs showing when the gRPC client establishes or re-establishes connections to a server.
- **Load Balancing Events**: Logs detailing how the round-robin load balancer distributes calls across available servers.
- **Error Details**: Detailed errors or warnings that may occur during RPCs, including retries or failures.

These logs can provide valuable insights into how the node interacts with other services via gRPC and can help pinpoint issues with connection management, load balancing, or request handling.

## Step 5: Disabling gRPC Logs

Once you’re done debugging or collecting logs, you may want to disable the additional logging to reduce the verbosity of the output. To do this, simply unset the environment variables or set them to less verbose options. For example:

```bash
unset GRPC_VERBOSITY
unset GRPC_TRACE
```

Alternatively, you can set `GRPC_VERBOSITY` to a less detailed level, like `ERROR`:

```bash
export GRPC_VERBOSITY=error
```

## Summary

By setting the `GRPC_VERBOSITY` and `GRPC_TRACE` environment variables, you can enable additional logging for gRPC to help debug and troubleshoot issues with client channels and load balancing. This can be particularly useful when diagnosing connectivity issues or fine-tuning the node’s interaction with other services.

For a more complete overview of gRPC logs and tracing, refer to the official gRPC documentation or additional project-specific logging documentation if available.
