# Aerospike pruner service

This service is responsible for pruning records in the Aerospike database based on block height.

## Overview

The service maintains a list of pruning jobs and a pool of workers that process these jobs. When notified of a new block height, it creates a job for that height and adds it to the list. Upon adding a new job, any pending jobs with lower block heights are marked as cancelled.

The service retains information about the last `DefaultMaxJobsHistory` (1000) jobs and their statuses, providing insights into the pruning operations.

Available workers select the most recent pending job from the list, mark it as "running", and execute an Aerospike query with a filter expression to delete records where the `fields.DeleteAtHeight` value is less than or equal to the job's block height. Workers then monitor query completion and update the job status accordingly.

## Job Statuses

Jobs can have the following statuses:

- `JobStatusPending`: Job is waiting to be processed
- `JobStatusRunning`: Job is currently being processed
- `JobStatusCompleted`: Job has been successfully completed
- `JobStatusFailed`: Job has failed due to an error
- `JobStatusCancelled`: Job has been cancelled (typically when a newer job supersedes it)

## Configuration

The service is configured using the `Options` struct, which contains the following fields:

- `Client`: The Aerospike client to use
- `Namespace`: The Aerospike namespace to use
- `Set`: The Aerospike set to use
- `WorkerCount`: The number of worker goroutines to use (default: 4)
- `MaxJobsHistory`: The maximum number of jobs to keep in history (default: 1000)
- `Logger`: The logger to use

## Usage

```go
// Create a new pruner service
service, err := NewService(Options{
    Client:         client,
    Namespace:      "test",
    Set:            "test",
    WorkerCount:    4,
    MaxJobsHistory: 1000,
    Logger:         logger,
})
if err != nil {
    log.Fatal(err)
}

// Start the service
service.Start()

// Update the current block height and trigger pruning
service.UpdateBlockHeight(1000)

// Get all jobs (including completed, failed, and pending)
jobs := service.GetJobs()

// Stop the service
service.Stop()
```

## Implementation Details

The pruner service uses Aerospike's query execution with a filter expression to find and delete records where the `fields.DeleteAtHeight` value is less than or equal to the specified block height. This works in conjunction with Aerospike's TTL (Time-To-Live) feature for UTXO record management, providing an additional mechanism for pruning records based on blockchain height.
