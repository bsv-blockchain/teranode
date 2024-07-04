# What is this thing?

The BlockPersister creates loads of artifacts; .block, .utxodiff, .utxoset and .subtree

These are stored in S3 buckets; block-store

There is a tool within S3 to create an csv inventory file listing all the files in the bucket

This is a tool to get the blocks from the blackchain DB and go through each block and check the expected files exist in the CSV file

Where the file does not exist, it checks the S3 store itself

It can be configured to check multiple S3 stores (you might want to check all the S3 stores of all the nodes for example)

# Usage

```
$ s3inventoryintegrity [-verbose] -d <postgres-URL> -f <csv-filename>
```

(verbose mode details each missing file)
Non-verbose mode give you a summary report only

# Example
```
SETTINGS_CONTEXT=docker.host.ubsv1 logLevel=INFO go run . -d postgres://poc2:poc2@localhost:5432/poc2  -f /Users/freemans/Downloads/d61fe22e-b3e7-4145-945b-b5689253aee7.csv -verbose
```