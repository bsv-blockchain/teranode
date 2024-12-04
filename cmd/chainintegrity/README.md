# Chain Integrity Checker
> A tool to check the integrity of a UBSV local blockchain

This tool will check the integrity of a local blockchain generated through testing. It is not intended to be used on
mainnet.

## Usage

```shell
# remove the old data
rm -rf data

# run the node for a bit to create at least 100 blocks
SETTINGS_CONTEXT=dev go run .

# stop the node (CTRL+C)

# run the node normally
SETTINGS_CONTEXT=dev go run .

# on a second terminal
cd cmd/txblaster

# run the tx blaster
go run . -workers=1 -print=100 -profile=:9193 -log=1

# stop the tx blaster

# stop the node

# go to cmd/chainintegrity
cd cmd/chainintegrity

# run the integrity checker
go run .

# also possible to run in debug mode
go run . -debug=1
```
