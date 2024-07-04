# What is this?

Sometimes in the node's log files we bump into this sort of thing

```
bsv-2  | 2024-07-04T10:01:52Z | ERROR | BlockValidation.go:632           | bval  | [ValidateBlock][Block 00c400b18415d4a00dc807526d1153c65b70abc4277321824bd162529a624f61 (height: 318, txCount: 2, size: 6096] InvalidateBlock block is not valid in background: Error: BLOCK_INVALID (error code: 11),  [BLOCK][00c400b18415d4a00dc807526d1153c65b70abc4277321824bd162529a624f61] error validating transaction order: %!v(MISSING): Error: BLOCK_INVALID (error code: 11),  parent transaction f2deccf48c7ac1c76dcc3fa85bee8ad0a5e72c2c2669f4a2c96fd67b9063ace7 of tx e49c5b80961f94d7329c47f732796c01b1a3684939b92e6842f367640065d206 has no block IDs: <nil>, data :, data :
```

This tool will go thru all the main chain blocks and forks, looking for the tx and the parent tx.
It will check:
- tx / parent tx exists in the utxo store
- tx / parent tx hash exists in a subtree
- tx / parent tx block ID matches the actual block it was found in

# Usage

txinfo -utxostore <utxo-store-URL> -blockchainstore <blockchain-store-URL> -subtreestore <subtree-store-URL> -txhash <tx-hash>

# Example

```
cmd/txblockidcheck$ go run . -utxostore="aerospike://localhost:3200/test?WarmUp=32&ConnectionQueueSize=32&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=8" -blockchainstore=postgres://miner2:miner2@localhost:15432/ubsv2 -subtreestore=file:///data/ubsv2/subtreestore -txhash=bf8f820d97a657d218b6b86fe8390f59b6505cc881f9c733ead9734461599216
```