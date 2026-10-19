# Early testnet regression fixture

`testnet_blocks_1_547.gz` contains real testnet blocks 1–547 in ascending order.
After gzip decompression, each record is a little-endian uint32 byte length
followed by the Bitcoin wire block. Downloaded from the public Blockstream
[testnet API](https://github.com/Blockstream/esplora/blob/master/API.md)
(`/testnet/api/block/<hash>/raw`). This history predates the BTC/BSV split.

The hashes and header linkage were cross-checked against WhatsOnChain's public
`https://api.whatsonchain.com/v1/bsv/test/block/headers/0_10000_headers.bin` archive.
The corresponding `services/blockchain/testdata/testnet_headers_0_547.bin`
contains its first 548 consecutive 80-byte headers, including genesis.

The tests pin the reported block-149 hash and checkpoint 546 from chaincfg,
and validate proof of work. No test downloads data or needs a running node.
