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

# Mainnet headers across a retarget and the DAA switch

`mainnet_headers_501900_506100.bin` contains 4,201 consecutive, serialized
80-byte BSV mainnet headers, heights 501900 to 506100 (336,080 bytes), cut from
WhatsOnChain's `https://api.whatsonchain.com/v1/bsv/main/block/headers/500001_510000_headers.bin`
archive, whose SHA-256 is the
`efb674e8fad6d5798e36f8a5d74762a8e39d71c3668241681f29bfe889a7f6c0` that
`services/blockchain/testdata/README.md` pins for the same archive.
SHA-256 of the cut: `456d2613596bf496d8706e962744abaac927bab8eab1a6d92baae5560c8d159a`.

The range holds the periodic retarget at 504000 with its whole 2016-block
window (from 501984), the last emergency-difficulty-era child at 504031 and
the first child judged by the 144-block DAA at 504032 (activation is measured
at the parent, `DaaForkHeight` 504031). The header-rules tests pin these hashes:

| Height | Hash |
| --- | --- |
| 501900 | `000000000000000006f12e51b8024c6b4a10f0fea7ebfe8af655230525503d0e` |
| 504000 | `0000000000000000006cdeece5716c9c700f34ad98cb0ed0ad2c5767bbe0bc8c` |
| 504031 | `0000000000000000011ebf65b60d0a3de80b8175be709d653b4c1a1beeb6ab9c` |
| 504032 | `00000000000000000343e9875012f2062554c8752929892c82a0c0743ac7dcfd` |
| 506100 | `0000000000000000003beb1044e40bef309e1dab9f64f6abe4c6d1596a060869` |

Reproduce from the repository root:

```sh
curl -s -o /tmp/a.bin https://api.whatsonchain.com/v1/bsv/main/block/headers/500001_510000_headers.bin
python3 -c "d=open('/tmp/a.bin','rb').read(); open('services/legacy/netsync/testdata/mainnet_headers_501900_506100.bin','wb').write(d[(501900-500001)*80:(506100-500001+1)*80])"
```

# Mainnet headers to the first checkpoint

`mainnet_headers_1_11111.bin` contains 11,111 consecutive, serialized 80-byte
BSV mainnet headers, heights 1 to 11111 (888,880 bytes): from the child of
genesis to the first mainnet checkpoint,
`0000000069e244f73d78e8fd29ba2fd2ed618bd6fa2ee92559f542fdb26e7c1d`. Every
header is in the difficulty-1 era (`nBits` `0x1d00ffff`), which is what the
header-request-rule tests need: a fake branch built from these headers by
changing each merkle root carries the same work as the real one.
SHA-256: `031de0ade9e449252be2ac8863357501515f47ac8cdb04af2137bc4adef30b3e`.

Reproduce from the repository root:

```sh
curl -s -o /tmp/a.bin https://api.whatsonchain.com/v1/bsv/main/block/headers/0_10000_headers.bin
curl -s -o /tmp/b.bin https://api.whatsonchain.com/v1/bsv/main/block/headers/10001_20000_headers.bin
cat /tmp/a.bin /tmp/b.bin | dd bs=80 skip=1 count=11111 of=services/legacy/netsync/testdata/mainnet_headers_1_11111.bin
```
