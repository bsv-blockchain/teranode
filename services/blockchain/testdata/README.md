# Historical mainnet headers

`mainnet_headers_477792_504031.bin` contains 26,240 consecutive, serialized
80-byte BSV mainnet headers (2,099,200 bytes), obtained from
[WhatsOnChain's header archives](https://api.whatsonchain.com/v1/bsv/main/block/headers/resources).
SHA-256: `5e5536bdc70c5fa860b0737984c8cc51c9f8fbb552694253bdcc46e594ed877f`.

`TestDifficultyHistoricalMainnet` compares the SQLite-backed Go calculator's
output with the mined `nBits` of every child from 478558 through 504031. This
includes the UAHF boundary, all 43 EDA target changes, and 13 periodic retargets.
The fixture starts at 477792 so the first checked retarget at 479808 has its
complete 2016-block ancestry. The expected targets come from the headers, not a
second implementation of the calculator. Every header's PoW and linkage are
checked; the first, UAHF and last hashes are pinned:

| Height | Hash |
| --- | --- |
| 477792 | `00000000000000000016ba7786309176445b838b36a16bd1ef3c3e3020473206` |
| 478558 | `0000000000000000011865af4122fe3b144e2cbeea86142e8ff2fb4107352d43` |
| 504031 | `0000000000000000011ebf65b60d0a3de80b8175be709d653b4c1a1beeb6ab9c` |

Only headers are seeded into SQLite, with actual parent links and relative
cumulative work. This tests the production calculator and store queries, not
full-block validation, coinbase processing, or the modern DAA.

Run from the repository root:

```sh
go test -race -tags testtxmetacache -count=1 -run '^TestDifficultyHistoricalMainnet$' ./services/blockchain
```

Reproduce the fixture from the public archives, also from the repository root:

```sh
python3 - <<'PY'
import hashlib
from pathlib import Path
from urllib.request import urlopen

archives = {
    (470001, 480000): "1b25a51c1a5e66dccf21760793092797a3259a8ed87785f398cf1ad33cb9f82c",
    (480001, 490000): "6b82295a7c5bacf444cd9573b1f001b57efc0e21697eb83ed5c66f25d3f42693",
    (490001, 500000): "155c6964946017e16be622bdd0ef9288c38759bc9bae78064b3c05423b8f8be8",
    (500001, 510000): "efb674e8fad6d5798e36f8a5d74762a8e39d71c3668241681f29bfe889a7f6c0",
}
first, last = 477792, 504031
parts = []
for (lo, hi), digest in archives.items():
    url = f"https://api.whatsonchain.com/v1/bsv/main/block/headers/{lo}_{hi}_headers.bin"
    with urlopen(url, timeout=60) as response:
        data = response.read()
    assert len(data) == (hi - lo + 1) * 80
    assert hashlib.sha256(data).hexdigest() == digest
    parts.append(data[(max(first, lo) - lo) * 80:(min(last, hi) - lo + 1) * 80])
fixture = b"".join(parts)
assert hashlib.sha256(fixture).hexdigest() == "5e5536bdc70c5fa860b0737984c8cc51c9f8fbb552694253bdcc46e594ed877f"
Path("services/blockchain/testdata/mainnet_headers_477792_504031.bin").write_bytes(fixture)
PY
```

## Scope of the pre-UAHF historical check

The committed regression covers the EDA era; it does not replay all pre-UAHF
mainnet through Go. The earlier independent Python scan used concatenated
WhatsOnChain headers 0–510000 (40,800,080 bytes, SHA-256
`797be3d775ca6a3a4b4d0d1d849f41dde4c74b62c4c5b116aa1230a9ed2f3949`).
It checked every link and PoW and pinned genesis, 478558 and 504031.

For each parent, MTP was the median of its timestamp and ten predecessors.
For non-retarget, non-powLimit parents below 478558, the scan compared this MTP
with the MTP six ancestors back. All 446,081 applicable parents (32256–478557)
were below 43,200 seconds; the maximum was 26,789 seconds at parent 74648.
Applying the ungated historical arithmetic to children 1–504031 gave zero
target mismatches: 250 periodic retargets and 43 EDA increases. Retarget elapsed
time used the parent and its 2015th ancestor, clamped to 302400–4838400 seconds,
then scaled the parent target by elapsed/1209600. EDA added `target >> 2` when
the MTP difference reached 43200. Both paths capped at the mainnet powLimit and
encoded the compact target before comparison with the mined child header.
