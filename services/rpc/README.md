
`go run . -all=0 -rpc=1 -blockchain=1`



use postman

`{"method": "getblock", "params": ["000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", 1]}`

`{"method": "getbestblockhash"}`

`{"method": "createrawtransaction", "params": [[{"txid":"0000000000000000000000000000000000000000000000000000000000000000","vout":0}],{"148EznH7niCQRUcsrtDgBBKUfYDYESscUQ":12.5}]}`

`{"method": "sendrawtransaction", "params": ["010000000100000000000000000000000000000000000000000000000000000000000000000000000000ffffffff01807c814a000000001976a9142246f8f846d04b71fbec79815c7ab487b47737a388ac00000000"]}`



curl --user bitcoin  -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "getblock", "params": ["000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"]}'

use curl

curl --user bitcoin  -X POST http://localhost:19292 \
     -H "Content-Type: application/json" \
     -d '{"method": "sendrawtransaction", "params": ["010000000100000000000000000000000000000000000000000000000000000000000000000000000000ffffffff01807c814a000000001976a9142246f8f846d04b71fbec79815c7ab487b47737a388ac00000000"]}'

curl --user bitcoin  -X POST http://localhost:19292 \
     -H "Content-Type: application/json" \
     -d '{"method": "getbestblockhash"}'

curl --user bitcoin  -X POST http://localhost:19292 \
     -H "Content-Type: application/json" \
     -d '{"method": "getblock", "params": ["003e8c9abde82685fdacfd6594d9de14801c4964e1dbe79397afa6299360b521", 1]}'
