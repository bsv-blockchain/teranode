
`go run . -all=0 -rpc=1 -blockchain=1`



use postman

`{"method": "getblock", "params": ["000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", 1]}`

`{"method": "getbestblockhash"}`


curl --user bitcoin  -X POST http://localhost:9292 \
     -H "Content-Type: application/json" \
     -d '{"method": "getblock", "params": ["000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"]}'


