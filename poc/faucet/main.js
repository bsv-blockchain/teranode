const express = require('express')
const bodyParser = require('body-parser')
const path = require('path')
const grpc = require('@grpc/grpc-js')

// Define your gRPC client
const protoLoader = require('@grpc/proto-loader')
const packageDefinition = protoLoader.loadSync('../../services/coinbase/coinbase_api/coinbase_api.proto', {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
})
const coinbaseProto = grpc.loadPackageDefinition(packageDefinition).coinbase_api
// const client = new coinbaseProto.CoinbaseAPI('localhost:8093', grpc.credentials.createInsecure())
const client = new coinbaseProto.CoinbaseAPI('localhost:8093', grpc.credentials.createSsl(
  null,
  null,
  null,
  {
    checkServerIdentity: () => { /* skip certificate verification */ }
  }
))

const app = express()

// Serve static files from the "public" directory
app.use(express.static(path.join(__dirname, 'public')))

// Parse JSON bodies
app.use(bodyParser.json())

app.post('/api/faucet', (req, res) => {
  const { address } = req.body

  // Make the gRPC request
  client.RequestFunds({ address, disableDistribute: true }, (error, response) => {
    if (error) {
      console.error(error)
      res.status(500).json({ error: 'Failed to make gRPC request' })
    } else {
      const tx = response.tx.toString('hex')
      res.json({ tx })
    }
  })
})

app.post('/api/submit', (req, res) => {
  const { tx } = req.body

  client.DistributeTransaction({ tx: Buffer.from(tx, 'hex') }, (error, response) => {
    if (error) {
      console.error(error)
      res.status(500).json({ error: 'Failed to make gRPC request', response })
    } else {
      res.json(response)
    }
  })
})

const PORT = process.env.PORT || 3000
app.listen(PORT, () => console.log(`Server running on port ${PORT}`))
