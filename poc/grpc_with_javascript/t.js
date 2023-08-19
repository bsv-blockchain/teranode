const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_PATH = './bootstrap_api.proto';

// Load the protobuf
const packageDefinition = protoLoader.loadSync(
    PROTO_PATH,
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    }
);

const bootstrap_api = grpc.loadPackageDefinition(packageDefinition).bootstrap_api;

// Create gRPC client
const client = new bootstrap_api.BootstrapAPI('localhost:8089', grpc.credentials.createInsecure());

// Invoke the GetNodes procedure
client.GetNodes({}, (error, response) => {
    if (!error) {
        console.log('Nodes list:', response);
    } else {
        console.error(error);
    }
});
