---
description: Service interface design pattern for gRPC microservices
globs: services/**/*.go
---

## Service Interface Design Pattern

Interfaces (`Interface.go`) must use native Go types only — no protobuf types in signatures. Use simple return types: `error`, `bool`, `[]string`, domain structs.

Clients (`Client.go`) keep protobuf/gRPC imports internal. Public methods match the interface. Convert between protobuf and native types using internal helper functions.

Reference implementation: `services/p2p/`
