# go-grpc-k8s-resolver
GRPC resolver for Kubernetes service endpoints

[![PkgGoDev](https://pkg.go.dev/badge/github.com/tcfw/go-grpc-k8s-resolver)](https://pkg.go.dev/github.com/tcfw/go-grpc-k8s-resolver)
[![Go Report Card](https://goreportcard.com/badge/github.com/tcfw/go-grpc-k8s-resolver)](https://goreportcard.com/report/github.com/tcfw/go-grpc-k8s-resolver)

## Overview
Based off the DNS resolver, rather than making DNS queries, the k8s resolver queries the Kubernetes API for service endpoints matching the service name.

Using a headless service can be slow to update and the reverse proxies in service meshes may increase latency. By using the endpoints Kubernetes, updating the client with new or removing old server endpoints can be much faster.

![overview](overview.png)

## Example
```go
package main

import (
	"log"
	pb "github.com/your-username/your-project/protos"
	_ "github.com/tcfw/go-grpc-k8s-resolver"
)

func main() {
	resolver := "dns"

	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		resolver = "k8s"
	}

	resolver.SetDefaultScheme(resolver)

	conn, err := grpc.Dial("my-service-name")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := pb.NewRouteGuideClient(conn)

	feature, err := client.GetFeature(context.Background(), &pb.Point{409146138, -746188906})
	if err != nil {
		panic(err)
	}

	log.Println(feature)
}
```
