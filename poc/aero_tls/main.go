package main

import (
	"fmt"

	"github.com/aerospike/aerospike-client-go/v6"
)

func main() {
	// u := "aerospike://read-write:i23nqwreak@aerospike.aerospike.svc.cluster.local:3000/test"
	// u := "aerospike://localhost:3000/test"

	policy := aerospike.NewClientPolicy()

	hosts := []*aerospike.Host{
		{Name: "192.168.86.103", Port: 3000}, // hardcoded for testing
		{Name: "192192.168.9.0", Port: 3000}, // hardcoded for testing
		{Name: "192.168.55.153", Port: 3000}, // hardcoded for testing
	}

	client, err := aerospike.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		panic(err)
	}

	defer client.Close()

	fmt.Printf("Connected %v\n", client.IsConnected())

	namespace := "test" // aUrl.Path[1:]
	fmt.Printf("Namespace %s\n", namespace)

	wPolicy := aerospike.NewWritePolicy(0, 0)
	wPolicy.RecordExistsAction = aerospike.CREATE_ONLY
	wPolicy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	key, err := aerospike.NewKey(namespace, "utxo", []byte("Hello"))
	if err != nil {
		panic(err)
	}

	bins := aerospike.BinMap{
		"txid": []byte{},
	}
	err = client.Put(wPolicy, key, bins)
	if err != nil {
		panic(err)
	}

	value, err := client.Get(nil, key, "txid")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Value %v", value)

}
