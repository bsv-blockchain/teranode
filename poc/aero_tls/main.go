package main

import (
	"fmt"

	"github.com/aerospike/aerospike-client-go/v6"
	asl "github.com/aerospike/aerospike-client-go/v6/logger"
)

func main() {
	// u := "aerospike://read-write:i23nqwreak@aerospike.aerospike.svc.cluster.local:3000/test"
	// u := "aerospike://localhost:3000/test"

	asl.Logger.SetLevel(asl.DEBUG)

	// aUrl, err := url.Parse(u)
	// if err != nil {
	// 	panic(err)
	// }

	// port, err := strconv.Atoi(aUrl.Port())
	// if err != nil {
	// 	panic(err)
	// }

	policy := aerospike.NewClientPolicy()
	// policy.User = aUrl.User.Username()
	// policy.Password, _ = aUrl.User.Password()
	// policy.Timeout = 10000 // Set timeout to 5 seconds

	// policy.AuthMode = aerospike.AuthModeExternal

	// policy.TlsConfig = &tls.Config{
	// 	InsecureSkipVerify: true,
	// }

	// hosts := []*aerospike.Host{
	// 	{Name: aUrl.Hostname(), Port: port}, // Add your cluster hosts and ports here
	// }

	hosts := []*aerospike.Host{
		{Name: "192.168.13.144", Port: 3000}, // hardcoded for testing
		{Name: "192.168.84.1", Port: 3000}, // hardcoded for testing
		{Name: "192.168.34.151", Port: 3000}, // hardcoded for testing
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
