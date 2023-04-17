package main

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func main() {
	// Different API versions may expose different runtime behaviors.
	fdb.MustAPIVersion(720)

	// Set up the client configuration with client locality
	// opts := []fdb.Option{
	// 	fdb.DataCenterId("dc1"),
	// 	fdb.MachineId("machine1"),
	// }
	// db := fdb.MustOpen(opts...)

	db := fdb.MustOpenDatabase("fdb.cluster")

	// Perform a read operation
	_, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		key := tuple.Tuple{"my-key"}
		value, err := tr.Get(key).Get()
		if err != nil {
			return nil, err
		}
		return value, nil
	})
	if err != nil {
		panic(err)
	}
}
