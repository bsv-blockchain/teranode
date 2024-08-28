Seeder
------

The seeder is a command line tool that can be used to seed the utxostore with data up to and including
a specific block.  It reads a UTXOSet and writes all the necessary records to Aerospike.

The UTXOStore needs the following minimum fields (buckets) for a Bitcoin node to be functional.

Key: TXID

Value:
- []Outputs
- []UTXOs

The outputs are used to extend future transactions by proving the value and locking script for an input in
a transaction.

The UTXOs slice is used to keep track of the unspent outputs in the UTXOSet.

