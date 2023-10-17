
# Aerospike test

# Usage

```shell

./aeropsiketest.run -help

  -aerospike_host string
    	aerospike host
  -aerospike_namespace string
    	aerospike namespace [utxostore] (default "utxostore")
  -aerospike_port int
    	aerospike port (default 3000)
  -asl_logger
    	enable aerospike logger
  -buffer_size int
    	buffer size (default 1000)
  -repeat int
    	number of time to repeat the test [1] (default 1)
  -strategy string
    	strategy to use [ubsv, direct, simple, nothing] (default "direct")
  -timeout string
    	timeout for aerospike
  -transactions int
    	number of transactions to process (default 100)
  -workers int
    	number of workers (default 10)


$ ./aerospiketest.run -aerospike_host:n.n.n.n -strategy=ubsv -workers=100 -transactions=1000000

```
## Result history


### 13/10/23
I’ve re-run aerospiketest.run with various options on utxostore pod which has 100CPUs available.

Using 500 workers, it managed to do
- store
- spend
- delete
10,000,000 times (3 aerospike calls each time)
in 56 seconds.

It maxed out on CPU doing this.

However, when I tried 1000 workers (surely this is the same or slower?)
it did the same thing in 40s.

Again when I tried 2000 workers
it did the same thing in 32s.

and then I tried 3000 workers
it did the same thing in 31s.

So, in theory we can convert this to TPS 32/10/3 = 1.03s per million
