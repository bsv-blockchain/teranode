## What we have learned so far

Over the past two weeks, we have been collecting a significant amount of data while operating Teranode in the test environment. Our findings indicate that the architecture works really well in validating transactions and generating blocks. Each of the three nodes can effortlessly manage 1.1 million transactions per second, simultaneously creating subtrees and blocks and propagating them across the network.

![Grafana screenshot](/blog/grafana.png "Grafana screenshot")

However, we continue to encounter issues in block validation. For the proof of concept, we designed the block validation to be running on a single instance responsible for both subtree and block validation. This approach was chosen because both tasks require access to transaction metadata, necessitating a large cache within the node to facilitate processing. For performance reasons, this cache must be local and cannot be shared across a network.

Despite our efforts to optimize the cache warm-up (see previous update), the dual responsibility of subtree and block validation introduces a bottleneck in block validation and exerts excessive memory pressure on that single machine. Over time, particularly with larger blocks, this machine struggles to keep up.

## Next Steps

Our current focus is on splitting up subtree and block validation into distinct services, and enabling subtree validation to be done on multiple instances. This change is anticipated to enhance the scalability of the node significantly and address the challenges associated with subtree validation not keeping up.

In addition to this substantial architectural modification, we are also working on incremental improvements and optimizations across various system components, with a particular emphasis on block validation.
