# Teranode Introduction

The Bitcoin (BTC) scalability issue refers to the challenge faced by the original Bitcoin network in processing a high volume of transactions efficiently. Initially, Bitcoin had a block size limit of 1 megabyte, which constrained the network’s throughput to around 3.3 to 7 transactions per second. As Bitcoin gained popularity, this limitation led to slower transaction times and increased fees.

With Bitcoin SV (BSV), the block size limit was significantly increased to 4 gigabytes, improving the network's performance. However, the current BSV node software (SV Node) still has scalability and performance limits, capping the network’s capacity at several thousand transactions per second.

**Teranode** is BSV’s solution to these limitations by employing a horizontal scaling approach. Instead of relying solely on increasing the capacity of a single node (vertical scaling), Teranode distributes the workload across multiple machines. This architecture, combined with an unbounded block size, allows the network’s capacity to grow in line with demand, as more cluster nodes can be added.

With Teranode, BSV achieves true scalability, consistently supporting over 1 million transactions per second. Teranode is designed as a collection of microservices, each responsible for different functions of the BSV network, ensuring high performance while staying true to the principles outlined in the original Bitcoin whitepaper.
