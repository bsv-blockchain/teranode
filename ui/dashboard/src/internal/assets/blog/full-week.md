## Teranode network processes over 600 billion transactions in a week
> Processes more transactions than all the credit card companies do in a year, combined

We have been running the Teranode test now for a full week, and, although we have had some incidents, the network has been performing excellent, proving that a UTXO based blockchain can scale on-chain, on layer 1.

![Full week Teranode dashboard screenshot](/blog/full-week.png "Full week")

The problems that we experienced were related to infrastructure incidents; although one of the nodes faced issues temporarily, the network recovered in every case and the main chain kept on going. This in itself is an amazing proof of the design of Bitcoin and the scalability of the original protocol. As Satoshi himself said:

> The current system where every user is a network node is not the intended configuration for large scale.  That would be like every Usenet user runs their own NNTP server.  The design supports letting users just be users.  The more burden it is to run a node, the fewer nodes there will be.  Those few nodes will be big server farms.  The rest will be client nodes that only do transactions and don't generate.

Source: [https://bitcointalk.org/index.php?topic=532.msg6306#msg6306](https://bitcointalk.org/index.php?topic=532.msg6306#msg6306)

Teranode is the embodiment of that vision and it is working.

There is still a lot of work to do. We need to harden the code, enhance the communication layers, improve error handling, and build better quality gates, among other improvements. This work will continue in the coming months, leading up to our beta release in Q3-Q4 of this year.

Follow our progress in more detail on our Datadog public dashboard: 

[https://teranode-metrics.bsvblockchain.org](https://teranode-metrics.bsvblockchain.org)

Also check out all the forks that have been processed by Teranode in the last week on the forks view: 

[https://teranode.bsvblockchain.org/forks/?hash=000aabe3ca66fcf05a1af86b4e7cebd6b4c05b99dc32691e1c7c00b4f19b69f2](https://teranode.bsvblockchain.org/forks/?hash=000aabe3ca66fcf05a1af86b4e7cebd6b4c05b99dc32691e1c7c00b4f19b69f2)

You can double-click on a block to see the forks from that point onwards.
