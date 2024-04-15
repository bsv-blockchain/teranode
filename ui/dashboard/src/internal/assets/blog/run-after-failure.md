## Why we let it run after failure

In the last couple of months we have been learning more and more, not only about how the software we created behaves, but also how it behaves in our configuration at AWS, and using the third party services we are using. Many problems have popped up and we have had to add a lot of logging, both on the application side and the server side.

Everytime we start a new run with our 3 node Teranode cluster, we are tracking and logging a tremendous amount of data. We are generating about 5TB of logs per hour, every hour that the test runs.

When one or more Teranode nodes fail, we sometimes let it keep on running, because on those cases some of the system is actually working harder and under more stress. Think of a situation where the three nodes are all forked, the block validation services of each node is still validating all the blocks produced by all the other nodes, and therefore validating more blocks than when the network would be functioning correctly. It is working much harded than under normal conditions and is hitting the database and storage harder.

We are confident that we have solution for the problems we have been experiencing and this is partly due to running the network out of sync, and seeing what happens on the service under stress.

The long-running test is now coming very close to start. 