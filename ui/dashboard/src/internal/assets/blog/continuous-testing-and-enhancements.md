## Continuous Testing and Enhancements

Having just completed our third week of alpha testing, our team continues to focus on enhancing the efficiency of our services and optimizing resource utilization. With the key goal of processing 1 million transactions per second (tps) for Teranode now achieved, our emphasis has shifted towards ensuring stability.

### Improving Validation Performance

Over the past three weeks, we have been testing with a 3-node cluster, achieving impressive results that still have room for improvement. The cluster has successfully processed 1.1 million transactions per second, consistently above our target. However, we've observed an increase in the number of forks, attributed to delays in our block validation service. These delays in processing blocks and subtrees from other nodes have resulted in forks resolving slower than desired.

To address this issue, our team has implemented an architectural change by separating node block validation from ongoing subtree validation. This modification enhances horizontal scalability and performance. Additionally, we have optimized our data structures to minimize unnecessary IO operations and have made more extensive use of message queues to improve reliability. Testing of this new setup has already begun, showing promise in streamlining the validation process and reducing the occurrence of forks.

#### Enhancing Network and Server Configurations

From the onset of the alpha testing phase, one of the main challenges we faced was the high demand on our network and the complexities of inter-node communication. To mitigate these issues, our team has conducted thorough tests with various service and server combinations. Our objective was to identify a configuration that not only minimizes the risk of disconnections but also optimizes operational costs, a critical consideration for node operators.

#### Preparing for Long-Term Performance Testing

In the past few weeks, testing was periodically interrupted to facilitate iterative improvements. We are now wrapping up this experimental stage, and getting ready for a multi-month, sustained, and uninterrupted test phase, aiming for long-term performance validation.
