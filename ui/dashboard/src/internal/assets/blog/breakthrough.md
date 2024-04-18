## Breakthrough: Teranode Runs for 24 Hrs

We’re thrilled to announce a significant achievement in our journey with Teranode: a successful 24-hour run! Our engineering team has been hard at work, striving for stability, and we’re excited to share the results of our latest milestone.

## What is Equilibrium?

Equilibrium, for us, signifies a state where all our microservices, databases, and systems settle into a stable or cyclical operational pattern. Scaling to 1M TPS is a journey with multiple operational stages. Initially, we ramp up, ensuring all necessary connections within services, with databases, and supporting services are established. This is where the Transaction Blasters start to send transactions. After comes the run period, where we process all incoming transactions for a couple of hours.

Then comes the third phase, where we start cycling through transactions, triggering TTLs (time to live), and initiating cleaning jobs in our distributed database and storage. Behind the scenes, millions of records are cleaned, and disk defragmentation is performed to maintain the health and speed of our database. Similarly, our distributed block storage and relational database undergo storage-related optimization. The cleaning often adds additional load on our system as we need to clean and keep ingesting at around the same rate. After this 24-hour run, we’re pleased to confirm that all these metrics have reached equilibrium and are sustainable for long-term operation.

## Final Tweaks for Success

The key changes were primarily focused on the distributed database layer. When dealing with massive clusters of distributed databases, discussions often revolve around P99 metrics. We’re proud to report that these metrics indicate sub-millisecond inserts for us. However, addressing the remaining 1%, which was causing occasional headaches, required dedicated attention. We enhanced our monitoring to track this 1% separately and optimized and fortified our system to handle these edge cases effectively.

## Looking Ahead

While celebrating this achievement, we recognize that there’s always room for improvement. Our future work includes stability enhancements to comfortably surpass 1M TPS, providing us with a buffer and reaction time in case of unexpected issues. Currently, we’re observing throughput dips when mining massive blocks with over 500,000 transactions, and our team is actively working on optimizing these processes.

We’re incredibly proud of our team’s dedication and the progress we’ve made with Teranode. Stay tuned for more updates as we continue to push the boundaries of what’s possible in blockchain technology!
