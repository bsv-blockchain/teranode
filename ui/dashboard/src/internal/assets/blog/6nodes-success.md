# BSV Blockchain Update: Teranode Testing Success
## What Just Happened?

We are thrilled to announce the successful run of Teranode in a private network environment on AWS. Over the past two weeks, we ran a collection of six Teranode nodes, each composed of microservices orchestrated by Kubernetes, across globally distributed regions including Ireland, Central Canada, Seoul, North Virginia, Oregon, and Mumbai. 

This extensive setup sustained an impressive average throughput exceeding 1 million transactions per second (TPS) throughout the testing period. The scale of this operation, utilizing over 500 machines of various instance types, demonstrates our commitment to pushing the boundaries of blockchain technology.

![6_Teranodes dashboard screenshot](/blog/6nodes-summary.png "Successful test")

## Next Steps

Our focus shifts to getting our Minimum Viable Product (MVP) production ready and capable of fully operating on the mainnet. Significant progress has been made, and our current efforts are directed towards adapting Teranode and SV node to work seamlessly together. We are hardening the system to ensure a smooth and secure transition from the lab to the live environment. This involves extensive code cleanups, security enhancements, DevOps improvements, and comprehensive documentation. 

We are working towards feature completeness to match current mining software standards. While we strive to maintain backward compatibility, it is essential to recognize this as the next evolution of our blockchain journey, with certain concepts like the mempool and others being phased out. We will be providing detailed guidance and documentation to back the community in understanding all the changes and support in the transition over the next two years. You can find our long term roadmap [here](https://www.bsvblockchain.org/roadmap). 

Additionally, we will now focus on getting a test network up and running. Once the test network is stable internally, we will open it for beta testers who have signed up with us. In this environment, we will be running the miners in debug mode with all tracing enabled and the sampling rate set to 1. We want to capture and understand all the internals of the system and solidify a robust test suite.


## Geeky Extra Details
    
In the spirit of transparency, we would like to share some of the challenges we faced during our test run. 

- Initially, we observed a higher rate of forking than anticipated. This was traced back to our subtree validation services, which struggled to keep up when multiple nodes forked simultaneously, necessitating rapid validation. Fortunately, our design allowed for horizontal scaling of these stateless services, effectively addressing the issue. 
- Additionally, we had to increase the capacity of our distributed storage solution (Lustre FS), which supports the sharing of large data blobs between services. Our proactive alert system, set to trigger at 50% threshold, provided ample time to adjust without causing disruptions, especially as we scaled the test from three to six nodes.

We are excited about the progress we've made and look forward to the continued development and enhancement of BSV blockchain technology. Stay tuned for more updates as we advance towards a new era of scalable and efficient blockchain solutions. The future is now!