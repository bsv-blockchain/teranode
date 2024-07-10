_this document is meant for internal purposes only for the dev and devops team of teranode. do not share publicly_
# Version: Draft 1
# Devops Roadmap & Direction Setting

The goal of this document is to provide a roadmap for the devops team to follow. This roadmap will be broken down into milestones, aligning with the milestone defined by the business. The goal is to stir a conversation regarding the trajectory of the devops developments and understand what needs to be implemented when. This is a live document and your contributions are more than welcome!

Note while these are not exactly part of the deliverable for the milestone, these tasks and products will accompany the deliverable to better support the business and people using it.

# Milestone 3: Listening on Main Net
# Alpha Testing
It seems like the alpha testing will be done by a selected group of miners. This is a good thing as it will allow us to have a controlled environment to test the software. This will allow us to gather feedback and improve the software before we go to beta testing. So far, it seems that the alpha testers will be a group of engs from Gorilla Pool and from Taal.
The goal is to start giving them a taster of the software, how to run it, how to work on the ui, settings and get them familiarized with SOP(being written by vicente). **This is a requirement for milestone 3**.

We decided that the two docker compose, single node and three nodes blockchain would satisfy the requirement for this milestone. The goal is to train them on the dependencies, get them familiar with them, how to run them and familiar with the software. They will not be running anything in production as we'll still be collecting feedback from them to make the software robust
## UBSV Binary
The ubsv repo where the current teranode code resides will be open sourced. This repo will include a runnable version of the teranode code as well as ci/cd to build the executable. 
- People will have the option to build from source. This will be available at later stages as we will need to have a stable version of the software before we can publish it.
- People will also have the ability to run pre-built and published executable for both arm64 and amd64. This will be deployed to a package manager probably github packages (as not to host and manage yet another thing). These will follow a versioning scheme that will be defined later. #todo define versioning scheme and schedules (feature based, scheduled releases, release train)

At the end of the day, this is the core of what we're building and having an executable will be the most important thing to support. As a user, you will always have the ability to take that binary executable and run it or even build from source.

> Teranode is a complex piece of software that requires multiple dependencies. Our job is not to hold hands for developers but to provide the tools, resources and documentation to make it easier for them to build and run the software. 
> 
> For example, if you want to run aerospike or any complex db, you have multiple options to do so. From a binary, docker, docker-compose, kubernetes etc. You also have multiple modes (in mem, flash, hybrid) of configuring it and it's your job to figure out what's the best setup for you. Choosing the right solution is up to you, depending on your needs. 
> 
> There will be a learning curve that people will have to go through to understand how to run the software. Our job is to make this as simple as possible but not to hold hands.

The binary has the ability to run one or more service via the settings file (#todo we need to define this. as of right now, this is a big topic of debate. how to manage the settings).

If you want to run this binary, you will be responsible to have all the dependencies installed. Choosing these internal and external dependencies is an opinionated topic and will be decided based on what type of deployment you want to have. If you decide to go down that route, you will need to run these external services yourself.

This satisfy simon's argument that we will still need to have a binary that can run on a single machine. This will be the easiest way to run the software as stand alone and the most portable way as you don't need to learn docker or kubernetes or any extra complexity to run the software.
### A note on our dependencies
We built the main engines of our application using the ports and adapters architecture. This means that we can swap out the dependencies of the software without changing the core of the software. This is a big advantage as it allows us to run the software using different external dependencies. For example, you can easily swap the utxo store from an aerospike to a postgres database. By default, we will be supporting a few database and storage solutions out of the box. If you want to run the software with a different dependency, you will have to implement the adapter yourself using golang dependency injection strategies. We might be accepting PRs for this but we will not be responsible for maintaining these dependencies as they will be framed under community supported dependencies. 

#todo define long term supported dependencies
    
- aerospike for utxostore
- postgres for coinbase
- others?

#todo define community supported dependencies
- redis for utxostore?
- others?

## Docker Image
Along with the binary, we will also provide a docker image. This docker image will be published to a docker registry (probably docker hub). This will be the most portable way to run the software.
This image will come with prepackaged internal dependencies. Also note that while the docker image will have all the internal dependencies (for example underlying crypto linux packages). you will still need to run the external dependencies yourself (for example databases and storage). We will build that docker image for the main platforms available out there (arm64, amd64).

## Docker Compose - Single Node
This is the first step to running teranode **with external dependencies**. This will be a docker-compose file that will run the docker image mentioned above and the external dependencies (db, utxostore, shared storage, etc). This will be the easiest way to run the software  **including external dependencies**. This will be published to the main repo (now called ubsv, #todo this needs to be changed to teranode before making it public).

This will be the easiest but opinionated way to run the software with external dependencies. You're gonna have to adopt the external dependencies already chosen for you.

This docker compose will be maintained as it will be the main development environment used by the devs and QA when they want to run one instance of the software. Do not expect this to be the most performant way to run the software nor the most persistent. This is meant for development and testing purposes only.

As part of the requirement of milestone 3, this will be one of the deliverable to hand to the miners for us to "train them" on using the software.

This docker compose will be running using the microservices architecture where each container is running only one process. This is to adhere to docker's best practices.
## Docker Compose - Multiple Nodes (Probably 3)
Similar as the one above but with multiple nodes. This will be the easiest way to run the blockchain(3 miners) **including external dependencies**  with multiple nodes.

You would want to run this if you want to experiment with the networking and communication in between nodes. Or for whatever reason you want to have a local blockchain to run some tests or experiments.
> Do not confuse this docker compose from the single node one. This is meant to run multiple nodes of the software. This is meant for development and testing purposes only!

This docker compose will be maintained as it will be the main development environment used by the devs and QA when they want to run more than one instance of the software. 

This docker compose will be running using the microservices architecture where each container is running only one process. This is to adhere to docker's best practices.

This satisfy Oli's argument that we need to have a software that's easy to run and experiment with. This is especially the case for alpha and beta testers who want to run a full blockchain without much hassle. These people will not be running at scale, they probably just want to valid their applications against the miner's api or experiment with its features. Same as above, this is an opinionated file. If you want to try with different external dependencies, you will have to figure out how to do so. 
## Testing Environment
Think of this environment as the staging environment that we need to run 3 miners continuously in a microservices settings. This is used by the engineering team to develop teranode. As a courtesy, the engineering team will be sharing a reference implementation. This will however not be maintained for the public and is subject to change at any time. 

Tags with the testing prefix `v` will release code to that environment. This is the equivalent to a develop or staging environment. This means that we're deploying 11 microservices for each miner making it a total of 33 microservices for the miners alone without external dependencies.

These microservices will be using the docker image mentioned above. #to_answer we might to split the binary rather than one big one, into smaller binaries for each of the services. 

In order to make our life easier, we have chosen to use kubernetes to orchestrate all this operation. You can definitely run these as a collection of binaries or docker containers but we have chosen kubernetes as it's the most flexible, scalable solution and industry standard.

> Kubernetes will be the only orchestration system that we will officially support. If you want to run the software in a different orchestration system(ECS, Mesos, OpenShift, Bare Metal), you will have to do it yourself.

Now this environment will consists of three parts:
- The underlying infrastructure
- Teranode Dependencies
- Teranode Microservices

### Infrastructure
This will be the kubernetes cluster that will run the teranode microservices. We will not be responsible for the underlying infrastructure management for the public. People have their weird and specific ways of running kubernetes. Let them figure that out.

For us internally, we will be using a collection of **private** terraform modules to manage the underlying infrastructure. You can find these modules in the ubsv-infra. The code will be structured in a way to allow fast iteration of multiple environments in multiple regions.
 
### Teranode Microservices
#### What's a Kubernetes Operator?
A Kubernetes Operator is a method of packaging, deploying, and managing a Kubernetes application. Operators use custom resources to manage applications and their components. Here are the key roles and functions of a Kubernetes Operator:
- **Automation of Operational Tasks:** Operators automate routine tasks such as deployment, scaling, and backup. They continuously monitor the state of the application and perform necessary actions to maintain the desired state.
- **Application-Specific Logic:** Operators encapsulate operational knowledge in code. This includes handling failure recovery, upgrades, and complex orchestration that is specific to the application. This includes things like scale the miner before the coinbase etc.
- **Lifecycle Management:** Operators manage the entire lifecycle of an application, from installation and configuration to updates and decommissioning.
- **State Management:** Operators maintain and reconcile the desired state of an application with the actual state, ensuring consistency and reliability.

#### Teranode Operator
Running 11 microservices that have a certain order to boot and complex relationship is not easy. Expecting people to have that knowledge out of the box is unrealistic. Therefore, we will be providing a kubernetes operator that will manage the lifecycle of the teranode microservices. This operator will be responsible for deploying, scaling, and managing the **teranode microservices**.

This is the industry standard to deploy complex application via kubernetes. You can find multiple big projects using the operator pattern:
- [Aerospike](https://github.com/aerospike/aerospike-kubernetes-operator)
- [Jaeger](https://github.com/jaegertracing/jaeger-operator)
- [Prometheus](https://github.com/prometheus-operator/prometheus-operator)
- [Elasticsearch](https://github.com/elastic/cloud-on-k8s)

The operator assumes that the dependencies are already running. The operator will not be responsible for managing the dependencies. The operator will be responsible for managing the teranode microservices only.

The operator can be installed via [helm chart](https://helm.sh/) or [OLM](https://olm.operatorframework.io/). Helm is the most popular way to deploy operators and widely supported and understood. OLM is a more recent way but is supposed to be better for managing multiple operators, version upgrades and rollbacks (specially challenging for CRDs and RBAC). 


### Teranode Dependencies
This will be all the external dependencies that the teranode(one miner node) microservices will need. This will be the databases, storage, and other services that the teranode microservices will need to run. 

This is a subject of big debate for multiple reasons. 
1. The way the code is written, the plugin architecture allow us to swap out the dependencies. This means that we can run multiple databases, storage, and other services.
   2. We need to first agree what will be supported and what will not be supported. This is a big decision as it will dictate the direction of the project. #to_answer
   3. Will people be able to write their own plugins? If someone wants to write a plugin for a database that we don't support, will they be able to do so? #to_answer: my assumption above was that yes they can
4. We need to agree on a way to run and manage these dependencies. This is a big decision as it will dictate the direction of the project. #to_answer
   5. There must be some sort of documentation and deployment to run these dependencies it on cloud agnostic services. We cannot be tied to a single cloud provider as by doing so we'll be tying the hands of the users. #to_answer this might be dropped as it might not be required 
   6. For our sake though, where we're at now, it seems like we want to leverage managed services on the cloud to run this. Do we try to move as much as possible to k8s, or is it okay to share IasC for the dependencies? #to_answer if we share IasC, we'll have to maintain it. If we move to k8s, we'll have to maintain it too but it might be easier to maintain as we won't have to support multiple providers. k8s offers a shared plane of operation vs terraform is customized for AWS, GCP, Azure, On Prem, etc.
   7. Other options are the on prem where it could completely change the way we run the software. #to_answer

Some suggestions are:
- Manage all dependencies via teraform. This could be hard to make public and cloud agnostic (view comment above about multiple providers)
- Create an opinionated helm chart that will run all the dependencies. This would allow people to have an easily installable and manageable dependencies. It will definitely not be the production way to run it, as you'll need to have your own automation on top of that but that could be something that developers would maintain. 
  - Take a look at the [Jaeger Helm Chart](https://github.com/jaegertracing/helm-charts/blob/main/charts/jaeger/values.yaml) for example. It allows you to deploy all the dependencies (cassandra and elastic search) from one file. Is it the best idea for prod deployment, probably not. You'd want to manage the dependencies yourself but it's a good way to get started. 
  - Looking at this any reasonable devops would realize that running a cassandra cluster within the jaeger deployment is not the best way. 
  - They'll figure out what would be the best way to do so. We could make it clear in the documentation that this is not the best way to run it but it's the easiest way to run it in k8s.
- Other ideas? 

#todo more research

## Main Net Environment
# Milestone 4: Mining on Main Net
At this stage, we will have a running teranode that's mining on main net. For the first time, we do not need to be running a full blockchain. However, this means we have some new dependencies to install.

So far, we've been using a miner from the provisioning team. #todo check with oli if this is fine to go to prod
# Beta Testing
During beta testing, we need to share with the focus group the production way to run the software (whatever we end up agreeing on). This will be a phase where they try to run it in prod but failure would be acceptable as we're still in beta. This will be a good time to gather feedback on the software and the documentation to harden the system and the deployment strategy  

#todo this section needs further development but for now wanted to focus on the milestone 3
# Milestone 5: Onboarding and Handover
#todo this section needs further development but for now wanted to focus on the milestone 3


 
