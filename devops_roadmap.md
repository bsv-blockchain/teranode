_this document is meant for internal purposes only for the dev and devops team of teranode. do not share publicly_
# Devops Roadmap & Direction Setting

The goal of this document is to provide a roadmap for the devops team to follow. This roadmap will be broken down into milestones, aligning with the milestone defined by the business. The goal is to stir a conversation regarding the trajectory of the devops developments and understand what needs to be implemented when.

Note while these are not exactly part of the deliverable for the milestone, these tasks and products will accompany the deliverable to better support the business and people using it.

# Milestone 3: Listening on Main Net
## UBSV Binary
The ubsv repo where the current teranode code resides will be open sourced. This repo will include a runnable version of the teranode code as well as ci/cd to build the executable. 
- People will have the option to build from source. This will be available at later stages as we will need to have a stable version of the software before we can publish it.
- People will also have the ability to run pre-built and published executable for both arm64 and amd64. This will be deployed to a package manager probably github packages.

At the end of the day, this is the core of what we're building and having an executable will be the most important thing to support. As a user, you will always have the ability to take that binary executable and run it.

> Teranode is a complex piece of software that requires multiple dependencies. Our job is not to hold hands for developers but to provide the tools, resources and documentation to make it easier for them to build and run the software. 
> 
> For example, if you want to run aerospike, you have multiple options to do so. From a binary, docker, docker-compose, kubernetes etc. Choosing the right solution is up to you, depending on your needs. 
> 
> There will be a learning curve that people will have to go through to understand how to run the software. Our job is to make this as simple as possible but not to hold hands.

The binary has the ability to run one or more service via the settings file (#todo we need to define this).

If you want to run this binary, you will be responsible to have all the dependencies installed. Choosing these internal and external dependencies is an opinionated topic and will be decided based on what type of deployment you want to have. If you decide to go down that route, you will need to run these external services yourself.

This satisfy simon's argument that we will still need to have a binary that can run on a single machine. This will be the easiest way to run the software and the most portable way as you don't need to learn docker or kubernetes or any extra complexity to run the software.
## Docker Image
Along with the binary, we will also provide a docker image. This docker image will be published to a docker registry. This will be the easiest way to run the software.
This image will come with prepackaged internal dependencies. This will be the easiest way to run the software. Also note that while the docker image will have all the internal dependencies (for example underlying crypto linux packages), you will still need to run the external dependencies yourself (for example databases and storage).

## Docker Compose - Single Node
This is the first step to running teranode with external dependencies. This will be a docker-compose file that will run the docker image and the external dependencies. This will be the easiest way to run the software with external dependencies. This will be published to the ubsv-dev repo.

This will be the easiest but opinionated way to run the software with external dependencies. However, you're gonna have to adopt the chosen external dependencies. 
This docker compose will be maintained as it will be the main development environment used by the devs and QA when they want to run one instance of the software. Do not expect this to be the most performant way to run the software nor the most persistent. This is meant for development and testing purposes only.

## Docker Compose - Multiple Nodes (Probably 3)
Similar as the one above but with multiple nodes. This will be the easiest way to run the software with external dependencies with multiple nodes.

You would want to run this if you want to experiment with the networking and communication in between nodes. Or for whatever reason you want to have a local blockchain to run some tests or experiments. 

This docker compose will be maintained as it will be the main development environment used by the devs and QA when they want to run more than one instance of the software. Do not expect this to be the most performant way to run the software nor the most persistent. This is meant for development and testing purposes only.

This satisfy Oli's argument that we need to have a software that's easy to run and experiment with. This is especially the case for alpha and beta testers who want to run the software without much hassle. These people will not be running at scale, they probably just want to valid their applications against the miner's api. 
## Testing Environment
Up until now, all these previous environments are running multiple processes in a single docker container. We understand that this is not ideal but as it's for development purposes then it's a risk we're accepting. A reminder, you should not run this configuration in production. 

Think of this environment as the staging environment that we need to run 3 miners continuously in a microservices settings. Tags with the testing prefix `v` will release code to that environment. This is the equivalent to a develop or staging environment. This means that we're deploying 11 microservices for each miner making it a total of 33 microservices for the miners alone without external dependencies.
These micro services will be using the docker image mentioned above.

In order to make our life easier, we have chosen to use kubernetes to orchestrate this operation. You can definitely run these as a collection of binaries or docker containers but we have chosen kubernetes as it's the most flexible and scalable solution.

> Kubernetes will be the only orchestration system that we will support. If you want to run the software in a different orchestration system(ECS, Mesos, OpenShift, Bare Metal), you will have to do it yourself.

Now this environment will consists of three parts:
- The underlying infrastructure
- Teranode Dependencies
- Teranode Microservices

### Infrastructure
This will be the kubernetes cluster that will run the teranode microservices. We will not be responsible for the underlying infrastructure management for the public. People have their weird and specific ways of running kubernetes. Let them figure that out.

For us internally, we will be using a collection of **private** terraform modules to manage the underlying infrastructure. You can find these modules in the ubsv-infra. The code will be structured in a way to allow fast iteration of multiple environments in multiple regions.
 
### Teranode Microservices
Running 11 microservices that have a certain order and relationship is not easy. Expecting people to have that knowledge out of the box is unrealistic. Therefore, we will be providing a kubernetes operator that will manage the lifecycle of the teranode microservices. This operator will be responsible for deploying, scaling, and managing the **teranode microservices**.

The operator assumes that the dependencies are already running. The operator will not be responsible for managing the dependencies. The operator will be responsible for managing the teranode microservices only.

This is the industry standard to deploy complex application via kubernetes. You can find multiple big projects using it:
- [Aerospike](https://github.com/aerospike/aerospike-kubernetes-operator)
- [Jaeger](https://github.com/jaegertracing/jaeger-operator)
- [Prometheus](https://github.com/prometheus-operator/prometheus-operator)
- [Elasticsearch](https://github.com/elastic/cloud-on-k8s)

A Kubernetes Operator is a method of packaging, deploying, and managing a Kubernetes application. Operators use custom resources to manage applications and their components. Here are the key roles and functions of a Kubernetes Operator:
- **Automation of Operational Tasks:** Operators automate routine tasks such as deployment, scaling, and backup. They continuously monitor the state of the application and perform necessary actions to maintain the desired state.
- **Application-Specific Logic:** Operators encapsulate operational knowledge in code. This includes handling failure recovery, upgrades, and complex orchestration that is specific to the application. This includes things like scale the miner before the coinbase etc.
- **Lifecycle Management:** Operators manage the entire lifecycle of an application, from installation and configuration to updates and decommissioning.
- **State Management:** Operators maintain and reconcile the desired state of an application with the actual state, ensuring consistency and reliability.

### Teranode Dependencies
This will be all the external dependencies that the teranode microservices will need. This will be the databases, storage, and other services that the teranode microservices will need to run. 

This is a subject of big debate for multiple reasons. 
1. The way the code is written, the plugin architecture allow us to swap out the dependencies. This means that we can run multiple databases, storage, and other services.
   2. We need to first agree what will be supported and what will not be supported. This is a big decision as it will dictate the direction of the project. #to_answer
   3. Will people be able to write their own plugins? If someone wants to write a plugin for a database that we don't support, will they be able to do so? #to_answer
4. We need to agree on a way to run and manage these dependencies. This is a big decision as it will dictate the direction of the project. #to_answer
   5. There must be some sort of documentation and deployment to run these dependencies it on cloud agnostic services. We cannot be tied to a single cloud provider as by doing so we'll be tying the hands of the users. #to_answer this might be dropped as it might not be required 
   6. For our sake though, where we're at now, it seems like we want to leverage managed services on the cloud to run this
   7. Other options are the on prem where it could completely change the way we run the software. #to_answer

Some suggestions are:
- Manage all dependencies via teraform. This could be hard to make public and cloud agnostic
- Create an opinionated helm chart that will run all the dependencies. This would allow people to have an easily installable and manageable dependencies. It will definitely not be the production way to run it, as you'll need to have your own automation on top of that but that could be something that developers would maintain. 
  - Take a look at the jhttps://github.com/jaegertracing/helm-charts/blob/main/charts/jaeger/values.yaml for example. It allows you to deploy all the dependencies (cassandra and elastic search) from one file. 
  - Looking at this any reasonable devops would realize that running a cassandra cluster within the jaeger deployment is not the best way. 
  - They'll figure out what would be the best way to do so. We could make it clear in the documentation that this is not the best way to run it but it's the easiest way to run it.  

#todo more research
### Reference Implementation
## Main Net Environment
# Milestone 4: Mining on Main Net

# Milestone 5: Onboarding and Handover

# Alpha Testing
I am not exactly sure when this will happen in relations to the milestones, but I am sure that it will happen. The goal of alpha testing is to ensure that the product is ready for beta testing. This will be done by the devops team and the development team. The goal is to ensure that the product is ready for beta testing.
We will open up the product for alpha testers to use and give us feedback. This is useful to define separately as it helps us dictate the minimum requirements to support a viable alpha test. 

 
# Beta Testing