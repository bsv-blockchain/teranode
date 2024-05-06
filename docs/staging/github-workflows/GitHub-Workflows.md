
## GitHub Workflow Documentation: test-build-deploy

### Overview
The `test-build-deploy` GitHub workflow is defined in the `gke.yaml` file, and it is triggered on two specific conditions:
1. Push events to the tags that match 'v*' or 'scaling-v*'.
2. Push events to the branches: 'master' and 'staging'.

### Environment Variables
- `REPO`: Specifies the repository name, set to 'ubsv'.
- `ECR_REGION`: Specifies the AWS ECR region, set to 'eu-north-1'.

### Jobs

#### 1. `get_tag`
- **Purpose**: Determines the deployment tag based on the GitHub event (tag or SHA).
- **Runner**: Ubuntu latest version.
- **Steps**:
- Check if the event is a tag and export it; otherwise, use the SHA.
- **Outputs**:
- `deployment_tag`: The determined tag or SHA.

#### 2. `test_and_lint`
- **Purpose**: Run tests and perform code linting.
- **Uses**: Reference to `.github/workflows/gke_tests.yaml`.
- **Secrets**: Inherits all secrets from the context.

#### 3. `build_amd64`
- **Purpose**: Build the Docker image for AMD64 architecture.
- **Dependencies**: Depends on the `get_tag` job to fetch the tag.
- **Uses**: Reference to `.github/workflows/gke_build.yaml`.
- **Parameters**:
- Architecture, repository, tag, region.

#### 4. `build_arm64`
- **Purpose**: Build the Docker image for ARM64 architecture.
- **Dependencies**: Depends on the `get_tag` job.
- **Uses**: Same as `build_amd64` but with ARM64 architecture.

#### 5. `create_manifest`
- **Purpose**: Create a Docker manifest for multi-architecture support.
- **Dependencies**: Depends on `test_and_lint`, `build_amd64`, `build_arm64`, and `get_tag`.
- **Uses**: Reference to `.github/workflows/gke_manifest.yaml`.
- **Parameters**:
- Region, repository, tag.

#### 6. `deploy`
- **Purpose**: Deploy the application using the created Docker image.
- **Dependencies**: Depends on `create_manifest` and `get_tag`.
- **Uses**: Reference to `.github/workflows/gke_deploy.yaml`.
- **Parameters**:
- Repository, tag.


### The `test_and_lint` Job

The `gke_tests.yaml` GitHub workflow is designed to be invoked through a workflow call. This workflow is focused on running linting and testing for Go code, as well as performing a SonarQube scan for code quality analysis.

#### Jobs

##### 1. `go-lint-and-test`
- **Purpose**: Performs linting and testing of Go code.
- **Runner**: Custom runner labeled `ubsv-runner`.
- **Strategy**:
    - `fail-fast`: True (If any job fails, the entire workflow will terminate).
- **Steps**:
    - **Checkout**: Checks out the source code at the full depth to ensure better relevancy of reporting and to support tools that require complete git history.
    - **Set up Go**: Sets up Go environment with specified version `1.21.0` using the `actions/setup-go@v5` action.
    - **Run Go tests**: Executes long-running Go tests using `make longtests`. This likely includes integration and other extensive testing procedures.

##### 2. `sonarqube`
- **Purpose**: Runs a SonarQube scan to analyze the code quality and detect bugs, vulnerabilities, and code smells.
- **Runner**: Uses the same custom runner `ubsv-runner`.
- **Steps**:
    - **Checkout**: Performs a repository checkout similar to the lint and test job.
    - **Twingate Action**: Establishes a secure connection using Twingate, necessary for accessing internal resources during the CI process.
    - **SonarQube Scan**:
        - Utilizes the `sonarsource/sonarqube-scan-action@v2.0.1`.
        - Configured with the SonarQube server URL and a token for authentication, facilitating the scanning of the checked-out code.




### The `build_amd64` and `build_arm64` Job

The `gke_build.yaml` workflow is designed to build Docker images for specified architectures using AWS services, specifically targeting Amazon Elastic Container Registry (ECR). It is triggered via a `workflow_call` event, allowing it to be invoked by other workflows that provide the necessary parameters.

#### Trigger
- **Event**: Triggered by a `workflow_call`.
- **Inputs**:
    - `repo`: Repository name.
    - `tag`: Deployment tag.
    - `region`: AWS region where operations will be performed.
    - `arch`: Architecture for which the Docker image will be built, such as 'amd64' or 'arm64'.

#### Job: `build-docker`
- **Purpose**: Builds and pushes a Docker image to AWS ECR based on the input parameters.
- **Runner**: Dynamically determined based on the architecture input; supports both 'ARM64' and 'X64' on self-hosted Linux runners.
- **Strategy**:
    - `fail-fast`: True. If any step fails, the entire job will terminate.
- **Steps**:
    1. **Checkout**: Clones the repository using `actions/checkout@v4`.
    2. **Setup Docker Buildx**: Configures Docker Buildx for building multi-architecture Docker images using `docker/setup-buildx-action@v3`.
    3. **Configure AWS CLI**: Sets up AWS CLI credentials to interact with AWS services using `aws-actions/configure-aws-credentials@v4`.
    4. **Login to Amazon ECR**: Authenticates to Amazon ECR using `aws-actions/amazon-ecr-login@v2`.
    5. **Get Cluster Base ID**: Retrieves the ID for the base image from a predefined file.
    6. **Get Cluster Run ID**: Retrieves the ID for the run image from a predefined file.
    7. **Build and Push Docker Image**:
        - Executes the Docker build and push using `docker/build-push-action@v5`.
        - Utilizes build arguments to include specific settings like SHA, debug flags, and references to the base and run images.


### The `deploy` Job

The `gke_deploy.yaml` GitHub workflow is designed for deploying Docker images to Amazon Elastic Kubernetes Service (EKS) across different geographic regions based on the deployment tag. This workflow is invoked via a `workflow_call`, making it reusable across multiple projects or pipelines.

#### Trigger
- **Event**: Triggered by a `workflow_call`.
- **Inputs**:
    - `repo`: The repository name for which the deployment is being made.
    - `tag`: The specific deployment tag associated with the Docker image.

#### Environment Variables
- `ECR_URL`: Specifies the Amazon ECR URL where the Docker images are hosted.

#### Jobs

##### 1. `send_msg`
- **Purpose**: Logs the deployment details.
- **Runner**: Ubuntu latest version.
- **Steps**:
    - Logs the full repository and tag being deployed.
    - Logs the GitHub event reference.

##### 2. Regional Deployment Jobs
These jobs deploy the Docker image to specific AWS EKS clusters based on the region and the type of deployment tag. The deployment is conditional on the tag reference starting with specific prefixes.

- **Common Settings**:
    - **Runner**: Uses a reusable workflow located at `.github/workflows/deploy-to-region.yaml`.
    - **Secrets**: Inherits all secrets from the parent workflow.
    - **Conditions**: Each job has a conditional start based on the prefix of the GitHub event ref (e.g., 'refs/tags/v' for version releases, 'refs/tags/scaling-v' for scaling deployments).
    - **Parameters**:
        - `region`: Specifies the AWS region for deployment, such as `eu-west-1`, `us-east-1`, `ap-south-1`, etc.
        - `ubsv_env`: Specifies the environment configuration, either `allinone` or `scaling`.




### The `create_manifest` Job

The `gke_manifest.yaml` GitHub workflow is designed to create Docker manifests for multi-architecture Docker images. This process ensures that Docker images are available across different computing architectures, such as AMD64 and ARM64. The workflow is triggered via a `workflow_call`, making it reusable and modular, suitable for integration into various CI/CD pipelines.

#### Trigger
- **Event**: Triggered by a `workflow_call`.
- **Inputs**:
    - `repo`: The repository name where the Docker images are stored.
    - `tag`: The specific tag associated with the Docker images.
    - `region`: The AWS region where the Amazon ECR resides.

#### Job: `build_manifest`
- **Purpose**: To create a Docker manifest that represents an image available in multiple architectures.
- **Runner**: Ubuntu latest version.
- **Strategy**:
    - `fail-fast`: True. The workflow will terminate if any step fails.
- **Outputs**:
    - `registry`: The Amazon ECR registry URL returned from the login step.
- **Steps**:
    1. **Twingate VPN Setup**: Establishes a secure connection for actions that might require access to protected resources using Twingate's GitHub action.
    2. **AWS CLI Configuration**: Configures AWS credentials to interact with AWS services.
    3. **Amazon ECR Login**: Authenticates with Amazon ECR to allow subsequent operations such as pushing or pulling Docker images.
    4. **Deployment Message Logging**: Logs a deployment message indicating the creation of the Docker manifest for the given tag.
    5. **Create Docker Manifest**:
        - Utilizes `int128/docker-manifest-create-action@v1` to create a Docker manifest.
        - Configures the action to create a manifest that includes images with suffixes `-amd64` and `-arm64`, thus supporting these two architectures.
