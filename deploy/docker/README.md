# Build/Run Workflow Update documentation

UBSV uses intermediate Docker layers in order increase efficiency of the overall build and deployment process.

The build process uses `Dockerfile_build_base`
The application runtime environment uses `Dockerfile_run_base`

When there are changes made to either of these files, a new image must be built and incorporated into the application.
This is accomplished by using two separate GitHub Actions workflows within the ubsv GitHub repo:
- [build_base_image.yaml](https://github.com/bitcoin-sv/ubsv/actions/workflows/build_base_image.yaml)
- [build_run_image.yaml](https://github.com/bitcoin-sv/ubsv/actions/workflows/build_run_image.yaml)

You can manually start one of these jobs by clicking on the workflow in the GitHub Actions interface and then clicking the "Run workflow" button and selecting a branch to run the job on.

A new container image will be pushed to the EU-North-1 (Stockholm) Elastic Container Repository (ECR). The image name will contain the short git commit hash identifier.

The new hash identifier(s) must be added to two files located in this folder:

`CLUSTER_BASE_ID`
`CLUSTER_RUN_ID`

The contents of these files should match the short SHA1 hash of the latest build or run image. If both workflows are run from the same commit, this is likely to be the same value for both as long as both jobs were successful.

When those files are updated, you must tag appropriately and commit them into the repo and kick off a full deployment build. The main Dockerfile will pull in the correct images based on these identifiers. It is also setup with a set of hard-coded defaults which are old images we pushed in the past. The buildx process within the main GKE workflows pull the data from these files to get the latest images, and then override these arguments in the main Dockerfile.
