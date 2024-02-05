# Build/Run Workflow Update documentation

UBSV uses intermediate Docker layers in order increase efficiency of the overall build and deployment process.

The build process uses `Dockerfile_build_base`
The application runtime environment uses `Dockerfile_run_base`

When there are changes made to either of these files, a new image must be built and incorporated into the application. This is accomplished by enabling two separate GitHub Actions workflows within the ubsv GitHub repo. When these jobs are activated, any commits pushed to any branch will kick off the update process. These jobs will run independently of any other configured workflows. 

Steps to disable jobs:

1) Go to URL: <https://github.com/bitcoin-sv/ubsv/actions>
2) Click on "Build base container image" or "Build run container image" to the left under Actions > All workflows.
3) The workflow should show in yellow as disabled manually. Click "Enable Workflow".
4) Commit a change and push to the repo to trigger the workflow.

When the jobs are complete, *be sure to disable the workflow jobs* in the GitHub Actions interface so they are not run every time code is pushed to the repo.

A new container image will be pushed to the EU-North-1 (Stockholm) Elastic Container Repository (ECR). The image name will contain the short git commit hash identifier.

The new hash identifier(s) must be added to two files located in this folder:

`CLUSTER_BASE_ID`
`CLUSTER_RUN_ID`

The contents of these files should match the short SHA1 hash of the latest build or run image. If both workflows are run from the same commit, this is likely to be the same value for both as long as both jobs were successful.

When those files are updated, you must tag appropriately and commit them into the repo and kick off a full deployment build. The main Dockerfile will pull in the correct images based on these identifiers. It is also setup with a set of hard-coded defaults which are old images we pushed in the past. The buildx process within the main GKE workflows pull the data from these files to get the latest images, and then override these arguments in the main Dockerfile.

*MAKE SURE YOU DISABLE THE WORKFLOW ACTIONS WHEN COMPLETE!*
