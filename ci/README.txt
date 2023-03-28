** Please note the Amazon Redshift cluster is not automatically paused at this time, so please remember to pause it once
testing is complete to avoid additional charges to your AWS account **

ci-pipeline.yml
Prerequisites: Creating a CFN stack from this template requires a pre-existing AWS CodeCommit repository has been
configured for sqlalchemy-redshift, and its ARN has been included in the template. See "CodeCommitRepoARN" parameter
for more details.

This CFN template creates an AWS CodePipeline including the following attributes
- A VPC
- A persistent Amazon Redshift cluster, only accessible via said VPC
- A simple build step (build actions defined in build-spec.yml), which can access the Amazon Redshift cluster
- An AWS Secret used for storing Amazon Redshift credentials and passing them to the AWS CodeBuild project
- IAM roles needed for CI infrastructure and test suite
- Pipeline triggers on commits to the main branch

Environment variables needed for integration test suite are passed to the build step via AWS Secrets Manager or
AWS CodeBuild project environment variables.

** Please note the Amazon Redshift cluster is not automatically paused at this time, so please remember to pause it once
testing is complete to avoid additional charges to your AWS account **

build-spec.yml
This AWS CodeBuild build spec file defines what occurs during the AWS CodePipeline build step. A persistent Amazon
Redshift cluster is configured for tests to be run against.


