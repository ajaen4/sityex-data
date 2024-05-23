<p align="center">
<img src="https://d1dshnpqadx0e7.cloudfront.net/images/logos/big_logo_blue.png">
</p>

# SityEx

## Introduction

[SityEx](https://sityex.com) is a one-stop platform for expats living in Madrid, Spain.

This project's objective is to contain all the back-end data ingestion and processing and the CI/CD to deploy to AWS.

## Tech Stack

Technologies used: 
- Python
- Pulumi
- AWS

## Requirements

- You must own an AWS account.

## Deployment

First, you will need to create and activate a new virtual environment to hold the project's dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

To install the project's dependencies:

```bash
make install
```


To run the tests:

```bash
pytest
```

To deploy you need an active AWS account and valid credentials, you must also install the AWS cli. You will also need to set up Pulumi, see the [Getting Started Guide](https://www.pulumi.com/docs/clouds/aws/get-started/). Create a new project with two stacks, dev and main.


Once you have those you can go into the iac folder and run the following commands to create a pulumi stack and deploy the infrastructure:

```bash
cd iac/
pulumi up
```


# Project Structure

The project's structure is the following:

- .github: GitHub Actions workflows. 
- containers: contains every container used in the data pipeline. Each container has its own Dockerfile and folder.
- db_scripts: one-shot scripts to use on our Firebase database.
- internal_lib: sityex internal library. Contains code to interact with APIs, AWS, files, GCP and parse data.
- job_scripts: PySpark jobs to process data. These are used as the scripts for our glue jobs.
- local_scripts: scripts to execute the container's code locally for testing purposses.
- iac: Pulumi IaC code.
- tests: pytest unit tests.
