<p align="center">
<img src="https://d1dshnpqadx0e7.cloudfront.net/images/logos/big_logo_blue.png">
</p>

# SityEx

[SityEx](https://sityex.com) is a one-stop platform for expats living in Madrid, Spain.

This project's objective is to contain all the back-end data ingestion and processing and the CI/CD to deploy to Vercel. Technologies used: 
- Python
- Pulumi
- Firebase
- AWS

# Project Structure

The project's structure is the following:

- .github: GitHub Actions workflows. 
- containers: contains every container used in the data pipeline. Each container has its own Dockerfile and folder.
- db_scripts: one-shot scripts to use on our Firebase database.
- internal_lib: sityex internal library. Contains code to interact with APIs, AWS, files, GCP and parse data.
- job_scripts: PySpark jobs to process data. These are used as the scripts for our glue jobs.
- local_scripts: scripts to execute the container's code locally for testing purposses.
- pulumi: Pulumi IaC code.
- tests: pytests unit tests.
