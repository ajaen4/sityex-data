import json

import pulumi
from pulumi_aws import iam, s3, glue, cloudwatch

from input_schemas import Input, JobConfig


class LakeJobs:
    def __init__(self, baseline_stack_ref: pulumi.StackReference, input: Input):
        self.baseline_stack_ref = baseline_stack_ref
        self.create_role()
        self.create_common_res()
        self.create_jobs(input)

    def create_common_res(self):
        stack_name = pulumi.get_stack()
        self.logger_script = s3.BucketObject(
            "logger-script",
            bucket=self.baseline_stack_ref.get_output("jobs_bucket_name"),
            key=f"{stack_name}/logger.py",
            source=pulumi.FileAsset("../job_scripts/logger.py"),
        )

    def create_role(self):
        stack_name = pulumi.get_stack()

        self.glue_role = iam.Role(
            "glue-job-role",
            name=f"glue-job-role-{stack_name}",
            assume_role_policy={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Principal": {"Service": "glue.amazonaws.com"},
                        "Effect": "Allow",
                    }
                ],
            },
        )

        job_policy = iam.Policy(
            "job-policy",
            name=f"job-policy-{stack_name}",
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": ["logs:*", "s3:*", "cloudwatch:*"],
                            "Effect": "Allow",
                            "Resource": "*",
                        }
                    ],
                }
            ),
        )

        iam.RolePolicyAttachment(
            "job-policy-attachment",
            role=self.glue_role.name,
            policy_arn=job_policy.arn,
        )

    def create_jobs(self, input: Input):
        for job_config in input.jobs_configs:
            self._create_job(job_config)

    def _create_job(self, job_config: JobConfig):
        stack_name = pulumi.get_stack()

        job_name = job_config.job_name
        job_script = s3.BucketObject(
            f"{job_name}-script",
            bucket=self.baseline_stack_ref.get_output("jobs_bucket_name"),
            key=f"{stack_name}/{job_name}.py",
            source=pulumi.FileAsset(f"../job_scripts/{job_name}.py"),
        )

        log_group = cloudwatch.LogGroup(
            f"{job_name}-log-group",
            name=f"{stack_name}-{job_name}-log-group",
            retention_in_days=30,
        )

        glue_job = glue.Job(
            f"{job_name}-job",
            glue.JobArgs(
                name=f"{job_name}-{stack_name}",
                role_arn=self.glue_role.arn,
                glue_version="4.0",
                number_of_workers=job_config.number_of_workers,
                worker_type="G.1X",
                default_arguments={
                    "--job-language": "python",
                    "--job-bookmark-option": "job-bookmark-enable",
                    "--continuous-log-logGroup": log_group.name.apply(
                        lambda name: name
                    ),
                    "--enable-continuous-cloudwatch-log": "true",
                    "--enable-continuous-log-filter": "true",
                    "--enable-metrics": "",
                    "--DATA_BUCKET_NAME": self.baseline_stack_ref.get_output(
                        "data_bucket_id"
                    ).apply(lambda bucket_name: bucket_name),
                    "--extra-py-files": pulumi.Output.all(
                        bucket_name=self.baseline_stack_ref.get_output(
                            "jobs_bucket_name"
                        ),
                        script_key=self.logger_script.key,
                    ).apply(
                        lambda args: f's3://{args["bucket_name"]}/{args["script_key"]}'
                    ),
                    **job_config.args,
                },
                command=glue.JobCommandArgs(
                    script_location=pulumi.Output.all(
                        bucket_name=self.baseline_stack_ref.get_output(
                            "jobs_bucket_name"
                        ),
                        script_key=job_script.key,
                    ).apply(
                        lambda args: f's3://{args["bucket_name"]}/{args["script_key"]}'
                    ),
                    python_version="3",
                ),
            ),
        )

        cron_expression = job_config.cron_expression

        if cron_expression:
            glue.Trigger(
                f"{job_config.job_name}-trigger",
                name=f"{job_config.job_name}-trigger-{stack_name}",
                actions=[glue.TriggerActionArgs(job_name=glue_job.name)],
                schedule=cron_expression,
                type="SCHEDULED",
            )
