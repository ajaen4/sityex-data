import json

import pulumi
from pulumi_aws import iam, s3, glue, cloudwatch

from input_schemas import Input, JobConfig


class LakeJobs:
    def __init__(self, baseline_stack_ref: pulumi.StackReference, input: Input):
        self.baseline_stack_ref = baseline_stack_ref
        self.create_role()
        self.create_common_res()
        self.resources = self.create_jobs(input)

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
                    },
                    {
                        "Action": "sts:AssumeRole",
                        "Principal": {"Service": "translate.amazonaws.com"},
                        "Effect": "Allow",
                    },
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
                            "Action": ["logs:*", "s3:*", "cloudwatch:*", "translate:*"],
                            "Effect": "Allow",
                            "Resource": "*",
                        },
                        {
                            "Action": ["iam:PassRole"],
                            "Effect": "Allow",
                            "Resource": "arn:aws:iam::744516196303:role/service-role/AmazonTranslateServiceRole-test-DELETE",
                        },
                    ],
                }
            ),
        )

        iam.RolePolicyAttachment(
            "job-policy-attachment",
            role=self.glue_role.name,
            policy_arn=job_policy.arn,
        )

    def create_jobs(self, input: Input) -> dict[dict]:
        resources = dict()
        for job_cfg in input.jobs_cfgs:
            resources.update(self._create_job(job_cfg))
        return resources

    def _create_job(self, job_cfg: JobConfig) -> dict:
        stack_name = pulumi.get_stack()

        job_name = job_cfg.job_name
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

        if job_cfg.additional_python_modules:
            add_python_modules = {
                "--additional-python-modules": ",".join(
                    job_cfg.additional_python_modules
                ),
            }
        else:
            add_python_modules = {}

        job_arguments = pulumi.Output.all(
            log_group_name=log_group.name,
            data_bucket_name=self.baseline_stack_ref.get_output("data_bucket_name"),
            jobs_bucket_name=self.baseline_stack_ref.get_output("jobs_bucket_name"),
            logger_script_key=self.logger_script.key,
        ).apply(
            lambda args: {
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-enable",
                "--continuous-log-logGroup": args["log_group_name"],
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-continuous-log-filter": "true",
                "--enable-metrics": "",
                "--DATA_BUCKET_NAME": args["data_bucket_name"],
                "--extra-py-files": f's3://{args["jobs_bucket_name"]}/{args["logger_script_key"]}',
                **add_python_modules,
                **job_cfg.args,
            }
        )

        glue_job = glue.Job(
            f"{job_name}-job",
            glue.JobArgs(
                name=f"{job_name}-{stack_name}",
                role_arn=self.glue_role.arn,
                glue_version="4.0",
                number_of_workers=job_cfg.number_of_workers,
                worker_type="G.1X",
                default_arguments=job_arguments,
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

        cron_expression = job_cfg.cron_expression

        if cron_expression:
            glue.Trigger(
                f"{job_cfg.job_name}-trigger",
                name=f"{job_cfg.job_name}-trigger-{stack_name}",
                actions=[glue.TriggerActionArgs(job_name=glue_job.name)],
                schedule=cron_expression,
                type="SCHEDULED",
            )

        return {job_name: {"type": "glue_job", "job": glue_job}}

    def get_resources(self) -> dict[dict]:
        return self.resources
