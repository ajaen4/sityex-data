import json

import pulumi
from pulumi_aws import iam, s3

from input_schemas import Input
from .job import Job


class LakeJobs:
    def __init__(self, baseline_stack_ref: pulumi.StackReference, input: Input):
        self.create_common_res(baseline_stack_ref)

        self.roles: dict[str, iam.Role] = dict()
        self.roles["translate"] = self.create_translate_role()
        self.roles["job"] = self.create_role()
        self.create_jobs(input, baseline_stack_ref)

    def create_common_res(self, baseline_stack_ref: pulumi.StackReference):
        stack_name = pulumi.get_stack()
        self.logger_script = s3.BucketObject(
            "logger-script",
            bucket=baseline_stack_ref.get_output("jobs_bucket_name"),
            key=f"{stack_name}/logger.py",
            source=pulumi.FileAsset("../job_scripts/logger.py"),
        )

    def create_role(self):
        stack_name = pulumi.get_stack()

        glue_role = iam.Role(
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
            policy=self.roles["translate"].arn.apply(
                lambda translate_role_arn: json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Action": ["logs:*", "s3:*", "cloudwatch:*"],
                                "Effect": "Allow",
                                "Resource": "*",
                            },
                            {
                                "Action": ["iam:PassRole"],
                                "Effect": "Allow",
                                "Resource": translate_role_arn,
                            },
                        ],
                    }
                )
            ),
        )

        iam.RolePolicyAttachment(
            "job-policy-attachment",
            role=glue_role.name,
            policy_arn=job_policy.arn,
        )

        return glue_role

    def create_translate_role(self):
        stack_name = pulumi.get_stack()

        translate_role = iam.Role(
            "translate-role",
            name=f"translate-role-{stack_name}",
            assume_role_policy={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Principal": {"Service": "translate.amazonaws.com"},
                        "Effect": "Allow",
                    },
                ],
            },
        )

        translate_policy = iam.Policy(
            "translate-policy",
            name=f"translate-policy-{stack_name}",
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": ["s3:*", "translate:*"],
                            "Effect": "Allow",
                            "Resource": "*",
                        },
                    ],
                }
            ),
        )

        iam.RolePolicyAttachment(
            "translate-policy-attachment",
            role=translate_role.name,
            policy_arn=translate_policy.arn,
        )

        return translate_role

    def create_jobs(
        self, input: Input, baseline_stack_ref: pulumi.StackReference
    ) -> dict[dict]:
        self.jobs: list[Job] = list()
        for job_cfg in input.jobs_cfgs:
            self.jobs.append(
                Job(job_cfg, self.roles, self.logger_script, baseline_stack_ref)
            )

    def get_resources(self) -> dict[str, dict]:
        resources = dict()
        for job in self.jobs:
            resources.update(job.get_resource_mapping())
        return resources
