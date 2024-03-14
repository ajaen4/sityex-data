import json

import pulumi
from pulumi_aws import iam

from input_schemas import Input
from .container import Container


class ContainerTasks:
    def __init__(
        self,
        baseline_stack_ref: pulumi.StackReference,
        input: Input,
    ):
        self.baseline_stack_ref = baseline_stack_ref
        self.input = input
        self.resources = self.create_resources()

    def create_resources(self) -> dict[dict]:
        self.create_role()

        self.containers: list[Container] = list()
        for container_cfg in self.input.containers_cfg:
            self.containers.append(
                Container(container_cfg, self.task_exec_role, self.baseline_stack_ref)
            )

    def create_role(self):
        stack_name = pulumi.get_stack()

        self.task_exec_role = iam.Role(
            "task-exec-role",
            name=f"task-exec-role-{stack_name}",
            assume_role_policy={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Effect": "Allow",
                        "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                    }
                ],
            },
        )

        task_exec_policy = iam.Policy(
            "task-exec-policy",
            name=f"task-exec-policy-{stack_name}",
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": ["logs:*", "ecr:*", "s3:*", "ssm:*"],
                            "Effect": "Allow",
                            "Resource": "*",
                        }
                    ],
                }
            ),
        )

        iam.RolePolicyAttachment(
            "task-exec-policy-attachment",
            role=self.task_exec_role.name,
            policy_arn=task_exec_policy.arn,
        )

    def get_resources(
        self, baseline_stack_ref: pulumi.StackReference
    ) -> dict[str, dict]:
        resources = dict()
        for container in self.containers:
            resources.update(container.get_resource_mapping(baseline_stack_ref))
        return resources
