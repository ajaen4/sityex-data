import json

import pulumi
from pulumi_aws import iam

from input_schemas import Input
from orchestrators import Orchestrator


class Orchestrators:
    def __init__(
        self,
        baseline_stack_ref: pulumi.StackReference,
        input: Input,
        all_resources: dict[dict],
    ):
        self.all_resources = all_resources
        self.baseline_stack_ref = baseline_stack_ref

        self.create_role()
        self.create_orchests(input)

    def create_role(self):
        stack_name = pulumi.get_stack()

        self.orchestrator_role = iam.Role(
            "orchestrator-role",
            name=f"orchestrator-role-{stack_name}",
            assume_role_policy={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Effect": "Allow",
                    },
                ],
            },
        )

        orchestrator_policy = iam.Policy(
            "orchestrator-policy",
            name=f"orchestrator-policy-{stack_name}",
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": [
                                "ecs:*",
                                "glue:*",
                                "states:*",
                                "logs:*",
                                "events:*",
                                "iam:PassRole",
                            ],
                            "Effect": "Allow",
                            "Resource": "*",
                        },
                    ],
                }
            ),
        )

        iam.RolePolicyAttachment(
            "orchestrator-policy-attachment",
            role=self.orchestrator_role.name,
            policy_arn=orchestrator_policy.arn,
        )

    def create_orchests(self, input: Input):
        for orchestrator_cfg in input.orchestrators_cfgs:
            Orchestrator(
                self.baseline_stack_ref,
                orchestrator_cfg,
                self.orchestrator_role,
                self.all_resources,
            )
