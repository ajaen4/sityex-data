import json

import pulumi
from pulumi_aws import ecs, iam, cloudwatch

from .repository import Repository
from .image import Image

from input_schemas import ContainerConfig, SubnetType
from resource_types import ResourceTypes


class Container:
    def __init__(
        self,
        container_cfg: ContainerConfig,
        task_exec_role: iam.Role,
        baseline_stack_ref: pulumi.StackReference,
    ):
        self.container_cfg = container_cfg
        self.task_exec_role = task_exec_role

        self._create_task_def(baseline_stack_ref)
        if self.container_cfg.cron_expression:
            self._attach_event_rule(baseline_stack_ref)

    def _create_task_def(self, baseline_stack_ref: pulumi.StackReference) -> dict:
        container_name = self.container_cfg.container_name
        version = self.container_cfg.build_version
        env_variables = self.container_cfg.env_variables
        stack_name = pulumi.get_stack()

        self.cluster = ecs.Cluster(
            f"{container_name}-cluster", name=f"{container_name}-cluster-{stack_name}"
        )

        log_group = cloudwatch.LogGroup(
            f"{container_name}-log-group",
            name=f"{container_name}-log-group-{stack_name}",
            retention_in_days=30,
        )

        repository = Repository(f"{container_name}-repository-{stack_name}")
        image = Image(container_name, repository.get_repository())
        image_uri = image.push_image(version)

        container_defs = pulumi.Output.all(
            log_group_name=log_group.name,
            image_uri=image_uri,
            data_bucket_name=baseline_stack_ref.get_output("data_bucket_name"),
        ).apply(
            lambda args: json.dumps(
                [
                    {
                        "name": container_name,
                        "image": args["image_uri"],
                        "memory": self.container_cfg.memory,
                        "cpu": self.container_cfg.cpu,
                        "essential": True,
                        "portMappings": [{"containerPort": 80, "hostPort": 80}],
                        "logConfiguration": {
                            "logDriver": "awslogs",
                            "options": {
                                "awslogs-group": args["log_group_name"],
                                "awslogs-region": "eu-west-1",
                                "awslogs-stream-prefix": container_name,
                            },
                        },
                        "environment": [
                            {
                                "name": "DATA_BUCKET_NAME",
                                "value": args["data_bucket_name"],
                            },
                        ]
                        + env_variables,
                    }
                ]
            )
        )

        self.task_def = ecs.TaskDefinition(
            f"{container_name}-task-def",
            family=f"{container_name}-task-def",
            cpu=str(self.container_cfg.cpu),
            memory=str(self.container_cfg.memory),
            network_mode="awsvpc",
            requires_compatibilities=["FARGATE"],
            execution_role_arn=self.task_exec_role.arn,
            task_role_arn=self.task_exec_role.arn,
            container_definitions=container_defs,
            tags={
                "Name": f"{container_name}-task-def",
            },
        )

    def _attach_event_rule(
        self,
        baseline_stack_ref: pulumi.StackReference,
    ):
        container_name = self.container_cfg.container_name
        stack_name = pulumi.get_stack()

        event_role = iam.Role(
            f"{container_name}-event-rule-role",
            name=f"{container_name}-event-rule-role-{stack_name}",
            assume_role_policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Principal": {"Service": "events.amazonaws.com"},
                            "Effect": "Allow",
                            "Sid": "",
                        }
                    ],
                }
            ),
        )

        iam.RolePolicyAttachment(
            f"{container_name}-event-rule-policy-attachment",
            role=event_role.name,
            policy_arn="arn:aws:iam::aws:policy/AmazonECS_FullAccess",
        )

        event_rule = cloudwatch.EventRule(
            f"{container_name}-event-rute",
            name=f"{container_name}-event-rute-{stack_name}",
            schedule_expression=self.container_cfg.cron_expression,
            description=f"Event rule for {container_name}-{stack_name}",
        )

        cloudwatch.EventTarget(
            f"{container_name}-event-target",
            rule=event_rule.name,
            arn=self.cluster.arn,
            role_arn=event_role.arn,
            ecs_target=cloudwatch.EventTargetEcsTargetArgs(
                task_count=1,
                task_definition_arn=self.task_def.arn,
                launch_type="FARGATE",
                network_configuration={
                    "assign_public_ip": self._has_public_ip(),
                    "subnets": [self._get_subnet(baseline_stack_ref)],
                    "security_groups": [self._get_security_group(baseline_stack_ref)],
                },
            ),
        )

    def get_resource_mapping(
        self,
        baseline_stack_ref: pulumi.StackReference,
    ):
        subnet = self._get_subnet(baseline_stack_ref)
        security_group = self._get_security_group(baseline_stack_ref)
        return {
            self.container_cfg.container_name: {
                "type": ResourceTypes.CONTAINER,
                "task_def": self.task_def,
                "cluster": self.cluster,
                "subnet": subnet,
                "security_group": security_group,
                "assign_public_ip": "ENABLED" if self._has_public_ip() else "DISABLED",
            }
        }

    def _get_security_group(self, baseline_stack_ref: pulumi.StackReference):
        return baseline_stack_ref.get_output("security_group_id")

    def _get_subnet(self, baseline_stack_ref: pulumi.StackReference):
        if self.container_cfg.subnet_type == SubnetType.PRIVATE:
            return baseline_stack_ref.get_output("private_subnet_id")
        else:
            return baseline_stack_ref.get_output("public_subnet_id")

    def _has_public_ip(
        self,
    ):
        return self.container_cfg.subnet_type == SubnetType.PUBLIC
