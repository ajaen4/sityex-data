import json

import pulumi
from pulumi_aws import ecs, iam, cloudwatch

from .repository import Repository
from .image import Image

from input_schemas import Input, ContainerConfig


class ContainerTasks:
    def __init__(
        self,
        baseline_stack_ref: pulumi.StackReference,
        input: Input,
    ):
        self.baseline_stack_ref = baseline_stack_ref
        self.input = input

        self.create_resources()

    def create_resources(self):
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

        for container_config in self.input.containers_config:
            self._create_task_def(container_config)

    def _create_task_def(self, container_config: ContainerConfig):
        container_name = container_config.container_name
        version = container_config.build_version
        env_variables = container_config.env_variables
        stack_name = pulumi.get_stack()

        cluster = ecs.Cluster(
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
            data_bucket_name=self.baseline_stack_ref.get_output("data_bucket_name"),
        ).apply(
            lambda args: json.dumps(
                [
                    {
                        "name": container_name,
                        "image": args["image_uri"],
                        "memory": container_config.memory,
                        "cpu": container_config.cpu,
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

        task_def = ecs.TaskDefinition(
            f"{container_name}-task-def",
            family=f"{container_name}-task-def",
            cpu=str(container_config.cpu),
            memory=str(container_config.memory),
            network_mode="awsvpc",
            requires_compatibilities=["FARGATE"],
            execution_role_arn=self.task_exec_role.arn,
            task_role_arn=self.task_exec_role.arn,
            container_definitions=container_defs,
            tags={
                "Name": f"{container_name}-task-def",
            },
        )

        if container_config.cron_expression:
            self._attach_event_rule(
                container_config, cluster, task_def, container_config.cron_expression
            )

    def _attach_event_rule(
        self,
        container_config: ContainerConfig,
        cluster: ecs.Cluster,
        task_def: ecs.TaskDefinition,
        cron: str = "cron(00 12 3 * ? 1997)",
    ):
        container_name = container_config.container_name
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
            schedule_expression=cron,
            description=f"Event rule for {container_name}-{stack_name}",
        )

        cloudwatch.EventTarget(
            f"{container_name}-event-target",
            rule=event_rule.name,
            arn=cluster.arn,
            role_arn=event_role.arn,
            ecs_target=cloudwatch.EventTargetEcsTargetArgs(
                task_count=1,
                task_definition_arn=task_def.arn,
                launch_type="FARGATE",
                network_configuration={
                    "assign_public_ip": False,
                    "subnets": [
                        self.baseline_stack_ref.get_output("private_subnet_id")
                    ],
                    "security_groups": [
                        self.baseline_stack_ref.get_output("security_group_id")
                    ],
                },
            ),
        )
