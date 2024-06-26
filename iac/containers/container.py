import json
from typing import Any

import pulumi
from pulumi_aws import ecs, iam, cloudwatch

from .repository import Repository
from .image import Image

from input_schemas import ContainerConfig, SubnetType, EnvVarType
from resource_types import ResourceTypes
from services import ssm_client


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

    def _create_task_def(self, baseline_stack_ref: pulumi.StackReference):
        container_name = self.container_cfg.container_name
        version = self.container_cfg.build_version
        env_vars = self.get_env_vars()
        stack_name = pulumi.get_stack()

        self.cluster = ecs.Cluster(
            f"{container_name}-cluster",
            name=f"{container_name}-cluster-{stack_name}",
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
                        "portMappings": [
                            {"containerPort": 80, "hostPort": 80}
                        ],
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
                        + env_vars,
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

    def get_env_vars(self) -> list[dict]:
        env_vars = list()
        for env_var in self.container_cfg.env_vars:
            if env_var.type == EnvVarType.SSM:
                param = ssm_client.get_parameter(env_var.path)
                json_param = json.loads(param)
                for key, value in json_param.items():
                    env_vars.append({"name": key, "value": value})
            else:
                env_vars.append({"name": env_var.name, "value": env_var.value})

        return env_vars

    def _attach_event_rule(
        self,
        baseline_stack_ref: pulumi.StackReference,
    ) -> None:
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
                network_configuration=cloudwatch.EventTargetEcsTargetNetworkConfigurationArgs(
                    assign_public_ip=self._has_public_ip(),
                    subnets=self._get_subnets(baseline_stack_ref),
                    security_groups=[
                        self._get_security_group(baseline_stack_ref)
                    ],
                ),
            ),
        )

    def get_resource_mapping(
        self,
        baseline_stack_ref: pulumi.StackReference,
    ) -> dict[str, dict[str, Any]]:
        subnets = self._get_subnets(baseline_stack_ref)
        security_group = self._get_security_group(baseline_stack_ref)
        return {
            self.container_cfg.container_name: {
                "type": ResourceTypes.CONTAINER,
                "task_def": self.task_def,
                "cluster": self.cluster,
                "subnets": subnets,
                "security_group": security_group,
                "assign_public_ip": "ENABLED"
                if self._has_public_ip()
                else "DISABLED",
            }
        }

    def _get_security_group(
        self, baseline_stack_ref: pulumi.StackReference
    ) -> pulumi.Output:
        return baseline_stack_ref.get_output("security_group_id")

    def _get_subnets(
        self, baseline_stack_ref: pulumi.StackReference
    ) -> pulumi.Output:
        if self.container_cfg.subnet_type == SubnetType.PRIVATE:
            return baseline_stack_ref.get_output("private_subnet_ids")
        else:
            return baseline_stack_ref.get_output("public_subnet_ids")

    def _has_public_ip(
        self,
    ) -> bool:
        return self.container_cfg.subnet_type == SubnetType.PUBLIC
