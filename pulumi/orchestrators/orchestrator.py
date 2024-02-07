import json

import pulumi
from pulumi_aws.sfn import StateMachine
from pulumi_aws import iam, cloudwatch

from input_schemas import OrchestratorConfig, OrchestratorState, StateType
from resource_types import ResourceTypes


class Orchestrator:
    def __init__(
        self,
        orchest_cfg: OrchestratorConfig,
        role: iam.Role,
        all_resources: dict[dict],
    ):
        self.role = role
        self.orchest_cfg = orchest_cfg
        self.orchest_resources = self._get_orchest_resources(all_resources)
        self._create_state_machine()

    def _get_orchest_resources(self, all_resources: dict[dict]):
        orchest_resources = dict()

        for state in self.orchest_cfg.states:
            if state.type == StateType.TASK:
                resource_name = state.resource
                orchest_resources[resource_name] = all_resources[resource_name]
            if state.type == StateType.PARALLEL:
                for branch in state.branches:
                    for branch_state in branch.states:
                        resource_name = branch_state.resource
                        orchest_resources[resource_name] = all_resources[resource_name]

        return orchest_resources

    def _create_state_machine(self):
        initial_state = {
            "Comment": f"state Function for {self.orchest_cfg.orchestrator_name}",
        }

        stack_name = pulumi.get_stack()

        definition = self._create_definition(
            self.orchest_cfg.states, initial_state
        ).apply(lambda definition: json.dumps(definition, default=str))

        self.state_machine = StateMachine(
            self.orchest_cfg.orchestrator_name,
            name=f"{self.orchest_cfg.orchestrator_name}-{stack_name}",
            role_arn=self.role.arn,
            definition=definition,
        )

        if self.orchest_cfg.cron_expression:
            self._create_cron_trigger()

    def _create_definition(
        self,
        states_cfg: list[OrchestratorState],
        initial_state: dict = dict(),
    ) -> pulumi.Output:
        states = self._get_states(states_cfg)

        return states.apply(
            lambda states: {
                **initial_state,
                "StartAt": states_cfg[0].name,
                "States": states,
            }
        )

    def _get_states(self, states: list[OrchestratorState]) -> pulumi.Output:
        state_outputs = {}

        for index, state in enumerate(states):
            state_args = dict()
            state_args["Type"] = state.type.value

            if index == len(states) - 1 or state.is_end:
                state_args["End"] = True
            else:
                next_state_name = states[index + 1].name
                state_args["Next"] = next_state_name

            if state.type == StateType.TASK:
                resource_output = self._get_resource(state.resource)

                state_outputs[state.name] = pulumi.Output.all(
                    resource_output=resource_output, state_args=state_args
                ).apply(lambda args: {**args["state_args"], **args["resource_output"]})

            elif state.type == StateType.PARALLEL:
                branches_states = list()
                for branch in state.branches:
                    branch_definition = self._create_definition(branch.states)
                    branches_states.append(branch_definition)

                state_outputs[state.name] = {**state_args, "Branches": branches_states}

            else:
                raise Exception("Invalid state type")

        return pulumi.Output.all(**state_outputs).apply(lambda outputs: outputs)

    def _get_resource(self, resource_name: str) -> pulumi.Output:
        resource = self.orchest_resources[resource_name]

        if resource["type"] == ResourceTypes.GLUE_JOB:
            return resource["job"].name.apply(
                lambda job_name: {
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {"JobName": job_name},
                }
            )

        elif resource["type"] == ResourceTypes.CONTAINER:
            return pulumi.Output.all(
                cluster_arn=resource["cluster"].arn,
                task_def_arn=resource["task_def"].arn,
                subnet=resource["subnet"],
                security_group=resource["security_group"],
                assign_public_ip=resource["assign_public_ip"],
            ).apply(
                lambda args: {
                    "Resource": "arn:aws:states:::ecs:runTask.sync",
                    "Parameters": {
                        "LaunchType": "FARGATE",
                        "Cluster": args["cluster_arn"],
                        "TaskDefinition": args["task_def_arn"],
                        "NetworkConfiguration": {
                            "AwsvpcConfiguration": {
                                "Subnets": [args["subnet"]],
                                "SecurityGroups": [args["security_group"]],
                                "AssignPublicIp": args["assign_public_ip"],
                            }
                        },
                    },
                }
            )

    def _create_cron_trigger(self) -> None:
        stack_name = pulumi.get_stack()

        event_role = iam.Role(
            f"{self.orchest_cfg.orchestrator_name}-event-rule-role",
            name=f"{self.orchest_cfg.orchestrator_name}-event-rule-role-{stack_name}",
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
            f"{self.orchest_cfg.orchestrator_name}-event-rule-policy-attachment",
            role=event_role.name,
            policy_arn="arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess",
        )

        rule = cloudwatch.EventRule(
            f"{self.orchest_cfg.orchestrator_name}-rule",
            name=f"{self.orchest_cfg.orchestrator_name}-rule-{stack_name}",
            schedule_expression=self.orchest_cfg.cron_expression,
            description=f"Schedule rule for {self.orchest_cfg.orchestrator_name}",
        )

        cloudwatch.EventTarget(
            f"{self.orchest_cfg.orchestrator_name}-target",
            rule=rule.name,
            arn=self.state_machine.arn,
            role_arn=event_role.arn,
        )
