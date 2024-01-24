import json

import pulumi
from pulumi_aws.sfn import StateMachine
from pulumi_aws import iam, cloudwatch

from input_schemas import OrchestratorConfig, OrchestratorState


class Orchestrator:
    def __init__(
        self,
        baseline_stack_ref: pulumi.StackReference,
        orchest_cfg: OrchestratorConfig,
        role: iam.Role,
        all_resources: dict[dict],
    ):
        self.baseline_stack_ref = baseline_stack_ref
        self.role = role
        self.orchest_cfg = orchest_cfg
        self.orchest_resources = self._get_orchest_resources(all_resources)
        self.create_state_machine()

    def _get_orchest_resources(self, all_resources: dict[dict]):
        orchest_resources = dict()

        for state in self.orchest_cfg.states:
            if state.type == "Task":
                resource_name = state.resource
                orchest_resources[resource_name] = all_resources[resource_name]
            if state.type == "Parallel":
                for branch in state.branches:
                    for branch_state in branch.states:
                        resource_name = branch_state.resource
                        orchest_resources[resource_name] = all_resources[resource_name]

        return orchest_resources

    def create_state_machine(self):
        initial_state = {
            "Comment": f"state Function for {self.orchest_cfg.orchestrator_name}",
        }

        definition = self.create_definition(
            self.orchest_cfg.states, initial_state
        ).apply(lambda definition: json.dumps(definition, default=str))

        StateMachine(
            self.orchest_cfg.orchestrator_name,
            name=self.orchest_cfg.orchestrator_name,
            role_arn=self.role.arn,
            definition=definition,
        )

    def create_definition(
        self,
        states_cfg: list[OrchestratorState],
        initial_state: dict = dict(),
    ) -> pulumi.Output:
        states = self.get_states(states_cfg)

        return states.apply(
            lambda states: {
                **initial_state,
                "StartAt": states_cfg[0].name,
                "States": states,
            }
        )

    def get_states(self, states: list[OrchestratorState]) -> pulumi.Output:
        state_outputs = {}

        for index, state in enumerate(states):
            state_args = dict()
            state_args["Type"] = state.type

            if index == len(states) - 1 or state.is_end:
                state_args["End"] = True
            else:
                next_state_name = states[index + 1].name
                state_args["Next"] = next_state_name

            if state.type == "Task":
                resource_output = self.get_resource(state.resource)

                state_outputs[state.name] = pulumi.Output.all(
                    resource_output=resource_output, state_args=state_args
                ).apply(lambda args: {**args["state_args"], **args["resource_output"]})

            elif state.type == "Parallel":
                branches_states = list()
                for branch in state.branches:
                    branch_definition = self.create_definition(branch.states)
                    branches_states.append(branch_definition)

                state_outputs[state.name] = {**state_args, "Branches": branches_states}

            else:
                raise Exception("Invalid state type")

        return pulumi.Output.all(**state_outputs).apply(lambda outputs: outputs)

    def get_resource(self, resource_name: str) -> pulumi.Output:
        resource = self.orchest_resources[resource_name]

        if resource["type"] == "glue_job":
            return resource["job"].name.apply(
                lambda job_name: {
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {"JobName": job_name},
                }
            )

        elif resource["type"] == "ecs":
            return pulumi.Output.all(
                cluster_arn=resource["cluster"].arn,
                task_def_arn=resource["task_def"].arn,
                subnets=self.baseline_stack_ref.get_output("private_subnet_id"),
                security_groups=self.baseline_stack_ref.get_output("security_group_id"),
            ).apply(
                lambda args: {
                    "Resource": "arn:aws:states:::ecs:runTask.sync",
                    "Parameters": {
                        "LaunchType": "FARGATE",
                        "Cluster": args["cluster_arn"],
                        "TaskDefinition": args["task_def_arn"],
                        "NetworkConfiguration": {
                            "AwsvpcConfiguration": {
                                "Subnets": [args["subnets"]],
                                "SecurityGroups": [args["security_groups"]],
                            }
                        },
                    },
                }
            )
