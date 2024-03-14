from dataclasses import dataclass, field
from typing import List, Optional

from enum import Enum


class StateType(Enum):
    TASK = "Task"
    PARALLEL = "Parallel"


@dataclass
class OrchestratorState:
    name: str
    type: StateType
    resource: str
    is_end: bool = False
    branches: Optional[List["OrchestratorBranch"]] = field(default_factory=list)

    def __init__(self, state: dict):
        self.name = state["name"]
        self.type = StateType(state["type"])
        self.resource = state["resource"] if "resource" in state else None
        self.is_end = state["is_end"] if "is_end" in state else False

        self.branches = (
            [OrchestratorBranch(**s) for s in state["branches"]]
            if "branches" in state
            else None
        )


@dataclass
class OrchestratorBranch:
    name: str
    states: List["OrchestratorState"]

    def __init__(self, name: str, states: dict):
        self.name = name
        self.states = [OrchestratorState(s) for s in states]


@dataclass
class OrchestratorConfig:
    orchestrator_name: str
    type: str
    states: list[OrchestratorState]
    cron_expression: str = None
