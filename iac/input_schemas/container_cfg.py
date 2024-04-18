from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class SubnetType(Enum):
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"


class EnvVarType(Enum):
    LITERAL = "LITERAL"
    SSM = "SSM"


@dataclass
class EnvVariable:
    name: str = ""
    value: str = ""
    type: Optional[EnvVarType] = None
    path: str = ""


@dataclass
class ContainerConfig:
    container_name: str
    build_version: str
    cpu: int = 256
    memory: int = 512
    env_vars: list[EnvVariable] = field(default_factory=list)
    cron_expression: str = ""
    subnet_type: SubnetType = SubnetType.PRIVATE
