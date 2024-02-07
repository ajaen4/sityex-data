from dataclasses import dataclass, field
from enum import Enum


class SubnetType(Enum):
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"


@dataclass
class ContainerConfig:
    container_name: str
    build_version: str
    cpu: int = 256
    memory: int = 512
    env_variables: list[dict[str, str]] = field(default_factory=list)
    cron_expression: str = None
    subnet_type: SubnetType = SubnetType.PRIVATE
