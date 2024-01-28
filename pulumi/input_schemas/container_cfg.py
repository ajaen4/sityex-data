from dataclasses import dataclass


@dataclass
class ContainerConfig:
    container_name: str
    build_version: str
    cpu: int = 256
    memory: int = 512
    env_variables: list[dict[str, str]] = None
    cron_expression: str = None
