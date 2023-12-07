from dataclasses import dataclass

from pulumi import Config


@dataclass
class ContainerConfig:
    container_name: str
    build_version: str
    env_variables: list[dict[str, str]] = None
    cron_expression: str = None


@dataclass
class JobConfig:
    job_name: str
    number_of_workers: int
    args: dict[str, str] = None
    cron_expression: str = None


@dataclass
class Input:
    containers_config: list[ContainerConfig] = None
    jobs_configs: list[JobConfig] = None

    @classmethod
    def from_config(cls, iac_config: Config):
        containers_config_fmt = Input.serialize_containers_config(iac_config)
        jobs_configs_fmt = Input.serialize_jobs_config(iac_config)

        return cls(
            containers_config=containers_config_fmt,
            jobs_configs=jobs_configs_fmt,
        )

    @staticmethod
    def serialize_containers_config(iac_config: Config):
        containers_config = iac_config.get_object("containers_config", {})
        containers_config_fmt = list()

        for name, config in containers_config.items():
            env_variables = config["env_variables"] if "env_variables" in config else []
            cron_expression = (
                config["cron_expression"] if "cron_expression" in config else None
            )
            containers_config_fmt.append(
                ContainerConfig(
                    container_name=name,
                    build_version=config["build_version"],
                    env_variables=env_variables,
                    cron_expression=cron_expression,
                )
            )

        return containers_config_fmt

    @staticmethod
    def serialize_jobs_config(iac_config: Config):
        jobs_configs = iac_config.get_object("jobs_configs", {})
        jobs_configs_fmt = list()

        for name, config in jobs_configs.items():
            args = config["args"] if "args" in config else []
            cron_expression = (
                config["cron_expression"] if "cron_expression" in config else None
            )
            number_of_workers = (
                config["number_of_workers"] if "number_of_workers" in config else 2
            )

            jobs_configs_fmt.append(
                JobConfig(
                    job_name=name,
                    number_of_workers=number_of_workers,
                    args=args,
                    cron_expression=cron_expression,
                )
            )

        return jobs_configs_fmt
