from dataclasses import dataclass

from pulumi import Config
from .container_cfg import ContainerConfig
from .job_cfg import JobConfig
from .orchestrator_cfg import OrchestratorConfig, OrchestratorState


@dataclass
class Input:
    containers_cfg: list[ContainerConfig] = None
    jobs_cfgs: list[JobConfig] = None
    orchestrators_cfgs: list[OrchestratorConfig] = None

    @classmethod
    def from_cfg(cls, iac_cfg: Config):
        containers_cfg_fmt = Input.serialize_containers_cfg(iac_cfg)
        jobs_cfgs_fmt = Input.serialize_jobs_cfg(iac_cfg)
        orchestrators_cfgs_fmt = Input.serialize_orchest_cfg(iac_cfg)

        return cls(
            containers_cfg=containers_cfg_fmt,
            jobs_cfgs=jobs_cfgs_fmt,
            orchestrators_cfgs=orchestrators_cfgs_fmt,
        )

    @staticmethod
    def serialize_containers_cfg(iac_cfg: Config) -> list[ContainerConfig]:
        containers_cfg = iac_cfg.get_object("containers", {})
        containers_cfg_fmt = list()

        for name, config in containers_cfg.items():
            env_variables = config["env_variables"] if "env_variables" in config else []
            cron_expression = (
                config["cron_expression"] if "cron_expression" in config else None
            )
            cpu = config["cpu"] if "cpu" in config else 256
            memory = config["memory"] if "memory" in config else 512

            containers_cfg_fmt.append(
                ContainerConfig(
                    container_name=name,
                    build_version=config["build_version"],
                    cpu=cpu,
                    memory=memory,
                    env_variables=env_variables,
                    cron_expression=cron_expression,
                )
            )

        return containers_cfg_fmt

    @staticmethod
    def serialize_jobs_cfg(iac_cfg: Config) -> list[JobConfig]:
        jobs_cfgs = iac_cfg.get_object("jobs", {})
        jobs_cfgs_fmt = list()

        for name, config in jobs_cfgs.items():
            args = config["args"] if "args" in config else []
            cron_expression = (
                config["cron_expression"] if "cron_expression" in config else None
            )
            additional_python_modules = (
                config["additional_python_modules"]
                if "additional_python_modules" in config
                else []
            )
            number_of_workers = (
                config["number_of_workers"] if "number_of_workers" in config else 2
            )

            jobs_cfgs_fmt.append(
                JobConfig(
                    job_name=name,
                    number_of_workers=number_of_workers,
                    args=args,
                    cron_expression=cron_expression,
                    additional_python_modules=additional_python_modules,
                )
            )

        return jobs_cfgs_fmt

    @staticmethod
    def serialize_orchest_cfg(iac_cfg: Config) -> list[OrchestratorConfig]:
        orchestrator_cfg = iac_cfg.get_object("orchestrators", {})
        orchestrator_cfg_fmt = list()

        for name, config in orchestrator_cfg.items():
            type = config["type"]

            states = list()
            for state in config["states"]:
                states.append(OrchestratorState(state))

            orchestrator_cfg_fmt.append(
                OrchestratorConfig(
                    orchestrator_name=name,
                    type=type,
                    states=states,
                )
            )

        return orchestrator_cfg_fmt
