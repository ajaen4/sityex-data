from dataclasses import dataclass

from pulumi import Config
from .container_cfg import ContainerConfig, SubnetType
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
            build_version = config["build_version"]

            extra_container_args = dict()

            if "env_variables" in config:
                extra_container_args["env_variables"] = config["env_variables"]
            if "cron_expression" in config:
                extra_container_args["cron_expression"] = config["cron_expression"]
            if "cpu" in config:
                extra_container_args["cpu"] = config["cpu"]
            if "memory" in config:
                extra_container_args["memory"] = config["memory"]
            if "subnet_type" in config:
                extra_container_args["subnet_type"] = SubnetType(config["subnet_type"])

            containers_cfg_fmt.append(
                ContainerConfig(
                    container_name=name,
                    build_version=build_version,
                    **extra_container_args,
                )
            )

        return containers_cfg_fmt

    @staticmethod
    def serialize_jobs_cfg(iac_cfg: Config) -> list[JobConfig]:
        jobs_cfgs = iac_cfg.get_object("jobs", {})
        jobs_cfgs_fmt = list()

        for name, config in jobs_cfgs.items():
            number_of_workers = (
                config["number_of_workers"] if "number_of_workers" in config else 2
            )

            extra_job_args = dict()

            if "args" in config:
                extra_job_args["args"] = config["args"]
            if "cron_expression" in config:
                extra_job_args["cron_expression"] = config["cron_expression"]
            if "additional_python_modules" in config:
                extra_job_args["additional_python_modules"] = config[
                    "additional_python_modules"
                ]

            jobs_cfgs_fmt.append(
                JobConfig(
                    job_name=name,
                    number_of_workers=number_of_workers,
                    **extra_job_args,
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

            extra_orchest_args = dict()

            if "cron_expression" in config:
                extra_orchest_args["cron_expression"] = config["cron_expression"]

            orchestrator_cfg_fmt.append(
                OrchestratorConfig(
                    orchestrator_name=name,
                    type=type,
                    states=states,
                    **extra_orchest_args,
                )
            )

        return orchestrator_cfg_fmt
