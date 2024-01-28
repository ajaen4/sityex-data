from dataclasses import dataclass


@dataclass
class JobConfig:
    job_name: str
    number_of_workers: int
    args: dict[str, str] = None
    cron_expression: str = None
    additional_python_modules: list[str] = None
