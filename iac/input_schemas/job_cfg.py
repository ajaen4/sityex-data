from dataclasses import dataclass, field


@dataclass
class JobConfig:
    job_name: str
    number_of_workers: int
    args: dict[str, str] = field(default_factory=dict)
    cron_expression: str = ""
    additional_python_modules: list[str] = field(default_factory=list)
