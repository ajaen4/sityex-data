from pulumi import Config
import pulumi

from containers import ContainerTasks
from lake_jobs import LakeJobs
from input_schemas import Input

config = Config()
input = Input.from_config(config)

baseline_infra_ref = pulumi.StackReference("ajaen4/sityex-baseline/main")

container_tasks = ContainerTasks(
    baseline_infra_ref,
    input,
)

lake_jobs = LakeJobs(baseline_infra_ref, input)
