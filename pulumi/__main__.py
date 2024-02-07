from pulumi import Config
import pulumi

from containers import ContainerTasks
from lake_jobs import LakeJobs
from orchestrators import Orchestrators
from input_schemas import Input

config = Config()
input = Input.from_cfg(config)

baseline_stack_ref = pulumi.StackReference("ajaen4/sityex-baseline/main")

container_tasks = ContainerTasks(
    baseline_stack_ref,
    input,
)

lake_jobs = LakeJobs(baseline_stack_ref, input)

all_resources = dict()
all_resources.update(container_tasks.get_resources(baseline_stack_ref))
all_resources.update(lake_jobs.get_resources())

orchestrators = Orchestrators(input, all_resources)
