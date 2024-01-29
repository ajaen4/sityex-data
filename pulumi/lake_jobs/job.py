import pulumi
from pulumi_aws import s3, cloudwatch, glue, iam

from input_schemas import JobConfig
from resource_types import ResourceTypes


class Job:
    def __init__(
        self,
        job_cfg: JobConfig,
        roles: dict[str, iam.Role],
        logger_script: s3.BucketObject,
        baseline_stack_ref: pulumi.StackReference,
    ):
        self.job_cfg = job_cfg
        self.roles = roles

        self._create_job(logger_script, baseline_stack_ref)

    def _create_job(
        self, logger_script: s3.BucketObject, baseline_stack_ref: pulumi.StackReference
    ) -> dict:
        stack_name = pulumi.get_stack()

        job_name = self.job_cfg.job_name
        job_script = s3.BucketObject(
            f"{job_name}-script",
            bucket=baseline_stack_ref.get_output("jobs_bucket_name"),
            key=f"{stack_name}/{job_name}.py",
            source=pulumi.FileAsset(f"../job_scripts/{job_name}.py"),
        )

        log_group = cloudwatch.LogGroup(
            f"{job_name}-log-group",
            name=f"{stack_name}-{job_name}-log-group",
            retention_in_days=30,
        )

        if self.job_cfg.additional_python_modules:
            add_python_modules = {
                "--additional-python-modules": ",".join(
                    self.job_cfg.additional_python_modules
                ),
            }
        else:
            add_python_modules = {}

        job_arguments = pulumi.Output.all(
            log_group_name=log_group.name,
            data_bucket_name=baseline_stack_ref.get_output("data_bucket_name"),
            jobs_bucket_name=baseline_stack_ref.get_output("jobs_bucket_name"),
            logger_script_key=logger_script.key,
            translate_role_arn=self.roles["translate"].arn,
        ).apply(
            lambda args: {
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-enable",
                "--continuous-log-logGroup": args["log_group_name"],
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-continuous-log-filter": "true",
                "--enable-metrics": "",
                "--DATA_BUCKET_NAME": args["data_bucket_name"],
                "--TRANSLATE_ROLE_ARN": args["translate_role_arn"],
                "--extra-py-files": f's3://{args["jobs_bucket_name"]}/{args["logger_script_key"]}',
                **add_python_modules,
                **self.job_cfg.args,
            }
        )

        self.glue_job = glue.Job(
            f"{job_name}-job",
            glue.JobArgs(
                name=f"{job_name}-{stack_name}",
                role_arn=self.roles["job"].arn,
                glue_version="4.0",
                number_of_workers=self.job_cfg.number_of_workers,
                worker_type="G.1X",
                default_arguments=job_arguments,
                command=glue.JobCommandArgs(
                    script_location=pulumi.Output.all(
                        bucket_name=baseline_stack_ref.get_output("jobs_bucket_name"),
                        script_key=job_script.key,
                    ).apply(
                        lambda args: f's3://{args["bucket_name"]}/{args["script_key"]}'
                    ),
                    python_version="3",
                ),
            ),
        )

        cron_expression = self.job_cfg.cron_expression

        if cron_expression:
            glue.Trigger(
                f"{self.job_cfg.job_name}-trigger",
                name=f"{self.job_cfg.job_name}-trigger-{stack_name}",
                actions=[glue.TriggerActionArgs(job_name=self.glue_job.name)],
                schedule=cron_expression,
                type="SCHEDULED",
            )

    def get_resource_mapping(self) -> dict:
        return {
            self.job_cfg.job_name: {
                "type": ResourceTypes.GLUE_JOB,
                "job": self.glue_job,
            }
        }
