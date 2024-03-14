from pulumi_aws import ecr


class Repository:
    def __init__(self, name: str):
        self.ecr_repository = ecr.Repository(
            name,
            name=name,
            image_scanning_configuration=ecr.RepositoryImageScanningConfigurationArgs(
                scan_on_push=True,
            ),
            image_tag_mutability="IMMUTABLE",
        )

    def get_repository(self):
        return self.ecr_repository
