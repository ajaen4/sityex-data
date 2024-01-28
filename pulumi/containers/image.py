import pulumi
from pulumi_aws import ecr
import pulumi_docker as docker

from services import ecr_client


class Image:
    def __init__(self, name: str, ecr_repository: ecr.Repository):
        self.name = name
        self.ecr_repository = ecr_repository

    def push_image(self, version: str):
        image_tag = f"{self.name}-{version}"
        skip_push = self.ecr_repository.repository_url.apply(
            lambda repository_url: self._is_image_in_ecr(repository_url, image_tag)
        )
        self._authenticate()
        image = docker.Image(
            f"{self.name}-image",
            skip_push=skip_push,
            build=docker.DockerBuildArgs(
                args={
                    "BUILDKIT_INLINE_CACHE": "1",
                },
                context="../",
                dockerfile=f"../containers/{self.name}/Dockerfile",
                platform="linux/amd64",
            ),
            image_name=self.ecr_repository.repository_url.apply(
                lambda repository_url: f"{repository_url}:{image_tag}"
            ),
            registry=docker.RegistryArgs(
                username="AWS",
                password=pulumi.Output.secret(self.auth_token.password),
                server=self.ecr_repository.repository_url,
            ),
        )

        return image.image_name

    def _is_image_in_ecr(self, repository_url: str, tag: str):
        repository_name = repository_url.split("/")[-1].split(":")[0]
        response = ecr_client.list_images(
            repositoryName=repository_name, filter={"tagStatus": "TAGGED"}
        )

        return any(
            [
                image_id
                for image_id in response["imageIds"]
                if image_id["imageTag"] == tag
            ]
        )

    def _authenticate(self):
        self.auth_token = ecr.get_authorization_token_output(
            registry_id=self.ecr_repository.registry_id
        )
