from boto3.session import Session


class SSM:
    def __init__(self, session: Session):
        self.session = session

    def get_parameter(self, parameter_name: str, decrypt: bool = False):
        ssm_client = self.session.client("ssm")

        response = ssm_client.get_parameter(
            Name=parameter_name,
            WithDecryption=decrypt,
        )

        return response["Parameter"]["Value"]
