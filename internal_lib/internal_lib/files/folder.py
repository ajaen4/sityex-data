import os


def create_folder(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)
