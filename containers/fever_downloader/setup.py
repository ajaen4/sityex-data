from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="fever_downloader",
    version="0.0.4",
    packages=find_packages(),
    install_requires=requirements,
)
