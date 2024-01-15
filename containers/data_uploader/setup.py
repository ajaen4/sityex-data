from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="data_uploader",
    version="0.0.6",
    packages=find_packages(),
    install_requires=requirements,
)
