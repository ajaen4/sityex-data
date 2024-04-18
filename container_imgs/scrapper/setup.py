from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="scrapper",
    version="0.1.4",
    packages=find_packages(),
    install_requires=requirements,
)
