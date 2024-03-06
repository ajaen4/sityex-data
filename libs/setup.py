from setuptools import setup, find_packages
import os

curr_dir = os.path.dirname(os.path.abspath(__file__))
libs = ["aws_lib", "files", "gcp_lib", "parser"]
requirements = []
for lib in libs:
    with open(f"{curr_dir}/{lib}/requirements.txt") as f:
        requirements.extend(f.read().splitlines())

setup(
    name="libs",
    version="0.0.1",
    packages=find_packages(),
    install_requires=requirements,
)
