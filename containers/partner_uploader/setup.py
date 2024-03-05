from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="partner_uploader",
    version="1.3.10",
    packages=find_packages(),
    install_requires=requirements,
)
