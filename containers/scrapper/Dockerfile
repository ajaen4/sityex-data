FROM python:3.11-slim-buster
LABEL maintainer="SityEx"

COPY libs/parser/requirements.txt /parser/
COPY libs/files/requirements.txt /files/
COPY libs/aws_lib/requirements.txt /aws_lib/
COPY containers/scrapper/requirements.txt /

RUN pip install --no-cache-dir -r /parser/requirements.txt \
    && pip install --no-cache-dir -r /files/requirements.txt \
    && pip install --no-cache-dir -r /aws_lib/requirements.txt \
    && pip install --no-cache-dir -r /requirements.txt

RUN mkdir /outputs

COPY libs/parser/ /parser/
COPY libs/files/ /files/
COPY libs/aws_lib/ /aws_lib/
COPY containers/scrapper/ /

ENTRYPOINT ["python", "app.py"]

