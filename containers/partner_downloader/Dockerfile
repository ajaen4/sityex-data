FROM python:3.11-slim-buster
LABEL maintainer="SityEx"

COPY libs/parser/requirements.txt /parser/requirements.txt
COPY libs/files/requirements.txt /files/requirements.txt
COPY libs/aws_lib/requirements.txt /aws_lib/requirements.txt
COPY gcp_lib/requirements.txt /gcp_lib/requirements.txt
COPY containers/partner_downloader/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /parser/requirements.txt \
    && pip install --no-cache-dir -r /files/requirements.txt \
    && pip install --no-cache-dir -r /aws_lib/requirements.txt \
    && pip install --no-cache-dir -r /gcp_lib/requirements.txt \
    && pip install --no-cache-dir -r /requirements.txt

RUN mkdir /outputs

COPY libs/parser/ /parser/
COPY libs/files/ /files/
COPY libs/aws_lib/ /aws_lib/
COPY gcp_lib/ /gcp_lib/
COPY containers/partner_downloader/ /

ENTRYPOINT ["python", "app.py"]
