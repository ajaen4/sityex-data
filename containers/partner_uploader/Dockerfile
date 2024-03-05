FROM python:3.11-slim-buster
LABEL maintainer="SityEx"

COPY parser/requirements.txt /parser/
COPY files/requirements.txt /files/
COPY aws_lib/requirements.txt /aws_lib/
COPY containers/partner_uploader/requirements.txt /

RUN pip install --no-cache-dir -r /parser/requirements.txt && \
    pip install --no-cache-dir -r /files/requirements.txt && \
    pip install --no-cache-dir -r /aws_lib/requirements.txt && \
    pip install --no-cache-dir -r /requirements.txt

RUN mkdir /outputs

COPY parser/ /parser/
COPY files/ /files/
COPY aws_lib/ /aws_lib/
COPY containers/partner_uploader/ /

ENTRYPOINT ["python", "app.py"]
