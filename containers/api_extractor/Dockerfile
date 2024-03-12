FROM python:3.11-slim-buster
LABEL maintainer="SityEx"

COPY internal_lib/requirements.txt /internal_lib/requirements.txt
COPY containers/api_extractor/requirements.txt /

RUN pip install --no-cache-dir -r /internal_lib/requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN mkdir /outputs

COPY internal_lib/internal_lib /internal_lib/
COPY containers/api_extractor/ /

ENTRYPOINT ["python", "app.py"]
