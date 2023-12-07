FROM python:3.11-slim-buster
LABEL maintainer="SityEx"

COPY parser/ ./parser/
COPY files/ ./files/
COPY aws_lib/ ./aws_lib/
COPY containers/fever_downloader/ ./

RUN pip install -r parser/requirements.txt
RUN pip install -r files/requirements.txt
RUN pip install -r aws_lib/requirements.txt
RUN pip install -r requirements.txt

RUN mkdir /outputs

ENTRYPOINT ["python", "app.py"]
