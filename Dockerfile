FROM		alpine:latest
MAINTAINER	Fabien Zouaoui <fzo@sirdata.fr>
LABEL		Description="Base alpine with python3 and stuff to bypass rabbitmq"

RUN apk update && \
    apk add ca-certificates openssl python3 py3-pip less gcc g++ python3-dev musl-dev libffi-dev openssl-dev snappy snappy-dev && \
    update-ca-certificates && \
    rm -f /var/cache/apk/*

RUN pip3 install --upgrade --no-cache-dir pip
RUN pip3 install --no-cache-dir kafka-python
RUN pip3 install --no-cache-dir pika
RUN pip3 install --no-cache-dir aio-pika
RUN pip3 install --no-cache-dir python-snappy
RUN pip3 install kubernetes && rm -rf /root/.cache

RUN apk del gcc g++ python3-dev musl-dev libffi-dev openssl-dev snappy-dev

COPY bypass-rabbitmq.py /bypass-rabbitmq.py

USER daemon

ENTRYPOINT ["/bin/sh"]
