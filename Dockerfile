FROM python:3.9-alpine

RUN apk update && apk add libpq
RUN apk add --virtual .build-deps gcc python3-dev musl-dev postgresql-dev

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ADD crontab /crontab
RUN /usr/bin/crontab /crontab

USER root

RUN cd /root

COPY met-api-scraper.py /root/met-api-scraper.py 
COPY sources.json /root/sources.json


CMD cd root && python met-api-scraper.py & /usr/sbin/crond -f -l 8
