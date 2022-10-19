FROM python:3.9

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY met-api-scraper.py met-api-scraper.py 
COPY sources.json sources.json

USER root

ENTRYPOINT ["python", "met-api-scraper.py"]
