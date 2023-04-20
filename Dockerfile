FROM python:3.9.16-slim-buster

WORKDIR /usr/src/etl

COPY .env .env
COPY ./requirements.txt requirements.txt

RUN pip install --upgrade pip \
    && pip3 install -r requirements.txt --no-cache-dir

COPY ./etl .
ENV PYTHONPATH "${PYTHONPATH}:/usr/src"

ENTRYPOINT ["python", "main.py"]