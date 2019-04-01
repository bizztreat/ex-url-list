FROM quay.io/keboola/docker-custom-python:latest

RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --upgrade python-dateutil

COPY . /code/
WORKDIR /data/
CMD ["python3", "-u", "/code/main.py"]
