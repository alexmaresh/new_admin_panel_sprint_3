FROM python:3.10
WORKDIR /opt/app
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
COPY ./requirements.txt .
RUN pip install -r requirements.txt
COPY ./ .
ENTRYPOINT ["python3", "pg_to_es.py"]