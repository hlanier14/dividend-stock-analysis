FROM python:3.9-slim

COPY requirements.txt /
RUN pip install -r requirements.txt

COPY . /app

EXPOSE 5000
ENV PORT 5000
WORKDIR /app

CMD exec gunicorn --bind :$PORT main:app --workers 1 --threads 1 --timeout 1800