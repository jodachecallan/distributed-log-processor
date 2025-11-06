#Docker file for a Python test application using Pika to connect to RabbitMQ
FROM python:3.9-slim

WORKDIR /app

# Build args allow embedding default creds or script at build time
ARG USERNAME=guest
ARG PASSWORD=guest
ARG SCRIPT=log-producer.py
ARG HOST=localhost
ARG PORT=5672

# expose as environment variables so entrypoint can use them and they can be overridden at runtime
ENV USERNAME=$USERNAME
ENV PASSWORD=$PASSWORD
ENV SCRIPT=$SCRIPT
ENV HOST=$HOST
ENV PORT=$PORT

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
