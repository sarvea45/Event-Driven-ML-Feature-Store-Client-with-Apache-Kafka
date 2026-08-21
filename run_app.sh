#!/bin/bash
set -e

echo "Waiting for PostgreSQL to start..."
while ! nc -z db 5432; do
  sleep 1
done
echo "PostgreSQL is up."

echo "Waiting for Kafka to start..."
while ! nc -z kafka 29092; do
  sleep 1
done
echo "Kafka is up."

echo "Starting FastAPI server..."
exec uvicorn src.main:app --host 0.0.0.0 --port 8000
