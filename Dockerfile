FROM python:3.11-slim

WORKDIR /app

# Install netcat for the wait script and libpq for psycopg2
RUN apt-get update && apt-get install -y netcat-traditional libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x run_app.sh

CMD ["./run_app.sh"]
