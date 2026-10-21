FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py .

# -u: unbuffered, so `docker compose logs -f producer` shows output immediately
CMD ["python", "-u", "producer.py"]
