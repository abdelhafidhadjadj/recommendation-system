FROM python:3.10-slim
WORKDIR /app
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
COPY requirements_collector.txt .
RUN pip install --no-cache-dir -r requirements_collector.txt
COPY . .
CMD ["python", "pubmed_collector.py"]
