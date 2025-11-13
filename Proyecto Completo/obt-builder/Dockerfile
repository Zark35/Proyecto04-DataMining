# Dockerfile for obt-builder service
FROM python:3.10-slim

WORKDIR /app

# Install required packages
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the CLI script and any other source files
COPY build_obt.py ./

# Entrypoint for CLI script
ENTRYPOINT ["python", "build_obt.py"]
