# Source image (contains vulnerability but as this is a dummy experiment will be dismissed)
FROM python:3.11-slim

# Set workdir folder
WORKDIR /app
COPY . /app/python_ingest

# Copy requirements file and run install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Alter pythonpath for execution
ENV PYTHONPATH=/app

# Run script
CMD ["python", "-m", "python_ingest.src.main"]