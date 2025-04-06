# Use an official Python base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy all files into the container
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose FastAPI port (optional, since you're not running FastAPI here)
EXPOSE 8000

# Default command to start the consumer
CMD ["python3", "main.py"]
