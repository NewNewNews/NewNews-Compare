# Use a lightweight Python base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy only the requirements file first (for better layer caching)
COPY requirements.txt .

# Install dependencies, including wheel, and upgrade pip for latest package support
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code to the working directory
COPY . .

# Expose the gRPC port
EXPOSE 50053

# Install supervisord for running multiple processes
# RUN apt-get update && apt-get install -y supervisor && rm -rf /var/lib/apt/lists/*

# Copy supervisord configuration file
# COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Run supervisord to manage multiple processes
# CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]

# CMD ["python", "-u", "consumer.py"]
CMD ["python", "-u", "server.py"]
