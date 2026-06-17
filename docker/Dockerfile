# Use a lightweight Python base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy common dependencies and code first to leverage Docker caching
COPY common/ /app/common
COPY producers/ .

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Expose default Kafka port if necessary (optional, depends on consumer setup)
# EXPOSE 9092

# The entrypoint script will handle the execution of the specific producer module
COPY docker/run_producer.sh ./run_producer.sh

# Set the command to run the main logic
ENTRYPOINT ["/bin/bash", "./run_producer.sh"]