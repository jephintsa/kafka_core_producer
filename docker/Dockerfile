FROM python:3.10-slim

WORKDIR /app
ENV PYTHONPATH=/app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY common/ ./common/
COPY producers/ ./producers/
COPY run_producer.sh ./run_producer.sh

# Ensure script is executable
RUN chmod +x ./run_producer.sh

# IMPORTANT: use ENTRYPOINT so compose "command" is passed as argument
ENTRYPOINT ["./run_producer.sh"]