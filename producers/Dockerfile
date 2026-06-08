FROM python:3.11-slim

WORKDIR /app

# install dependencies
COPY ../requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# copy producers
COPY . /app/producers

WORKDIR /app/producers

CMD ["python3", "host_metrics.py"]
