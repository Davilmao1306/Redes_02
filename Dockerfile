FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common.py broker.py sensor.py drone.py ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "broker.py"]
