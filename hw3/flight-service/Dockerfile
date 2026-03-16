FROM python:3.12-slim

WORKDIR /app

COPY ./flight-service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./flight-service /app
COPY ./proto /app/proto

ENV PYTHONPATH=/app:/app/proto

EXPOSE 50051

CMD ["python", "app.py"]