FROM python:3.12-slim

WORKDIR /app

COPY ./booking-service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./booking-service /app
COPY ./proto /app/proto

ENV PYTHONPATH=/app:/app/proto

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]