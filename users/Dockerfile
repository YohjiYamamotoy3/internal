FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY users.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. users.proto

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

