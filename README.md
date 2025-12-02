# internal-crm

микросервисная crm система для внутреннего использования. управление пользователями, обработка платежей, отправка уведомлений и хранение файлов.

## микросервисы

- **users** (8001, grpc 50051) - управление пользователями, crud операции
- **payments** (8002, grpc 50052) - обработка платежей, транзакции
- **notifications** (8003) - отправка уведомлений через email, slack, telegram
- **files** (8004, grpc 50054) - хранение файлов, доступ через rest api

## взаимодействие

микросервисы общаются через grpc и rest api. redis используется как очередь сообщений для асинхронной обработки и кеш для часто запрашиваемых данных.

примеры взаимодействия:
- users создает пользователя через rest, данные кешируются в redis
- payments создает платеж, отправляет id в redis очередь
- notifications читает из очереди и отправляет уведомления
- files хранит метаданные в postgresql, файлы на диске

## запуск

```bash
docker-compose up -d
```

это запустит:
- postgresql на порту 5432
- redis на порту 6379
- все четыре микросервиса на портах 8001-8004

## примеры эндпоинтов

создать пользователя:
```bash
curl -X POST http://localhost:8001/users \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","name":"test user","role":"user"}'
```

создать платеж:
```bash
curl -X POST http://localhost:8002/payments \
  -H "Content-Type: application/json" \
  -d '{"user_id":1,"amount":100.50,"currency":"USD","description":"payment"}'
```

отправить уведомление:
```bash
curl -X POST http://localhost:8003/notifications \
  -H "Content-Type: application/json" \
  -d '{"user_id":1,"type":"info","channel":"email","subject":"test","message":"hello"}'
```

загрузить файл:
```bash
curl -X POST http://localhost:8004/files/upload \
  -F "file=@test.txt" \
  -F "user_id=1"
```

## grpc

для использования grpc нужно сгенерировать клиент из proto файлов:

```bash
cd users
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. users.proto
```

пример grpc вызова:
```python
import grpc
import users_pb2
import users_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = users_pb2_grpc.UsersServiceStub(channel)
response = stub.GetUser(users_pb2.UserRequest(user_id=1))
```

## тесты

запуск тестов для python сервисов:
```bash
cd users
pip install pytest httpx
pytest test_users.py

cd ../payments
pytest test_payments.py

cd ../files
pytest test_files.py
```

для notifications (node.js):
```bash
cd notifications
npm test
```

## структура проекта

```
internal/
├── users/
│   ├── main.py
│   ├── users.proto
│   ├── requirements.txt
│   ├── Dockerfile
│   └── test_users.py
├── payments/
│   ├── main.py
│   ├── payments.proto
│   ├── requirements.txt
│   ├── Dockerfile
│   └── test_payments.py
├── notifications/
│   ├── index.js
│   ├── package.json
│   └── Dockerfile
├── files/
│   ├── main.py
│   ├── files.proto
│   ├── requirements.txt
│   ├── Dockerfile
│   └── test_files.py
├── docker-compose.yml
├── .gitlab-ci.yml
└── README.md
```

## технологии

- python 3.11 + fastapi для users, payments, files
- node.js 18 + express для notifications
- postgresql для хранения данных
- redis для очередей и кеша
- grpc для межсервисного взаимодействия
- docker и docker-compose для контейнеризации
- pytest для тестирования
