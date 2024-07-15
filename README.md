# techno_kafka

# Описание
- `docker-compose.yml` описывает конфигурацию контейнеров с Kafka, Zookeper, PostgreSQL.
- `producer/producer.go` запускает сервер, который принимает запросы и записывает в топик Kafka.
- `consumer/consemer.go` запускает сервер, который подключается к Kafka и PostgreSQL. Читает сообщения из Kafka топика, обрабатывает и записывает в другой топик новые или измененные сообщения. В качестве хранилища уникальных сообщений выступает бд PostgeSQL.

## Запуск 
Запустить контейнеры с Kafka, Zookeper и PostgreSQL:
```sh
$ docker-compose up -d
```
Далее необходимо подключиться к бд и создать таблицу `test_db` (в моем случае это было сделано через IDE)

Далее запустить `producer/producer.go` :
```sh
$ go run producer/producer.go
```
Затем запустить `consumer/consumer.go` :
```sh
$ go run consumer/consumer.go
```
Теперь сервис обрабатывает входящие запросы по адрессу `localhost:8080/send` и добавляет сообщения в БД и Kafka
