# Инструкция по запуску приложения "Service"

## Требования

Перед запуском приложения убедитесь, что у вас установлены следующие компоненты:
- Go (версия 1.16 и выше)
- Docker
- Docker Compose

## Установка и настройка

1. Клонируйте репозиторий:

```shell
git clone https://github.com/zatrasz.75/Service.git
```
2. Установка Kafka  в Docker

```shell
cd kafka
docker-compose up -d
```

3. Запуск приложения
```shell
go mod download
go run cmd/main.go
```
## Использование

* GET /data: Получение данных с различными фильтрами и пагинацией.

* POST /data: Добавление новых записей о людях.

* DELETE /data/{id}: Удаление записи по идентификатору.

* PUT /data/{id}: Изменение данных о человеке по идентификатору.

* PATCH /data/{id}: Частичное обновление данных о человеке по идентификатору
