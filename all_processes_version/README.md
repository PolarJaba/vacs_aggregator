# Парсинг данных о вакансиях с использованием Apache Airflow, Selenium и ClickHouse

В данном проекте реализован процесс получения данных о вакансиях с различных веб-сайтов и их сохранения в базе данных ClickHouse. Механизм сценария python и расписание автоматизированы с помощью Apache Airflow.

## Описание скрипта.

1. **`create_raw_table`**: Функция, создающая таблицы в базе данных ClickHouse, если они не существует.

2. **`run_vk_parser`** и **`run_sber_parser`**: Функции, которые переходят на указанные веб-сайты, собирают информацию о вакансиях и сохраняют ее в ClickHouse.

Эти задачи определены и выполняются в конкретном порядке в Apache Airflow DAG под названием `scrap_dag`.

## Запуск проекта с помощью Docker

Для упрощения запуска проекта и обеспечения его воспроизводимости в процессе развертывания приложения использован Docker. Файл `docker-compose.yml` содержит набор сервисов, необходимых для работы приложения. Для запуска проекта необходимо выполнить команду `docker-compose up -d` в директории с файлом `docker-compose.yml`.

### Сервисы проекта:

- `postgres`: Сервер базы данных PostgreSQL для Airflow.
- `clickhouse`: Сервер базы данных ClickHouse для хранения данных вакансий.
- `click-ui`: Web-интерфейс для работы с базой данных ClickHouse.
- `redis`: Сервер Redis для работы с Airflow.
- `grafana`: Сервер Grafana для визуализации данных.
- `prometheus`: Сервер для сбора метрик.
- `airflow-*`: Набор сервисов, образующих среду выполнения Apache Airflow.
- `selenium-*`: Набор сервисов для выполнения сбора данных с помощью Selenium Grid.
- `chrome`: Сервис для запуска экземпляров браузера Chrome.

### Соединение с DBeaver:

    Создать соединение -> ClickHouse

    Host: localhost
    Schema: vacancy
    Login: admin
    Password: password
    Port: 8123

### Соединение с Grafana:

Откройте в браузере адрес `localhost:3000`. Используйте имя пользователя 'admin' и пароль 'password' для входа в систему. ClickHouse используется в качестве источника данных для получения и отображения собираемых данных.

### Соединение с Prometheus:

Откройте в браузере адрес `localhost:9090`. Prometheus собирает метрики с различных компонентов системы для последующего их анализа и визуализации в Grafana.

### Selenium Grid:

Selenium Grid используется для параллельного и распределенного сбора данных. Это обеспечивает более эффективное использование ресурсов и ускоряет процесс. Можно открыть `localhost:4444` в браузере, чтобы увидеть статус Selenium Grid.