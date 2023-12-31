# Описание

В этом репозитории ведется <b>разработка парсеров</b> различных сайтов в рамках создания агрегатора вакансий. 

### MVP продукта включает:

1. <b>Разработку:</b>
   - инфраструктуры проекта (Clickhouse, Postgres, Airflow, Prometheus, Grafana)
   - программ для парсинга и обработки данных с различных сайтов (Сбер, Тинькофф, VK, Яндекс)
   - моделей для очистки описаний, выделения стека технологий в описаниях вакансий, распределения вакансий по уровням специалистов (junior, middle, senior) и направлениям
2. <b>Автоматизацию:</b>
   - получения и загрузки данных в БД
   - определения актуальности вакансии (открыта/закрыта)
   - перехода к core-слою
   - сборку слоя витрин (стандартных и по запросам пользователя)
   - отслеживания состояния системы

<i>Прим. БД поддерживает версионность</i>

