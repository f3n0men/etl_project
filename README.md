Этот проект реализует ETL процесс для анализа заказов пользователей в магазинах.


## Структура проекта

```
etl_project/
   src/
      main.py              
      etl_process.py       # ETL логика
      data_generator.py    # Генератор данных
   data/
      input/               # Parquet файлы
      output/              # Результаты ETL
   logs/                    # Логи
   tests/                   # Тесты
   config/                  # Конфиг
   docker-compose.yml       # Оркестрация контейнеров
   Dockerfile               # Образ приложения
   requirements.txt         # зависимости
   README.md              
```



### Локальный запуск без Docker

#### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

#### 2. Генерация тестовых данных

```bash
cd src
python data_generator.py
```

Это создаст 3 Parquet файла в `data/input/`:
- `stores.parquet` 
- `users.parquet` 
- `orders.parquet`

#### 3. Запуск ETL

```bash
python etl_process.py
```

Результат будет сохранен в `data/output/`:
- `result.parquet` - результат в Parquet
- `result.csv` - результат в CSV 

Логи и метрики будут в директории `logs/`.

---

### Запуск в Docker с MinIO

#### 1. Подготовка данных

Сначала сгенерируйте тестовые данные локально:

```bash
cd src
python data_generator.py
cd ..
```

#### 2. Запуск всего стека

```bash
docker-compose up --build
```

#### 3. Доступ к MinIO Web UI

Откройте браузер: `http://localhost:9001`

**Логин**: `minioadmin`  
**Пароль**: `minioadmin`

Будет создан bucket `etl-data` с данными:
- `input/stores.parquet`
- `input/users.parquet`
- `input/orders.parquet`
- `output/result.parquet`

#### 4. Просмотр результатов

После завершения работы ETL:

```bash
# Локальный результат
cat data/output/result.csv


```
#### 5. Остановка

```bash
docker-compose down
```

Для полной очистки (включая данные MinIO):

```bash
docker-compose down -v
```


### Выходные данные

**result.parquet**
| Поле | Тип | Описание |
|------|-----|----------|
| city | varchar | Название города |
| store_name | varchar | Название магазина |
| target_amount | decimal | Сумма заказов от пользователей 2025 года |

---

### Запуск тестов 

```bash
pytest tests/ -v
```

### С покрытием кода

```bash
pytest tests/ --cov=src --cov-report=html
```

---


### Логи

Все логи сохраняются в `logs/` с временными метками:
- `etl_YYYYMMDD_HHMMSS.log` - детальные логи выполнения

### Метрики

Метрики сохраняются в JSON формате в `logs/`:
- `metrics_YYYYMMDD_HHMMSS.json`

### Изменение параметров генератора данных

В `src/data_generator.py` измените:

```python
generator = DataGenerator(
    num_stores=10, #магазины
    num_users=10000,    #пользователи
    num_orders=20000    #заказы
)
```

