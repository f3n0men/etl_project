## Самый быстрый способ запуска (докер)

```bash
git clone <URL>
cd etl_project

python3 src/data_generator.py

# только docker desktop не забыть открыть надо
docker-compose up --build 


#  Проверить результаты
cat data/output/result.csv
# или открыть http://localhost:9001 (MinIO: minioadmin/minioadmin)
```

## (локально)

```bash
pip install -r requirements.txt

cd src && python data_generator.py && cd ..

cd src && python etl_process.py && cd ..
```


### Структура данных

**Входные файлы** (в `data/input/`):
- `stores.parquet` - магазины
- `users.parquet` - пользователи
- `orders.parquet` - заказы

**Выходной файл** (в `data/output/`):
- `result.parquet` - топ-3 магазинов по городам
- `result.csv` - тот же результат в CSV

### Логи и метрики

```bash
# Смотрим логи
ls -lh logs/
cat logs/etl_*.log

# Смотрим метрики
cat logs/metrics_*.json
```



### Тесты

```bash
# Запустить все тесты
pytest tests/ -v

# С покрытием кода
pytest tests/ --cov=src --cov-report=term
```


## 

### Проверить что контейнеры работают

```bash
docker-compose ps
```


### Проверить логи контейнеров

```bash
# Логи MinIO
docker-compose logs minio

# Логи ETL приложения
docker-compose logs etl-app
```


