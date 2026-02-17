# Используем официальный Python образ
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем requirements.txt
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код приложения
COPY src/ ./src/
COPY config/ ./config/

# Создаем директории для данных и логов
RUN mkdir -p /app/data/input /app/data/output /app/logs

# Устанавливаем переменные окружения
ENV PYTHONUNBUFFERED=1

# По умолчанию запускаем main.py
CMD ["python", "src/main.py"]
