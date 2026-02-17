# расчет топ-3 магазинов по городам

import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import logging


class StoreAnalyticsETL:
    # класс для анализа магазинов

    
    def __init__(self, input_dir='../data/input', output_dir='../data/output', log_dir='../logs'):
        """
        Args:
            input_dir: директория с входными данными
            output_dir: директория для результатов
            log_dir: директория для логов
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.log_dir = Path(log_dir)
        
        # создаем дериктории если нет
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self._setup_logging()
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'duration_seconds': None,
            'records_processed': {},
            'result_records': 0
        }
    
    def _setup_logging(self):
        log_file = self.log_dir / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Лог файл создан: {log_file}")
    
    def extract(self):
    # извлечение из parquet
        
        self.metrics['start_time'] = datetime.now()
        self.logger.info("\nЗагрузка данных")
        
        try:
            stores_path = self.input_dir / 'stores.parquet'
            stores_df = pd.read_parquet(stores_path)
            self.logger.info(f"Загружено магазинов: {len(stores_df)}")
            self.metrics['records_processed']['stores'] = len(stores_df)
            
            users_path = self.input_dir / 'users.parquet'
            users_df = pd.read_parquet(users_path)
            self.logger.info(f"Загружено пользователей: {len(users_df)}")
            self.metrics['records_processed']['users'] = len(users_df)
            
            orders_path = self.input_dir / 'orders.parquet'
            orders_df = pd.read_parquet(orders_path)
            self.logger.info(f"Загружено заказов: {len(orders_df)}")
            self.metrics['records_processed']['orders'] = len(orders_df)
            
            return stores_df, users_df, orders_df
            
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке данных: {e}")
            raise
    
    def transform(self, stores_df, users_df, orders_df):
        # Фильтруем пользователей: только те, кто зарегистрирован в 2025
        # Джойним с магазинами чтобы получить город
        # Группируем по городу и магазину, суммируем amount
        # Для каждого города берем топ-3 магазина

        self.logger.info("\nОбработка данных...")
        
        try:
            # Фильтруем пользователей 
            users_2025 = users_df[users_df['created_at'].dt.year == 2025].copy()
            self.logger.info(f"Пользователи 2025 года: {len(users_2025)} из {len(users_df)}")
            self.metrics['records_processed']['users_2025'] = len(users_2025)
            
            # Фильтруем заказы 
            self.logger.info("Заказы от пользователей 2025 года")
            user_ids_2025 = set(users_2025['id'])
            orders_filtered = orders_df[orders_df['user_id'].isin(user_ids_2025)].copy()
            self.logger.info(f"Найдено заказов: {len(orders_filtered)} из {len(orders_df)}")
            self.metrics['records_processed']['orders_filtered'] = len(orders_filtered)
            
            # джойним заказы с магазинами
            self.logger.info("Объединение заказов с магазинами")
            merged = orders_filtered.merge(
                stores_df[['id', 'name', 'city']], 
                left_on='store_id', 
                right_on='id',
                how='inner'
            )
            self.logger.info(f"Объединено записей: {len(merged)}")
            
            # группируем по городу и магазину , считаем сумму
            self.logger.info("Группировка по городу и магазину")
            grouped = merged.groupby(['city', 'name'])['amount'].sum().reset_index()
            grouped.columns = ['city', 'store_name', 'target_amount']
            grouped = grouped.sort_values(['city', 'target_amount'], ascending=[True, False])
            
            # берем топ-3 магазина в каждом городе
            self.logger.info("Шаг 5: Выбор топ-3 магазинов для каждого города...")
            result = grouped.groupby('city').head(3).reset_index(drop=True)
            self.logger.info(f"✓ Итоговых записей в результате: {len(result)}")
            
            # Выводим статистику по городам
            self.logger.info("\nСтатистика по городам:")
            for city in result['city'].unique():
                city_data = result[result['city'] == city]
                self.logger.info(f"  {city}: {len(city_data)} магазинов, "
                               f"общая сумма топ-3: {city_data['target_amount'].sum():,.2f} руб.")
            
            self.metrics['result_records'] = len(result)
            return result
            
        except Exception as e:
            self.logger.error(f"Ошибка при обработке данных: {e}")
            raise
    
    def load(self, result_df):

        # сохранение в Parquet
        
        self.logger.info("\n[Сохранение результата")
        
        try:
            output_path = self.output_dir / 'result.parquet'
            result_df.to_parquet(output_path, index=False, engine='pyarrow')
            self.logger.info(f"Результат сохранен: {output_path}")
            
            # + в csv
            csv_path = self.output_dir / 'result.csv'
            result_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

        except Exception as e:
            self.logger.error(f"Ошибка при сохранении результата: {e}")
            raise
    
    def save_metrics(self):
        self.metrics['end_time'] = datetime.now()
        self.metrics['duration_seconds'] = (
            self.metrics['end_time'] - self.metrics['start_time']
        ).total_seconds()
        
        # делаем из datetime строки для JSON
        metrics_to_save = {
            'start_time': self.metrics['start_time'].isoformat(),
            'end_time': self.metrics['end_time'].isoformat(),
            'duration_seconds': self.metrics['duration_seconds'],
            'records_processed': self.metrics['records_processed'],
            'result_records': self.metrics['result_records']
        }
        
        metrics_path = self.log_dir / f"metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(metrics_path, 'w', encoding='utf-8') as f:
            json.dump(metrics_to_save, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"\Метрики сохранены: {metrics_path}")
        self.logger.info(f"Время выполнения: {self.metrics['duration_seconds']:.2f} секунд")
    
    def run(self):
        # весь процесс (extract transform load)
        try:
            stores_df, users_df, orders_df = self.extract()
            result_df = self.transform(stores_df, users_df, orders_df)
            self.load(result_df)
            self.save_metrics()
        
            return result_df
            
        except Exception as e:
            self.logger.error(f"\nОШИБКА: {e}")
            raise


if __name__ == '__main__':
    etl = StoreAnalyticsETL()
    result = etl.run()

    # смотрим первые 10 строк результата для наглядности
    print(result.head(10).to_string(index=False))
