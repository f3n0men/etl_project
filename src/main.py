# main file

import os
import pandas as pd
from pathlib import Path
import logging
import boto3
from botocore.exceptions import ClientError


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class S3Handler:
    # работа с minio, s3
    def __init__(self, endpoint, access_key, secret_key, bucket_name):
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1' 
        )
        
        logger.info(f"S3 клиент создан для {endpoint}")
        
    def ensure_bucket_exists(self):
        # создание бакета
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' уже существует")
        except ClientError:
            self.s3_client.create_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' создан")
    
    def upload_file(self, local_path, s3_key):
        # Загрузка в s3
        try:
            self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key)
            logger.info(f"Загружено в S3: {s3_key}")
        except Exception as e:
            logger.error(f"Ошибка загрузки {local_path} в S3: {e}")
            raise
    
    def download_file(self, s3_key, local_path):
        # выгрузка из S3
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self.s3_client.download_file(self.bucket_name, s3_key, str(local_path))
            logger.info(f"Скачано из S3: {s3_key} в {local_path}")
        except Exception as e:
            logger.error(f"Ошибка скачивания {s3_key} из S3: {e}")
            raise
    
    def read_parquet_from_s3(self, s3_key):
        # parquet из s3 в df
        try:
            import tempfile
            import os
            
            #  cкачиваем во временный файл
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                self.s3_client.download_fileobj(self.bucket_name, s3_key, tmp)
                tmp_path = tmp.name
            
            df = pd.read_parquet(tmp_path)
            os.remove(tmp_path)
            
            logger.info(f"Прочитано из S3: {s3_key}, строк: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"Ошибка чтения {s3_key} из S3: {e}")
            raise
    
    def write_parquet_to_s3(self, df, s3_key):
        # запись df в parquet в S3

        try:
            # Сохраняем во временный файл
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                df.to_parquet(tmp.name, index=False, engine='pyarrow')
                tmp_path = tmp.name
            
            # загружаем в S3
            self.upload_file(tmp_path, s3_key)
            os.remove(tmp_path)
            
            logger.info(f"Записано в S3: {s3_key}, строк: {len(df)}")
        except Exception as e:
            logger.error(f"Ошибка записи в S3 {s3_key}: {e}")
            raise


class ETLRunner:
    # режимы запуска
    
    def __init__(self, run_mode='local'):
    # режимы запуска с3 ил или локально
        self.run_mode = run_mode
        self.s3_handler = None
        
        if run_mode == 's3':
            self._setup_s3()
    
    def _setup_s3(self):
        endpoint = os.getenv('S3_ENDPOINT', 'http://minio:9000')
        access_key = os.getenv('S3_ACCESS_KEY', 'minioadmin')
        secret_key = os.getenv('S3_SECRET_KEY', 'minioadmin')
        bucket_name = os.getenv('S3_BUCKET', 'etl-data')
        
        logger.info(f"Настройка S3: endpoint={endpoint}, bucket={bucket_name}")
        
        self.s3_handler = S3Handler(endpoint, access_key, secret_key, bucket_name)
        self.s3_handler.ensure_bucket_exists()
    
    def run_local_mode(self):
        # локал
        from etl_process import StoreAnalyticsETL
        
        etl = StoreAnalyticsETL(
            input_dir='./data/input',
            output_dir='./data/output',
            log_dir='./logs'
        )
        
        result = etl.run()
        return result
    
    def run_s3_mode(self):
        # с3
        try:
            logger.info("\nзагрузка входных данных в S3...")
            local_input_dir = Path('./data/input')
            if local_input_dir.exists():
                for file_name in ['stores.parquet', 'users.parquet', 'orders.parquet']:
                    local_file = local_input_dir / file_name
                    if local_file.exists():
                        self.s3_handler.upload_file(local_file, f'input/{file_name}')
            
            logger.info("\nЧтение данных из S3")
            stores_df = self.s3_handler.read_parquet_from_s3('input/stores.parquet')
            users_df = self.s3_handler.read_parquet_from_s3('input/users.parquet')
            orders_df = self.s3_handler.read_parquet_from_s3('input/orders.parquet')
            
            logger.info("\nПреобразование данны")
            from etl_process import StoreAnalyticsETL
            
            # временный объект только для transform
            etl = StoreAnalyticsETL()
            result_df = etl.transform(stores_df, users_df, orders_df)
        
            logger.info("\nСохранение результата в S3")
            self.s3_handler.write_parquet_to_s3(result_df, 'output/result.parquet')
            logger.info("\nСохранение результата локально...")
            output_dir = Path('./data/output')
            output_dir.mkdir(parents=True, exist_ok=True)
            result_df.to_parquet(output_dir / 'result.parquet', index=False)
            result_df.to_csv(output_dir / 'result.csv', index=False, encoding='utf-8-sig')
            logger.info(f"Результат сохранен в {output_dir}")  
            return result_df
            
        except Exception as e:
            logger.error(f"Ошибка в S3 режиме: {e}")
            raise
    
    def run(self):
    # запуск
        if self.run_mode == 's3':
            return self.run_s3_mode()
        else:
            return self.run_local_mode()


if __name__ == '__main__':
    run_mode = os.getenv('RUN_MODE', 'local')
    
    logger.info(f"Запуск приложения в режиме: {run_mode}")
    runner = ETLRunner(run_mode=run_mode)
    result = runner.run()
    