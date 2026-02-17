# Генератор данных


import pandas as pd
from datetime import datetime
import random


class DataGenerator:
    
    def __init__(self, num_stores=10, num_users=10000, num_orders=20000):
        self.num_stores = num_stores
        self.num_users = num_users
        self.num_orders = num_orders
        
        self.cities = ['Москва', 'Санкт-Петербург', 'Магнитогорск', 'Рыбинск', 'Казань']

        self.statuses = ['completed', 'pending', 'cancelled', 'processing']
        
    def generate_stores(self):
        # данные о магазинах

        
        stores = []
        for i in range(1, self.num_stores + 1):
            store = {
                'id': i,
                'name': f'Магазин_{i}',
                'city': random.choice(self.cities)
            }
            stores.append(store)
        
        df = pd.DataFrame(stores)
        return df
    
    def generate_users(self):
        # данные о пользователях        
        users = []
        for i in range(1, self.num_users + 1):

            if random.random() < 0.75:
                year = 2025
                month = random.randint(1, 12)
                day = random.randint(1, 28)
            else:
                # Пользователи других лет
                year = random.choice([2023, 2024, 2026])
                month = random.randint(1, 12)
                day = random.randint(1, 28)
            
            created_at = datetime(year, month, day, 
                                 random.randint(0, 23), 
                                 random.randint(0, 59))
            
            user = {
                'id': i,
                'name': f'User_{i}',
                'phone': f'+7{random.randint(9000000000, 9999999999)}',
                'created_at': created_at
            }
            users.append(user)
        
        df = pd.DataFrame(users)   
        return df
    
    def generate_orders(self, store_ids, user_ids):
        # Генерирует заказы
        
        orders = []
        for i in range(1, self.num_orders + 1):
            amount = round(random.uniform(100, 10000), 2)
            
            # Случайная дата заказа в 2025 году
            created_at = datetime(2025, 
                                 random.randint(1, 12), 
                                 random.randint(1, 28),
                                 random.randint(0, 23),
                                 random.randint(0, 59))
            
            order = {
                'id': i,
                'amount': amount,
                'user_id': random.choice(user_ids),
                'store_id': random.choice(store_ids),
                'status': random.choice(self.statuses),
                'created_at': created_at
            }
            orders.append(order)
        
        df = pd.DataFrame(orders)
        return df
    
    def save_to_parquet(self, df, filename, output_dir='../data/input'):
    #    сохранение в parquet
        filepath = f"{output_dir}/{filename}"
        
        try:
            df.to_parquet(filepath, index=False, engine='pyarrow')
        except (ImportError, ModuleNotFoundError):
            # на всякий случай в CSV (если не получилось)
            csv_filepath = filepath.replace('.parquet', '.csv')
            df.to_csv(csv_filepath, index=False, encoding='utf-8-sig')
            filepath = csv_filepath
        
        return filepath
    
    def generate_all(self, output_dir='../data/input'):
        #генерирация ВСЕХ данных и сохранение в Parquet

        stores_df = self.generate_stores()
        self.save_to_parquet(stores_df, 'stores.parquet', output_dir)
        
        users_df = self.generate_users()
        self.save_to_parquet(users_df, 'users.parquet', output_dir)

        orders_df = self.generate_orders(
            store_ids=stores_df['id'].tolist(),
            user_ids=users_df['id'].tolist()
        )
        self.save_to_parquet(orders_df, 'orders.parquet', output_dir)

        return stores_df, users_df, orders_df


if __name__ == '__main__':
    generator = DataGenerator(
        num_stores=10,    
        num_users=10000,   
        num_orders=20000    
    )
    
    # Генерируем данные
    generator.generate_all()
