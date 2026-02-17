# Unit тесты


import pytest
import pandas as pd
from datetime import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from etl_process import StoreAnalyticsETL


class TestETLTransform:
    @pytest.fixture
    def sample_stores(self):
        return pd.DataFrame({
            'id': [1, 2, 3, 4],
            'name': ['Store_A', 'Store_B', 'Store_C', 'Store_D'],
            'city': ['Moscow', 'Moscow', 'SPB', 'SPB']
        })
    
    @pytest.fixture
    def sample_users(self):
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['User_1', 'User_2', 'User_3', 'User_4', 'User_5'],
            'phone': ['+71111111111'] * 5,
            'created_at': [
                datetime(2025, 1, 15),  
                datetime(2025, 6, 20),  
                datetime(2024, 12, 31), 
                datetime(2025, 12, 1),  
                datetime(2023, 5, 10)   
            ]
        })
    
    @pytest.fixture
    def sample_orders(self):
        return pd.DataFrame({
            'id': list(range(1, 11)),
            'amount': [100.0, 200.0, 150.0, 300.0, 250.0, 400.0, 180.0, 220.0, 350.0, 120.0],
            'user_id': [1, 1, 2, 2, 3, 3, 4, 4, 5, 1],  # Заказы от разных пользователей
            'store_id': [1, 2, 1, 2, 1, 2, 3, 4, 3, 1],  # В разных магазинах
            'status': ['completed'] * 10,
            'created_at': [datetime(2025, 1, i) for i in range(1, 11)]
        })
    
    def test_filter_users_2025(self, sample_users):
        etl = StoreAnalyticsETL()
        users_2025 = sample_users[sample_users['created_at'].dt.year == 2025]
        
        # Должно быть 3 пользователя из 2025 года (id: 1, 2, 4)
        assert len(users_2025) == 3
        assert set(users_2025['id'].tolist()) == {1, 2, 4}
    
    def test_filter_orders_by_users(self, sample_users, sample_orders):

        users_2025 = sample_users[sample_users['created_at'].dt.year == 2025]
        user_ids_2025 = set(users_2025['id'])

        orders_filtered = sample_orders[sample_orders['user_id'].isin(user_ids_2025)]
    
        assert set(orders_filtered['user_id'].unique()) == {1, 2, 4}
        assert 3 not in orders_filtered['user_id'].values
        assert 5 not in orders_filtered['user_id'].values
    
    def test_top3_per_city(self, sample_stores, sample_users, sample_orders):
        etl = StoreAnalyticsETL()
        
        result = etl.transform(sample_stores, sample_users, sample_orders)
        
        assert 'city' in result.columns
        assert 'store_name' in result.columns
        assert 'target_amount' in result.columns
        
        # Проверяем, что для каждого города не больше 3 магазинов
        for city in result['city'].unique():
            city_stores = result[result['city'] == city]
            assert len(city_stores) <= 3
    
    def test_amount_calculation(self, sample_stores, sample_users, sample_orders):
        etl = StoreAnalyticsETL()
        result = etl.transform(sample_stores, sample_users, sample_orders)
        
        assert all(result['target_amount'] > 0)
        
        # Проверяем тип данных
        assert result['target_amount'].dtype in ['float64', 'float32']
    
    def test_sorting_within_city(self, sample_stores, sample_users, sample_orders):
        etl = StoreAnalyticsETL()
        result = etl.transform(sample_stores, sample_users, sample_orders)
        
        for city in result['city'].unique():
            city_data = result[result['city'] == city]
            amounts = city_data['target_amount'].tolist()
            
            # Суммы должны быть отсортированы по убыванию
            assert amounts == sorted(amounts, reverse=True)
    
    def test_no_duplicates(self, sample_stores, sample_users, sample_orders):
        etl = StoreAnalyticsETL()
        result = etl.transform(sample_stores, sample_users, sample_orders)
        
        # Проверяем уникальность пар (city, store_name)
        duplicates = result.duplicated(subset=['city', 'store_name'])
        assert not duplicates.any()
    
    def test_empty_users_2025(self, sample_stores, sample_orders):
        # Создаем пользователей только из 2024 года
        users_2024 = pd.DataFrame({
            'id': [1, 2],
            'name': ['User_1', 'User_2'],
            'phone': ['+71111111111'] * 2,
            'created_at': [datetime(2024, 1, 1), datetime(2024, 6, 1)]
        })
        
        etl = StoreAnalyticsETL()
        result = etl.transform(sample_stores, users_2024, sample_orders)
        assert len(result) == 0


class TestDataGenerator:
    
    def test_stores_generation(self):
        from data_generator import DataGenerator
        
        gen = DataGenerator(num_stores=10, num_users=50, num_orders=100)
        stores_df = gen.generate_stores()
        
        assert len(stores_df) == 10
        
        assert 'id' in stores_df.columns
        assert 'name' in stores_df.columns
        assert 'city' in stores_df.columns
        

        assert stores_df['id'].is_unique
    
    def test_users_generation(self):
        from data_generator import DataGenerator
        
        gen = DataGenerator(num_stores=10, num_users=100, num_orders=200)
        users_df = gen.generate_users()
        
        assert len(users_df) == 100
        
        users_2025 = users_df[users_df['created_at'].dt.year == 2025]
        assert len(users_2025) > 0  # Должны быть пользователи 2025 года
        
        assert users_df['id'].is_unique
    
    def test_orders_generation(self):
        from data_generator import DataGenerator
        gen = DataGenerator(num_stores=5, num_users=20, num_orders=50)
        stores_df = gen.generate_stores()
        users_df = gen.generate_users()
        orders_df = gen.generate_orders(
            store_ids=stores_df['id'].tolist(),
            user_ids=users_df['id'].tolist()
        )
        
        # Проверяем количество
        assert len(orders_df) == 50
        
        # Проверяем корректность foreign keys
        assert all(orders_df['user_id'].isin(users_df['id']))
        assert all(orders_df['store_id'].isin(stores_df['id']))
        
        # Проверяем положительные суммы
        assert all(orders_df['amount'] > 0)


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
