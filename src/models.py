# модели данных

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import pandas as pd


@dataclass
class Store:
    # магазин
    id: int
    name: str
    city: str
    
    def __post_init__(self):
        if self.id <= 0:
            raise ValueError(f"айди меньше нуля")
        if not self.name or not self.name.strip():
            raise ValueError("Название пустое")
        if not self.city or not self.city.strip():
            raise ValueError("Город пустой")
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            id=int(data['id']),
            name=str(data['name']),
            city=str(data['city'])
        )
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'city': self.city
        }


@dataclass
class User:
    # юзер
    id: int
    name: str
    phone: str
    created_at: datetime
    
    def __post_init__(self):
        if self.id <= 0:
            raise ValueError(f"айди меньше нуля")
        if not self.name or not self.name.strip():
            raise ValueError("имя пустое")
        if not isinstance(self.created_at, datetime):
            raise ValueError("created_at не datetime")
    
    @property
    def registration_year(self):
        return self.created_at.year
    
    def is_registered_in_2025(self) -> bool:
        return self.registration_year == 2025
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            id=int(data['id']),
            name=str(data['name']),
            phone=str(data['phone']),
            created_at=pd.to_datetime(data['created_at'])
        )
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'phone': self.phone,
            'created_at': self.created_at
        }


@dataclass
class Order:
    # заказ
    id: int
    amount: float
    user_id: int
    store_id: int
    status: str
    created_at: datetime
    
    def __post_init__(self):

        if self.id <= 0:
            raise ValueError(f"айди заказа меньше нуля")
        if self.amount < 0:
            raise ValueError(f"сумма меньше")
        if self.user_id <= 0:
            raise ValueError(f"айди пользователя меньше нуля")
        if self.store_id <= 0:
            raise ValueError(f"айди магазина меньше нуля")
        if not isinstance(self.created_at, datetime):
            raise ValueError("created_at не datetime")
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            id=int(data['id']),
            amount=float(data['amount']),
            user_id=int(data['user_id']),
            store_id=int(data['store_id']),
            status=str(data['status']),
            created_at=pd.to_datetime(data['created_at'])
        )
    
    def to_dict(self):
        return {
            'id': self.id,
            'amount': self.amount,
            'user_id': self.user_id,
            'store_id': self.store_id,
            'status': self.status,
            'created_at': self.created_at
        }


@dataclass
class Result:
    # результат
    city: str
    store_name: str
    target_amount: float
    
    def __post_init__(self):
        if not self.city or not self.city.strip():
            raise ValueError("Город пустой")
        if not self.store_name or not self.store_name.strip():
            raise ValueError("Название магазина пустое")
        if self.target_amount < 0:
            raise ValueError(f"сумма меньше нуля")
    
    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            city=str(data['city']),
            store_name=str(data['store_name']),
            target_amount=float(data['target_amount'])
        )
    
    def to_dict(self):
        return {
            'city': self.city,
            'store_name': self.store_name,
            'target_amount': self.target_amount
        }

