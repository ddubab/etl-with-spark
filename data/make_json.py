import json
import random
from datetime import datetime, timedelta

# 랜덤 데이터 생성 함수
def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_data():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 1, 1)
    
    products = ["Laptop", "Smartphone", "Tablet", "Headphones", "Monitor"]
    data = []
    
    for i in range(100):
        entry = {
            "customer_id": random.randint(1000, 9999),
            "order_id": random.randint(100000, 999999),
            "order_date": random_date(start_date, end_date).strftime('%Y-%m-%d'),
            "order_amount": f"{random.randint(100, 1000)}.00",
            "product_name": random.choice(products)
        }
        data.append(entry)
    
    return data

# 데이터 생성 및 저장
data = generate_data()

with open("orders.json", "w") as f:
    json.dump(data, f, indent=4)

print("JSON 데이터 100개 생성 완료!")
