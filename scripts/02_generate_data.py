import os
import random
from datetime import datetime, timedelta
from dotenv_loader import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

# Загружаем переменные окружения
load_dotenv()


def generate_test_data(num_records: int = 100000, start_date: str = "2024-01-01"):
    """Генерация тестовых данных о продажах"""

    # Список продуктов
    products = [
        {'id': 1, 'name': 'Ноутбук', 'category': 'Электроника', 'price': 50000.00},
        {'id': 2, 'name': 'Смартфон', 'category': 'Электроника', 'price': 30000.00},
        {'id': 3, 'name': 'Наушники', 'category': 'Электроника', 'price': 5000.00},
        {'id': 4, 'name': 'Книга', 'category': 'Книги', 'price': 500.00},
        {'id': 5, 'name': 'Кофе', 'category': 'Продукты', 'price': 300.00},
        {'id': 6, 'name': 'Футболка', 'category': 'Одежда', 'price': 1500.00},
        {'id': 7, 'name': 'Кресло', 'category': 'Мебель', 'price': 10000.00},
    ]

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    data = []

    for i in range(num_records):
        product = random.choice(products)
        days_ago = random.randint(0, 365)  # Данные за последний год
        sale_date = start_dt + timedelta(days=days_ago)
        quantity = random.randint(1, 10)
        price = product['price'] * random.uniform(0.8, 1.2)  # Случайная цена ±20%

        # 10% шанс, что запись была обновлена
        updated_at = sale_date + timedelta(days=random.randint(0, 30)) if random.random() < 0.1 else sale_date

        data.append({
            'sale_date': sale_date,
            'product_id': product['id'],
            'product_name': product['name'],
            'category': product['category'],
            'quantity': quantity,
            'price': round(price, 2),
            'created_at': sale_date,
            'updated_at': updated_at
        })

    return pd.DataFrame(data)


def load_data_to_db(df: pd.DataFrame):
    """Загрузка данных в базу данных"""

    # Создаем строку подключения для SQLAlchemy
    connection_string = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"

    engine = create_engine(connection_string)

    try:
        # Загружаем данные в таблицу raw.sales
        df.to_sql(
            name='sales',
            schema='raw',
            con=engine,
            if_exists='replace',  # Для первого запуска
            index=False,
            method='multi',  # Пакетная вставка для производительности
            chunksize=10000
        )
        print(f"Загружено {len(df)} записей в таблицу raw.sales")

        # Инициализируем таблицу метаданных
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO meta.load_dates (table_name, last_load_date) 
                VALUES ('raw.sales', :start_date)
                ON CONFLICT (table_name) DO NOTHING
            """), {'start_date': df['sale_date'].min().date()})
            conn.commit()

        print("Метаданные инициализированы")

    except Exception as e:
        print(f"Ошибка при загрузке данных в БД: {e}")
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    # Генерируем 100,000 записей
    print("Генерация тестовых данных...")
    df = generate_test_data(num_records=100000)

    print(f"Сгенерировано {len(df)} записей")
    print(f"Период данных: с {df['sale_date'].min()} по {df['sale_date'].max()}")

    # Загружаем в БД
    print("Загрузка данных в базу данных...")
    load_data_to_db(df)

    print("Готово! Данные успешно загружены в PostgreSQL")