import os
from dotenv_loader import load_dotenv
import psycopg2


# Загружаем переменные окружения
load_dotenv()


def init_database():
    """Инициализация базы данных: создание схем и таблиц"""

    # Параметры подключения
    conn_params = {
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT'),
        'database': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD')
    }

    try:
        # Подключаемся к БД
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cursor = conn.cursor()

        print("Подключение к базе данных установлено")

        # Читаем SQL-скрипт
        with open('sql/01_schema.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()

        # Выполняем SQL-скрипт
        cursor.execute(sql_script)
        print("Схемы и таблицы успешно созданы")

        # Закрываем соединение
        cursor.close()
        conn.close()
        print("Соединение закрыто")

    except Exception as e:
        print(f"Ошибка при инициализации базы данных: {e}")
        raise


if __name__ == "__main__":
    init_database()