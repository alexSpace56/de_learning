-- Создаем схему для сырых данных
CREATE SCHEMA IF NOT EXISTS raw;

-- Создаем таблицу продаж
CREATE TABLE IF NOT EXISTS raw.sales (
    id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    quantity INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) GENERATED ALWAYS AS (quantity * price) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу для хранения метаданных о загрузках
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.load_dates (
    table_name VARCHAR(100) PRIMARY KEY,
    last_load_date DATE NOT NULL
);

-- Создаем индексы для ускорения выборок
CREATE INDEX IF NOT EXISTS idx_sales_date ON raw.sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_updated ON raw.sales(updated_at);