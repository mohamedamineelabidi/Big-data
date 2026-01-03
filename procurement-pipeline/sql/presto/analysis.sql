-- Presto SQL Analysis Script

-- 1. Create Schema for HDFS Data (Hive Catalog)
-- This assumes the Hive Metastore is running and configured
CREATE SCHEMA IF NOT EXISTS hive.raw
WITH (location = 'hdfs://namenode:9000/raw');

-- 2. Register External Table for POS Orders (JSON)
-- We use the JSON format. Ensure your Presto/Hive setup supports it.
CREATE TABLE IF NOT EXISTS hive.raw.orders (
    order_id VARCHAR,
    pos_id VARCHAR,
    timestamp VARCHAR,
    items ARRAY(ROW(sku VARCHAR, quantity INT, price DOUBLE))
)
WITH (
    format = 'JSON',
    external_location = 'hdfs://namenode:9000/raw/orders'
);

-- 3. Register External Table for Stock (CSV)
CREATE TABLE IF NOT EXISTS hive.raw.stock (
    warehouse_id VARCHAR,
    date VARCHAR,
    sku VARCHAR,
    quantity_on_hand INT
)
WITH (
    format = 'CSV',
    skip_header_line_count = 1,
    external_location = 'hdfs://namenode:9000/raw/stock'
);

-- 4. View: Daily Demand per SKU
-- Aggregates demand from all POS orders for a given date
CREATE OR REPLACE VIEW hive.raw.daily_demand AS
SELECT 
    CAST(substring(timestamp, 1, 10) AS VARCHAR) as date,
    item.sku,
    SUM(item.quantity) as total_demand
FROM hive.raw.orders
CROSS JOIN UNNEST(items) AS t(item)
GROUP BY 1, 2;

-- 5. View: Net Demand Calculation
-- Joins Demand (Hive) + Stock (Hive) + Product Info (Postgres)
CREATE OR REPLACE VIEW hive.raw.net_demand_calculation AS
SELECT 
    d.date,
    d.sku,
    p.product_name,
    p.case_size,
    d.total_demand,
    COALESCE(s.total_stock, 0) as total_stock,
    -- Net Demand = Demand - Stock (cannot be negative)
    GREATEST(d.total_demand - COALESCE(s.total_stock, 0), 0) as net_demand
FROM hive.raw.daily_demand d
LEFT JOIN (
    SELECT date, sku, SUM(quantity_on_hand) as total_stock
    FROM hive.raw.stock
    GROUP BY 1, 2
) s ON d.sku = s.sku AND d.date = s.date
LEFT JOIN postgresql.public.products p ON d.sku = p.product_id;
