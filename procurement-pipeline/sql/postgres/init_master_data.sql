-- Initialize Master Data

-- 1. Clean up existing tables
DROP TABLE IF EXISTS replenishment_rules;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS suppliers;

-- 2. Create Tables
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    case_size INT NOT NULL,
    price DECIMAL(10, 2)
);

CREATE TABLE suppliers (
    supplier_id VARCHAR(50) PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255),
    lead_time_days INT
);

CREATE TABLE replenishment_rules (
    rule_id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) REFERENCES products(product_id),
    supplier_id VARCHAR(50) REFERENCES suppliers(supplier_id),
    moq INT NOT NULL DEFAULT 1,
    safety_stock_level INT NOT NULL DEFAULT 0,
    UNIQUE(product_id, supplier_id)
);

-- 3. Populate Mock Data
-- Suppliers
INSERT INTO suppliers (supplier_id, supplier_name, contact_email, lead_time_days) VALUES
('SUP-001', 'Acme Corp', 'orders@acme.com', 3),
('SUP-002', 'Global Foods', 'sales@globalfoods.com', 5),
('SUP-003', 'TechParts Inc', 'supply@techparts.com', 7);

-- Products
INSERT INTO products (product_id, product_name, category, case_size, price) VALUES
('SKU-0001', 'Organic Apple Juice', 'Beverages', 24, 15.50),
('SKU-0002', 'Whole Wheat Bread', 'Bakery', 12, 3.20),
('SKU-0003', 'Wireless Mouse', 'Electronics', 10, 25.00),
('SKU-0004', 'LED Monitor 24"', 'Electronics', 1, 150.00),
('SKU-0005', 'Almond Milk', 'Beverages', 12, 4.50);

-- Replenishment Rules
INSERT INTO replenishment_rules (product_id, supplier_id, moq, safety_stock_level) VALUES
('SKU-0001', 'SUP-001', 50, 100),
('SKU-0002', 'SUP-002', 20, 50),
('SKU-0003', 'SUP-003', 100, 200),
('SKU-0004', 'SUP-003', 5, 10),
('SKU-0005', 'SUP-001', 40, 80);
