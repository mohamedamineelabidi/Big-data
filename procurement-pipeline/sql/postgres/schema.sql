-- Master Data Schema for Procurement System

-- Products Table
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    case_size INT NOT NULL, -- Number of units per case
    price DECIMAL(10, 2)
);

-- Suppliers Table
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id VARCHAR(50) PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255),
    lead_time_days INT
);

-- Replenishment Rules Table
CREATE TABLE IF NOT EXISTS replenishment_rules (
    rule_id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) REFERENCES products(product_id),
    supplier_id VARCHAR(50) REFERENCES suppliers(supplier_id),
    moq INT NOT NULL DEFAULT 1, -- Minimum Order Quantity
    safety_stock_level INT NOT NULL DEFAULT 0,
    UNIQUE(product_id, supplier_id)
);
