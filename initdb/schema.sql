-- Dimension tables
CREATE TABLE IF NOT EXISTS regions (
  region_id INT PRIMARY KEY,
  region_name VARCHAR(50) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS segments (
  segment_id INT PRIMARY KEY,
  segment_name VARCHAR(50) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS customers (
  customer_id INT PRIMARY KEY,
  region_id INT NOT NULL,
  segment_id INT NOT NULL,
  created_at DATETIME NOT NULL,
  INDEX idx_customers_region (region_id),
  INDEX idx_customers_segment (segment_id)
) ENGINE=InnoDB;

-- Fact table
CREATE TABLE IF NOT EXISTS orders (
  order_id INT PRIMARY KEY,
  customer_id INT NOT NULL,
  order_amount DECIMAL(10,2) NOT NULL,
  order_date DATE NOT NULL,
  INDEX idx_orders_customer (customer_id),
  INDEX idx_orders_date (order_date)
) ENGINE=InnoDB;

-- Denormalized fact table
CREATE TABLE IF NOT EXISTS orders_denorm (
  order_id INT PRIMARY KEY,
  customer_id INT NOT NULL,
  region_id INT NOT NULL,
  region_name VARCHAR(50) NOT NULL,
  segment_id INT NOT NULL,
  segment_name VARCHAR(50) NOT NULL,
  order_amount DECIMAL(10,2) NOT NULL,
  order_date DATE NOT NULL,
  customer_created_at DATETIME NOT NULL,
  INDEX idx_denorm_customer (customer_id),
  INDEX idx_denorm_date (order_date),
  INDEX idx_denorm_region (region_id),
  INDEX idx_denorm_segment (segment_id)
) ENGINE=InnoDB;
