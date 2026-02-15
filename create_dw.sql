-- Data Warehouse Schema Creation 

-- Drop existing database
DROP DATABASE IF EXISTS walmart_dw;
CREATE DATABASE walmart_dw;
USE walmart_dw;

-- =====================================================================
-- DIMENSION TABLES
-- =====================================================================

-- Dimension: Customer
DROP TABLE IF EXISTS DimCustomer;
CREATE TABLE DimCustomer (
    Customer_Key INT AUTO_INCREMENT PRIMARY KEY,
    Customer_ID INT NOT NULL UNIQUE,
    Gender VARCHAR(10),
    Age VARCHAR(10),
    Occupation INT,
    City_Category VARCHAR(10),
    Stay_In_Current_City_Years INT,
    Marital_Status INT,
    INDEX idx_customer_id (Customer_ID),
    INDEX idx_gender (Gender),
    INDEX idx_age (Age),
    INDEX idx_occupation (Occupation),
    INDEX idx_city (City_Category)
) ENGINE=InnoDB;

-- Dimension: Product
DROP TABLE IF EXISTS DimProduct;
CREATE TABLE DimProduct (
    Product_Key INT AUTO_INCREMENT PRIMARY KEY,
    Product_ID VARCHAR(20) NOT NULL UNIQUE,
    Product_Category VARCHAR(50),
    Price DECIMAL(10,2),
    Store_ID INT,
    Supplier_ID INT,
    INDEX idx_product_id (Product_ID),
    INDEX idx_category (Product_Category),
    INDEX idx_store (Store_ID),
    INDEX idx_supplier (Supplier_ID)
) ENGINE=InnoDB;

-- Dimension: Store
DROP TABLE IF EXISTS DimStore;
CREATE TABLE DimStore (
    Store_Key INT AUTO_INCREMENT PRIMARY KEY,
    Store_ID INT NOT NULL UNIQUE,
    Store_Name VARCHAR(100),
    INDEX idx_store_id (Store_ID)
) ENGINE=InnoDB;

-- Dimension: Supplier
DROP TABLE IF EXISTS DimSupplier;
CREATE TABLE DimSupplier (
    Supplier_Key INT AUTO_INCREMENT PRIMARY KEY,
    Supplier_ID INT NOT NULL UNIQUE,
    Supplier_Name VARCHAR(100),
    INDEX idx_supplier_id (Supplier_ID)
) ENGINE=InnoDB;

-- Dimension: Date
DROP TABLE IF EXISTS DimDate;
CREATE TABLE DimDate (
    Date_Key INT AUTO_INCREMENT PRIMARY KEY,
    Full_Date DATE NOT NULL UNIQUE,
    Day INT,
    Month INT,
    Month_Name VARCHAR(20),
    Quarter INT,
    Year INT,
    Day_Of_Week INT,
    Day_Name VARCHAR(20),
    Is_Weekend BOOLEAN,
    Week_Of_Year INT,
    Season VARCHAR(20),
    Half_Year INT,
    INDEX idx_full_date (Full_Date),
    INDEX idx_month (Month),
    INDEX idx_quarter (Quarter),
    INDEX idx_year (Year),
    INDEX idx_weekend (Is_Weekend)
) ENGINE=InnoDB;

-- =====================================================================
-- FACT TABLE
-- =====================================================================

-- Fact: Sales Transactions
DROP TABLE IF EXISTS FactSales;
CREATE TABLE FactSales (
    Sale_Key BIGINT AUTO_INCREMENT PRIMARY KEY,
    Order_ID BIGINT NOT NULL,
    Customer_Key INT NOT NULL,
    Product_Key INT NOT NULL,
    Store_Key INT NOT NULL,
    Supplier_Key INT NOT NULL,
    Date_Key INT NOT NULL,
    Quantity INT NOT NULL,
    Unit_Price DECIMAL(10,2),
    Total_Amount DECIMAL(12,2),
    
    -- Foreign Key Constraints
    CONSTRAINT fk_customer FOREIGN KEY (Customer_Key) 
        REFERENCES DimCustomer(Customer_Key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    
    CONSTRAINT fk_product FOREIGN KEY (Product_Key) 
        REFERENCES DimProduct(Product_Key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    
    CONSTRAINT fk_store FOREIGN KEY (Store_Key) 
        REFERENCES DimStore(Store_Key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    
    CONSTRAINT fk_supplier FOREIGN KEY (Supplier_Key) 
        REFERENCES DimSupplier(Supplier_Key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    
    CONSTRAINT fk_date FOREIGN KEY (Date_Key) 
        REFERENCES DimDate(Date_Key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    
    -- Indexes for query optimization
    INDEX idx_order (Order_ID),
    INDEX idx_customer (Customer_Key),
    INDEX idx_product (Product_Key),
    INDEX idx_store (Store_Key),
    INDEX idx_supplier (Supplier_Key),
    INDEX idx_date (Date_Key),
    INDEX idx_composite_date_product (Date_Key, Product_Key),
    INDEX idx_composite_customer_date (Customer_Key, Date_Key)
) ENGINE=InnoDB;

-- =====================================================================
-- VIEWS FOR OPTIMIZED QUERIES (Q20)
-- =====================================================================

-- View: Store Quarterly Sales
CREATE OR REPLACE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    ds.Store_Name,
    dd.Year,
    dd.Quarter,
    SUM(fs.Total_Amount) AS Total_Sales,
    SUM(fs.Quantity) AS Total_Quantity,
    COUNT(DISTINCT fs.Order_ID) AS Total_Orders
FROM FactSales fs
JOIN DimStore ds ON fs.Store_Key = ds.Store_Key
JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
GROUP BY ds.Store_Name, dd.Year, dd.Quarter
ORDER BY ds.Store_Name, dd.Year, dd.Quarter;

-- =====================================================================
-- VERIFICATION QUERIES
-- =====================================================================

-- Show all tables
SHOW TABLES;

-- Verify table structures
DESCRIBE DimCustomer;
DESCRIBE DimProduct;
DESCRIBE DimStore;
DESCRIBE DimSupplier;
DESCRIBE DimDate;
DESCRIBE FactSales;

-- Display foreign key relationships
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    CONSTRAINT_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = 'walmart_dw'
    AND REFERENCED_TABLE_NAME IS NOT NULL;
