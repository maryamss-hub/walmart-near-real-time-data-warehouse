# Walmart Near-Real-Time Data Warehouse using HYBRIDJOIN

## Overview

This project builds a **near-real-time data warehouse for Walmart** to analyze customer purchasing behavior efficiently.

It uses the **HYBRIDJOIN algorithm implemented in Python** to join streaming transaction data with master data and stores it in a **MySQL star schema** for fast analytical queries.

The system processes **550,000+ transactions in minutes**, enabling real-time business intelligence and OLAP analysis.

---

## Key Features

* Near-real-time ETL pipeline
* HYBRIDJOIN algorithm implementation
* Star schema data warehouse design
* 5 Dimension tables and 1 Fact table
* 20 OLAP analytical queries
* Processes 550,000+ records efficiently

---

## Technologies Used

* Python
* MySQL
* SQL
* Pandas
* Data Warehousing
* ETL Processing

---

## Project Structure

```
create_dw.sql        # Database schema
hybrid_join.py      # ETL pipeline using HYBRIDJOIN
queries_dw.sql      # OLAP analytical queries
Project_Report.pdf  # Detailed documentation
README.md
```

---

## Dataset Files Required

* customer_master_data.csv
* product_master_data.csv
* transactional_data.csv

---

## Installation

Install required packages:

```
pip install pandas mysql-connector-python
```

Requirements:

* Python 3.8+
* MySQL 8.0+

---

## How to Run

### Step 1: Create Database

Run in MySQL:

```
create_dw.sql
```

---

### Step 2: Run ETL Pipeline

```
python hybrid_join.py
```

This loads all transaction data into the warehouse.

---

### Step 3: Run Analytical Queries

Execute:

```
queries_dw.sql
```

---

## Data Warehouse Schema

Star Schema:

Fact Table:

* FactSales

Dimension Tables:

* DimCustomer
* DimProduct
* DimStore
* DimSupplier
* DimDate

---

## Performance

* Transactions processed: 550,068
* Processing time: ~2 minutes
* High-performance real-time processing

---

## Skills Demonstrated

* Data Warehousing
* ETL Development
* SQL
* Python
* Dimensional Modeling
* OLAP Queries

---

## Author

Maryam Khalid
Data Science Student
