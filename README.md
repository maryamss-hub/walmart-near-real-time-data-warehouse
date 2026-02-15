# Walmart Near-Real-Time Data Warehouse using HYBRIDJOIN

## Overview

This project implements a **near-real-time data warehouse for Walmart** to analyze customer purchasing behavior and generate business insights quickly. The system processes streaming transaction data and joins it with master data efficiently using the **HYBRIDJOIN algorithm**.

The project demonstrates real-world data warehousing concepts including **star schema design, OLAP queries, dimensional modeling, and stream processing**.

---

## Key Features

* Processed **550,000+ transactions**
* Achieved **near-real-time performance**
* Implemented **HYBRIDJOIN algorithm in Python**
* Designed **Star Schema** with fact and dimension tables
* Built and executed **20 OLAP analytical queries**
* Optimized data loading and query performance

---

## Technologies Used

* Python
* MySQL
* SQL
* Pandas
* Data Warehousing Concepts
* OLAP Queries

---

## Data Warehouse Schema

### Fact Table

**FactSales**

* Order ID
* Quantity
* Unit Price
* Total Amount
* Foreign Keys to all dimensions

### Dimension Tables

* DimCustomer
* DimProduct
* DimStore
* DimSupplier
* DimDate

Star schema was used to improve query performance and simplify analysis.

---

## HYBRIDJOIN Algorithm

HYBRIDJOIN enables efficient joining of:

* Streaming transaction data
* Disk-based master data

### Components Used

* Hash Table
* Queue
* Stream Buffer
* Disk Buffer

This approach avoids memory overflow and supports continuous data processing.

---

## OLAP Analysis

Implemented 20 analytical queries including:

* Top revenue generating products
* Customer demographic analysis
* Monthly and quarterly sales trends
* Seasonal analysis
* Revenue growth analysis
* Store and supplier performance

These queries demonstrate:

* Drill-down
* Roll-up
* Slicing and dicing
* Trend analysis

---

## Performance

* Total transactions processed: **550,068**
* Processing time: **~2 minutes**
* Processing speed: **278,000+ records per second**

---

## Skills Demonstrated

* Data Warehousing
* Dimensional Modeling
* Star Schema Design
* SQL Optimization
* Stream Processing
* Python Data Processing
* Business Intelligence Concepts

---

## Project Structure

```
project/
│
├── data/
├── scripts/
├── sql/
├── results/
├── README.md
└── report.pdf
```

---

## Real-World Applications

This system can be used for:

* Retail sales analysis
* Business intelligence dashboards
* Customer behavior analysis
* Revenue forecasting

---

## Author

Maryam Khalid
Data Science Student

---

## License

This project is licensed under the MIT License.
