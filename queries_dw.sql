-- OLAP QUERIES FOR WALMART DATA WAREHOUSE

USE walmart_dw;

-- =====================================================================
-- Q1: Top Revenue-Generating Products on Weekdays and Weekends 
--     with Monthly Drill-Down
-- OLAP Operations: Slice (by weekend/weekday), Drill-down (year→month), 
--                  Top-N filtering
-- =====================================================================

SELECT 
    dd.Year,
    dd.Month,
    dd.Month_Name,
    CASE 
        WHEN dd.Is_Weekend = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END AS Day_Type,
    dp.Product_ID,
    dp.Product_Category,
    SUM(fs.Total_Amount) AS Total_Revenue,
    SUM(fs.Quantity) AS Total_Quantity
FROM FactSales fs
JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
WHERE dd.Year = 2017  -- Slice by specific year
GROUP BY dd.Year, dd.Month, dd.Month_Name, dd.Is_Weekend, 
         dp.Product_ID, dp.Product_Category
ORDER BY Day_Type, dd.Month, Total_Revenue DESC
LIMIT 60;  -- Top 5 per month * 12 months


-- =====================================================================
-- Q2: Customer Demographics by Purchase Amount with City Category 
--     Breakdown
-- OLAP Operations: Dice (Gender, Age), Drill-down (by City_Category)
-- =====================================================================

SELECT 
    dc.Gender,
    dc.Age,
    dc.City_Category,
    COUNT(DISTINCT fs.Customer_Key) AS Total_Customers,
    SUM(fs.Total_Amount) AS Total_Purchase_Amount,
    AVG(fs.Total_Amount) AS Avg_Purchase_Amount,
    SUM(fs.Quantity) AS Total_Items_Purchased
FROM FactSales fs
JOIN DimCustomer dc ON fs.Customer_Key = dc.Customer_Key
GROUP BY dc.Gender, dc.Age, dc.City_Category
ORDER BY dc.Gender, dc.Age, dc.City_Category;


-- =====================================================================
-- Q3: Product Category Sales by Occupation
-- OLAP Operations: Slice (by occupation), Aggregation by category
-- =====================================================================

SELECT 
    dc.Occupation,
    dp.Product_Category,
    COUNT(DISTINCT fs.Order_ID) AS Total_Orders,
    SUM(fs.Quantity) AS Total_Quantity_Sold,
    SUM(fs.Total_Amount) AS Total_Sales,
    AVG(fs.Total_Amount) AS Avg_Order_Value
FROM FactSales fs
JOIN DimCustomer dc ON fs.Customer_Key = dc.Customer_Key
JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
GROUP BY dc.Occupation, dp.Product_Category
ORDER BY dc.Occupation, Total_Sales DESC;


-- =====================================================================
-- Q4: Total Purchases by Gender and Age Group with Quarterly Trend
-- OLAP Operations: Dice (Gender, Age), Drill-down (year→quarter)
-- =====================================================================

SELECT 
    dd.Year,
    dd.Quarter,
    dc.Gender,
    dc.Age,
    COUNT(DISTINCT fs.Order_ID) AS Total_Orders,
    SUM(fs.Total_Amount) AS Total_Purchase_Amount,
    SUM(fs.Quantity) AS Total_Items_Purchased,
    AVG(fs.Total_Amount) AS Avg_Purchase_Per_Order
FROM FactSales fs
JOIN DimCustomer dc ON fs.Customer_Key = dc.Customer_Key
JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
WHERE dd.Year = 2017  -- CHANGED: Use year that exists in data
GROUP BY dd.Year, dd.Quarter, dc.Gender, dc.Age
ORDER BY dd.Quarter, dc.Gender, dc.Age;


-- =====================================================================
-- Q5: Top Occupations by Product Category Sales
-- OLAP Operations: Top-N analysis, Group by category
-- =====================================================================

WITH OccupationSales AS (
    SELECT 
        dp.Product_Category,
        dc.Occupation,
        SUM(fs.Total_Amount) AS Total_Sales,
        COUNT(DISTINCT fs.Customer_Key) AS Unique_Customers,
        RANK() OVER (PARTITION BY dp.Product_Category 
                     ORDER BY SUM(fs.Total_Amount) DESC) AS Sales_Rank
    FROM FactSales fs
    JOIN DimCustomer dc ON fs.Customer_Key = dc.Customer_Key
    JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
    GROUP BY dp.Product_Category, dc.Occupation
)
SELECT 
    Product_Category,
    Occupation,
    Total_Sales,
    Unique_Customers,
    Sales_Rank
FROM OccupationSales
WHERE Sales_Rank <= 5
ORDER BY Product_Category, Sales_Rank;


-- =====================================================================
-- Q6: City Category Performance by Marital Status with Monthly 
--     Breakdown (Past 6 Months)
-- OLAP Operations: Slice (last 6 months), Drill-down (month)
-- =====================================================================

SELECT 
    dd.Year,
    dd.Month,
    dd.Month_Name,
    dc.City_Category,
    CASE 
        WHEN dc.Marital_Status = 1 THEN 'Married'
        ELSE 'Single'
    END AS Marital_Status,
    COUNT(DISTINCT fs.Order_ID) AS Total_Orders,
    SUM(fs.Total_Amount) AS Total_Purchase_Amount,
    AVG(fs.Total_Amount) AS Avg_Order_Value
FROM FactSales fs
JOIN DimCustomer dc ON fs.Customer_Key = dc.Customer_Key
JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
WHERE dd.Year = 2017 AND dd.Month >= 7  -- CHANGED: Last 6 months of 2017
GROUP BY dd.Year, dd.Month, dd.Month_Name, dc.City_Category, dc.Marital_Status
ORDER BY dd.Year DESC, dd.Month DESC, dc.City_Category;


-- =====================================================================
-- Q7: Average Purchase Amount by Stay Duration and Gender
-- OLAP Operations: Dice (Stay duration, Gender), Aggregation
-- =====================================================================

SELECT 
    dc.Stay_In_Current_City_Years,
    dc.Gender,
    COUNT(DISTINCT fs.Customer_Key) AS Total_Customers,
    COUNT(DISTINCT fs.Order_ID) AS Total_Orders,
    SUM(fs.Total_Amount) AS Total_Purchase_Amount,
    AVG(fs.Total_Amount) AS Avg_Purchase_Amount,
    SUM(fs.Quantity) AS Total_Items_Purchased
FROM FactSales fs
JOIN DimCustomer dc ON fs.Customer_Key = dc.Customer_Key
GROUP BY dc.Stay_In_Current_City_Years, dc.Gender
ORDER BY dc.Stay_In_Current_City_Years, dc.Gender;


-- =====================================================================
-- Q8: Top 5 Revenue-Generating Cities by Product Category
-- OLAP Operations: Top-N, Slice by category
-- =====================================================================

WITH CityRevenue AS (
    SELECT 
        dp.Product_Category,
        dc.City_Category,
        SUM(fs.Total_Amount) AS Total_Revenue,
        COUNT(DISTINCT fs.Order_ID) AS Total_Orders,
        RANK() OVER (PARTITION BY dp.Product_Category 
                     ORDER BY SUM(fs.Total_Amount) DESC) AS Revenue_Rank
    FROM FactSales fs
    JOIN DimCustomer dc ON fs.Customer_Key = dc.Customer_Key
    JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
    GROUP BY dp.Product_Category, dc.City_Category
)
SELECT 
    Product_Category,
    City_Category,
    Total_Revenue,
    Total_Orders,
    Revenue_Rank
FROM CityRevenue
WHERE Revenue_Rank <= 5
ORDER BY Product_Category, Revenue_Rank;


-- =====================================================================
-- Q9: Monthly Sales Growth by Product Category (Current Year)
-- OLAP Operations: Temporal analysis, Growth calculation
-- =====================================================================

WITH MonthlySales AS (
    SELECT 
        dp.Product_Category,
        dd.Year,
        dd.Month,
        dd.Month_Name,
        SUM(fs.Total_Amount) AS Monthly_Sales
    FROM FactSales fs
    JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
    JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
    WHERE dd.Year = 2017  -- CHANGED: Use year that exists
    GROUP BY dp.Product_Category, dd.Year, dd.Month, dd.Month_Name
),
SalesWithLag AS (
    SELECT 
        Product_Category,
        Year,
        Month,
        Month_Name,
        Monthly_Sales,
        LAG(Monthly_Sales) OVER (PARTITION BY Product_Category 
                                 ORDER BY Month) AS Previous_Month_Sales
    FROM MonthlySales
)
SELECT 
    Product_Category,
    Year,
    Month,
    Month_Name,
    Monthly_Sales,
    Previous_Month_Sales,
    CASE 
        WHEN Previous_Month_Sales IS NULL THEN NULL
        WHEN Previous_Month_Sales = 0 THEN NULL
        ELSE ROUND(((Monthly_Sales - Previous_Month_Sales) / Previous_Month_Sales) * 100, 2)
    END AS Growth_Percentage
FROM SalesWithLag
ORDER BY Product_Category, Month;


-- =====================================================================
-- Q10: Weekend vs. Weekday Sales by Age Group (Current Year)
-- OLAP Operations: Slice (weekend), Dice (age), Comparison
-- =====================================================================

SELECT 
    dc.Age,
    CASE 
        WHEN dd.Is_Weekend = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END AS Day_Type,
    COUNT(DISTINCT fs.Order_ID) AS Total_Orders,
    SUM(fs.Total_Amount) AS Total_Sales,
    AVG(fs.Total_Amount) AS Avg_Order_Value,
    SUM(fs.Quantity) AS Total_Items_Sold
FROM FactSales fs
JOIN DimCustomer dc ON fs.Customer_Key = dc.Customer_Key
JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
WHERE dd.Year = 2017  -- CHANGED: Use year that exists
GROUP BY dc.Age, dd.Is_Weekend
ORDER BY dc.Age, Day_Type;

-- =====================================================================
-- Q11: Top 5 Products by Revenue (Weekday vs Weekend) - Monthly Drill
-- OLAP Operations: Top-N, Slice (weekend), Drill-down (month)
-- =====================================================================

WITH ProductRevenue AS (
    SELECT 
        dd.Year,
        dd.Month,
        dd.Month_Name,
        CASE 
            WHEN dd.Is_Weekend = 1 THEN 'Weekend'
            ELSE 'Weekday'
        END AS Day_Type,
        dp.Product_ID,
        dp.Product_Category,
        SUM(fs.Total_Amount) AS Total_Revenue,
        RANK() OVER (PARTITION BY dd.Year, dd.Month, dd.Is_Weekend 
                     ORDER BY SUM(fs.Total_Amount) DESC) AS Revenue_Rank
    FROM FactSales fs
    JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
    JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
    WHERE dd.Year = 2017
    GROUP BY dd.Year, dd.Month, dd.Month_Name, dd.Is_Weekend, 
             dp.Product_ID, dp.Product_Category
)
SELECT 
    Year,
    Month,
    Month_Name,
    Day_Type,
    Product_ID,
    Product_Category,
    Total_Revenue,
    Revenue_Rank
FROM ProductRevenue
WHERE Revenue_Rank <= 5
ORDER BY Month, Day_Type, Revenue_Rank;


-- =====================================================================
-- Q12: Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
-- OLAP Operations: Roll-up (store level), Temporal growth analysis
-- =====================================================================

WITH QuarterlyStoreRevenue AS (
    SELECT 
        ds.Store_Name,
        dd.Year,
        dd.Quarter,
        SUM(fs.Total_Amount) AS Quarterly_Revenue
    FROM FactSales fs
    JOIN DimStore ds ON fs.Store_Key = ds.Store_Key
    JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
    WHERE dd.Year = 2017
    GROUP BY ds.Store_Name, dd.Year, dd.Quarter
),
RevenueWithLag AS (
    SELECT 
        Store_Name,
        Year,
        Quarter,
        Quarterly_Revenue,
        LAG(Quarterly_Revenue) OVER (PARTITION BY Store_Name 
                                      ORDER BY Quarter) AS Previous_Quarter_Revenue
    FROM QuarterlyStoreRevenue
)
SELECT 
    Store_Name,
    Year,
    Quarter,
    Quarterly_Revenue,
    Previous_Quarter_Revenue,
    CASE 
        WHEN Previous_Quarter_Revenue IS NULL THEN NULL
        WHEN Previous_Quarter_Revenue = 0 THEN NULL
        ELSE ROUND(((Quarterly_Revenue - Previous_Quarter_Revenue) / 
                    Previous_Quarter_Revenue) * 100, 2)
    END AS Growth_Rate_Percentage
FROM RevenueWithLag
ORDER BY Store_Name, Quarter;


-- =====================================================================
-- Q13: Detailed Supplier Sales Contribution by Store and Product Name
-- OLAP Operations: Drill-down (Store→Supplier→Product)
-- =====================================================================

SELECT 
    ds.Store_Name,
    dsup.Supplier_Name,
    dp.Product_ID,
    dp.Product_Category,
    COUNT(DISTINCT fs.Order_ID) AS Total_Orders,
    SUM(fs.Quantity) AS Total_Quantity_Sold,
    SUM(fs.Total_Amount) AS Total_Sales_Contribution,
    AVG(fs.Total_Amount) AS Avg_Order_Value
FROM FactSales fs
JOIN DimStore ds ON fs.Store_Key = ds.Store_Key
JOIN DimSupplier dsup ON fs.Supplier_Key = dsup.Supplier_Key
JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
GROUP BY ds.Store_Name, dsup.Supplier_Name, dp.Product_ID, dp.Product_Category
ORDER BY ds.Store_Name, dsup.Supplier_Name, Total_Sales_Contribution DESC;


-- =====================================================================
-- Q14: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
-- OLAP Operations: Drill-down (Season), Aggregation by product
-- =====================================================================

SELECT 
    dd.Season,
    dp.Product_ID,
    dp.Product_Category,
    COUNT(DISTINCT fs.Order_ID) AS Total_Orders,
    SUM(fs.Quantity) AS Total_Quantity_Sold,
    SUM(fs.Total_Amount) AS Total_Sales,
    AVG(fs.Total_Amount) AS Avg_Order_Value
FROM FactSales fs
JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
GROUP BY dd.Season, dp.Product_ID, dp.Product_Category
ORDER BY dd.Season, Total_Sales DESC;


-- =====================================================================
-- Q15: Store-Wise and Supplier-Wise Monthly Revenue Volatility
-- OLAP Operations: Temporal analysis, Volatility calculation
-- =====================================================================

WITH MonthlyRevenue AS (
    SELECT 
        ds.Store_Name,
        dsup.Supplier_Name,
        dd.Year,
        dd.Month,
        SUM(fs.Total_Amount) AS Monthly_Revenue
    FROM FactSales fs
    JOIN DimStore ds ON fs.Store_Key = ds.Store_Key
    JOIN DimSupplier dsup ON fs.Supplier_Key = dsup.Supplier_Key
    JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
    GROUP BY ds.Store_Name, dsup.Supplier_Name, dd.Year, dd.Month
),
RevenueWithLag AS (
    SELECT 
        Store_Name,
        Supplier_Name,
        Year,
        Month,
        Monthly_Revenue,
        LAG(Monthly_Revenue) OVER (PARTITION BY Store_Name, Supplier_Name 
                                    ORDER BY Year, Month) AS Previous_Month_Revenue
    FROM MonthlyRevenue
)
SELECT 
    Store_Name,
    Supplier_Name,
    Year,
    Month,
    Monthly_Revenue,
    Previous_Month_Revenue,
    CASE 
        WHEN Previous_Month_Revenue IS NULL THEN NULL
        WHEN Previous_Month_Revenue = 0 THEN NULL
        ELSE ROUND(ABS((Monthly_Revenue - Previous_Month_Revenue) / 
                       Previous_Month_Revenue) * 100, 2)
    END AS Volatility_Percentage
FROM RevenueWithLag
ORDER BY Store_Name, Supplier_Name, Year, Month;


-- =====================================================================
-- Q16: Top 5 Products Purchased Together (Product Affinity Analysis)
-- OLAP Operations: Association analysis, Co-occurrence mining
-- =====================================================================

WITH ProductPairs AS (
    SELECT 
        fs1.Order_ID,
        fs1.Product_Key AS Product1_Key,
        dp1.Product_ID AS Product1_ID,
        dp1.Product_Category AS Product1_Category,
        fs2.Product_Key AS Product2_Key,
        dp2.Product_ID AS Product2_ID,
        dp2.Product_Category AS Product2_Category
    FROM FactSales fs1
    JOIN FactSales fs2 ON fs1.Order_ID = fs2.Order_ID 
                       AND fs1.Product_Key < fs2.Product_Key
    JOIN DimProduct dp1 ON fs1.Product_Key = dp1.Product_Key
    JOIN DimProduct dp2 ON fs2.Product_Key = dp2.Product_Key
    LIMIT 100000  -- Add this to prevent timeout
)
SELECT 
    Product1_ID,
    Product1_Category,
    Product2_ID,
    Product2_Category,
    COUNT(DISTINCT Order_ID) AS Co_Purchase_Count
FROM ProductPairs
GROUP BY Product1_ID, Product1_Category, Product2_ID, Product2_Category
ORDER BY Co_Purchase_Count DESC
LIMIT 5;


-- =====================================================================
-- Q17: Yearly Revenue Trends by Store, Supplier, and Product with 
--      ROLLUP
-- OLAP Operations: ROLLUP (hierarchical aggregation)
-- =====================================================================

SELECT 
    dd.Year,
    ds.Store_Name,
    dsup.Supplier_Name,
    dp.Product_Category,
    SUM(fs.Total_Amount) AS Total_Revenue,
    SUM(fs.Quantity) AS Total_Quantity,
    COUNT(DISTINCT fs.Order_ID) AS Total_Orders
FROM FactSales fs
JOIN DimStore ds ON fs.Store_Key = ds.Store_Key
JOIN DimSupplier dsup ON fs.Supplier_Key = dsup.Supplier_Key
JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
GROUP BY dd.Year, ds.Store_Name, dsup.Supplier_Name, dp.Product_Category 
WITH ROLLUP
ORDER BY dd.Year, ds.Store_Name, dsup.Supplier_Name, dp.Product_Category;


-- =====================================================================
-- Q18: Revenue and Volume-Based Sales Analysis for Each Product 
--      (H1 and H2)
-- OLAP Operations: Pivot (half-year comparison), Aggregation
-- =====================================================================

SELECT 
    dp.Product_ID,
    dp.Product_Category,
    SUM(CASE WHEN dd.Half_Year = 1 THEN fs.Total_Amount ELSE 0 END) AS H1_Revenue,
    SUM(CASE WHEN dd.Half_Year = 2 THEN fs.Total_Amount ELSE 0 END) AS H2_Revenue,
    SUM(CASE WHEN dd.Half_Year = 1 THEN fs.Quantity ELSE 0 END) AS H1_Quantity,
    SUM(CASE WHEN dd.Half_Year = 2 THEN fs.Quantity ELSE 0 END) AS H2_Quantity,
    SUM(fs.Total_Amount) AS Yearly_Total_Revenue,
    SUM(fs.Quantity) AS Yearly_Total_Quantity
FROM FactSales fs
JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
WHERE dd.Year = 2017  -- Change year as needed
GROUP BY dp.Product_ID, dp.Product_Category
ORDER BY Yearly_Total_Revenue DESC;


-- =====================================================================
-- Q19: Identify High Revenue Spikes in Product Sales (Outlier 
--      Detection)
-- OLAP Operations: Statistical analysis, Anomaly detection
-- =====================================================================

WITH DailySales AS (
    SELECT 
        dp.Product_ID,
        dp.Product_Category,
        dd.Full_Date,
        SUM(fs.Total_Amount) AS Daily_Sales
    FROM FactSales fs
    JOIN DimProduct dp ON fs.Product_Key = dp.Product_Key
    JOIN DimDate dd ON fs.Date_Key = dd.Date_Key
    GROUP BY dp.Product_ID, dp.Product_Category, dd.Full_Date
),
ProductAverage AS (
    SELECT 
        Product_ID,
        Product_Category,
        AVG(Daily_Sales) AS Avg_Daily_Sales,
        STDDEV(Daily_Sales) AS StdDev_Daily_Sales
    FROM DailySales
    GROUP BY Product_ID, Product_Category
)
SELECT 
    ds.Product_ID,
    ds.Product_Category,
    ds.Full_Date,
    ds.Daily_Sales,
    pa.Avg_Daily_Sales,
    ROUND(ds.Daily_Sales / pa.Avg_Daily_Sales, 2) AS Sales_Multiple,
    CASE 
        WHEN ds.Daily_Sales > (2 * pa.Avg_Daily_Sales) THEN 'SPIKE DETECTED'
        ELSE 'Normal'
    END AS Anomaly_Flag
FROM DailySales ds
JOIN ProductAverage pa ON ds.Product_ID = pa.Product_ID
WHERE ds.Daily_Sales > (2 * pa.Avg_Daily_Sales)  -- Flagging outliers
ORDER BY Sales_Multiple DESC
LIMIT 50;


-- =====================================================================
-- Q20: Create View STORE_QUARTERLY_SALES for Optimized Analysis
-- OLAP Operations: Materialized aggregation (View)
-- =====================================================================

-- View already created in create_dw.sql
-- Query to use the view:

SELECT * 
FROM STORE_QUARTERLY_SALES
ORDER BY Store_Name, Year, Quarter;

-- Additional analysis using the view
SELECT 
    Store_Name,
    Year,
    SUM(Total_Sales) AS Yearly_Total_Sales,
    AVG(Total_Sales) AS Avg_Quarterly_Sales
FROM STORE_QUARTERLY_SALES
GROUP BY Store_Name, Year
ORDER BY Yearly_Total_Sales DESC;
