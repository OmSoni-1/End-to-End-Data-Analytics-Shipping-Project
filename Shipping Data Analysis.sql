-- Create a table df_orders that takes in the cleaned data FROM the Python Dataframe
use pyth_sql_project


create table df_orders(
	[order_id] int primary key,
	[order_date] date,
	[ship_mode] varchar(20),
	[segment] varchar(20),
	[country] varchar(20),
	[city] varchar(20),
	[state] varchar(20),
	[postal_code] varchar(20),
	[region] varchar(20),
	[category] varchar(20),
	[sub_category] varchar(20),
	[product_id] varchar(50),
	[quantity] int,
	[discount] decimal(7,2),
	[sale_price] decimal(7,2),
	[profit] decimal(7,2)
)

SELECT * FROM df_orders

-- Problem statement 1: Find the top 10 highest revenue-generating products
SELECT top 10 product_id as Product, SUM(sale_price) as Revenue
FROM df_orders
GROUP BY product_id
ORDER BY revenue DESC;

--Problem Statement 2: Find top 5 highest selling products in each region
WITH cte AS(
SELECT region, product_id, SUM(sale_price) as Total_Sales
FROM df_orders
GROUP BY region, product_id
)
SELECT * FROM (SELECT *, ROW_NUMBER() over(partition by region ORDER BY Total_Sales desc) as rn FROM cte) A
where rn <= 5;

-- Month-over-month growth
-- Problem statement 3: Calculate month-over-month sales growth comparison for 2022 and 2023

WITH cte as(
SELECT YEAR(order_date) as order_year, MONTH(order_date) as order_month, SUM(sale_price) as sales
FROM df_orders
GROUP BY YEAR(order_date), MONTH(order_date)
),
cte1 as(
SELECT order_month,
SUM(case when order_year=2022 then sales else 0 end) as sales_2022,
SUM(case when order_year=2023 then sales else 0 end) as sales_2023
FROM cte
GROUP BY order_month
)
SELECT *, (sales_2023 - sales_2022) as Growth
FROM cte1;

-- Highest sales month by category
-- Problem statement 4: Determine the month with the highest sales for each product category

WITH cte as(
SELECT category, YEAR(order_date) as order_year, MONTH(order_date) as order_month, SUM(sale_price) as sales
FROM df_orders
GROUP BY category, YEAR(order_date), MONTH(order_date)
--ORDER BY category, sales desc
) SELECT * FROM
(SELECT *, ROW_NUMBER() over(partition by category ORDER BY sales desc) as rn FROM cte) A
where rn = 1;


-- Highest growth by profit:
-- Problem statement 5: Identify which subcategory had the highest growth by profit in 2023 
-- compared to 2022

WITH cte as(
SELECT sub_category, YEAR(order_date) as order_year, SUM(sale_price) as sales
FROM df_orders
GROUP BY sub_category, YEAR(order_date)
--ORDER BY sub_category
), cte1 as(
SELECT * FROM(
SELECT sub_category,
SUM(case when order_year=2022 then sales else 0 end) as sales_2022,
SUM(case when order_year=2023 then sales else 0 end) as sales_2023
FROM cte
GROUP BY sub_category
) A)
SELECT top 1 *, (sales_2023 - sales_2022) as Growth
FROM cte1
ORDER BY Growth desc;