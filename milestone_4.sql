/*
The Operations team would like to know which countries we currently operate in and which country now has the most stores. 
Perform a query on the database to get the information, it should return the following information:
*/

select count(store_code) as total_no_stores,country_code as country 
from dim_store_details
GROUP BY country_code
ORDER BY count(store_code) DESC;

/*
The business stakeholders would like to know which locations currently have the most stores.

They would like to close some stores before opening more in other locations.

Find out which locations have the most stores currently.
*/

select count(store_code) as total_no_stores,locality as location
from dim_store_details
GROUP BY locality
HAVING count(store_code)>=10
ORDER BY count(store_code) DESC,locality ASC;

/*
Query the database to find out which months typically have the most sales
*/

select sum(orders_table.product_quantity*dim_products.product_price) as sale_value,
dim_date_times.month
from orders_table
JOIN dim_products on dim_products.product_code=orders_table.product_code
JOIN dim_date_times on dim_date_times.date_uuid=orders_table.date_uuid
GROUP BY dim_date_times.month
ORDER BY sum(orders_table.product_quantity*dim_products.product_price) DESC
LIMIT 6;

/*
The company is looking to increase its online sales.

They want to know how many sales are happening online vs offline.

Calculate how many products were sold and the amount of sales made for online and offline purchases.

*/

select sum(orders_table.product_quantity) as product_qunatity_count,
count(*) number_of_sales,
CASE
    WHEN dim_store_details.store_type='Web Portal' THEN 'Web'
    ElSE 'Offline'
END AS location
from orders_table
JOIN dim_store_details on dim_store_details.store_code=orders_table.store_code
GROUP by location;

/*
The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.

Find out the total and percentage of sales coming from each of the different store types.

*/

WITH TotalSales AS(
    SELECT 
    round(sum(orders_table.product_quantity * dim_products.product_price)::numeric,2) as sale_value,
    dim_store_details.store_type
    FROM orders_table
    JOIN dim_products ON dim_products.product_code = orders_table.product_code
    JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
    GROUP BY dim_store_details.store_type
)

SELECT store_type,
sale_value,
ROUND((sale_value / sum(sale_value) OVER()) * 100::numeric, 2) as percentage_of_sales
FROM TotalSales
ORDER BY sale_value DESC;

/*
The company stakeholders want assurances that the company has been doing well recently.

Find which months in which years have had the most sales historically.
*/

SELECT SUM(orders_table.product_quantity*dim_products.product_price) as sale_value,
dim_date_times.year,
dim_date_times.month
FROM orders_table
JOIN dim_products on dim_products.product_code=orders_table.product_code
JOIN dim_date_times on dim_date_times.date_uuid=orders_table.date_uuid
GROUP BY dim_date_times.year,dim_date_times.month
ORDER BY sum(orders_table.product_quantity*dim_products.product_price) DESC
LIMIT 10;

/*
The operations team would like to know the overall staff numbers in each location around the world. 
Perform a query to determine the staff numbers in each of the countries the company sells in.
*/

SELECT sum(staff_numbers) as total_staff_numbers,country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY sum(staff_numbers) DESC;

/*
The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany.
*/

SELECT 
    round(sum(orders_table.product_quantity * dim_products.product_price)::numeric,2) as sale_value,
    dim_store_details.store_type,dim_store_details.country_code
    FROM orders_table
    JOIN dim_products ON dim_products.product_code = orders_table.product_code
    JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
    GROUP BY dim_store_details.store_type, dim_store_details.country_code
    HAVING dim_store_details.country_code='DE'
    ORDER BY sale_value ASC;


/*
Sales would like the get an accurate metric for how quickly the company is making sales.

Determine the average time taken between each sale grouped by year, the query should return the following information:
*/

WITH 
ordered_sales AS (
    SELECT
        orders_table.date_uuid as date_id,
        dim_date_times.timestamp::time as time,
        dim_date_times.year as year,
        dim_date_times.day as day,
        dim_date_times.month as month
    FROM 
        orders_table
    JOIN 
        dim_date_times on dim_date_times.date_uuid = orders_table.date_uuid
    ORDER BY 
        dim_date_times.year, dim_date_times.month, dim_date_times.day, dim_date_times.timestamp::time
),
next_sales AS (
    SELECT
        LEAD(ordered_sales.time) OVER (
            ORDER BY 
            ordered_sales.day ) -ordered_sales.time as next_timestamp,
        ordered_sales.year
    FROM 
        ordered_sales
)

SELECT 
    AVG(next_timestamp) as average_time_between_sales,
    next_sales.year
from next_sales
GROUP BY next_sales.year



-- WITH original_time_info AS (
--     SELECT dim_date_times.year,
--     dim_date_times.month,
--     dim_date_times.day,
--     dim_date_times.timestamp::time as timestamp
--     FROM dim_date_times
--     JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
--     ORDER BY 1,2,3,4
--     ),
--     full_datetime AS (
--     SELECT
--     year, month, day, timestamp,
--     MAKE_DATE(CAST(year AS int), CAST(month AS int), CAST(day AS int)) + timestamp AS complete_datetime
--     FROM original_time_info
--     ),
--     time_intervals AS (
--     SELECT year, month, day,
--     LEAD(complete_datetime) 
--         OVER (ORDER BY year, month, day) - complete_datetime AS difference
--     FROM full_datetime
--     )

-- SELECT year, AVG(difference)
-- FROM time_intervals
-- GROUP BY 1
-- ORDER BY 2 DESC;
