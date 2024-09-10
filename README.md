# Eccomerce data analysis in sql and python
![image](https://github.com/user-attachments/assets/e46c77b0-cd40-4aa7-bbaf-c93113982bcf)

## Table of Contents
- [Introduction](#introduction)
- [Data Extraction](#Data-Extraction)
- [Data Import using python](#Data-Import-using-python)
- [clean and insert data to mysql database using python](#clean-and-insert-data-to-mysql-database-using-python)
- [Data Modelling](#Data-Modelling)
- [Data Analysis in mysql and python](#Data-Analysis-in-mysql-and-python)
- [Basic Queries](#Basic-Queries)
- [Intermediate Queries](#Intermediate-Queries)
- [Advanced Queries](#Advanced-Queries)
- [Findings](#Findings)

## Introduction
This project is focused on eCommerce Data Analysis using a combination of MySQL for data storage and Python for data manipulation and visualization. The goal of the project is to explore and analyze key business metrics from an eCommerce platform, such as sales trends, customer behavior, and product performance, to generate actionable insights that can drive business decisions.

## Data Extraction
Download eccomerce data from <a href = "https://www.kaggle.com/datasets/aaditshukla/flipkart-fasion-products-dataset"> Kaggle</a>

## Data Import using python
- Import necessary python libraries
```python
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
import mysql.connector
import os
```
- specify your filename and name of your table in a list
```python
csv_files = [
    ('customers.csv', 'customers'),
    ('geolocation.csv', 'geolocation'),
    ('order_items.csv', 'order_items'),
    ('orders.csv', 'orders'),
    ('payments.csv', 'payments'),
    ('products.csv', 'produts'),
    ('sellers.csv', 'sellers')
]
```
- connect to your mysql database
```python
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='1234',
    database='ecommerce'
)
cursor = conn.cursor()
```
- Specify the path to the folder that contains the eccomerce csv files downloaded
```python
folder_path = r'C:\Users\Admin\Desktop\ecommerce'
```
- Define data types of columns in tables
```python
def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'
```
## clean and insert data to mysql database using python 
- Use a for loop to add data to your database
- clean column names to a more readable format
- 
```python
for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

# Close the connection
conn.close()
```
## Data Modelling

## Data Analysis in mysql and python
- Import python libraries to analyse your data
```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')
import mysql.connector
plt.style.use('ggplot')
```
- Create a connection to mysql database
```python
db = mysql.connector.connect(host = "localhost",
                            username = "root",
                            password = "1234",
                            database = "ecommerce")

cur = db.cursor()
```
### Basic Queries
1. List all unique cities where customers are located.
```python
query = """ SELECT DISTINCT(customer_city) FROM customers """

cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data)
df
```
![image](https://github.com/user-attachments/assets/7cf28341-3985-4214-a7b1-2ac47d7104d9)

2. Count the number of orders placed in 2017
```python
query = """ SELECT count(order_id) FROM orders WHERE date(order_purchase_timestamp) BETWEEN '2017-01-01' AND '2017-12-31' """

cur.execute(query)
data = cur.fetchall()


print('Number of orders placed in 2017 is ',data[0][0])
```
![image](https://github.com/user-attachments/assets/942d4e53-4773-4083-b1dd-422180da6f6f)

3.  Find the total sales per category.
```python
query = """ SELECT  product_category, count(product_id) as total_sales FROM produts GROUP BY product_category """

cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ['product category', 'number_of_products'])
df
```
![image](https://github.com/user-attachments/assets/2ded8824-fd74-4ef8-82d8-c1c8fbef0db5)

4. Calculate the percentage of orders that were paid in installments.
```python
query = """ select round(((sum(case when payment_installments >= 1 then 1
else 0 end))/count(*))*100, 3) from payments
"""

cur.execute(query)

data = cur.fetchall()

print("the percentage of orders that were paid in installments is", data[0][0])
```
![image](https://github.com/user-attachments/assets/6554dd2c-2ed5-4b53-b80e-a445df905915)

5.  Count the number of customers from each state.
```python
query = """ select customer_state,
                   COUNT(customer_id) as num_of_customers 
            from customers 
            GROUP BY customer_state
            """

cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ["state", "customer_count" ])
df = df.sort_values(by = 'customer_count', ascending = False)

plt.figure(figsize = (8, 3))
plt.bar(df['state'], df['customer_count'])
plt.xticks(rotation = 90)
plt.xlabel("states")
plt.ylabel("customer_count")
plt.title("Count of Customers by States")
plt.show()
```
![image](https://github.com/user-attachments/assets/90f34df9-5dc0-4132-a4a9-829746a30a53)

### Intermediate Queries
1. Calculate the number of orders per month in 2018.
```python
query = """ SELECT  monthname(order_purchase_timestamp),
                    count(order_id) as orders
            FROM orders
            WHERE date(order_purchase_timestamp) BETWEEN '2018-01-01' AND '2018-12-31'
            GROUP BY monthname(order_purchase_timestamp)"""

cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ['month', 'number_of_orders'])
o = ["January", "February","March","April","May","June","July","August","September","October"]

plt.figure(figsize = (10, 5))
ax = sns.barplot(x = df["month"],y =  df["number_of_orders"], data = df, order = o, color = 'red')
plt.xticks(rotation = 45)
ax.bar_label(ax.containers[0])
plt.title("Count of Orders by Months is 2018")

plt.show()
```
![image](https://github.com/user-attachments/assets/8748d364-4ab9-4d80-8473-17533ac9eef9)

2. Find the average number of products per order, grouped by customer city.
```python
query = """ WITH count_of_orders as
                (SELECT o.order_id,
                        o.customer_id,
                        count(oi.order_id) as order_count
                FROM orders o
                INNER JOIN order_items oi on o.order_id = oi.order_id
                GROUP BY o.order_id, o.customer_id)

SELECT  c.customer_city,
        round(avg(co.order_count), 1) as average
from customers c 
JOIN count_of_orders co on c.customer_id = co.customer_id
GROUP BY c.customer_city
ORDER BY average DESC"""

cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ["customer city", "average products"])
df
```
![image](https://github.com/user-attachments/assets/6512e5a4-1a10-4249-861e-f062769986d4)

3. Calculate the percentage of total revenue contributed by each product category.
```python
query = """ select upper(produts.product_category) category, 
                   round((sum(payments.payment_value)/(select sum(payment_value) from payments))*100,2) sales_percentage
            from produts 
            join order_items on produts.product_id = order_items.product_id
            join payments on payments.order_id = order_items.order_id
            group by category 
            order by sales_percentage desc
"""
cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ['category', 'sales_percentage'])
df.head()
```
![image](https://github.com/user-attachments/assets/bfde0e67-70ee-4454-99bc-ae3ac6280e00)

4. Identify the correlation between product price and the number of times a product has been purchased.
```python
query = """ 
SELECT  p.product_category,
        count(oi.product_id),
        round(avg(oi.price), 2)
from produts p
JOIN order_items oi on p.product_id = oi.product_id
GROUP BY p.product_category
"""
cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ["Category", "order_count","price"])

arr1 = df["order_count"]
arr2 = df["price"]

a = np.corrcoef([arr1,arr2])
print("The correlation of number of products to price is", a[0][-1])
```
5. Calculate the total revenue generated by each seller, and rank them by revenue.
```python
query = """ WITH total_revenue (seller_id, revenue) AS (
    SELECT s.seller_id,
           p.payment_value as revenue
    FROM sellers s
    JOIN order_items oi on s.seller_id = oi.seller_id
    join payments p on p.order_id = oi.order_id)
SELECT  seller_id,
        round(sum(revenue), 2) as revenue,
        rank() over(ORDER BY sum(revenue) DESC) as rn
from total_revenue
GROUP BY seller_id"""

cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ["seller_id", "revenue", "rank"])
df1 = df.head()

plt.figure(figsize = (8, 5))
sns.barplot(x = 'seller_id', y = 'revenue', data = df1, color = "red")
plt.title("Total Revenue Generated by top 5 sellers")
plt.xticks(rotation = 90)
plt.show()
```
![image](https://github.com/user-attachments/assets/34b31ed6-ede0-4f18-b5b9-a127fa494744)

## Advanced Queries
1. Calculate the moving average of order values for each customer over their order history.
```python
query = """
select customer_id, 
       order_purchase_timestamp, 
       payment,
       avg(payment) over(partition by customer_id order by order_purchase_timestamp rows between 2 preceding and current row) as mov_avg
from
(select o.customer_id, 
        o.order_purchase_timestamp, 
        p.payment_value as payment
from payments p 
join orders o on p.order_id = o.order_id) as a
"""
cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ['customer_id', 'order_purchase_timestamp', 'payment', 'mov_avg'])
df
```
![image](https://github.com/user-attachments/assets/423814e8-8f56-4f3d-bf13-c018edf57172)

2. Calculate the year-over-year growth rate of total sales.
```python
query = """
with yearly_sales as(
    select year(orders.order_purchase_timestamp) as years,
            round(sum(payments.payment_value),2) as payment 
    from orders 
    join payments on orders.order_id = payments.order_id
    group by years 
    order by years)

select years, 
       round(((payment - lag(payment, 1) over(order by years))/
       lag(payment, 1) over(order by years)) * 100, 2)
from yearly_sales"""
cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ["Year", "Year over Year %"])
df
```
![image](https://github.com/user-attachments/assets/3b0b80ba-e389-4b7b-b6da-981595f5c235)

3. Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.
```python
query = """
with a as (
select  customers.customer_id,
        min(orders.order_purchase_timestamp) first_order
from customers join orders
on customers.customer_id = orders.customer_id
group by customers.customer_id),

b as (
select  a.customer_id, 
        count(distinct orders.order_purchase_timestamp) next_order
from a join orders
on orders.customer_id = a.customer_id 
and orders.order_purchase_timestamp > first_order 
and orders.order_purchase_timestamp < date_add(first_order, interval 6 month)
group by a.customer_id) 

select 100 * (count( distinct a.customer_id)/ count(distinct b.customer_id)) 
from a left join b 
on a.customer_id = b.customer_id"""

cur.execute(query)
data = cur.fetchall()

print(data[0][0])
```
![image](https://github.com/user-attachments/assets/4035271f-e174-4590-9552-186e44874f97)

4. Identify the top 3 customers who spent the most money in each year.
```python
query = """
WITH top_3_customer AS (
    SELECT year(o.order_purchase_timestamp) as year,
            c.customer_id,
            round(sum(p.payment_value), 2) as payment,
            DENSE_RANK() OVER(PARTITION BY year(o.order_purchase_timestamp) ORDER BY round(sum(p.payment_value), 2) DESC) as rnk
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN payments p ON o.order_id = p.order_id
    GROUP BY year(o.order_purchase_timestamp), c.customer_id
    ORDER BY year ASC, payment DESC)
SELECT *
from top_3_customer
WHERE rnk <= 3"""

cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ['year', 'customer_id', 'payment', 'rank'])

plt.figure(figsize = (8, 5))
sns.barplot(x = df['customer_id'], y = df['payment'], data= df, hue = 'year')
plt.title('Top 3 Customers in each year')
plt.xticks(rotation = 90)
plt.legend()
plt.show()
```
![image](https://github.com/user-attachments/assets/abad45de-6cad-4268-b7c8-d143558b23ca)

## Findings
- SP state had the highest number of customers of over 4000 customers
- states like AC AP RR had no customers
- All orders were paid in terms of installments
- In 2017 the company had 45101 orders
- Over the years that is 2016, 2017, 2018 the top most customer in 2017 making orders over $12,000
- 2017 had a very big Year over Year percentage change because 2016 we had only data for three months
- Year over Year pecentage change for 2018 was 20%

## Reccomendations
- Introduce a customer loyalty program where customers who spent the most in the purchase gets different VIP perks depending on their brand spend
- Offer personalised discounts and freebies to our top purchasing customers
- Provide early access to sales and products for your loyalty club members.
- Offer discounts to repeat customers
- A “Refer a Friend” program is an effective way to reward your current customers while also gaining new ones.
- You can offer a free gift, such as: every other purchase, to encourage your customers to shop more often
