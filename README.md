# Eccomerce data analysis in sql and python
![image](https://github.com/user-attachments/assets/e46c77b0-cd40-4aa7-bbaf-c93113982bcf)

## Table of Contents
- [Introduction](#introduction)
- [Data Extraction](#Data-Extraction)
- [Data Import using python](#Data-Import-using-python)
- [clean and insert data to mysql database using python](#clean-and-insert-data-to-mysql-database-using-python)

## Introduction
This project is focused on eCommerce Data Analysis using a combination of MySQL for data storage and Python for data manipulation and visualization. The goal of the project is to explore and analyze key business metrics from an eCommerce platform, such as sales trends, customer behavior, and product performance, to generate actionable insights that can drive business decisions.

## Data Extraction
Import eccomerce data from <a href = "https://www.kaggle.com/datasets/aaditshukla/flipkart-fasion-products-dataset"> Kaggle</a>

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
