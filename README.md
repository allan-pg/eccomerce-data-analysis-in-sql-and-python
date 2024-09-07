# Eccomerce data analysis in sql and python
![image](https://github.com/user-attachments/assets/e46c77b0-cd40-4aa7-bbaf-c93113982bcf)

## Table of Contents

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
## clean and insert data to mysql database 
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
