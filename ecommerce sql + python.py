#!/usr/bin/env python
# coding: utf-8

# In[15]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')
import mysql.connector
plt.style.use('ggplot')


# In[2]:


db = mysql.connector.connect(host = "localhost",
                            username = "root",
                            password = "1234",
                            database = "ecommerce")

cur = db.cursor()


# ## Basic Queries

# ### 1. List all unique cities where customers are located.

# In[3]:


query = """ SELECT DISTINCT(customer_city) FROM customers """

cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data)
df


# ## 2. Count the number of orders placed in 2017.

# In[4]:


query = """ SELECT count(order_id) FROM orders WHERE date(order_purchase_timestamp) BETWEEN '2017-01-01' AND '2017-12-31' """

cur.execute(query)
data = cur.fetchall()


print('Number of orders placed in 2017 is ',data[0][0])


# ## 3. Find the total sales per category.

# In[18]:


query = """ SELECT  product_category, count(product_id) as total_sales FROM produts GROUP BY product_category """

cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ['product category', 'number_of_products'])
df


# ## 4. Calculate the percentage of orders that were paid in installments. 

# In[6]:


query = """ select round(((sum(case when payment_installments >= 1 then 1
else 0 end))/count(*))*100, 3) from payments
"""

cur.execute(query)

data = cur.fetchall()

print("the percentage of orders that were paid in installments is", data[0][0])


# ## 5. Count the number of customers from each state. 

# In[7]:


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


# # Intermediate Queries

# ## 1. Calculate the number of orders per month in 2018.

# In[35]:


query = """ SELECT  monthname(order_purchase_timestamp),
                    count(order_id) as orders
            FROM orders
            WHERE date(order_purchase_timestamp) BETWEEN '2018-01-01' AND '2018-12-31'
            GROUP BY monthname(order_purchase_timestamp)"""

cur.execute(query)
data = cur.fetchall()

df = pd.DataFrame(data, columns = ['month', 'number_of_orders'])
o = ["January", "February","March","April","May","June","July","August","September","October"]

plt.figure(figsize = (10, 4))
ax = sns.barplot(x = df["month"],y =  df["number_of_orders"], data = df, order = o, color = 'red')
plt.ylabel("Number of orders")
plt.xticks(rotation = 45)
ax.bar_label(ax.containers[0])
plt.title("Count of Orders by Months is 2018")

plt.show()


# ## 2. Find the average number of products per order, grouped by customer city.

# In[9]:


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


# ## 3. Calculate the percentage of total revenue contributed by each product category.

# In[10]:


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


# ## 4. Identify the correlation between product price and the number of times a product has been purchased.

# In[17]:


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


# ## 5. Calculate the total revenue generated by each seller, and rank them by revenue.

# In[12]:


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


# ## ADVANCED QUERIES

# ## 1. Calculate the moving average of order values for each customer over their order history.

# In[32]:


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


# ##  2. Calculate the year-over-year growth rate of total sales.
# 

# In[23]:


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


# ## 3. Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.

# In[34]:


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


# ## 4. Identify the top 3 customers who spent the most money in each year.

# In[30]:


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


# In[ ]:




