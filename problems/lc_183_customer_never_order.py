"""
Table: Customers

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| name        | varchar |
+-------------+---------+
id is the primary key (column with unique values) for this table.
Each row of this table indicates the ID and name of a customer.
 

Table: Orders

+-------------+------+
| Column Name | Type |
+-------------+------+
| id          | int  |
| customerId  | int  |
+-------------+------+
id is the primary key (column with unique values) for this table.
customerId is a foreign key (reference columns) of the ID from the Customers table.
Each row of this table indicates the ID of an order and the ID of the customer who ordered it.
 

Write a solution to find all customers who never order anything.

Return the result table in any order.
"""

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("customerorder").getOrCreate()

customerDf=spark.read.csv('../data/customers.csv',header=True)
orderDf=spark.read.csv('../data/order.csv',header=True)

JoinDf=customerDf.join(orderDf,customerDf.id!=orderDf.customerid)
JoinDf.show()