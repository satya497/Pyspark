# Pyspark
About Pyspark Basics with an example.
## Pyspark Example
This Pyspark Example Project will gives you the basic concepts of Pyspak.
you can mainly find the below topics.They are,

`Pyspark Datatables Reading from Excel files`

`Pyspark Database Connections`

`Pyspark Datatables Reading from Database`

### Introduction
For running Python Pyspark in your Jupyter Notebook, You have to follow some steps:
* Install pySpark
  Before installing pySpark, you must have Python and Spark installed.
  To install Spark, make sure you have Java 8 or higher installed on your computer.
* Install Jupyter Notebook
  you can install jupyter notebook from either Anaconda Prompt or pip install jupyter.
 
### Cell by Cell Code Explanation:
Before running any script of a code we have to install the required packages like an ingredients for a dish,
below are the some required packages installation.
```bash
from pyspark import SparkContext, SparkConf, SQLContext
import pandas.io.sql
import pandas as pd
from pyspark.sql.functions import col
import pyspark.sql.functions as fn
import numpy as np
# required libraries
from pyspark import SparkContext, SparkConf #
from pyspark.sql import SparkSession # for dataframe conversions
# for type conversions
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import col, udf, sum # col, udf (user defined functions)
from pyspark.sql.types import DateType, IntegerType # type
from pyspark.sql.functions import trim # for trimming
from pyspark.sql.functions import collect_list, sort_array, row_number # for grouping and taking the last/first element
from pyspark.sql.functions import *
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
#Create spark configuration object
conf = SparkConf()
conf.setMaster("local").setAppName("My app")
 
#Create spark context and sparksession
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)
```
## Reading and operations on a CSV file using Pyspark
Below cell will gives how to read a file and select some columns in a file
```
final_scheme_data = spark.read.csv('AXA_EF_March.csv', inferSchema=True, header=True)
final_scheme_data = final_scheme_data.select(['scheme_plan', 'calculated_date', 'today_PU', 'today_RU', 'balance_units'])
final_scheme_data.show() #used for showing the table.
```
The output is:
```
+-----------+-------------------+--------+--------+--------------+
|scheme_plan|    calculated_date|today_PU|today_RU| balance_units|
+-----------+-------------------+--------+--------+--------------+
|      EF_DG|2020-03-01 00:00:00|     0.0|     0.0|   6046169.596|
|      EF_DB|2020-03-01 00:00:00|     0.0|     0.0|      5503.782|
|      EF_EB|2020-03-01 00:00:00|     0.0|     0.0|      9436.988|
|      EF_RQ|2020-03-01 00:00:00|     0.0|     0.0|    845877.916|
|      EF_RG|2020-03-01 00:00:00|     0.0|     0.0|4.1670482989E7|
```
