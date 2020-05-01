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
