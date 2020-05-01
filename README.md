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
* we are reading csv file named 'AXA_EF_March.csv'
```bash
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
* Converting the [calculated_date] in a specific date format

```
final_scheme_data = final_scheme_data.withColumn('calculated_date', to_timestamp(col('calculated_date'), 'yyyy-MM-dd').cast('date'))
```

* Creating a column 'EffectiveNav' and Assigning a before day of 'calculated_date'

```
final_scheme_data = final_scheme_data.withColumn('EffectiveNav', date_sub(col('calculated_date'), 1))
```

* Concating the two columns 'scheme_plan' and 'EffectiveNav' with '_' and assigning into 'scheme_plan_date'

```
final_scheme_data = final_scheme_data.withColumn('scheme_plan_date', concat(col('scheme_plan'), lit('_'), col('EffectiveNav')))
final_scheme_data.orderBy([col('scheme_plan'), col('calculated_date')]).show()
```

the output with orderby of 'scheme_plan' and 'calculated_date' is:
  
```
+-----------+---------------+--------+--------+-------------+------------+----------------+
|scheme_plan|calculated_date|today_PU|today_RU|balance_units|EffectiveNav|scheme_plan_date|
+-----------+---------------+--------+--------+-------------+------------+----------------+
|      EF_DB|     2020-03-01|     0.0|     0.0|     5503.782|  2020-02-29|EF_DB_2020-02-29|
|      EF_DB|     2020-03-02|     0.0|     0.0|     5503.782|  2020-03-01|EF_DB_2020-03-01|
|      EF_DB|     2020-03-03|     0.0|     0.0|     5503.782|  2020-03-02|EF_DB_2020-03-02|
|      EF_DB|     2020-03-04|     0.0|     0.0|     5503.782|  2020-03-03|EF_DB_2020-03-03|
|      EF_DB|     2020-03-05|     0.0|     0.0|     5503.782|  2020-03-04|EF_DB_2020-03-04|
```

* Now we are Reading the another csv file with Named 'nav_master.csv' and applying some operations like above,they are:

  * Changing the date format of 'fn_fromdt' column.
  
  * we are doing orderby on 'fn_fromdt' column.
  
  * As before table we are concating the 'fn_scheme' and 'fn_plan' with '_' and assigning to 'scheme_plan' column.(NOTE:if column not exists it will automatically create a column)
  
  * Then we are Concating the two columns 'scheme_plan' and 'fn_fromdt' with '_' and assigning into 'scheme_plan_date'.
  [output is:]
  
```
+---------+-------+----------+------+----------+----------+----------+----------+-----------+----------------+
|fn_scheme|fn_plan|      code|fn_nav| fn_fromdt|   fn_todt|  fn_entdt|  fn_enddt|scheme_plan|scheme_plan_date|
+---------+-------+----------+------+----------+----------+----------+----------+-----------+----------------+
|       EF|     DB|EF_DB43864| 20.17|2020-02-03|03-02-2020|03-02-2020|03-02-2020|      EF_DB|EF_DB_2020-02-03|
|       EF|     DB|EF_DB43865| 20.51|2020-02-04|04-02-2020|04-02-2020|04-02-2020|      EF_DB|EF_DB_2020-02-04|
|       EF|     DB|EF_DB43866| 20.71|2020-02-05|05-02-2020|05-02-2020|05-02-2020|      EF_DB|EF_DB_2020-02-05|
|       EF|     DB|EF_DB43867| 20.78|2020-02-06|06-02-2020|06-02-2020|06-02-2020|      EF_DB|EF_DB_2020-02-06|
|       EF|     DB|EF_DB43868| 20.85|2020-02-07|07-02-2020|07-02-2020|07-02-2020|      EF_DB|EF_DB_2020-02-07|
```
 
 * Now we are joining the Two Tables based on some conditions.
 
    * we joining based on 'scheme_plan_date' column (final_scheme_data table left join to nav_data table)
  
    * Applying filter on result table as Where 'scheme_plan' == 'EF_DB'
  
```
final_scheme_data.join(nav_data.select(['scheme_plan_date','fn_fromdt', 'fn_nav']), on='scheme_plan_date', how='left').filter(col('scheme_plan') == 'EF_DB').show()
```

The resultant output is:

```
+----------------+-----------+---------------+--------+--------+-------------+------------+----------+------+
|scheme_plan_date|scheme_plan|calculated_date|today_PU|today_RU|balance_units|EffectiveNav| fn_fromdt|fn_nav|
+----------------+-----------+---------------+--------+--------+-------------+------------+----------+------+
|EF_DB_2020-02-29|      EF_DB|     2020-03-01|     0.0|     0.0|     5503.782|  2020-02-29|      null|  null|
|EF_DB_2020-03-01|      EF_DB|     2020-03-02|     0.0|     0.0|     5503.782|  2020-03-01|      null|  null|
|EF_DB_2020-03-02|      EF_DB|     2020-03-03|     0.0|     0.0|     5503.782|  2020-03-02|2020-03-02| 19.74|
|EF_DB_2020-03-03|      EF_DB|     2020-03-04|     0.0|     0.0|     5503.782|  2020-03-03|2020-03-03| 20.02|
|EF_DB_2020-03-04|      EF_DB|     2020-03-05|     0.0|     0.0|     5503.782|  2020-03-04|2020-03-04| 19.82|
```
Thank You.

Happy Coding.

Satya.y
