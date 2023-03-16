# Databricks notebook source
# MAGIC %md
# MAGIC Del 2 berika data

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC Berika data med en Kalender

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from {name_db}.staging.calendar

# COMMAND ----------

# MAGIC %md
# MAGIC **Notera** att resultatet från SQL-frågan sparas i variabeln ```_sqldf``` när man exekvera en SQL fråga med ```%sql select ...```
# COMMAND ----------

# MAGIC %md
# MAGIC Skriv resultatet till en bestämd variabel föra att inte skriva över den senare

# COMMAND ----------

spark_cal = _sqldf

# COMMAND ----------
spark_cal.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC Läs in data som vi sparade i tidigare del 
# MAGIC 
# MAGIC ```%sql select * from <catalog>.<schema>.<table>```
# MAGIC 
# MAGIC eller 
# MAGIC 
# MAGIC ```spark.read.table("<catalog>.<schema>.<table>")```

# COMMAND ----------


# COMMAND ----------
# MAGIC %md 
# MAGIC Manuiplulera någon datum kolumn i tabellerna så att de får samma format. Lämpligt vis ```date``` i calendar och ```time_start```i eldatat. 
# MAGIC Text manipulation: ```F.substring('date',1,10)```
# MAGIC Omvanlda till date. ```F.to_date()```
# MAGIC Vi gör detta för att kunna joina in kalender data i vår tabell.
# MAGIC ```df.withColumn/s('new/old column name', what should be the result in this new/old column)```
# COMMAND ----------
spark_cal = spark_cal.withColumn('date_start', ...)
spark_elpris = spark_elpris.withColumn('date_start', ...)
spark_elpris.show()

# COMMAND ----------

spark_elpris.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Addera kalender på eldata med join.
# MAGIC 
# MAGIC ```df_res = df1.join(<df2>, how='left',on=['key'])``` [docs](https://sparkbyexamples.com/pyspark/pyspark-join-explained-with-examples/)
# MAGIC 
# MAGIC Kontrollera att joinen har gått rätt till (med tex ```df_res.count()```)

# COMMAND ----------

spark_elpris_ber = ...
spark_elpris_ber.show()
spark_elpris_ber.count()

# COMMAND ----------

spark_elpris_ber.printSchema()

# COMMAND ----------
# MAGIC %md
# MAGIC Ändra datatyperna för EUR_per_kWh, SEK_per_kWh, EXR till lämplig typ
# MAGIC {'<column>': df.<column>.cast('DataType'),...}
                                                 
# COMMAND ----------
spark_elpris_ber = spark_elpris_ber.withColumns(
    {

    }
)
spark_elpris_ber.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Nu är vi klara med berikningen och vill spara vår nya DF i schemat silver
# MAGIC 
# MAGIC Använd ```spark.sql("<Query>")``` för att skapa schemat i din catalog

# COMMAND ----------

spark.sql("create schema if not exists <catalog>.silver")

# COMMAND ----------

# MAGIC %md 
# MAGIC Skriv data till ditt nya schema

# COMMAND ----------

catalog = ...
schema = 'silver'
table_name = ...
spark_elpris_ber.write.saveAsTable(f"{catalog}.{schema}.{table_name}")

# COMMAND ----------

spark.read.table(f"{catalog}.{schema}.{table_name}").show()

# COMMAND ----------


