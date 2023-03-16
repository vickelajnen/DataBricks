# Databricks notebook source
# MAGIC %md
# MAGIC Sista delen i vår "Pipeline" - Gold
# MAGIC Målet är att skapa en vy som är redo att konsumeras av olika användare

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md 
# MAGIC Läs in vår berikade tabell vi skapade i förra delen

# COMMAND ----------

catalog = ...
source_schema = ...
source_table_name = ...

# COMMAND ----------

spark_df = spark.read.table(f"{catalog}.{source_schema}.{source_table_name}")
display(spark_df)

# COMMAND ----------

spark_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC Skapa vårt nya schema **Gold**

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists <catalog>.gold

# COMMAND ----------

# MAGIC %md 
# MAGIC Skapa en vy över tabellen

# COMMAND ----------

target_schema = 'gold'
target_table_name = ...

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view <catalog>.gold.<view_name>
# MAGIC as 
# MAGIC select 
# MAGIC   column 1,
# MAGIC   column 2,
# MAGIC   ...
# MAGIC from <catalog>.silver.<table_name>

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from <catalog>.gold.<view_name>

# COMMAND ----------


