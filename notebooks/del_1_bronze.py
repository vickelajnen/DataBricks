# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Första delen

# COMMAND ----------

import pandas as pd
import requests
from datetime import date
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType, FloatType, DecimalType
from helper_functions.calendar import create_date_table

# COMMAND ----------

# MAGIC %md
# MAGIC spark session

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC [elprisjustnu](https://www.elprisetjustnu.se) har ett öppet API för att hämta el-data från de senaste månaderna.
# MAGIC 
# MAGIC Hämta eldata via API
# MAGIC GET https://www.elprisetjustnu.se/api/v1/prices/[ÅR]/[MÅNAD]-[DAG]_[PRISKLASS].json

# COMMAND ----------

# MAGIC %md
# MAGIC <table class="table table-sm mb-5">
# MAGIC             <tbody><tr>
# MAGIC               <th>Variabel</th>
# MAGIC               <th>Beskrivning</th>
# MAGIC               <th>Exempel</th>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>ÅR</th>
# MAGIC               <td>Alla fyra siffror</td>
# MAGIC               <td>2023</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>MÅNAD</th>
# MAGIC               <td>Alltid två siffror, med en inledande nolla</td>
# MAGIC               <td>03</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>DAG</th>
# MAGIC               <td>Alltid två siffror, med en inledande nolla</td>
# MAGIC               <td>13</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>PRISKLASS</th>
# MAGIC               <td>
# MAGIC                 SE1 = Luleå / Norra Sverige<br>
# MAGIC                 SE2 = Sundsvall / Norra Mellansverige<br>
# MAGIC                 SE3 = Stockholm / Södra Mellansverige<br>
# MAGIC                 SE4 = Malmö / Södra Sverige
# MAGIC               </td>
# MAGIC               <td>SE3</td>
# MAGIC             </tr>
# MAGIC           </tbody></table>

# COMMAND ----------

# MAGIC %md
# MAGIC Använd endpointen för att hämta historisk data. Tidigast tillängliga datum är **2022-10-26**. 
# MAGIC 
# MAGIC Lämpliga paket i Python att använda:
# MAGIC 
# MAGIC - ```requests``` - API-anrop. [docs](https://requests.readthedocs.io/en/latest/)
# MAGIC - ```pandas``` - Pandas DataFrame. [docs](https://pandas.pydata.org/docs/)
# MAGIC - ```datetime``` - Datumhantering [docs](https://docs.python.org/3/library/datetime.html)
# MAGIC - ```pyspark``` - Datahantering i spark. [docs](https://spark.apache.org/docs/3.1.3/api/python/index.html)
# MAGIC 
# MAGIC Skapa en funktion som loopar över en array med datum för att hämta historisk data
# MAGIC Dagens datum ```date.today().strftime("%Y-%m-%d")```. Datum-Array i pandas: ```pd.date_range(start,end).strftime("%Y-%m-%d")```

# COMMAND ----------

PRISKLASS = ...
start_date = ...
end_date = ...
DATUM =  ...# Format: List(YYYY-MM-DD)
DATUM 

# COMMAND ----------

# MAGIC %md
# MAGIC skapa en funktion som loopar över datumen-arrayen definerad ovan

# COMMAND ----------

def fetch_data():
    """API-anropet tar emot ett datum i taget. Skapa en funktion som går igenom listan med datum samt de olika prisklasserna.
        spara data i en text en lista. Retunera datat i något format (json)"""
    data = []
    for p in PRISKLASS:
        for d in DATUM:
            api_url = f'https://www.elprisetjustnu.se/api/v1/prices/{d[:4]}/{d[5:7]}-{d[8:]}_{p}.json'
            response = requests.get(url=api_url)
            print(response.status_code)
            data_json = response.json()
            # Notera atat elzoon eller prisklass inte är med i datat vi får tillbaka och måste lägga till det själva.
            data_json = [dict(item, **{'elzoon': p}) for item in data_json]
            data += data_json
    
    return data
    
    


# COMMAND ----------
# Kallar på vår nya funktion
data = fetch_data()

# COMMAND ----------

# MAGIC %md
# MAGIC Skapa ett schema för datastrukturen

# COMMAND ----------

schema = StructType([ \
    StructField("SEK_per_kWh",StringType(),True), \
    StructField("EUR_per_kWh",StringType(),True), \
    StructField("EXR",StringType(),True), \
    # Gör klart schemat för de sista 3 kolumnerna
  ])
type(schema)

# COMMAND ----------
# MAGIC Skapa en spark Dataframe: ```spark.createDataFrame(data=data, schema=schema)```
# COMMAND ----------

spark_df = ...

# COMMAND ----------

# MAGIC %md
# MAGIC Inspektera schema med ```df.printSchema()```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Visa data i dataframen i spark ```display(df)``` eller ```df.show()```

# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC Få en summering dataframen genom ```df.describe().show()```

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Skapa catalog ```%sql create catalog if not exists emanuel_db```

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists emanuel_db

# COMMAND ----------

# MAGIC %md
# MAGIC Skapa schema (databas) ```%sql create schema if not exists <catalog>.bronze```

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema if not exists emanuel_db.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC Skriv data till direkt till en tabell
# MAGIC 
# MAGIC ```df.write.option("").mode("").saveAsTable("<catalog>.<schema>.<table>")```

# COMMAND ----------

spark_df.write....

# COMMAND ----------

# MAGIC %md
# MAGIC Kika på tabellen du precis skapade
# MAGIC 
# MAGIC ```%sql select * from <catalog>.<schema>.<table>```
# MAGIC 
# MAGIC eller
# MAGIC 
# MAGIC ```spark.read.table(<catalog>.<schema>.<table>)```

# COMMAND ----------



# COMMAND ----------
# MAGIC %md 
# MAGIC Skapa en kalender-tabell för att berika data i nästa steg.
# COMMAND ----------

df = create_date_table() 
df.head()

# COMMAND ----------

spark_df = spark.createDataFrame(df)
spark_df.show()

# COMMAND ----------
# MAGIC %md 
# MAGIC Ändra variabeln ```catalog````
# COMMAND ----------
catalog = 'your_catalog_name'

# COMMAND ----------

spark_df.write.option("overwriteSchema", True).mode("ovewrite").saveAsTable(f"{catalog}.staging.calendar")

# COMMAND ----------
