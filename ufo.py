# Databricks notebook source
# DBTITLE 1, load data from Azure Blob Storage into a DataFrame
dbutils.fs.mount(
    source='wasbs://ufo-data@moviesdatasa.blob.core.windows.net',
    mount_point='/mnt/ufo-data',
    extra_configs = {'fs.azure.account.key.moviesdatasa.blob.core.windows.net': dbutils.secrets.get('moviesScopeProject', 'storageAccountKey')}
)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/ufo-data"

# COMMAND ----------

ufo = spark.read.format("csv").option("header","true").load("/mnt/ufo-data/raw/ufo-sightings-transformed.csv")

# COMMAND ----------

ufo.show()

# COMMAND ----------

ufo.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

ufo = ufo.withColumn("Encounter_Duration", col("Encounter_Duration").cast(IntegerType()))


# COMMAND ----------

ufo = ufo.withColumn("Year", col("Year").cast(DateType()))

# COMMAND ----------

from pyspark.sql.functions import col, date_format

ufo = ufo.withColumn("Year", date_format(col("Year"), "yyyy"))

# COMMAND ----------

ufo = ufo.withColumn("date_documented", col("date_documented").cast(DateType()))

# COMMAND ----------

from pyspark.sql import functions as F
ufo= ufo.withColumn('date_documented',F.to_date(ufo.date_documented))

# COMMAND ----------

ufo = ufo.withColumn("Year", date_format(col("Year"), "yyyy"))

# COMMAND ----------

ufo.write.mode("overwrite").option("header",'true').csv("/mnt/ufo-data/transformed/ufo")

# COMMAND ----------

# Register the DataFrame as a temporary table to run SQL queries
ufo.createOrReplaceTempView("ufo_table")

# Example query: Get the count of UFO sightings per country
result = spark.sql("""
    SELECT Country, COUNT(*) as CountOfSightings
    FROM ufo_table
    GROUP BY Country
    ORDER BY CountOfSightings DESC
""")
result.show()

# COMMAND ----------

# Example query: Find the average length of UFO encounters per UFO shape
result = spark.sql("""
    SELECT UFO_shape, AVG(length_of_encounter_seconds) as AvgEncounterLength
    FROM ufo_table
    GROUP BY UFO_shape
    ORDER BY AvgEncounterLength DESC
""")
result.show()

# COMMAND ----------

ufo.createOrReplaceTempView("ufo_table")

# Example query: Find the average length of UFO encounters per UFO shape
result = spark.sql("""
    SELECT UFO_shape, AVG(length_of_encounter_seconds) as AvgEncounterLength
    FROM ufo_table
    GROUP BY UFO_shape
    ORDER BY AvgEncounterLength DESC
""")

# Display the result as a bar chart
display(result)

# COMMAND ----------

# DBTITLE 1,UFO Shapes Distribution
# Count the occurrences of each UFO shape
shape_counts = ufo.groupBy("UFO_shape").count().orderBy("count", ascending=False)

# Display the distribution of UFO shapes as a pie chart
display(shape_counts)

# COMMAND ----------

# Count the number of UFO sightings per country
sightings_per_country = ufo.groupBy("Country").count().orderBy("count", ascending=False)

# Display the UFO sightings count per country as a bar chart
display(result)

# COMMAND ----------

from pyspark.sql.functions import desc

# Find the longest UFO encounter for each country
longest_encounter_per_country = ufo.groupBy("Country").agg({"length_of_encounter_seconds": "max"}) \
    .withColumnRenamed("max(length_of_encounter_seconds)", "Longest_Encounter_Seconds") \
    .orderBy(desc("Longest_Encounter_Seconds"))

# Display the longest UFO encounters per country as a bar chart
display(longest_encounter_per_country)
