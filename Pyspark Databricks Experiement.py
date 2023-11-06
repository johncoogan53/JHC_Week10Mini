# Databricks notebook source
# MAGIC %md
# MAGIC Below we will be experimenting with pyspark and microsoft azure databricks. We will use a databricks dataset for this experimentation and demonstrate both the powerful visualization functionality of this tool as well as its ability to perform CRUD operations on large datasets. 

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
spark = SparkSession \
    .builder \
    .appName("PySpark Demo") \
    .config("spark.some.config.option","some-values") \
    .getOrCreate()

# COMMAND ----------

df = (spark.read
  .format("csv")
  .option("mode", "PERMISSIVE")
  .option("header", "true")
  .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
)


# COMMAND ----------

# MAGIC %md
# MAGIC First we want to look at how big the dataset we imported is, below we will see that it is 53,940 observation, which is decently large. 
# MAGIC

# COMMAND ----------

#display the size of the df
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC show is used to see the first few rows of our dataset

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC printSchema is used to view the types of data that are in the dataset
# MAGIC

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC In order to get good plots with this data we have to clean the price variable from a string into a float, otherwise some of the visualizations struggle to capture it as a linear variable.

# COMMAND ----------

df = df.withColumn("price", df["price"].cast(FloatType()))

# COMMAND ----------

# MAGIC %md
# MAGIC With display, we can make use of the built in visualizer which makes short work of many complex visualizations. Below, by plotting carat and price, we can see the bimodal clustering of diamonds. This is likely due to CZ diamonds and natural diamonds.

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC By subsetting price we can look at the cut, carat, price distribution for natural diamonds

# COMMAND ----------

filter_price = df.filter(df.price>1000)
display(filter_price)

# COMMAND ----------

# MAGIC %md
# MAGIC and we can conclude that, clearly, diamond pricing is much more complex than simply its carat and its cut. By not seeing any real discernable pattern here, other variables like color, clarity, table, and depth clearly play a role. What is of note from this plot is the preponderance of diamonds at 0.7 carats. This seems to be a distinct line that may have something to do with the way diamonds are refined. 

# COMMAND ----------

# MAGIC %md
# MAGIC Now we move on to sql CRUD operations. We will make a temporary database view of this csv file so we can then query it appropriately. We will also make a function to more easily execute queries:

# COMMAND ----------

df.createOrReplaceTempView("my_table")

def query(q_string):
    query = q_string
    results = spark.sql(query)
    return results

# COMMAND ----------

query("SELECT * FROM my_table LIMIT 5")
