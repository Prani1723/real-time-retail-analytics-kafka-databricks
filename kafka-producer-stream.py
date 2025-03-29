# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

confluentBootstrapServers = 'pkc-56d1g.eastus.azure.confluent.cloud:9092' 
confluentApiKey = 'YTUN35X3UTSBIBJX'
confluentSecret = 'mRFhJ0RSsP8DkFCHjqttm2uZxlo/YOGjDSxG0Fa0oOKVjTVc5tQy7Ahrzzxtm2EJ'
confluentTopicNmae = 'retail-data-new'
confluentTargetTopicName = 'processed_orders'

# COMMAND ----------

orders_df = spark \
.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", confluentBootstrapServers) \
.option("kafka.security.protocol", "SASL_SSL") \
.option("kafka.sasl.mechanism", "PLAIN") \
.option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
.option('kafka.ssl.endpoint.identification.algorithm', "https") \
.option("subscribe", confluentTopicNmae) \
.option("startingTimestamp",1) \
.option("maxOffsetsPerTrigger", 50) \
.load()


# COMMAND ----------

display(orders_df)

# COMMAND ----------

converted_orders_df = orders_df.selectExpr("CAST(key as string) AS key","CAST(value as string) AS value","topic","partition","offset","timestamp","timestampType")

# COMMAND ----------

orders_schema = "order_id long,customer_id long,customer_fname string,customer_lname string,city string,state string,pincode long,line_items array<struct<order_item_id: long,order_item_product_id: long,order_item_quantity: long,order_item_product_price: float,order_item_subtotal: float>>"

# COMMAND ----------

parsed_df = converted_orders_df.select("key", from_json("value", orders_schema).alias("value"),"topic","partition","offset","timestamp","timestampType")

# COMMAND ----------

display(parsed_df)

# COMMAND ----------

parsed_df.createOrReplaceTempView("parsed_orders")

# COMMAND ----------

filtered_orders= spark.sql("select cast(key as string) as key, cast(value as string) as value from parsed_orders where value.city = 'Chicago'")

# COMMAND ----------

display(filtered_orders)

# COMMAND ----------

filtered_orders \
.writeStream \
.queryName("ingestionquery") \
.format("kafka") \
.outputMode("append") \
.option("checkpointLocation","checkpointdir307") \
.option("kafka.bootstrap.servers",confluentBootstrapServers) \
.option("kafka.security.protocol","SASL_SSL") \
.option("kafka.sasl.mechanism","PLAIN") \
.option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
.option("kafka.ssl.endpoint.identification.algorithm","https") \
.option("topic",confluentTargetTopicName) \
.start()

# COMMAND ----------



# COMMAND ----------

spark.sql("select * from orders_newtable").show()