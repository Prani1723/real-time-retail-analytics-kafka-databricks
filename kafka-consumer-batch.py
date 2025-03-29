# Databricks notebook source
confluentBootstrapServers = 'pkc-56d1g.eastus.azure.confluent.cloud:9092' 
confluentApiKey = 'YTUN35X3UTSBIBJX'
confluentSecret = 'mRFhJ0RSsP8DkFCHjqttm2uZxlo/YOGjDSxG0Fa0oOKVjTVc5tQy7Ahrzzxtm2EJ'
confluentTopicNmae = 'retail-data-new'

# COMMAND ----------

orders_df = spark \
.read \
.format("kafka") \
.option("kafka.bootstrap.servers", confluentBootstrapServers) \
.option("kafka.security.protocol", "SASL_SSL") \
.option("kafka.sasl.mechanism", "PLAIN") \
.option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(confluentApiKey, confluentSecret)) \
.option('kafka.ssl.endpoint.identification.algorithm', "https") \
.option("subscribe", confluentTopicNmae) \
.load()

# COMMAND ----------

display(orders_df)

# COMMAND ----------

converted_orders_df = orders_df.selectExpr("CAST(key as string) AS key","CAST(value as string) AS value","topic","partition","offset","timestamp","timestampType")

# COMMAND ----------

display(converted_orders_df)

# COMMAND ----------

