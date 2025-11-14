# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run "../0_includes/configuracion"

# COMMAND ----------

catalogo = Catalog_Proy
esquema_source = Schema_Silver
esquema_sink = Schema_Golden

# COMMAND ----------

valores_transformed = spark.table(f"{catalogo}.{esquema_source}.valores_transformed")

# COMMAND ----------

valores_transformed = valores_transformed.groupBy(col("anio"), col("mes"), col("cod_inversion")).agg(
                                                     count(col("fec_valor")).alias("fec_conteo"),
                                                     max(col("valor")).alias("max_valor"),
                                                     min(col("valor")).alias("min_valor"),
                                                     avg(col("valor")).alias("Prom_valor")
                                                     ).orderBy(col("mes").desc())

valores_transformed.display()


# COMMAND ----------

#valores_transformed.write.mode("overwrite").format("delta").saveAsTable(f"{catalogo}.{esquema_sink}.valores_insights")
valores_transformed.write.mode("append").format("delta").insertInto(f"{catalogo}.{esquema_sink}.valores_insights")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalog_proy.golden.valores_insights