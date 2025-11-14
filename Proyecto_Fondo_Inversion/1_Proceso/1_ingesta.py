# Databricks notebook source
 #Importacion de librerias y funciones

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "../0_includes/configuracion"

# COMMAND ----------

catalogo = Catalog_Proy
esquema_source = Schema_Raw
esquema_sink = Schema_Bronze

# COMMAND ----------

#Path de la data a tratar
fondos_data = f"abfss://{esquema_source}@{storage_account}/fondos.csv"
valores_data = f"abfss://{esquema_source}@{storage_account}/valores.csv"
persona_ult_aporte_data = f"abfss://{esquema_source}@{storage_account}/persona_ult_aporte.csv"


# COMMAND ----------

#Esquema catalogo de FONDOS
fondos_schema= StructType([
StructField('cod_inversion',StringType(),nullable=True ),
StructField('cod_empresa',StringType(),nullable=True),
StructField('descripcion',StringType(),nullable=True),
StructField('ind_activo',StringType(),nullable=True)
])

# COMMAND ----------

#Esquema VALORES
valores_schema= StructType([
StructField('fec_valor',StringType(),nullable=True ),
StructField('cod_inversion',StringType(),nullable=True),
StructField('cod_empresa',StringType(),nullable=True),
StructField('valor',DoubleType(),nullable=True),
StructField('ind_autorizado',StringType(),nullable=True)
])

# COMMAND ----------

#Esquema PERSONA_ULT_APORTE
persona_ult_aporte_schema= StructType([
StructField('cod_cuenta',StringType(),nullable=True ),
StructField('cod_tipsaldo',StringType(),nullable=True),
StructField('cod_inversion',StringType(),nullable=True),
StructField('cod_empresa',StringType(),nullable=True),
StructField('can_cuotas',DoubleType(),nullable=True),
StructField('est_cuenta',StringType(),nullable=True),
StructField('fec_ult_aporte',StringType(),nullable=True)
])


# COMMAND ----------

# lectura de la data
df_fondos = spark.read.schema(fondos_schema).csv(fondos_data,header=True,sep=",")
df_valores = spark.read.schema(valores_schema).csv(valores_data,header=True,sep=",")
df_persona_ult_aporte = spark.read.schema(persona_ult_aporte_schema).csv(persona_ult_aporte_data,header=True,sep=",")

# COMMAND ----------

## Consultar los
df_fondos.limit(5).display()
df_valores.limit(5).display()
df_persona_ult_aporte.limit(5).display()

# COMMAND ----------

#transformacion de la data formato delta y almacenamiento usando INSERT
## Tambien hay Append para acumualar
#format("delta") : Puede ser Delta, Parquet
# Capa Delta Lake: Orientado a versiona Tablas, parque no versiona
#f" : para indcar la variable de un parametro


#df_fondos.write.mode('overwrite').format("delta").saveAsTable(f"{catalogo}.{esquema_sink}.fondos")
#df_valores.write.mode('overwrite').format("delta").saveAsTable(f"{catalogo}.{esquema_sink}.valores")
#df_persona_ult_aporte.write.mode('overwrite').format("delta").saveAsTable(f"{catalogo}.{esquema_sink}.persona_ult_aporte")

df_fondos.write.mode('append').format("delta").insertInto(f"{catalogo}.{esquema_sink}.fondos")
df_valores.write.mode('append').format("delta").insertInto(f"{catalogo}.{esquema_sink}.valores")
df_persona_ult_aporte.write.mode('append').format("delta").insertInto(f"{catalogo}.{esquema_sink}.persona_ult_aporte")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalog_proy.bronze.FONDOS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalog_proy.bronze.VALORES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalog_proy.bronze.PERSONA_ULT_APORTE