# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date

# COMMAND ----------

# MAGIC %run "../0_includes/configuracion"

# COMMAND ----------

catalogo = Catalog_Proy
esquema_source = Schema_Bronze
esquema_sink = Schema_Silver

# COMMAND ----------

def est_cuenta_categoria(est_cuenta):
    if est_cuenta == 'A':
        return "Activo"
    elif est_cuenta ==  'P':
        return "Pasivo"
    else:
        return "No Definido"

# COMMAND ----------

est_cuenta_udf = F.udf(est_cuenta_categoria, StringType())

# COMMAND ----------

df_fondos = spark.table(f"{catalogo}.{esquema_source}.fondos")
df_persona_ult_aporte = spark.table(f"{catalogo}.{esquema_source}.persona_ult_aporte")
df_valores = spark.table(f"{catalogo}.{esquema_source}.valores")


# COMMAND ----------

# Quitar el registro si esta nulo el campo cod_empresa
df_fondos = df_fondos.dropna(how="all")\
                        .filter((col("cod_empresa").isNotNull()) | (col("cod_empresa")).isNotNull())

# COMMAND ----------

df_fondos_Update = df_fondos.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #Personas Ultimo Aporte Transformado

# COMMAND ----------

df_persona_ult_aporte = df_persona_ult_aporte.withColumn("est_cuenta_categoria", est_cuenta_udf("est_cuenta"))

# COMMAND ----------

df_persona_ult_aporte_Update = df_persona_ult_aporte.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #Valores Cuotas Transformado

# COMMAND ----------

df_valores_joined = df_fondos.alias("df").join(df_valores.alias("dv"), col("df.cod_inversion") == col("dv.cod_inversion"), "inner").select(
        to_date(col("dv.fec_valor"), "dd/MM/yyyy").alias( "Fec_valor"),
        col("dv.cod_inversion").alias("Cod_inversion"),
        col("df.descripcion").alias("Descripcion"),
        col("dv.cod_empresa").alias("Cod_Empresa"),
        col("dv.valor").alias("Valor"),
        col("dv.ind_autorizado").alias("ind_autorizado"),
        year(to_date(col("dv.fec_valor"), "dd/MM/yyyy")).alias("anio"),
        month(to_date(col("dv.fec_valor"), "dd/MM/yyyy")).alias("mes"),
        quarter(to_date(col("dv.fec_valor"), "dd/MM/yyyy")).alias("quarter"),
        dayofweek(to_date(col("dv.fec_valor"), "dd/MM/yyyy")).alias("dia_semana"),
        dayofmonth(to_date(col("dv.fec_valor"), "dd/MM/yyyy")).alias("dia_del_mes"),
        dayname(to_date(col("dv.fec_valor"), "dd/MM/yyyy")).alias("nombre_dia_semana"),
        monthname(to_date(col("dv.fec_valor"), "dd/MM/yyyy")).alias("nombre_mes")
    )
#df_valores_joined  = df_valores_joined.withColumn("fecha_formateada", to_date(col("Fec_valor"), "dd/MM/yyyy"))
df_valores_joined.display()



# COMMAND ----------

df_valores_update = df_valores_joined.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

#df_fondos_Update.write.mode('overwrite').format("delta").saveAsTable(f"{catalogo}.{esquema_sink}.fo_fondos_transformed")
#df_valores_update.write.mode('overwrite').format("delta").saveAsTable(f"{catalogo}.{esquema_sink}.fo_valores_transformed")
#df_persona_ult_aporte_Update.write.mode('overwrite').format("delta").saveAsTable(f"{catalogo}.{esquema_sink}.df_persona_ult_aporte_transformed")

df_fondos_Update.write.mode('append').format("delta").insertInto(f"{catalogo}.{esquema_sink}.fondos_transformed")
df_valores_update.write.mode('append').format("delta").insertInto(f"{catalogo}.{esquema_sink}.valores_transformed")
df_persona_ult_aporte_Update.write.mode('append').format("delta").insertInto(f"{catalogo}.{esquema_sink}.persona_ult_aporte_transformed")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalog_proy.silver.persona_ult_aporte_transformed