# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storageName default "adlsrmendozaeu2d01";
# MAGIC create widget text storageLocation default "abfss://lhcldata@adlsrmendozaeu2d01.dfs.core.windows.net";

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storageLocation default "abfss://bronze@adlsrmendozaeu2d01.dfs.core.windows.net";

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminación tablas Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS catalog_proy.bronze.FONDOS;
# MAGIC DROP TABLE IF EXISTS catalog_proy.bronze.VALORES;
# MAGIC DROP TABLE IF EXISTS catalog_proy.bronze.PERSONA_ULT_APORTE;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminación tablas Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS catalog_proy.bronze.FONDOS_TRANSFORMED;
# MAGIC DROP TABLE IF EXISTS catalog_proy.bronze.VALORES_TRANSFORMED;
# MAGIC DROP TABLE IF EXISTS catalog_proy.bronze.PERSONA_ULT_APORTE_TRANSFORMED;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminación tablas Golden

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS catalog_proy.bronze.VALORES_INSIGHTS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminación permiso a grupos

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE USE SCHEMA ON SCHEMA catalog_proy.bronze FROM `Developers`;
# MAGIC REVOKE CREATE ON SCHEMA catalog_proy.bronze FROM `Developers`;
# MAGIC
# MAGIC REVOKE USE SCHEMA ON SCHEMA catalog_proy.silver FROM `Analistas`;
# MAGIC REVOKE CREATE ON SCHEMA catalog_proy.silver FROM `Analistas`;
# MAGIC
# MAGIC REVOKE USE SCHEMA ON SCHEMA catalog_proy.golden FROM `Analistas`;
# MAGIC REVOKE CREATE ON SCHEMA catalog_proy.golden FROM `Analistas`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminación permisos

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE SELECT ON TABLE catalog_proy.bronze.fondos FROM `Developers`;
# MAGIC REVOKE SELECT ON TABLE catalog_proy.bronze.valores FROM `Developers`;
# MAGIC REVOKE SELECT ON TABLE catalog_proy.bronze.persona_ult_aporte FROM `Developers`;
# MAGIC
# MAGIC REVOKE SELECT ON TABLE catalog_proy.silver.fo_valores_transformed FROM `Analistas`;
# MAGIC REVOKE SELECT ON TABLE catalog_proy.silver.fo_fondos_transformed FROM `Analistas`;
# MAGIC REVOKE SELECT ON TABLE catalog_proy.silver.df_persona_ult_aporte_transformed FROM `Analistas`;
# MAGIC
# MAGIC REVOKE SELECT ON TABLE catalog_proy.golden.fo_valores_insights FROM `Admins`;
# MAGIC REVOKE SELECT ON TABLE catalog_proy.golden.fo_valores_insights FROM `Analistas`;