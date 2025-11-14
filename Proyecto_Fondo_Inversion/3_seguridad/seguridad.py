# Databricks notebook source
# MAGIC %md
# MAGIC ### Creacion de Grant
# MAGIC #####La creación de los grants ya se encuentran incluidos en la ruta siguiente como "Preparacion de ambiente" por temas de orden de despliegue
# MAGIC ##### GRANT de catalogo a grupos de roles
# MAGIC ##### GRANT de tablas a grupos de roles
# MAGIC ##### GRANT de esquemas a grupos de roles
# MAGIC
# MAGIC
# MAGIC ##### /Workspace/Users/ing.raulmendozaq@gmail.com/Proyecto_Fondo_Inversion/1_Proceso/0_preparacion_ambientre
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG catalog_proy TO `Developers`;
# MAGIC GRANT USE CATALOG ON CATALOG catalog_proy TO `Admins`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_proy.bronze TO `Developers`;
# MAGIC GRANT CREATE ON SCHEMA catalog_proy.bronze TO `Developers`;
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_proy.silver TO `Analistas`;
# MAGIC GRANT CREATE ON SCHEMA catalog_proy.silver TO `Analistas`;
# MAGIC
# MAGIC GRANT USE SCHEMA ON SCHEMA catalog_proy.golden TO `Analistas`;
# MAGIC GRANT CREATE ON SCHEMA catalog_proy.golden TO `Analistas`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE catalog_proy.bronze.fondos TO `Developers`;
# MAGIC GRANT SELECT ON TABLE catalog_proy.bronze.valores TO `Developers`;
# MAGIC GRANT SELECT ON TABLE catalog_proy.bronze.persona_ult_aporte TO `Developers`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE catalog_proy.silver.fo_valores_transformed TO `Analistas`;
# MAGIC GRANT SELECT ON TABLE catalog_proy.silver.fo_fondos_transformed TO `Analistas`;
# MAGIC GRANT SELECT ON TABLE catalog_proy.silver.df_persona_ult_aporte_transformed TO `Analistas`;
# MAGIC
# MAGIC GRANT SELECT ON TABLE catalog_proy.golden.fo_valores_insights TO `Admins`;
# MAGIC GRANT SELECT ON TABLE catalog_proy.golden.fo_valores_insights TO `Analistas`;
# MAGIC