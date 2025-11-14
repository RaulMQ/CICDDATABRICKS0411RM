# Databricks notebook source
# MAGIC %md
# MAGIC ### Creacion de Scripts
# MAGIC #####La creación de los script ya se encuentran incluidos en la ruta siguiente como "Preparacion de ambiente"
# MAGIC ##### Creación de catalogo
# MAGIC ##### Creación de tablas
# MAGIC ##### Creación de esquemas
# MAGIC ##### Creación de external
# MAGIC
# MAGIC #### Ruta:
# MAGIC ##### /Workspace/Users/ing.raulmendozaq@gmail.com/Proyecto_Fondo_Inversion/1_Proceso/0_preparacion_ambientre

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creacion de catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_proy;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creacion de Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_proy.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_proy.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_proy.golden;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creacion tablas Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_proy.bronze.FONDOS (
# MAGIC   cod_inversion string,
# MAGIC   cod_empresa string,
# MAGIC   descripcion string,
# MAGIC   ind_activo string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/bronze/FONDOS';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_proy.bronze.VALORES (
# MAGIC   fec_valor string,
# MAGIC   cod_inversion string,
# MAGIC   cod_empresa string,
# MAGIC   valor double,
# MAGIC   ind_autorizado string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/bronze/VALORES';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_proy.bronze.PERSONA_ULT_APORTE (
# MAGIC   cod_cuenta string,
# MAGIC   cod_tipsaldo string,
# MAGIC   cod_inversion string,
# MAGIC   cod_empresa string,
# MAGIC   can_cuotas double,
# MAGIC   est_cuenta string,
# MAGIC   fec_ult_aporte string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/bronze/PERSONA_ULT_APORTE';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creacion tablas silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_proy.silver.FONDOS_TRANSFORMED (
# MAGIC cod_inversion string,
# MAGIC cod_empresa string,
# MAGIC descripcion string,
# MAGIC ind_activo string,
# MAGIC ingestion_date timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/silver/FONDOS_TRANSFORMED';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_proy.silver.VALORES_TRANSFORMED (
# MAGIC Fec_valor date,
# MAGIC Cod_inversion string,
# MAGIC Descripcion string,
# MAGIC Cod_Empresa string,
# MAGIC Valor double,
# MAGIC ind_autorizado string,
# MAGIC anio integer,
# MAGIC mes integer,
# MAGIC quarter integer,
# MAGIC dia_semana integer,
# MAGIC dia_del_mes integer,
# MAGIC nombre_dia_semana string,
# MAGIC nombre_mes string,
# MAGIC ingestion_date timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/silver/VALORES_TRANSFORMED';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_proy.silver.PERSONA_ULT_APORTE_TRANSFORMED (
# MAGIC cod_cuenta string,
# MAGIC cod_tipsaldo string,
# MAGIC cod_inversion string,
# MAGIC cod_empresa string,
# MAGIC can_cuotas double,
# MAGIC est_cuenta string,
# MAGIC fec_ult_aporte string,
# MAGIC est_cuenta_categoria string,
# MAGIC ingestion_date timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/silver/PERSONA_ULT_APORTE_TRANSFORMED';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creacion tablas golden

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_proy.golden.VALORES_INSIGHTS (
# MAGIC anio integer,
# MAGIC mes integer,
# MAGIC cod_inversion string,
# MAGIC fec_conteo long,
# MAGIC max_valor double,
# MAGIC min_valor double,
# MAGIC Prom_valor double
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '${storageLocation}/golden/VALORES_INSIGHTS';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creacion de External

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-raw`
# MAGIC URL 'abfss://raw@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas raw del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-bronze`
# MAGIC URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-silver`
# MAGIC URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas silver del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-golden`
# MAGIC URL 'abfss://golden@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL credential)
# MAGIC COMMENT 'Ubicación externa para las tablas golden del Data Lake';