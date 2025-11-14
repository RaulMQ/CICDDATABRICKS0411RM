
# ☕ Modelo Fondo de Inversón ETL Pipeline
### Arquitectura Medallon en Azure Databricks


## 🎯 Descripción

Pipeline ETL que transforma datos crudos de información de Fondos de inversion en insights accionables, implementando la **Arquitectura Medallon** (Raw-Bronze-Silver-Gold) en Azure Databricks con **CI/CD completo** y **Delta Lake** para garantizar consistencia ACID.

### ✨ Características Principales

- 🔄 **ETL Automatizado** - Pipeline completo con despliegue automático via GitHub Actions
- 🏗️ **Arquitectura Medallon** - Separación clara de capas Raw → Bronze → Silver → Gold
- 📊 **Modelo Dimensional** - Star Schema optimizado para análisis de negocio
- 🚀 **CI/CD Integrado** - Deploy automático en cada push a master
- 📈 **Power BI Ready** - Conexión directa con SQL Warehouse para el reporte especifico
- ⚡ **Delta Lake** - ACID transactions y time travel capabilities


---

## 🏛️ Arquitectura

### Flujo de Datos

```
📄 CSV (Raw Data)
    ↓
🥉 Bronze Layer (Ingesta sin transformación)
    ↓
🥈 Silver Layer (Limpieza + Modelo Dimensional)
    ↓
🥇 Gold Layer (Agregaciones de Negocio)
    ↓
📊 Power BI (Visualización)
```

### 📦 Capas del Pipeline

#### 📄 Raw Layer

**Propósito**: Corresponde a la zona de los insumos que corresponden a archivos en formato csv

- ✅ fondos.csv
- ✅ valores.csv
- ✅ persona_ult_aporte.csv

#### 🥉 Bronze Layer
**Propósito**: Corresponde a la zona de aterrizaje información desde el origen Raw

**Tabla**: 
- `FONDOS`
- `VALORES`
- `PERSONA_ULT_APORTE` 


#### 🥈 Silver Layer
**Propósito**: Modelo dimensional se realiza la transformacion de los datos.

**Tablas**:
- `FONDOS_TRANSFORMED`
- `VALORES_TRANSFORMED`
- `PERSONA_ULT_APORTE_TRANSFORMED`


#### 🥇 Gold Layer
**Propósito**: Analytics-ready para disponibilizar la información

**Tablas**:
- `VALORES_INSIGHTS`


## 📁 Estructura del Proyecto

```
Información de Fondos de Inversión ETL/
│
├── 📂 .github/
│   └── 📂 workflows/
│       └── 📄 databricks-deploy.yml    # Pipeline CI/CD
│
├── 📂 proceso/
│   ├── 🐍 0_preparacion_ambientre.py    # Creación de Catlogo, Creación de esquema, Creación de External, DDLS, GRANT
│   ├── 🐍 1_ingesta.py  	         # Bronze Layer (Consume el insumo ubicado en la capa RAW)
│   ├── 🐍 2_transform.py                # Silver Layer
│   └── 🐍 3_load.py                     # Gold Layer
│
└── 📄 README.md
```

---

## 🛠️ Tecnologías

Elaborado en Databricks

## ⚙️ Requisitos Previos

- ☁️ Cuenta de Azure con acceso a Databricks
- 💻 Workspace de Databricks configurado
- 🖥️ Cluster activo (nombre: `CLUSTER_SD`)
- 🐙 Cuenta de GitHub con permisos de administrador
- 📦 Azure Data Lake Storage Gen2 configurado
- 📊 Power BI Desktop (opcional para visualización)


## 👤 Autor


### Raúl Mendoza Quispe

[LinkedIn]https://www.linkedin.com/in/raul-mendozaq/
[GitHub]https://github.com/ltechdev
[Email] raulmendozaq@gmail.com

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.

**Data Engineering** | **Azure Databricks** | **Delta Lake** | **CI/CD**

---

**Proyecto**: Data Engineering - Arquitectura Medallon  
**Tecnología**: Azure Databricks + Delta Lake + CI/CD  
**Última actualización**: 2025

