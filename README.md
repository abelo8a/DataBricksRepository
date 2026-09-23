# E2E Sales Data Pipeline - Medallion Architecture in Databricks

This repository contains a production-ready, End-to-End (E2E) data engineering pipeline implementing the **Medallion Architecture** (Bronze -> Silver -> Gold) within **Databricks Community Edition**. It utilizes **Unity Catalog Volumes** for cloud storage and is natively designed for orchestration via **Azure Data Factory (ADF)** using dynamic widgets.

---

## 📐 Architecture Overview

The data pipeline simulates an automated daily batch ingestion governed by external parameters (Widgets), broken down into the following stages:

```text
[ Python Generator ] ➔ 💾 ADLS Gen2 / UC Volumes (Daily CSVs)
                              │
                              ▼
   [ BRONZE Layer ]   ➔ 🧹 Strict Validation (StructType + badRecordsPath)
                              │
                              ▼
   [ SILVER Layer ]   ➔ 📦 Delta Parquet (Hive-Style Partitioned by Year/Month - Deduplicated)
                              │
                              ▼
   [ GOLD Layer ]     ➔ 📊 Business Aggregations (BI & Power BI Ready Tables)
```

---

## 📂 Component Structure

The project consists of three core Python/PySpark scripts:

### 1. Data Generator (`GenerateSalesFile.py`)
* **Purpose:** Simulates the upstream source system by generating realistic test datasets.
* **Features:** Creates 1,000 sales records using **real corporate brand names** (Google, Microsoft, Oxxo, Cinepolis, etc.), maintaining strict geographic coherence between countries and cities, timestamps for the year 2026, and operational boolean control flags.
* **Output:** Chronologically named CSV files (e.g., `SalesSampleTest_20260921.csv`).

### 2. Bronze to Silver Layer (`SalesSampleTestBronzeToSilver.py`)
* **Purpose:** Ingests the daily batch, enforces data quality, and performs deep cleaning.
* **Best Practices Applied:**
  * **Dynamic Parameters:** Implements a Databricks text widget (`p_fecha_proceso`) ready to receive the parameterized execution date injected by **Azure Data Factory** via `@formatDateTime(utcNow(), 'yyyy-MM-dd')`.
  * **Strict Schema Enforcement:** Uses explicit `StructType` definitions to eliminate slow schema inference overhead.
  * **Error Isolation (Quarantine):** Configures `badRecordsPath` to divert malformed or corrupt rows into an isolated JSON repository without breaking production runs.
  * **Deduplication & Integrity:** Filters null identifiers and enforces primary key integrity via `.dropDuplicates(["ID_Venta"])`.
* **Output:** Optimized **Delta Parquet** format physically partitioned by `Anio` and `Mes` (Hive-style partitioning).

### 3. Silver to Gold Layer (`SalesSampleTestSilverToGold.py`)
* **Purpose:** Analytical transformations and business-level aggregations.
* **Features:** Applies strict business rules (filters out pending orders and cancelled transactions) to generate accurate financial metrics.
* **Calculated Metrics:** Total Revenues, Units Sold, Average Ticket, and Transaction Counts grouped by Client/Company and Geographic Region.
* **Output:** Delta tables optimized with an `overwrite` strategy to guarantee ultra-fast read latencies for BI dashboards.

---

## 🛠️ Azure Data Factory (ADF) Integration

To automate this workflow in Azure:
1. Add a **Databricks Notebook** activity inside your ADF pipeline pointing to the Silver layer notebook.
2. In the **Base Parameters** tab, add a new parameter named exactly: `p_fecha_proceso`.
3. Set its value using the following dynamic expression: `@formatDateTime(utcNow(), 'yyyy-MM-dd')`.

The Databricks notebook will dynamically compute the exact storage path to load only the specific daily file, gracefully failing via `raise ValueError` if the source file is missing.

---

## 🚀 Technologies Used
* **Apache Spark / PySpark** (Distributed Processing Engine)
* **Delta Lake** (ACID Transactional Storage Layer)
* **Databricks Community Edition & Unity Catalog** (Execution & Governance Environment)
* **Python & Pandas** (Synthetic Data Generation)

***

# Pipeline de Datos de Ventas E2E - Arquitectura Medallion en Databricks

Este repositorio contiene un pipeline de ingeniería de datos completo de extremo a extremo (End-to-End) que implementa la **Arquitectura Medallion** (Bronze -> Silver -> Gold) en **Databricks Community Edition**, utilizando **Unity Catalog Volumes** para el almacenamiento y diseñado para ser orquestado mediante **Azure Data Factory (ADF)**.

---

## 📐 Arquitectura del Proyecto

El flujo de datos simula una carga diaria automatizada controlada por parámetros externos (Widgets), dividida en las siguientes fases:

```text
[ Generador Python ] ➔ 💾 ADLS Gen2 / UC Volumes (CSV Diarios)
                              │
                              ▼
   [ Capa BRONZE ]   ➔ 🧹 Validaciones Estrictas (StructType + badRecordsPath)
                              │
                              ▼
   [ Capa SILVER ]   ➔ 📦 Delta Parquet (Particionado por Año/Mes - Sin Duplicados)
                              │
                              ▼
   [ Capa GOLD ]     ➔ 📊 Tablas de Agregados de Negocio (Listas para Power BI)
```

---

## 📂 Estructura de los Componentes

El proyecto se compone de tres scripts principales en Python/PySpark:

### 1. Generador de Datos (`GenerateSalesFile.py`)
* **Propósito:** Simula el sistema origen generando datasets de pruebas realistas.
* **Características:** Crea 1,000 registros de ventas con nombres de **empresas reales** (Google, Microsoft, Oxxo, etc.), manteniendo coherencia geográfica entre países y ciudades, marcas de tiempo para el año 2026 y flags booleanos de control operacional.
* **Salida:** Archivos CSV individuales nombrados cronológicamente (ej: `SalesSampleTest_20260921.csv`).

### 2. Capa Bronze a Silver (`SalesSampleTestBronzeToSilver.py`)
* **Propósito:** Ingesta del lote diario, control de calidad y limpieza profunda.
* **Mejores Prácticas Aplicadas:**
  * **Parámetros Dinámicos:** Utiliza un widget de Databricks (`p_fecha_proceso`) listo para recibir la fecha inyectada automáticamente por un pipeline de **Azure Data Factory** vía `@formatDateTime(utcNow(), 'yyyy-MM-dd')`.
  * **Esquema Estricto:** Forzado de datos mediante `StructType` para evitar la inferencia de esquemas lenta.
  * **Aislamiento de Errores:** Configuración de `badRecordsPath` para desviar registros corruptos a una carpeta de cuarentena sin romper el pipeline de producción.
  * **Deduplicación e Integridad:** Filtrado de valores nulos y eliminación de llaves primarias duplicadas a través de `.dropDuplicates(["ID_Venta"])`.
* **Salida:** Almacenamiento en formato **Delta Parquet** particionado físicamente por carpetas (`Anio` y `Mes`) aplicando *Hive-style partitioning*.

### 3. Capa Silver a Gold (`SalesSampleTestSilverToGold.py`)
* **Propósito:** Transformación analítica y agregación para consumo de negocio.
* **Características:** Aplica filtros basados en reglas de negocio estrictas (excluye transacciones canceladas o pendientes de consolidar) y genera resúmenes financieros optimizados.
* **Métricas Calculadas:** Ingresos Totales, Unidades Vendidas, Ticket Promedio y Volumen de Transacciones agrupados por Empresa/Cliente y por Región Geográfica.
* **Salida:** Tablas Delta optimizadas mediante estrategia de sobreescritura (`overwrite`) para mantener un rendimiento de lectura ultra veloz en herramientas de BI.

---

## 🛠️ Integración con Azure Data Factory (ADF)

Para automatizar este pipeline en Azure:
1. Añade una actividad de tipo **Databricks Notebook** en tu pipeline de ADF apuntando al script de la capa Silver.
2. En la pestaña **Base Parameters**, declara la variable exacta: `p_fecha_proceso`.
3. Asigna la expresión dinámica para cargas diarias: `@formatDateTime(utcNow(), 'yyyy-MM-dd')`.

El cuaderno resolverá la ruta del almacenamiento de forma dinámica buscando el archivo del día exacto de ejecución, aislando cualquier falla de origen de manera controlada (`raise ValueError`).

---

## 🚀 Tecnologías Utilizadas
* **Apache Spark / PySpark** (Procesamiento distribuido)
* **Delta Lake** (Capa de almacenamiento ACID)
* **Databricks Community Edition & Unity Catalog** (Gobernanza y entorno de ejecución)
* **Python & Pandas** (Generación de datos sintéticos)
