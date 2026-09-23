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
