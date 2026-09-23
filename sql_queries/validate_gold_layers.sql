-- ============================================================================
-- Proyecto: Pipeline de Datos de Ventas E2E - Arquitectura Medallion
-- Script: validate_gold_layers.sql
-- Propósito: Validar la integridad, consistencia y reglas de negocio de la capa Gold
-- Entorno: Databricks / Unity Catalog
-- ============================================================================

-- 1. CONFIGURACIÓN DEL ENTORNO DE TRABAJO (Idempotente)
-- Definimos el catálogo y esquema por defecto para las consultas si se registran en Unity Catalog
USE CATALOG workspace;
USE SCHEMA default;

-- ============================================================================
-- AUDITORÍA 1: Control de Calidad y Reglas de Negocio Estrictas
-- ============================================================================
-- Objetivo: Validar que no se hayan colado registros pendientes o cancelados en 
-- las tablas de agregados financieros de la capa Gold.

SELECT 
    'Validación Regla Gold' AS Tipo_Validacion,
    COUNT(*) AS Registros_Invalidos
FROM delta.`/Volumes/workspace/default/csvfiles/silver/sales_clean`
WHERE (Es_Venta = FALSE OR Cancelada = TRUE)
  AND ID_Venta IN (SELECT DISTINCT ID_Venta FROM delta.`/Volumes/workspace/default/csvfiles/gold/sales_by_customer`);

-- * Nota técnica: El conteo esperado debe ser exactamente 0 para confirmar que 
-- * las políticas de limpieza de Gold se ejecutaron sin fugas de datos.


-- ============================================================================
-- AUDITORÍA 2: Cuadrante de Totales (Consistencia Operacional)
-- ============================================================================
-- Objetivo: Asegurar que el monto total financiero de ventas efectivas en la capa 
-- Silver coincida exactamente con la sumatoria distribuida en los agregados de Gold.

WITH Silver_Total AS (
    SELECT ROUND(SUM(Total), 2) AS Monto_Silver
    FROM delta.`/Volumes/workspace/default/csvfiles/silver/sales_clean`
    WHERE Es_Venta = TRUE AND Cancelada = FALSE
),
Gold_Total AS (
    SELECT ROUND(SUM(Ingresos_Totales), 2) AS Monto_Gold
    FROM delta.`/Volumes/workspace/default/csvfiles/gold/sales_by_customer`
)
SELECT 
    s.Monto_Silver,
    g.Monto_Gold,
    (s.Monto_Silver - g.Monto_Gold) AS Desfase_Monetario
FROM Silver_Total s
CROSS JOIN Gold_Total g;


-- ============================================================================
-- AUDITORÍA 3: Coherencia Geográfica y Rendimiento Comercial
-- ============================================================================
-- Objetivo: Analizar la distribución de ventas efectivas por país y categoría 
-- para verificar que los datos agregados finales tengan sentido para el negocio.

SELECT 
    Pais,
    Producto_Categoria,
    SUM(Total_Unidades) AS Unidades_Por_Region,
    ROUND(SUM(Monto_Total), 2) AS Ingresos_Por_Region
FROM delta.`/Volumes/workspace/default/csvfiles/gold/sales_by_country`
GROUP BY Pais, Producto_Categoria
ORDER BY Pais ASC, Ingresos_Por_Region DESC;


-- ============================================================================
-- AUDITORÍA 4: Identificación de Registros en Cuarentena
-- ============================================================================
-- Objetivo: Consultar el estado de la carpeta de cuarentena para monitorear si 
-- el origen (ADF / CSV) está enviando filas corruptas o desalineadas de esquema.

-- * Nota: Dado que badRecordsPath guarda archivos estructurados JSON con los errores, 
-- * podemos inspeccionar las anomalías directamente si la ruta contiene datos.
SELECT * 
FROM json.`/Volumes/workspace/default/csvfiles/quarantine/sales_bad_records/*`
LIMIT 10;
