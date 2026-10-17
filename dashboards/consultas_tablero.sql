-- Consultas del tablero "Auditoría de facturación en salud"
-- Cada bloque es un dataset del AI/BI Dashboard de Databricks.
-- Catálogo y esquema: workspace.health_claims

-- =============================================================================
-- Dataset 1: kpis  →  6 contadores en la fila superior
-- =============================================================================
SELECT
  SUM(servicios)                                   AS servicios_auditados,
  SUM(valor_facturado)                             AS valor_facturado,
  SUM(valor_reconocido)                            AS valor_reconocido,
  SUM(glosa_potencial)                             AS glosa_potencial,
  ROUND(SUM(glosa_potencial) / SUM(valor_facturado), 4) AS pct_glosa,
  SUM(servicios_con_hallazgo)                      AS servicios_con_hallazgo,
  (SELECT SUM(valor_en_riesgo) FROM workspace.health_claims.gold_alertas) AS valor_en_riesgo_alertas
FROM workspace.health_claims.gold_prestador_mes;

-- =============================================================================
-- Dataset 2: tendencia_mensual  →  línea: facturado vs. reconocido por mes
-- =============================================================================
SELECT
  to_date(periodo, 'yyyyMM') AS mes,
  SUM(valor_facturado)       AS valor_facturado,
  SUM(valor_reconocido)      AS valor_reconocido,
  SUM(glosa_potencial)       AS glosa_potencial
FROM workspace.health_claims.gold_prestador_mes
GROUP BY periodo
ORDER BY mes;

-- =============================================================================
-- Dataset 3: glosa_por_prestador  →  barras horizontales, ordenadas de mayor a menor
-- =============================================================================
SELECT
  nombre_prestador,
  modalidad,
  SUM(valor_facturado)  AS valor_facturado,
  SUM(glosa_potencial)  AS glosa_potencial,
  ROUND(SUM(glosa_potencial) / SUM(valor_facturado), 4) AS pct_glosa
FROM workspace.health_claims.gold_prestador_mes
GROUP BY nombre_prestador, modalidad
ORDER BY glosa_potencial DESC;

-- =============================================================================
-- Dataset 4: hallazgos_por_tipo  →  barras: cantidad y glosa por tipo de hallazgo
-- =============================================================================
SELECT
  hallazgo_principal,
  COUNT(*)             AS servicios,
  SUM(vr_servicio)     AS valor_facturado,
  SUM(glosa_potencial) AS glosa_potencial
FROM workspace.health_claims.silver_servicios
WHERE tiene_hallazgo
GROUP BY hallazgo_principal
ORDER BY servicios DESC;

-- =============================================================================
-- Dataset 5: ejecucion_pgp  →  línea por prestador con límites de banda
-- =============================================================================
SELECT
  to_date(periodo, 'yyyyMM') AS mes,
  nombre_prestador,
  pct_ejecucion,
  1.10 AS limite_superior,
  0.90 AS limite_inferior,
  estado_banda,
  exceso_sobre_limite
FROM workspace.health_claims.gold_ejecucion_pgp
ORDER BY nombre_prestador, mes;

-- =============================================================================
-- Dataset 6: alertas  →  tabla priorizada por valor en riesgo
-- =============================================================================
SELECT
  periodo,
  nombre_prestador,
  modalidad,
  tipo_alerta,
  detalle,
  valor_en_riesgo
FROM workspace.health_claims.gold_alertas
ORDER BY valor_en_riesgo DESC;

-- =============================================================================
-- Dataset 7: valor_medio_consulta  →  línea por prestador (muestra el upcoding)
-- =============================================================================
SELECT
  to_date(periodo, 'yyyyMM') AS mes,
  nombre_prestador,
  valor_medio_consulta
FROM workspace.health_claims.gold_prestador_mes
WHERE modalidad = 'EVENTO'
ORDER BY nombre_prestador, mes;
