-- =========================================================================
-- SCRIPT DE INICIALIZACIÓN DE BASES DE DATOS - PLATAFORMA IoT (Ev2)
-- Asignatura: Gestión de Datos para IA
-- Escuela de Informática y Telecomunicaciones - Duoc UC
-- Integrantes: David Santibáñez, Luis Quijada Muñoz
-- Profesor: Sebastián Saavedra Espinosa
-- =========================================================================

-- =========================================================================
-- 1. CAPA BRONZE (Ejecutar en la Base de Datos Bronze - Puerto Local: 5433)
-- Repositorio inmutable para persistencia masiva.
-- =========================================================================

CREATE TABLE IF NOT EXISTS telemetria_cruda (
    id SERIAL NOT NULL,
    timestamp_lectura TIMESTAMP,
    id_maquina VARCHAR(50),
    revoluciones_rpm NUMERIC,
    temp_c NUMERIC
);

CREATE TABLE IF NOT EXISTS raw_records (
    timestamp_lectura TEXT,
    id_maquina TEXT,
    revoluciones_rpm TEXT,
    temp_c TEXT,
    op_id TEXT,
    nombre_operador TEXT,
    rut_op TEXT
);

-- =========================================================================
-- 3. CAPA DE CONSUMO / DATA MART (Ejecutar en la Base de Datos Gold)
-- Tabla optimizada para lectura directa desde el Frontend (Streamlit)
-- =========================================================================

CREATE TABLE IF NOT EXISTS dashboard_tiempo_real (
    timestamp_lectura TIMESTAMP,
    id_maquina VARCHAR(50),
    delta_temp FLOAT(4),
    delta_vibracion FLOAT(4),
    delta_corriente FLOAT(4),
    estado_maquina VARCHAR(30),
    probabilidad_falla_pct VARCHAR(10)
);