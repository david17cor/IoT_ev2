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
-- 2. CAPA ORO (Ejecutar en la Base de Datos Gold - Puerto Local: 5435)
-- Repositorio de negocio consolidado y limpio.
-- =========================================================================

CREATE TABLE IF NOT EXISTS telemetria_limpia (
    timestamp_lectura TIMESTAMP,
    id_maquina TEXT,
    rpm FLOAT(4),
    temperatura FLOAT(4),
    op_id TEXT,
    nombre_operador TEXT,
    rut_op TEXT
);
