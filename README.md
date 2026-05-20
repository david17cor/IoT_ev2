# 📡 IoT_ev2
## Proyecto IoT para la asignatura Gestión de Datos para IA — Evaluación N°2

---

# 🌐 Descripción General

Este proyecto implementa una arquitectura IoT orientada a procesamiento de datos en tiempo real, integración DataOps y visualización analítica.

El sistema simula sensores industriales capaces de emitir datos continuamente hacia una plataforma distribuida basada en Apache Kafka, donde posteriormente son procesados, limpiados, validados y almacenados en PostgreSQL para su consumo mediante servicios REST y dashboards interactivos.

---

# 🏗️ Arquitectura del Sistema

```text
┌──────────────────────┐
│   IoT Producer       │
│  Datos simulados     │
└─────────┬────────────┘
          │
          ▼
┌──────────────────────┐
│    Apache Kafka      │
│ Streaming de eventos │
└─────────┬────────────┘
          │
          ▼
┌──────────────────────┐
│    Data Pipeline     │
│ Limpieza y validación│
└─────────┬────────────┘
          │
          ▼
┌──────────────────────┐
│     PostgreSQL       │
│ Persistencia datos   │
└─────────┬────────────┘
          │
          ▼
┌──────────────────────┐
│      FastAPI API     │
│ Servicios Backend    │
└─────────┬────────────┘
          │
          ▼
┌──────────────────────┐
│   Streamlit Frontend │
│ Dashboard analítico  │
└──────────────────────┘
```

---

# 🚀 Requisitos Previos

Antes de ejecutar el proyecto, asegúrate de tener instalado:

- Docker
- Docker Compose
- Python 3.11 o superior
- Git (opcional)

---

# 📁 Configuración Inicial

## 🔐 Archivo .env

Crear un archivo `.env` en la raíz del proyecto:

```env
POSTGRES_DB=iot_db
POSTGRES_USER=admin
POSTGRES_PASSWORD=123456
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

---

# ⚙️ Instrucciones de Despliegue

---

# 1️⃣ Levantar la Infraestructura

Desde la raíz del proyecto, inicializa Kafka, Zookeeper y PostgreSQL:

```bash
docker compose up -d
```

---

# 2️⃣ Instalar Dependencias

Se recomienda utilizar un entorno virtual.

```bash
pip install -r requirements.txt
```

---

# 3️⃣ Ejecución del Pipeline Completo

Para visualizar el flujo completo en tiempo real, abrir 4 terminales distintas en la raíz del proyecto.

---

## 🖥️ Terminal 1 — Iniciar Productor IoT

Emisión de datos simulados hacia Kafka.

```bash
cd iot_producer
python productor_sucio_kafka.py
```

---

## 🧹 Terminal 2 — Iniciar Pipeline DataOps

Consumidor encargado de limpieza y validación de datos.

```bash
cd data_pipeline
python kafka_consumer_limpio.py
```

---

## ⚡ Terminal 3 — Iniciar Backend REST

Levantamiento de API mediante FastAPI.

```bash
cd backend_api
uvicorn api:app --reload
```

---

## 📊 Terminal 4 — Iniciar Dashboard

Ejecución del frontend analítico con Streamlit.

```bash
cd frontend_app
streamlit run app_frontend.py
```

---

# 🔄 Flujo de Datos

```text
Sensores IoT
     │
     ▼
Kafka Producer
     │
     ▼
Apache Kafka
     │
     ▼
Pipeline de Limpieza
     │
     ▼
PostgreSQL
     │
     ▼
FastAPI
     │
     ▼
Dashboard Streamlit
```

---

# 🛡️ Calidad y Seguridad de Datos

El sistema incorpora distintos mecanismos para asegurar integridad, confiabilidad y protección de información sensible.

---

## 📅 Estandarización

- Conversión automática de fechas a formato ISO 8601.
- Normalización de tipografía y formatos.
- Homogeneización estructural de registros.

---

## 🔍 Validación de Tipos

- Conversión automática de separadores decimales.
- Validación de tipos numéricos.
- Manejo de datos incompletos o corruptos.

Ejemplo:

```text
1,5 → 1.5
```

---

## 🚨 Filtros de Umbral

El pipeline descarta automáticamente lecturas anómalas:

- Temperaturas imposibles.
- Valores fuera de rango.
- Posibles errores de hardware.
- Datos inconsistentes.

---

## 🔐 Anonimización

Aplicación de funciones criptográficas unidireccionales sobre datos sensibles utilizando:

```text
SHA-256
```

---

# 📦 Tecnologías Utilizadas

| Tecnología | Función |
|---|---|
| Python | Desarrollo principal |
| Apache Kafka | Streaming de datos |
| PostgreSQL | Persistencia |
| FastAPI | Backend REST |
| Streamlit | Dashboard |
| Docker | Contenedores |
| Docker Compose | Orquestación |
| SHA-256 | Anonimización |

---

# 📈 Características del Proyecto

✅ Arquitectura desacoplada  
✅ Procesamiento en tiempo real  
✅ Pipeline DataOps  
✅ Validación automática  
✅ Persistencia de datos  
✅ API REST moderna  
✅ Dashboard interactivo  
✅ Integración modular  
✅ Escalable y mantenible  

---

# 🧠 Posibles Mejoras Futuras

- Integración de modelos de Machine Learning.
- Alertas en tiempo real.
- Integración con Grafana.
- Monitoreo distribuido.
- Despliegue en Kubernetes.
- CI/CD automatizado.
- Predicción de fallas industriales.

---

# 👨‍💻 Autor

Proyecto desarrollado para la asignatura:

## Gestión de Datos para Inteligencia Artificial

---

# 📌 Observaciones

El sistema fue diseñado con enfoque educativo y práctico, simulando un entorno real de procesamiento de datos IoT basado en arquitecturas modernas de streaming, microservicios y DataOps.
