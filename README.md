# 🏭 Centro de Control DataOps - Pipeline IoT en Tiempo Real

![Versión](https://img.shields.io/badge/Versi%C3%B3n-1.2-blue)
![Python](https://img.shields.io/badge/Python-3.9+-yellow)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-Streaming-orange)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Broker-black)
![FastAPI](https://img.shields.io/badge/FastAPI-REST-green)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)

Este proyecto implementa una arquitectura de datos **End-to-End** robusta para la ingesta, procesamiento continuo, limpieza y anonimización en tiempo real de telemetría industrial (IoT) proveniente de maquinaria CNC distribuida. 

El núcleo del proyecto se enfoca en aplicar principios avanzados de **DataOps** para transformar flujos de datos crudos e inestables en registros listos para la analítica de negocio (*Golden Records*), asegurando gobernanza de datos y el cumplimiento estricto de la legislación chilena sobre protección de la privacidad.

---

## 🏗️ Arquitectura de la Solución

El pipeline está diseñado bajo una arquitectura de microservicios completamente dockerizados que operan de forma síncrona:

1. **Productor IoT (Simulador):** Emula los sensores físicos de las máquinas CNC, inyectando ráfagas continuas de datos.
2. **Apache Kafka (KRaft Mode):** Actúa como la capa de mensajería intermedia distribuida de alta disponibilidad, absorbiendo la presión de la ingesta masiva sin pérdida de mensajes.
3. **Apache Spark Streaming:** Motor analítico que procesa la data en caliente mediante micro-lotes, ejecutando transformaciones tipográficas, lógicas de negocio y seguridad.
4. **PostgreSQL:** Repositorio transaccional que almacena exclusivamente los datos limpios y autorizados.
5. **DataOps Pipelines API (FastAPI):** Capa intermedia expuesta en el puerto `8000` con documentación interactiva Swagger/OAS 3.1. Ofrece endpoints como `/api/telemetria` para disponibilizar el estado operativo de forma segura y estandarizada.
6. **Centro de Control (Streamlit):** Frontend reactivo (puerto `8501`) diseñado bajo un enfoque corporativo oscuro para el monitoreo visual inmediato del rendimiento del pipeline.

---

## 📊 Estrategia de Calidad de Datos & Gobernanza (DataOps)

El dashboard expone una separación clara en paralelo del flujo de información para validar las transformaciones operadas por el pipeline:

### 🛑 1. Bloque IZQUIERDO: Datos Crudos de la Planta
Representa el estado original del sensor antes de ser procesado. Presenta las siguientes deficiencias simuladas de la realidad industrial:
* **Anomalías Críticas de Hardware:** Lecturas fuera de rango o erráticas de temperatura marcando un valor centinela de `-999.0 °C`.
* **Inconsistencia de Tipos:** Valores de `RPM` formateados incorrectamente como cadenas de texto usando comas en vez de puntos decimales estándar (`1465,68`).
* **Exposición de Datos Sensibles:** Datos del personal de planta expuestos sin ningún tipo de resguardo ni estructura.

### ❇️ 2. Bloque DERECHO: Datos Curados y Seguros
Muestra el resultado final tras el paso de los datos por las reglas del pipeline de Spark, garantizando calidad técnica y cumplimiento normativo:
* **Filtro de Anomalías Térmicas:** Remoción y descarte inmediato de registros corruptos (`-999.0 °C`) para no alterar los modelos predictivos de mantenimiento.
* **Estandarización Numérica:** Formateo y tipado correcto de la métrica `RPM`.
* **Privacidad por Diseño (Cumplimiento Ley 19.628 de Chile):**
  * **Estructuración:** Extracción de identificadores limpios del operador (`op_id`).
  * **Anonimización Irreversible:** Aplicación de algoritmo **Hashing SHA-256** sobre la identidad del operario.
  * **Enmascaramiento Dinámico (Masking):** Ocultamiento parcial de la cédula de identidad en la columna `rut_op` (ej: `XX.XXX.XX8-5`), protegiendo los datos personales sin romper la trazabilidad lógica de las auditorías.

---

## 🚀 Historial de Evolución (Changelog)

### 📌 Versión 1.1 - *Infraestructura Base*
* Conexión exitosa del flujo streaming completo entre Kafka, Spark y PostgreSQL.
* Despliegue de la API REST intermedia con FastAPI exponiendo el JSON de telemetría.
* Creación del Frontend inicial en Streamlit con diseño de pantalla dividida, sufriendo de volatilidad en los contadores métricos en cada ciclo de actualización.

### ✨ Versión 1.2 - *Observabilidad, Estado y Rendimiento (Versión Actual)*
Esta entrega introduce mejoras de fondo tanto en la estabilidad del procesamiento como en la experiencia interactiva del usuario:
* **🔧 Mitigación de Asfixia de Hilos en Spark (*Thread Starvation*):** Se corrigió un bug crítico en la lógica de Spark Streaming donde llamadas ansiosas reiteradas (`.isEmpty()`) bloqueaban el pool de conexiones. Se reestructuró el flujo implementando `.cache()` y persistencia optimizada, logrando un caudal constante de **20 Eventos/s** sin degradación de memoria.
* **🧠 Persistencia de Métricas Operativas:** Integración de `st.session_state` en el frontend corporativo. Las tarjetas superiores ahora acumulan de forma permanente el histórico global de registros (`Total Crudos Recibidos`, `Procesados con Éxito`, `Total Descartados`) sin resetearse a cero durante los refrescos continuos de pantalla.
* **🎲 Simulación de Estrés Realista:** Se elevó el abanico probabilístico de fallas a un rango dinámico del **20% al 30%** empleando selección aleatoria de índices, permitiendo observar fluctuaciones orgánicas y realistas en la tasa de descartes del centro de control.
* **⚡ Animación de Parpadeo Interactiva (Blinking Deltas):** Mediante la inyección de estilos CSS avanzados (`@keyframes latido`), las etiquetas delta de incremento (`+20 nuevos`, `+18 ok`, `+2 anomalías`) ejecutan un micropulso visual con cada actualización de lote cada 2 segundos, incrementando radicalmente la experiencia interactiva de monitoreo en vivo.

---

## 🛠️ Instrucciones de Despliegue Rápido

Para replicar el entorno completo de forma local, ejecuta los siguientes comandos en tu terminal:

1. Clonar el repositorio y acceder al directorio:
   ```bash
   git clone [https://github.com/david17cor/IoT_ev2.git](https://github.com/david17cor/IoT_ev2.git)
   cd IoT_ev2

2. Construir y levantar la orquestación de contenedores en segundo plano:
   ```bash
   docker-compose up -d --build

3. Puntos de Acceso Locales:
   
   - Dashboard de Monitoreo (Streamlit): http://localhost:8501

   - Documentación Interactiva de la API (Swagger UI): http://localhost:8000/docs

---

## 🗺️ Roadmap y Próximos Pasos
El desarrollo de este pipeline es de mejora continua. Las próximas iteraciones apuntan a la escalabilidad en la nube pública:

☁️ Migración a Cloud (Google Cloud Platform): Despliegue de la arquitectura dockerizada completa en una máquina virtual de Compute Engine para simular un entorno de producción accesible globalmente y sin dependencia de hardware local.

🔒 Hardening de Seguridad: Implementación de variables de entorno cifradas y gestión de accesos (IAM) para la base de datos PostgreSQL en el entorno cloud.

📈 Alertas Automatizadas: Integración de notificaciones vía webhook (Ej: Slack/Teams) cuando la tasa de anomalías térmicas supere el 30% en una ventana de tiempo de 5 minutos.
