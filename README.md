# 🏭 Centro de Control DataOps - Pipeline IoT en Tiempo Real (GCP Cloud Edition)

![Versión](https://img.shields.io/badge/Versi%C3%B3n-1.2--GCP-blue?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3.9+-yellow?style=for-the-badge)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-Streaming-orange?style=for-the-badge)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-KRaft-black?style=for-the-badge)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Bronze%20%26%20Gold-blue?style=for-the-badge)
![FastAPI](https://img.shields.io/badge/FastAPI-REST_API-green?style=for-the-badge)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red?style=for-the-badge)

Este proyecto implementa una arquitectura de datos **End-to-End** distribuida, resiliente y de alta disponibilidad para la ingesta, persistencia inmutable, procesamiento analítico continuo, limpieza y anonimización en tiempo real de telemetría industrial (IoT) proveniente de 10 máquinas CNC simuladas operadas de forma rotativa.

El núcleo del ecosistema se rige bajo principios avanzados de **DataOps**, operando con éxito dentro de una infraestructura robusta contenerizada en la nube pública (**Google Cloud Platform**). El diseño aplica una **Arquitectura Medallón** física para transformar flujos de datos crudos e inestables en registros listos para la analítica de negocio (*Golden Records*), asegurando la gobernanza de datos y el cumplimiento estricto de la legislación chilena (Ley 19.628) sobre protección de la privacidad.

---

## 🏗️ Arquitectura de la Solución e Infrachester en la Nube

El ecosistema completo opera de forma contenerizada dentro de una máquina virtual **Compute Engine (`e2-standard-2`: 2 vCPUs, 8 GB de RAM)** en Google Cloud Platform. Los servicios interactúan de forma aislada dentro de la red interna de Docker (`dataops_network`), aplicando el principio de mínimo privilegio para mitigar la superficie de ataques en la nube pública.

### 🗺️ Mapa de Arquitectura y Topología de Red

<img width="691" height="1021" alt="diagramadefinitivoo" src="https://github.com/user-attachments/assets/982889ed-4449-49dc-964d-be7691663c00" />


---

### 📦 Componentes del Ecosistema Dockerizado

* **Productor IoT (`productor-iot`):** Módulo en Python que simula de manera estocástica la telemetría industrial de 10 estaciones CNC (`MAQ-CNC-01` a `MAQ-CNC-10`) y 15 operadores rotativos. Inyecta intencionalmente ruido estructural, descalibraciones de sensor y datos sensibles sin procesar.
* **Apache Kafka KRaft (`kafka`):** Broker de mensajería asíncrona de alto rendimiento que unifica la gestión de metadatos y almacenamiento en un único proceso. **Elimina por completo la necesidad de ZooKeeper**, reduciendo el consumo de hardware en la VM y garantizando latencias de absorción de eventos inferiores a los 10 milisegundos en el topic `telemetria_sucia`.
* **Almacenamiento Capa Bronze (`postgres-bronze` - Puerto 5433):** Instancia relacional PostgreSQL inmutable. Recibe una copia exacta de los eventos JSON directamente desde Kafka, actuando como un repositorio histórico de auditoría cruda con el ruido y la información personal intacta.
* **Procesamiento de Eventos (`spark-streaming`):** Motor de cálculo distribuido ejecutado de forma silenciosa dentro de la red privada de Docker. Consume el stream original de Kafka, aplica las reglas de *Data Quality*, las directivas de ciberseguridad de la Ley 19.628, y distribuye las cargas. **No expone puertos web al exterior**, reduciendo drásticamente la superficie de vulnerabilidad de la máquina virtual.
* **Almacenamiento Capa Gold (`postgres-gold` - Puerto 5435):** Instancia transaccional PostgreSQL limpia y optimizada. Almacena única y exclusivamente los registros depurados que han superado con éxito todos los controles de calidad de la organización.
* **DataOps Pipelines API (`fastapi-api` - Puerto 8000):** Capa de abstracción y servicios web expuesta de forma segura. Interactúa exclusivamente con la Capa Gold, proporcionando endpoints JSON estandarizados bajo la especificación OpenAPI (Swagger UI) para su consumo externo controlado.
* **Centro de Control Analítico (`streamlit-dashboard` - Puerto 8501):** Interfaz gráfica de usuario reactiva, diseñada bajo estándares corporativos oscuros. Consume los datos de la API para visualizar el estado de la planta "al vuelo", alertar sobre eventos de `falla_inminente` y contrastar las métricas absolutas de la arquitectura Medallón.

---

## 🛠️ Principios de Calidad, Gobernanza y Privacidad (DataOps)

El pipeline valida y transforma la información segmentando los flujos a través del Centro de Control para auditar la correcta aplicación de las reglas analíticas:

### 🛑 Capa Bronze: Datos Crudos de la Planta (Espejo de Ingesta)
Representa la entrada inalterada de los sensores periféricos (*Edge*), visibilizando las deficiencias del entorno industrial:
* **Anomalías Críticas de Hardware:** Captura ráfagas de error donde un sensor térmico dañado registra lecturas centinela absurdas de `-999.0 °C`.
* **Inconsistencia Estructural:** Captura valores de velocidad angular (`RPM`) formateados erróneamente como cadenas de texto (`String`) con comas decimales (`1465,68`), lo que corrompe los modelos analíticos basados en floats.
* **Exposición de Datos de Personal:** Exposición en texto plano del Nombre del operador y su Cédula de Identidad (RUT).

### ❇️ Capa Gold: Datos Curados y Seguros (Golden Records)
Muestra el resultado tras el procesamiento por micro-lotes en Apache Spark Streaming, garantizando estabilidad analítica y cumplimiento legal:
* **Aislamiento de Anomalías Térmicas:** Detección de ráfagas aberrantes (`-999.0 °C`). El sistema las aísla de la base Gold para no sesgar las métricas de mantenimiento predictivo, permitiendo contabilizar el volumen exacto de descartes semánticos.
* **Estandarización y Tipado Numérico:** Conversión en tránsito de cadenas de texto a formato de punto flotante estándar (`Float`) sustituyendo comas por puntos en las variables de RPM.
* **Privacidad por Diseño (Cumplimiento de la Ley Chilena 19.628):**
    * **Seudonimización Irreversible:** Aplicación de un algoritmo de hash criptográfico de una sola vía **SHA-256** sobre los nombres de los operadores, transformándolos en firmas digitales fijas de 64 caracteres útiles para la correlación de modelos predictivos sin revelar la identidad real del personal.
    * **Enmascaramiento Parcial (Data Masking):** Ocultamiento destructivo de los caracteres iniciales del RUT (ej. transformando `19.384.725-K` en `*.*.XX8-5`) para resguardar la privacidad en la visualización pública del tablero sin romper la trazabilidad de auditorías en planta.

---

## ⚙️ Guía de Instalación y Despliegue del Pipeline

Sigue paso a paso estas instrucciones para clonar, aprovisionar y levantar el ecosistema completo en una máquina virtual de Google Cloud Platform (o entorno local compatible con Docker):

### 📋 1. Requisitos Previos del Sistema
Asegúrate de contar con los siguientes componentes instalados en tu sistema operativo:
* Git instalado (`git --version`)
* Docker Engine versión 20.10+ (`docker --version`)
* Docker Compose v2 instalado (`docker compose version`)

### 🔑 2. Configuración de Reglas de Cortafuegos en GCP (Firewall Inbound)
Si despliegas el pipeline en una Máquina Virtual de Google Cloud, antes de ejecutar los contenedores debes permitir el tráfico web externo hacia los servicios expuestos. Ve a **GCP -> Red de VPC -> Cortafuegos** y crea una regla de entrada (*Inbound*) con los siguientes parámetros:

* **Filtros de origen:** `0.0.0.0/0` (o la IP específica de tu red de confianza).
* **Protocolos y puertos especificados:** Selecciona TCP y abre los puertos de explotación:
    * `5433` (Acceso externo restringido a PostgreSQL Bronze)
    * `5435` (Acceso externo a PostgreSQL Gold)
    * `8000` (Acceso HTTP a la API REST de FastAPI)
    * `8501` (Acceso HTTP al Centro de Control de Streamlit)

> ⚠️ *Nota de Ciberseguridad: El puerto `29092` (Kafka) y el procesamiento de Spark operan de forma interna dentro de la red del Docker Host y no deben abrirse en el Firewall de GCP.*

### 🚀 3. Clonación del Repositorio y Despliegue de Contenedores
Abre una terminal SSH dentro de tu máquina virtual y ejecuta los siguientes comandos:

```bash
# 1. Clonar el repositorio oficial del proyecto
git clone [https://github.com/david17cor/IoT_ev2.git](https://github.com/david17cor/IoT_ev2.git)

# 2. Acceder al directorio raíz de la aplicación
cd iot_ev2

### ⚙️ Paso 2.5: Configuración de Variables de Entorno (.env)
Por lineamientos estrictos de ciberseguridad y buenas prácticas de DataOps, el archivo con las credenciales de bases de datos no se incluye en este repositorio. 

# 3. Construir las imágenes personalizadas y levantar los contenedores en segundo plano
docker compose up -d --build


### 🔍 4. Verificación del Estado de la Infraestructura
Para asegurarte de que todos los microservicios se encuentran estables y operando de forma correcta dentro de la red interna de la nube, ejecuta el siguiente comando en la terminal de tu máquina virtual:

```bash
docker ps


📈 Historial de Evolución Técnica (Changelog)
📌 Versión 1.1 - Estructura Base e Ingesta Diferenciada
Conexión inicial del flujo streaming completo entre Kafka, Spark y PostgreSQL de forma local.

Despliegue de la API REST intermedia con FastAPI exponiendo la telemetría curada.

Creación del Frontend inicial en Streamlit con diseño de pantalla dividida, sufriendo de volatilidad en los contadores métricos globales ante actualizaciones automáticas de lote.

✨ Versión 1.2 - GCP Deployment, KRaft Mode, Separación Medallón y Persistencia de Estados (Versión Actual)
Esta entrega consolida la madurez del proyecto mediante la migración a un entorno de nube productivo y la optimización drástica de recursos:

☁️ Migración Completa a Google Cloud Platform: Despliegue exitoso de la arquitectura contenerizada en una instancia productiva de Compute Engine de GCP con apertura segura de puertos mediante reglas de Firewall perimetrales.

🚀 Implementación de Apache Kafka Modo KRaft: Refactorización de la capa de ingesta para operar de forma nativa bajo el protocolo KRaft. Eliminación absoluta de la dependencia de ZooKeeper, logrando un ahorro masivo de uso de vCPU y memoria RAM dentro de la máquina virtual.

🧱 Consolidación de Arquitectura Medallón Multi-Instancia: Separación física del almacenamiento de datos. Se desplegó de manera paralela el repositorio de datos crudos (Capa Bronze en puerto 5433) y el repositorio de datos curados analíticos (Capa Gold en puerto 5435), asegurando la integridad referencial y la gobernanza de datos exigida por los estándares internacionales de DataOps.

🔒 Hardening de Seguridad de Procesamiento: Aislamiento del contenedor de Apache Spark Streaming dentro de la red interna de Docker, removiendo la exposición de sus puertos hacia el exterior de la máquina virtual como estrategia de seguridad activa contra intrusiones.

🔧 Mitigación de Asfixia de Hilos en Spark (Thread Starvation): Corrección analítica en la lógica de procesamiento de Spark Streaming, donde llamadas síncronas reiteradas (.isEmpty()) bloqueaban el pool de conexiones en la VM. Se optimizó el flujo implementando .cache() y persistencia sintonizada, logrando un caudal estable de 20 eventos/s sin degradación de memoria.

🧠 Control de Amnesia de Interfaz (Session State): Integración avanzada de st.session_state en el código de Streamlit. Las tarjetas analíticas acumulan de forma permanente las métricas operacionales (Total Crudos Recibidos, Procesados con Éxito, Total Descartados) sin resetearse a cero durante los refrescos cíclicos del navegador (F5).

🎲 Simulación de Estrés Realista y Blinking Deltas: Elevación del abanico probabilístico de fallas térmicas a un rango dinámico del 20% al 30%, permitiendo observar fluctuaciones orgánicas en la tasa de descartes del centro de control, acompañado de micropulsos visuales CSS (@keyframes latido) que se ejecutan cada 2 segundos con cada actualización de lote.




