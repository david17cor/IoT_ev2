import os
import json
import random
import sys
import time
from datetime import datetime

# 0. Imprimir de inmediato para confirmar que Python arrancó
print("🟢 Contenedor del Productor iniciado. Leyendo librerías...", flush=True)

from kafka import KafkaProducer

# 1. Capturamos el broker desde Docker (o usamos 'kafka:9092' por defecto)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
print(f"⏳ Intentando conectar a Kafka en: {KAFKA_BROKER}...", flush=True)

# 2. Configurar el Productor de Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 5, 0),
        request_timeout_ms=5000,
        max_block_ms=5000
    )
    print("✅ Conectado a Kafka exitosamente.", flush=True)
except Exception as e:
    print(f"❌ ERROR CRÍTICO al conectar con Kafka: {e}", flush=True)
    sys.exit(1)

TOPIC_NAME = 'telemetria_sucia'

OPERADORES_POOL = [
    {"id": "OP-001", "nombre": "   JUAN PEREZ   ", "rut": "15.444.333-2"},
    {"id": "OP-002", "nombre": "  MARIA GONZALEZ  ", "rut": "18.222.111-K"},
    {"id": "OP-003", "nombre": "   CARLOS SOTO   ", "rut": "12.999.888-5"},
    {"id": "OP-004", "nombre": "  ANA MUÑOZ  ", "rut": "16.777.666-4"},
    {"id": "OP-005", "nombre": "   PEDRO SILVA   ", "rut": "14.555.444-3"},
    {"id": "OP-006", "nombre": "  DIEGO TAPIA  ", "rut": "19.111.222-7"},
    {"id": "OP-007", "nombre": "   CLAUDIA DIAZ   ", "rut": "17.333.444-K"},
    {"id": "OP-008", "nombre": "  JOSE FUENTES  ", "rut": "11.666.555-8"},
    {"id": "OP-009", "nombre": "   RODRIGO ARAYA   ", "rut": "13.444.222-1"},
    {"id": "OP-010", "nombre": "  VALENTINA ROJAS  ", "rut": "20.888.999-0"}
]

def generar_y_enviar_en_vivo():
    maquinas = [f"MAQ-CNC-{str(i).zfill(2)}" for i in range(1, 51)]
    
    print(f"🚀 Iniciando Ingesta IoT Orientada a ML hacia Kafka...")
    print("✨ Generando variable objetivo 'estado_real' (0=Sana, 1=Falla)")
    print("------------------------------------------------------------")
    
    while True:
        maquina = random.choice(maquinas)
        op_elegido = random.choice(OPERADORES_POOL)
        
        # --- LÓGICA DE MACHINE LEARNING (Generación de Falla) ---
        # 15% de probabilidad de que la máquina presente una anomalía real (Falla)
        if random.random() < 0.15:
            estado_real = 1
            rpm_base = round(random.uniform(1520, 1600), 2) # RPM elevadas
            temp_base = round(random.uniform(76, 95), 2)    # Temperatura alta
        else:
            estado_real = 0
            rpm_base = round(random.uniform(1400, 1519), 2) # RPM normales
            temp_base = round(random.uniform(60, 75), 2)    # Temperatura normal

        rpm = rpm_base
        temp = temp_base
        
        # --- LÓGICA DE DATA QUALITY (Ruido para limpiar en Spark) ---
        if random.random() < 0.3:
            rpm = str(rpm).replace(".", ",")
            
        if random.random() < 0.2:
            temp = -999
            
        # Payload con nuestra nueva Variable Objetivo
        payload_sucio = {
            "timestamp_lectura": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "ID_Maquina": maquina.lower(), 
            "Revoluciones_RPM": rpm,
            "Temp_C": temp,
            "op_id": op_elegido["id"],
            "Nombre_Operador": op_elegido["nombre"],
            "rut_op": op_elegido["rut"],
            "estado_real": estado_real # <--- NUEVA COLUMNA PARA LA IA
        }
    
        try:
            producer.send(TOPIC_NAME, value=payload_sucio).get(timeout=5)
            # Imprimir un emoji distinto si es falla para verlo fácil en la consola
            icono = "🚨" if estado_real == 1 else "🟢"
            print(f"{icono} ENVIADO -> Maq: {payload_sucio['ID_Maquina']} | RPM: {payload_sucio['Revoluciones_RPM']} | Temp: {payload_sucio['Temp_C']} | Falla: {estado_real}", flush=True)
            
        except Exception as e:
            print(f"⚠️ Alerta: Fallo al enviar mensaje a Kafka: {e}", flush=True)
            
        time.sleep(0.01)

if __name__ == "__main__":
    generar_y_enviar_en_vivo()