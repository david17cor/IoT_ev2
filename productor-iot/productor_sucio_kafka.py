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

# 2. Configurar el Productor de Kafka apuntando a la red interna
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 5, 0), # Evita que se quede colgado buscando la versión
        request_timeout_ms=5000, # Si en 5 segundos no conecta, falla
        max_block_ms=5000
    )
    print("✅ Conectado a Kafka exitosamente.", flush=True)
except Exception as e:
    print(f"❌ ERROR CRÍTICO al conectar con Kafka: {e}", flush=True)
    sys.exit(1) # Forzamos a que el contenedor se caiga y se reinicie

TOPIC_NAME = 'telemetria_sucia'

# Pool de operadores chilenos con datos realistas pero "sucios" (espacios y mayúsculas)
# Esto simula un abanico completo de trabajadores para la planta
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
    
    print(f"🚀 Iniciando Ingesta IoT en Vivo hacia Kafka en: {KAFKA_BROKER}...")
    print("✨ Rotación aleatoria de 10 operadores y 10 máquinas activada.")
    print("------------------------------------------------------------")
    
    while True: # Bucle infinito para la demo
        maquina = random.choice(maquinas)
        
        # Selección aleatoria del operador en cada iteración (cambio de turno/evento)
        op_elegido = random.choice(OPERADORES_POOL)
        
        # Generar datos base de telemetría
        rpm = round(random.uniform(1450, 1550), 2)
        temp = round(random.uniform(60, 80), 2)
        
        # 1. Error de formato DataOps (30% de probabilidad: coma en vez de punto)
        if random.random() < 0.3:
            rpm = str(rpm).replace(".", ",")
            
        # 2. Error semántico DataOps (20% de probabilidad: Temperatura fuera de rango lógico)
        if random.random() < 0.2:
            temp = -999
            
        # Construcción del Payload Sucio usando los datos dinámicos del operador
        payload_sucio = {
            "timestamp_lectura": datetime.now().strftime("%d/%m/%Y %H:%M:%S"), # Formato no-estándar
            "ID_Maquina": maquina.lower(), 
            "Revoluciones_RPM": rpm,
            "Temp_C": temp,
            "op_id": op_elegido["id"],
            "Nombre_Operador": op_elegido["nombre"], # Ya incluye los espacios sucios de la lista
            "rut_op": op_elegido["rut"]
        }
    
        try:
            # 1. Enviar a Kafka y OBLIGAR a que confirme recepción (Síncrono)
            producer.send(TOPIC_NAME, value=payload_sucio).get(timeout=5)
            
            # 2. Imprimir con flush=True para que no se quede atrapado en memoria
            print(f"🔴 ENVIADO SUCIO -> Maquina: {payload_sucio['ID_Maquina']} | Op: {payload_sucio['Nombre_Operador'].strip()} | RPM: {payload_sucio['Revoluciones_RPM']} | Temp: {payload_sucio['Temp_C']}", flush=True)
            
        except Exception as e:
            # Si Kafka no responde en 5 segundos, nos avisará en lugar de congelarse
            print(f"⚠️ Alerta: Fallo al enviar mensaje a Kafka: {e}", flush=True)
            
        time.sleep(0.01) # Espera 0.01 segundos entre envíos

if __name__ == "__main__":
    generar_y_enviar_en_vivo()