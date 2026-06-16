import os
import json
import random
import sys
import time
from datetime import datetime
from kafka import KafkaProducer

print("Productor de Entrenamiento Multi-Falla iniciado...", flush=True)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'telemetria_sucia'
OPERADORES = [{"id": f"OP-00{i}", "nombre": f" Operador {i} ", "rut": f"1{i}.333.444-K"} for i in range(1, 10)]

def generar_datos_entrenamiento():
    print("Generando dataset balanceado (Normal, Temp Alta, RPM Alta)...")
    while True:
        maquina = f"maq-cnc-{random.randint(1,50):02d}"
        op = random.choice(OPERADORES)
        
        # Tirar los dados para elegir el escenario (0 a 100)
        probabilidad = random.randint(1, 100)
        
        if probabilidad <= 70:
            # 70% NORMAL
            estado_real = 0
            rpm = round(random.uniform(1400, 1510), 2)
            temp = round(random.uniform(60, 75), 2)
            tipo = "NORMAL"
            
        elif probabilidad <= 80:
            # 10% FALLA: Solo Temperatura (Recalentamiento)
            estado_real = 1
            rpm = round(random.uniform(1400, 1510), 2) # Normal
            temp = round(random.uniform(85, 105), 2)   # ALTA
            tipo = "TEMP ALTA"
            
        elif probabilidad <= 90:
            # 10% FALLA: Solo Vibración (RPM Altas)
            estado_real = 1
            rpm = round(random.uniform(1580, 1700), 2) # ALTA
            temp = round(random.uniform(60, 75), 2)    # Normal
            tipo = "RPM ALTA"
            
        else:
            # 10% FALLA: Catastrófica (Ambas)
            estado_real = 1
            rpm = round(random.uniform(1580, 1700), 2) # ALTA
            temp = round(random.uniform(85, 105), 2)   # ALTA
            tipo = "CATASTRÓFICA"

        # Añadir un poco de ruido de formato a veces
        if random.random() < 0.1: rpm = str(rpm).replace(".", ",")

        payload = {
            "timestamp_lectura": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "ID_Maquina": maquina, "Revoluciones_RPM": rpm, "Temp_C": temp,
            "op_id": op["id"], "Nombre_Operador": op["nombre"], "rut_op": op["rut"],
            "estado_real": estado_real
        }
    
        producer.send(TOPIC_NAME, value=payload)
        print(f"{tipo} -> RPM: {rpm} | Temp: {temp} | Falla: {estado_real}", flush=True)
        time.sleep(0.01)

if __name__ == "__main__":
    generar_datos_entrenamiento()