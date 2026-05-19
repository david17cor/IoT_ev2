import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configurar el Productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'telemetria_sucia'

def generar_y_enviar_en_vivo():
    maquinas = [f"MAQ-CNC-{str(i).zfill(2)}" for i in range(1, 11)]
    operadores = [{"id": "OP-001", "nombre": "Juan Perez", "rut": "15.444.333-2"}] # Simplificado para el ejemplo
    
    print("🚀 Iniciando Ingesta IoT en Vivo hacia Kafka...")
    
    while True: # Bucle infinito para la demo
        maquina = random.choice(maquinas)
        
        # Generar dato sucio
        rpm = round(random.uniform(1450, 1550), 2)
        temp = round(random.uniform(60, 80), 2)
        
        # 1. Error de formato (coma en vez de punto)
        if random.random() < 0.3:
            rpm = str(rpm).replace(".", ",")
            
        # 2. Error semántico (Temperatura imposible)
        if random.random() < 0.2:
            temp = -999
            
        payload_sucio = {
            "timestamp_lectura": datetime.now().strftime("%d/%m/%Y %H:%M:%S"), # Fecha en formato no-estándar
            "ID_Maquina": maquina.lower(), # Minúsculas
            "Revoluciones_RPM": rpm,
            "Temp_C": temp,
            "op_id": operadores[0]["id"],
            "Nombre_Operador": "   " + operadores[0]["nombre"].upper() + "  ", # Espacios y mayúsculas
            "rut_op": operadores[0]["rut"]
        }
        
        # Enviar a Kafka
        producer.send(TOPIC_NAME, value=payload_sucio)
        print(f"🔴 ENVIADO SUCIO -> Maquina: {payload_sucio['ID_Maquina']} | RPM: {payload_sucio['Revoluciones_RPM']} | Temp: {payload_sucio['Temp_C']}")
        
        time.sleep(1.5) # Espera 1.5 segundos entre envíos

if __name__ == "__main__":
    generar_y_enviar_en_vivo()