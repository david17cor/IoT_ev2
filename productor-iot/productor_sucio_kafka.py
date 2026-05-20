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
    maquinas = [f"MAQ-CNC-{str(i).zfill(2)}" for i in range(1, 11)]
    
    print("🚀 Iniciando Ingesta IoT en Vivo hacia Kafka...")
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
        
        # Enviar a Kafka
        producer.send(TOPIC_NAME, value=payload_sucio)
        
        # Mostrar en consola lo que se está enviando para verificar visualmente la variedad
        print(f"🔴 ENVIADO SUCIO -> Maquina: {payload_sucio['ID_Maquina']} | Op: {payload_sucio['Nombre_Operador'].strip()} | RPM: {payload_sucio['Revoluciones_RPM']} | Temp: {payload_sucio['Temp_C']}")
        
        time.sleep(1.5) # Espera 1.5 segundos entre envíos

if __name__ == "__main__":
    generar_y_enviar_en_vivo()