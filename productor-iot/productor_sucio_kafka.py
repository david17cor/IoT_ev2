import os
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

print("🚀 Productor IoT V2.0 (Física Stateful y Degradación Continua) iniciado...", flush=True)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'telemetria_sucia'
NUM_MAQUINAS = 50
OPERADORES = [{"id": f"OP-00{i}", "nombre": f" Operador {i} ", "rut": f"1{i}.333.444-K"} for i in range(1, 51)]

def iniciar_estado_maquinas():
    """Crea la memoria interna de la planta: inercia inicial y desgaste para las 50 máquinas"""
    estado = {}
    for i in range(1, NUM_MAQUINAS + 1):
        id_maquina = f"maq-cnc-{i:02d}"
        estado[id_maquina] = {
            "rpm": random.uniform(1420, 1480),
            "desgaste_interno": random.uniform(0, 30)
        }
    return estado

def generar_datos_streaming():
    # Inicializamos la "vida" de las máquinas en la RAM
    maquinas = iniciar_estado_maquinas()
    print("Emulando planta industrial de 50 máquinas CNC...", flush=True)
    
    while True:
        # En cada ciclo, hacemos avanzar el tiempo y la física para TODAS las máquinas
        for id_maquina, estado in maquinas.items():
            
            # 1. Inercia de RPM
            estado["rpm"] += random.uniform(-5.0, 5.0)
            estado["rpm"] = max(1400.0, min(estado["rpm"], 1500.0))
            
            # 2. Desgaste no lineal progresivo
            estado["desgaste_interno"] += random.uniform(0.10, 0.35)
            desgaste = estado["desgaste_interno"]
            
            if desgaste < 40: factor_desgaste = 0.10
            elif desgaste < 70: factor_desgaste = 0.15
            else: factor_desgaste = 0.25
                
            # 3. Física Multivariable Observada
            rpm = estado["rpm"]
            vibracion = 5.0 + ((rpm - 1400) * 0.01) + (desgaste * factor_desgaste) + random.uniform(-0.5, 0.5)
            temp = 55.0 + (vibracion * 1.5) + random.uniform(-1.0, 1.0)
            corriente = 8.0 + (vibracion * 0.3) + (desgaste * 0.05) + random.uniform(-0.2, 0.2)
            
            # 4. Falla Probabilística (El Colapso Real)
            prob_falla = 0.0
            if desgaste > 65: prob_falla += 0.01
            if vibracion > 14: prob_falla += 0.015
            if temp > 80: prob_falla += 0.015
                
            es_falla = False
            if prob_falla > 0 and random.random() < prob_falla:
                es_falla = True
                # Simulamos que los mecánicos reparan la máquina
                estado["desgaste_interno"] = random.uniform(0, 5)
                print(f"💥 ¡MANTENIMIENTO CORRECTIVO EN {id_maquina}! (Desgaste reiniciado)", flush=True)
                
            # 5. Anomalías de Sensores (Ruido)
            if random.random() < 0.005:
                temp_final = random.choice([200.0, 0.0])
                vibr_final = random.choice([50.0, 0.0])
            else:
                temp_final = temp
                vibr_final = vibracion
                
            # 6. Suciedad de Datos (Para que Spark limpie)
            rpm_str = round(rpm, 2)
            if random.random() < 0.1: 
                rpm_str = str(rpm_str).replace(".", ",")
                
            op = random.choice(OPERADORES)
            
            # 7. Empaquetado y envío
            payload = {
                "timestamp_lectura": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "id_maquina": id_maquina,
                "rpm": rpm_str,
                "vibracion_mms": round(vibr_final, 2),
                "temp_c": round(temp_final, 2),
                "corriente_motor_a": round(corriente, 2),
                "op_id": op["id"],
                "Nombre_Operador": op["nombre"],
                "rut_op": op["rut"],
                # Flag para el dashboard visual, el modelo Spark la ignorará
                "colapso_fisico_real": es_falla 
            }
            
            producer.send(TOPIC_NAME, value=payload)
        
        # Pausamos el script 1 segundo antes del próximo ciclo de la planta completa
        time.sleep(1)

if __name__ == "__main__":
    generar_datos_streaming()