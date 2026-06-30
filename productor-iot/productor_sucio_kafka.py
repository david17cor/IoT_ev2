import os
import json
import random
import time
import redis
from datetime import datetime
from kafka import KafkaProducer

print("Productor IoT V3.0 (Integracion Redis y Estado INACTIVO) iniciado...", flush=True)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    redis_client.ping()
    print("Conexion exitosa con Redis.", flush=True)
except Exception as e:
    print(f"Error conectando a Redis: {e}", flush=True)

TOPIC_NAME = 'telemetria_sucia'
NUM_MAQUINAS = 25
OPERADORES = [{"id": f"OP-00{i}", "nombre": f" Operador {i} ", "rut": f"1{i}.333.444-K"} for i in range(1, NUM_MAQUINAS + 1)]

def iniciar_estado_maquinas():
    estado = {}
    for i in range(1, NUM_MAQUINAS + 1):
        id_maquina = f"maq-cnc-{i:02d}"
        estado[id_maquina] = {
            "rpm": random.uniform(1420, 1480),
            "desgaste_interno": random.uniform(0, 30),
            "tiempo_critico_inicio": None,
            "inactivo": False
        }
    return estado

def generar_datos_streaming():
    maquinas = iniciar_estado_maquinas()
    print(f"Emulando planta industrial de {NUM_MAQUINAS} maquinas CNC...", flush=True)
    
    while True:
        # Revisar señales de mantenimiento desde Redis antes del ciclo fisico
        for id_maquina in maquinas.keys():
            clave_redis = f"reparacion:{id_maquina}"
            if redis_client.get(clave_redis):
                # Aplicar mantenimiento
                maquinas[id_maquina]["desgaste_interno"] = random.uniform(0, 5)
                maquinas[id_maquina]["tiempo_critico_inicio"] = None
                maquinas[id_maquina]["inactivo"] = False
                redis_client.delete(clave_redis)
                print(f"MANTENIMIENTO APLICADO EN {id_maquina}. Valores restablecidos.", flush=True)

        for id_maquina, estado in maquinas.items():
            
            # Si la maquina esta inactiva, se saltan las fisicas de desgaste
            if estado["inactivo"]:
                rpm_str = "0.0"
                vibr_final = 0.0
                temp_final = 25.0 # Temperatura ambiente
                corriente = 0.0
                es_falla = False
            else:
                # 1. Inercia de RPM
                estado["rpm"] += random.uniform(-5.0, 8.0)
                estado["rpm"] = max(1400.0, min(estado["rpm"], 1750.0))
                
                # 2. Desgaste progresivo con tope en 95
                estado["desgaste_interno"] += random.uniform(0.15, 0.40)
                estado["desgaste_interno"] = min(estado["desgaste_interno"], 95.0)
                desgaste = estado["desgaste_interno"]
                
                if desgaste < 40: factor_desgaste = 0.10
                elif desgaste < 70: factor_desgaste = 0.15
                else: factor_desgaste = 0.35
                    
                # Logica del Estado Inactivo (4 minutos en 85 de desgaste)
                if desgaste >= 95.0:
                    if estado["tiempo_critico_inicio"] is None:
                        estado["tiempo_critico_inicio"] = time.time()
                        print(f"ADVERTENCIA: {id_maquina} alcanzo limite critico. Iniciando cuenta regresiva de 3 minutos.", flush=True)
                    elif (time.time() - estado["tiempo_critico_inicio"]) >= 240:
                        estado["inactivo"] = True
                        print(f"APAGADO AUTOMATICO: {id_maquina} paso a estado INACTIVO por seguridad.", flush=True)
                else:
                    estado["tiempo_critico_inicio"] = None

                # 3. Fisica Multivariable Observada
                rpm = estado["rpm"]
                vibracion = 2.5 + ((rpm - 1400) * 0.005) + (desgaste * (factor_desgaste * 0.7)) + random.uniform(-0.2, 0.2)
                temp = 60.0 + (vibracion * 1.5) + random.uniform(-0.5, 0.5)
                corriente = 7.5 + (vibracion * 0.3) + (desgaste * 0.05) + random.uniform(-0.2, 0.2)
                
                # 4. Falla Probabilistica (Removida regeneracion automatica)
                prob_falla = 0.0
                if desgaste > 65: prob_falla += 0.01
                if vibracion > 14: prob_falla += 0.015
                if temp > 80: prob_falla += 0.015
                    
                es_falla = False
                if prob_falla > 0 and random.random() < prob_falla:
                    es_falla = True
                    
                # 5. Anomalias de Sensores
                if random.random() < 0.005:
                    temp_final = random.choice([160, -10.0])
                    vibr_final = random.choice([50.0, -5.0])
                else:
                    temp_final = temp
                    vibr_final = vibracion
                    
                # 6. Suciedad de Datos
                rpm_str = str(round(rpm, 2))
                if random.random() < 0.1: 
                    rpm_str = rpm_str.replace(".", ",")
                    
            op = random.choice(OPERADORES)
            
            # 7. Empaquetado
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
                "colapso_fisico_real": es_falla,
                "maquina_inactiva": estado["inactivo"]
            }
            
            producer.send(TOPIC_NAME, value=payload)
        
        time.sleep(1.5)

if __name__ == "__main__":
    generar_datos_streaming()