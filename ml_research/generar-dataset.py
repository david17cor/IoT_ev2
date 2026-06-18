import pandas as pd
import random
from datetime import datetime, timedelta

print("🚀 Generando Dataset Predictivo V2.0 (Inercia, Corriente y Fases)...")

NUM_MAQUINAS = 50
REGISTROS_POR_MAQ = 2000
VENTANA_PREDICCION = 60

datos_totales = []
tiempo_inicial = datetime.now()
fallas_totales = 0

for maq_id in range(1, NUM_MAQUINAS + 1):
    id_maquina = f"maq-cnc-{maq_id:02d}"
    tiempo_actual = tiempo_inicial
    
    desgaste_interno = random.uniform(0, 30) 
    # Inicializamos la inercia
    rpm = random.uniform(1420, 1480)
    
    historial_maquina = []
    
    for i in range(REGISTROS_POR_MAQ):
        tiempo_actual += timedelta(seconds=2)
        
        # 1. Inercia de RPM
        rpm += random.uniform(-5.0, 5.0)
        rpm = max(1400.0, min(rpm, 1500.0))
        
        # 2. Desgaste no lineal
        desgaste_interno += random.uniform(0.10, 0.35)
        
        if desgaste_interno < 40:
            factor_desgaste = 0.10
        elif desgaste_interno < 70:
            factor_desgaste = 0.15
        else:
            factor_desgaste = 0.25
            
        # 3. Física Multivariable
        vibracion = 5.0 + ((rpm - 1400) * 0.01) + (desgaste_interno * factor_desgaste) + random.uniform(-0.5, 0.5)
        temp = 55.0 + (vibracion * 1.5) + random.uniform(-1.0, 1.0)
        corriente = 8.0 + (vibracion * 0.3) + (desgaste_interno * 0.05) + random.uniform(-0.2, 0.2)
        
        # Lógica de falla probabilística
        prob_falla = 0.0
        if desgaste_interno > 65: prob_falla += 0.01
        if vibracion > 14: prob_falla += 0.015
        if temp > 80: prob_falla += 0.015
            
        es_falla = False
        if prob_falla > 0 and random.random() < prob_falla:
            es_falla = True
            fallas_totales += 1
            
        # Ruido
        if random.random() < 0.005:
            temp_guardar = random.choice([200.0, 0.0])
            vibr_guardar = random.choice([50.0, 0.0])
        else:
            temp_guardar = temp
            vibr_guardar = vibracion
            
        historial_maquina.append({
            "timestamp_lectura": tiempo_actual.strftime("%Y-%m-%d %H:%M:%S"),
            "id_maquina": id_maquina,
            "rpm": round(rpm, 2),
            "vibracion_mms": round(vibr_guardar, 2),
            "temp_c": round(temp_guardar, 2),
            "corriente_motor_a": round(corriente, 2),
            "falla_fisica": es_falla
        })
        
        if es_falla:
            desgaste_interno = random.uniform(0, 5)
            
    # Etiquetado temporal
    indices_falla = [idx for idx, row in enumerate(historial_maquina) if row["falla_fisica"]]
    
    for idx, row in enumerate(historial_maquina):
        etiqueta = 0
        for f_idx in indices_falla:
            if (f_idx - VENTANA_PREDICCION) <= idx <= f_idx:
                etiqueta = 1
                break
        
        datos_totales.append({
            "timestamp_lectura": row["timestamp_lectura"],
            "id_maquina": row["id_maquina"],
            "rpm": row["rpm"],
            "vibracion_mms": row["vibracion_mms"],
            "temp_c": row["temp_c"],
            "corriente_motor_a": row["corriente_motor_a"],
            "etiqueta_prediccion": etiqueta
        })

df = pd.DataFrame(datos_totales)
df.to_csv("dataset_v2.csv", index=False)
print(f"🔧 Fallas físicas reales: {fallas_totales}")
print(df['etiqueta_prediccion'].value_counts(normalize=True).apply(lambda x: f"{x*100:.2f}%"))