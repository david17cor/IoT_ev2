import pandas as pd
import random
from datetime import datetime, timedelta

print("🚀 Generando Dataset Predictivo (Ventana: 80 registros, ~3 mins)...")

NUM_MAQUINAS = 50
REGISTROS_POR_MAQ = 400 # 20.000 datos totales
VENTANA_PREDICCION = 80

datos_totales = []
tiempo_inicial = datetime.now()

for maq_id in range(1, NUM_MAQUINAS + 1):
    id_maquina = f"maq-cnc-{maq_id:02d}"
    tiempo_actual = tiempo_inicial
    
    historial_maquina = []
    # Las máquinas empiezan con desgaste aleatorio para no fallar todas juntas
    desgaste = random.uniform(0, 30) 
    
    # --- FASE 1: SIMULAR FÍSICA A CIEGAS ---
    for i in range(REGISTROS_POR_MAQ):
        tiempo_actual += timedelta(seconds=2)
        
        # El desgaste sube inexorablemente (entre 0.05% y 0.25% por ciclo)
        desgaste += random.uniform(0.05, 0.25)
        rpm = random.uniform(1400, 1500)
        
        # FÍSICA: A mayor desgaste y RPM, la máquina vibra más
        vibracion = 5.0 + ((rpm - 1400) * 0.01) + (desgaste * 0.15) + random.uniform(-0.5, 0.5)
        
        # FÍSICA: A mayor vibración, sube la temperatura
        temp = 55.0 + (vibracion * 1.5) + random.uniform(-1.0, 1.0)
        
        es_falla = False
        # CONDICIÓN DE FALLA MULTIVARIABLE (El colapso real)
        if temp > 85.0 and vibracion > 15.0 and desgaste > 75.0:
            es_falla = True
            
        # RUIDO DE SENSOR (0.5% de probabilidad) - El modelo debe aprender a ignorarlo
        if random.random() < 0.005:
            temp = random.choice([200.0, 0.0])
            vibracion = random.choice([50.0, 0.0])
            
        historial_maquina.append({
            "timestamp_lectura": tiempo_actual.strftime("%Y-%m-%d %H:%M:%S"),
            "id_maquina": id_maquina,
            "rpm": round(rpm, 2),
            "vibracion_mms": round(vibracion, 2),
            "temp_c": round(temp, 2),
            "desgaste_pct": round(desgaste, 2),
            "falla_fisica": es_falla # Solo se usa para calcular la etiqueta final
        })
        
        if es_falla:
            # Mantenimiento Mayor: Resetea el desgaste de la máquina
            desgaste = random.uniform(0, 5)
            
    # --- FASE 2: LA MÁQUINA DEL TIEMPO (Etiquetado Predictivo) ---
    # Encontramos en qué índices exactos falló la máquina
    indices_falla = [idx for idx, row in enumerate(historial_maquina) if row["falla_fisica"]]
    
    for idx, row in enumerate(historial_maquina):
        etiqueta = 0
        # Revisamos si este registro está dentro de la "Ventana de Peligro" de 80 pasos
        for f_idx in indices_falla:
            if (f_idx - VENTANA_PREDICCION) <= idx <= f_idx:
                etiqueta = 1
                break
        
        # Guardamos el registro final, ya sin la "trampa" de la falla_fisica
        datos_totales.append({
            "timestamp_lectura": row["timestamp_lectura"],
            "id_maquina": row["id_maquina"],
            "rpm": row["rpm"],
            "vibracion_mms": row["vibracion_mms"],
            "temp_c": row["temp_c"],
            "desgaste_pct": row["desgaste_pct"],
            "etiqueta_prediccion": etiqueta
        })

# Exportar Dataset
df = pd.DataFrame(datos_totales)
archivo_salida = "dataset_predictivo_horizonte_80.csv"
df.to_csv(archivo_salida, index=False)
print(f"✅ ¡Archivo '{archivo_salida}' generado!")
print("\nDistribución del Target (1 = Falla Próxima | 0 = Normal):")
print(df['etiqueta_prediccion'].value_counts(normalize=True).apply(lambda x: f"{x*100:.2f}%"))