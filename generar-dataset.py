import pandas as pd
import random
from datetime import datetime, timedelta

print("🚀 Iniciando simulación de tendencias para el dataset de entrenamiento...")

NUM_MAQUINAS = 50
NUM_REGISTROS = 15000

# Inicializar el estado de las 50 máquinas
maquinas = {}
for i in range(1, NUM_MAQUINAS + 1):
    maquinas[f"maq-cnc-{i:02d}"] = {
        "rpm": random.uniform(1420, 1500),
        "temp": random.uniform(62, 70),
        "tendencia": "normal" # Puede ser: normal, calentando, vibrando
    }

datos = []
tiempo_actual = datetime.now()

for _ in range(NUM_REGISTROS):
    id_maq = random.choice(list(maquinas.keys()))
    estado = maquinas[id_maq]
    tiempo_actual += timedelta(seconds=2)
    
    # 1. Cambios de tendencia aleatorios (El 2% de las veces una máquina se echa a perder)
    if random.random() < 0.02:
        estado["tendencia"] = random.choice(["calentando", "vibrando"])
    
    # 2. Aplicar la física según la tendencia
    if estado["tendencia"] == "normal":
        # Fluctuación normal
        estado["temp"] += random.uniform(-0.5, 0.5)
        estado["rpm"] += random.uniform(-2.0, 2.0)
        # Mantener en rangos normales
        estado["temp"] = max(60, min(estado["temp"], 75))
        estado["rpm"] = max(1400, min(estado["rpm"], 1520))
        
    elif estado["tendencia"] == "calentando":
        # Sube la temperatura gradualmente, RPM fluctúa un poco más
        estado["temp"] += random.uniform(0.2, 1.5)
        estado["rpm"] += random.uniform(-3.0, 5.0)
        
    elif estado["tendencia"] == "vibrando":
        # Sube las RPM gradualmente, temperatura sube un poco por fricción
        estado["rpm"] += random.uniform(2.0, 15.0)
        estado["temp"] += random.uniform(0.1, 0.5)

    # Variables a registrar
    temp_actual = round(estado["temp"], 2)
    rpm_actual = round(estado["rpm"], 2)
    etiqueta_ia = 0 # Por defecto Normal

    # 3. Clasificación estricta según nuestras reglas de negocio
    # Categoría 5: Ruido / Anomalía extrema (1% de probabilidad)
    if random.random() < 0.01:
        temp_actual = random.choice([200.0, -10.0, 0.0])
        rpm_actual = random.choice([1950.0, 0.0])
        etiqueta_ia = 5
    else:
        # Lógica de clasificación jerárquica (Prioriza lo crítico)
        if temp_actual > 85:
            etiqueta_ia = 2 # Crítico Temp
        elif rpm_actual > 1600:
            etiqueta_ia = 4 # Crítico RPM
        elif temp_actual >= 76:
            etiqueta_ia = 1 # Riesgo Temp
        elif rpm_actual >= 1521:
            etiqueta_ia = 3 # Riesgo RPM

    # Guardar fila
    datos.append({
        "timestamp_lectura": tiempo_actual.strftime("%Y-%m-%d %H:%M:%S"),
        "id_maquina": id_maq,
        "rpm": rpm_actual,
        "temperatura": temp_actual,
        "etiqueta_ia": etiqueta_ia
    })

# Exportar a CSV
df = pd.DataFrame(datos)
df.to_csv("dataset_entrenamiento_tendencias.csv", index=False)
print("✅ ¡Archivo 'dataset_entrenamiento_tendencias.csv' generado con éxito!")
print(df['etiqueta_ia'].value_counts()) # Para ver cuántos datos de cada clase se generaron