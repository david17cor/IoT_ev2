import json
import hashlib
from datetime import datetime
from kafka import KafkaConsumer

# Configurar el Consumidor de Kafka
consumer = KafkaConsumer(
    'telemetria_sucia',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def aplicar_hashing(texto):
    return hashlib.sha256(texto.encode()).hexdigest()

def enmascarar_rut(rut):
    return "XX.XXX.XX" + rut[-3:]

print("🟢 Consumidor DataOps LISTO. Esperando datos para limpiar en tiempo real...\n")

for mensaje in consumer:
    dato_sucio = mensaje.value
    dato_limpio = {}
    
    try:
        # 1. Limpieza de Formato: Arreglar Fechas al estándar ISO
        fecha_obj = datetime.strptime(dato_sucio["timestamp_lectura"], "%d/%m/%Y %H:%M:%S")
        dato_limpio["timestamp"] = fecha_obj.isoformat()
        
        # 2. Limpieza Estructural: Estandarizar nombre de máquina
        dato_limpio["machine_id"] = str(dato_sucio["ID_Maquina"]).upper()
        
        # 3. Limpieza de Formato: Arreglar comas en números y convertir a Float
        if isinstance(dato_sucio["Revoluciones_RPM"], str):
            rpm_corregido = float(dato_sucio["Revoluciones_RPM"].replace(",", "."))
        else:
            rpm_corregido = float(dato_sucio["Revoluciones_RPM"])
        dato_limpio["rpm"] = rpm_corregido
        
        # 4. Limpieza Semántica: Filtrar temperaturas imposibles
        temp = float(dato_sucio["Temp_C"])
        if temp < 0 or temp > 300:
            dato_limpio["temperatura_motor"] = None # O reemplazar por el promedio móvil (Imputación)
            anomalia_detectada = True
        else:
            dato_limpio["temperatura_motor"] = temp
            anomalia_detectada = False
            
        # 5. Seguridad PII (Cumplimiento Ley 19.628)
        nombre_limpio = dato_sucio["Nombre_Operador"].strip().title() # Quita espacios y capitaliza bien
        dato_limpio["operador_hash"] = aplicar_hashing(nombre_limpio)
        dato_limpio["rut_mask"] = enmascarar_rut(dato_sucio["rut_op"])
        
        # --- IMPRESIÓN PARA LA DEMO ---
        estado_temp = "⚠️ DESCARTADA (-999)" if anomalia_detectada else f"{dato_limpio['temperatura_motor']}°C"
        
        print("-" * 50)
        print(f"📥 RECIBIDO (Sucio): RPM='{dato_sucio['Revoluciones_RPM']}' | Temp={dato_sucio['Temp_C']} | Op='{dato_sucio['Nombre_Operador']}'")
        print(f"✨ PROCESADO (Limpio): RPM={dato_limpio['rpm']} | Temp={estado_temp} | RUT={dato_limpio['rut_mask']}")
        
        # Aquí iría el INSERT a PostgreSQL del dato_limpio
        
    except Exception as e:
        print(f"❌ Error procesando el mensaje: {e}")