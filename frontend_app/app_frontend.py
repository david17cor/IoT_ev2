import streamlit as st
import requests
import os

# 1. Configuración de página
st.set_page_config(page_title="Dashboard Predictivo CNC", layout="wide", initial_sidebar_state="expanded")

API_CAOS = "http://backend_api:8000/api/caos"
API_MANTENIMIENTO = "http://backend_api:8000/api/mantenimiento"

# ==========================================
# 🧭 MENÚ DE NAVEGACIÓN LATERAL
# ==========================================
with st.sidebar:
    st.title("Planta CNC v3")
    st.markdown("---")
    vista_actual = st.radio(
        "Navegación:",
        ["Monitor en Tiempo Real", "Reparación de Máquinas", "Inyección de Anomalías", "Analíticas del Modelo"]
    )

# ==========================================
# 📊 VISTA 1: MONITOR EN TIEMPO REAL
# ==========================================
if vista_actual == "Monitor en Tiempo Real":
    st.title("Monitor de Operaciones CNC")
    
    st.markdown("""
        <style>
            .top-metrics { display: flex; justify-content: space-between; margin-bottom: 2rem; border-bottom: 1px solid #334155; padding-bottom: 1rem; }
            .metric-item { display: flex; flex-direction: column; }
            .metric-label { font-size: 0.9rem; color: #cbd5e1; display: flex; align-items: center; gap: 0.5rem; }
            .metric-val { font-size: 2.2rem; font-weight: bold; }
            .cnc-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 15px; }
            .cnc-card { background-color: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 15px 10px; text-align: center; display: flex; flex-direction: column; justify-content: center; min-height: 130px; transition: all 0.3s ease; }
            .card-normal { background-color: #1e293b; }
            .card-riesgo { background-color: #92400e !important; border: 1px solid #b45309 !important; }
            .card-critico { background-color: #991b1b !important; border: 1px solid #ef4444 !important; box-shadow: 0 0 15px rgba(239, 68, 68, 0.4); }
            .card-inactivo { background-color: #475569 !important; border: 1px solid #94a3b8 !important; opacity: 0.9; }
            .card-offline { opacity: 0.3; border-style: dashed; }
            .mac-title { font-size: 1rem; font-weight: bold; margin: 0 0 10px 0; color: #f1f5f9; }
            .mac-status { font-size: 0.85rem; font-weight: bold; margin: 0 0 10px 0; }
            .mac-data { font-size: 0.75rem; color: #cbd5e1; margin: 2px 0; }
        </style>
    """, unsafe_allow_html=True)

    html_content = f"""
    <div class="top-metrics">
        <div class="metric-item"><div class="metric-label">Total Máquinas</div><div class="metric-val" id="val-total">-- / 25</div></div>
        <div class="metric-item"><div class="metric-label"><span style="color:#22c55e">●</span> Normal</div><div class="metric-val" id="val-normal">--</div></div>
        <div class="metric-item"><div class="metric-label"><span style="color:#eab308">●</span> En Riesgo</div><div class="metric-val" id="val-riesgo">--</div></div>
        <div class="metric-item"><div class="metric-label"><span style="color:#ef4444">●</span> Críticas</div><div class="metric-val" id="val-critico">--</div></div>
    </div>
    <div class="cnc-grid">
    {"".join([f'<div id="card-maq-cnc-{i:02d}" class="cnc-card card-offline"><div class="mac-title">maq-cnc-{i:02d}</div><div id="status-maq-cnc-{i:02d}" class="mac-status" style="color:#64748b;">⚪ OFFLINE</div><div class="mac-data">Δ Temp: <span id="dtemp-maq-cnc-{i:02d}">--</span>°C</div><div class="mac-data">Falla: <span id="ffallas-maq-cnc-{i:02d}">--</span></div></div>' for i in range(1, 26)])}
    </div>
    """
    st.markdown(html_content, unsafe_allow_html=True)

    js_updater = """
    <script>
    async function updateDashboard() {
        try {
            const res = await (await fetch("http://34.176.77.168:8000/api/dashboard-tiempo-real")).json();
            if (!res.success) return;
            const maquinas = {};
            res.data.forEach(reg => maquinas[reg.id_maquina] = reg);
            
            let normal = 0, riesgo = 0, critico = 0, total = 0;
            const doc = window.parent.document; 
            
            for (let i = 1; i <= 25; i++) {
                const id = `maq-cnc-${String(i).padStart(2, '0')}`;
                const card = doc.getElementById(`card-${id}`);
                if (!card) continue;
                
                if (maquinas[id]) {
                    total++;
                    const info = maquinas[id];
                    const estado = (info.estado_maquina || 'NORMAL').toUpperCase();
                    card.className = "cnc-card";
                    
                    if (estado.includes('NORMAL')) { card.classList.add('card-normal'); doc.getElementById(`status-${id}`).innerHTML = "🟢 NORMAL"; doc.getElementById(`status-${id}`).style.color = "#22c55e"; normal++; }
                    else if (estado.includes('RIESGO')) { card.classList.add('card-riesgo'); doc.getElementById(`status-${id}`).innerHTML = "⚠️ RIESGO"; doc.getElementById(`status-${id}`).style.color = "#fcd34d"; riesgo++; }
                    else if (estado.includes('INACTIVO')) { card.classList.add('card-inactivo'); doc.getElementById(`status-${id}`).innerHTML = "💤 INACTIVO"; doc.getElementById(`status-${id}`).style.color = "#cbd5e1"; }
                    else { card.classList.add('card-critico'); doc.getElementById(`status-${id}`).innerHTML = "🚨 CRITICO"; doc.getElementById(`status-${id}`).style.color = "#fca5a5"; critico++; }
                    
                    doc.getElementById(`dtemp-${id}`).innerText = info.delta_temp !== null ? parseFloat(info.delta_temp).toFixed(2) : "0.00";
                    doc.getElementById(`ffallas-${id}`).innerText = info.probabilidad_falla_pct || "0.0%";
                }
            }
            if(doc.getElementById('val-total')) doc.getElementById('val-total').innerText = `${total} / 25`;
            if(doc.getElementById('val-normal')) doc.getElementById('val-normal').innerText = normal;
            if(doc.getElementById('val-riesgo')) doc.getElementById('val-riesgo').innerText = riesgo;
            if(doc.getElementById('val-critico')) doc.getElementById('val-critico').innerText = critico;
        } catch (e) {}
    }
    updateDashboard(); setInterval(updateDashboard, 2000);
    </script>
    """
    st.components.v1.html(js_updater, height=0, width=0)

# ==========================================
# 🔧 VISTA 2: REPARACIÓN DE MÁQUINAS
# ==========================================
elif vista_actual == "Reparación de Máquinas":
    st.title("Mantenimiento y Reseteo Mecánico")
    st.markdown("Aplica mantenimientos preventivos o correctivos a las máquinas en estado crítico o inactivo.")
    
    col1, col2 = st.columns([1, 1])
    with col1:
        st.info("Al aplicar mantenimiento, la máquina detendrá su desgaste interno, se estabilizará su temperatura y vibración, y retornará al estado NORMAL.")
        maquina_mant = st.selectbox("Seleccione la máquina:", [f"maq-cnc-{i:02d}" for i in range(1, 26)])
        if st.button("APLICAR MANTENIMIENTO", type="primary", use_container_width=True):
            try:
                res = requests.post(API_MANTENIMIENTO, json={"id_maquina": maquina_mant})
                if res.status_code == 200: st.success(f"Señal enviada a {maquina_mant}.")
                else: st.error("Error al aplicar mantenimiento.")
            except: st.error("Error conectando con la API.")

# ==========================================
# 💥 VISTA 3: INYECCIÓN DE ANOMALÍAS
# ==========================================
elif vista_actual == "Inyección de Anomalías":
    st.title("Consola del Caos (Pruebas de Estrés)")
    st.markdown("Fuerza la inyección de anomalías directo al topic de Kafka para evaluar la latencia de respuesta de Spark y el modelo Predictivo.")
    
    col1, col2 = st.columns([1, 1])
    with col1:
        maquina_victima = st.selectbox("Máquina Objetivo:", [f"maq-cnc-{i:02d}" for i in range(1, 26)])
        tipo_falla = st.radio("Tipo de Falla Simulada:", ["Falla Térmica", "Desalineación (Vibración)", "Cortocircuito"])
        if st.button("EJECUTAR ANOMALÍA", use_container_width=True):
            try:
                res = requests.post(API_CAOS, json={"id_maquina": maquina_victima, "tipo_falla": tipo_falla})
                if res.status_code == 200: st.success(f"Anomalía inyectada exitosamente a {maquina_victima}.")
                else: st.error("Error al inyectar falla.")
            except: st.error("Error conectando con la API.")

# ==========================================
# 📈 VISTA 4: ANALÍTICAS DEL MODELO (Rúbrica)
# ==========================================
elif vista_actual == "Analíticas del Modelo":
    st.title("Rendimiento del Modelo: Random Forest")
    st.markdown("Evaluación técnica del algoritmo de Machine Learning predictivo.")
    
    # --- TRUCO DE RUTAS ABSOLUTAS ---
    # Obtiene la ruta exacta de la carpeta donde está este script (app_frontend.py)
    DIRECTORIO_BASE = os.path.dirname(os.path.abspath(__file__))
    
    # Construimos las rutas absolutas para cada imagen
    ruta_matriz = os.path.join(DIRECTORIO_BASE, "matriz_confusion.png")
    ruta_gini = os.path.join(DIRECTORIO_BASE, "gini.png") # Ajustado a 'gini.png' como lo tienes en VS Code
    ruta_roc = os.path.join(DIRECTORIO_BASE, "curva_roc.png")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Matriz de Confusión")
        if os.path.exists(ruta_matriz):
            st.image(ruta_matriz, use_container_width=True)
        else:
            st.warning(f"Imagen no encontrada en: {ruta_matriz}")
            
        st.subheader("Importancia de Variables (Índice Gini)")
        if os.path.exists(ruta_gini):
            st.image(ruta_gini, use_container_width=True)
        else:
            st.warning(f"Imagen no encontrada en: {ruta_gini}")
            
    with col2:
        st.subheader("Métricas de Clasificación")
        # Resultados exactos de nuestro script de Colab
        st.metric("Accuracy (Exactitud)", "87.6%") 
        st.metric("Recall (Sensibilidad)", "94.2%")
        st.metric("F1-Score", "75.7%")
        
        st.subheader("Curva ROC / AUC")
        if os.path.exists(ruta_roc):
            st.image(ruta_roc, use_container_width=True)
        else:
            st.warning(f"Imagen no encontrada en: {ruta_roc}")