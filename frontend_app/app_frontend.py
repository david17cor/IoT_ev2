import streamlit as st
import requests

# 1. Configuración de página (Nativa de Streamlit)
st.set_page_config(page_title="Dashboard Predictivo CNC", layout="wide", initial_sidebar_state="expanded")

API_CAOS = "http://backend_api:8000/api/caos"

# ==========================================
# 🍔 MENÚ LATERAL: CONSOLA DEL CAOS
# ==========================================
with st.sidebar:
    st.title("Consola del Caos")
    st.markdown("Inyecta anomalías directo a Kafka para probar la IA en tiempo real.")
    
    maquina_victima = st.selectbox("Selecciona la Víctima:", [f"maq-cnc-{i:02d}" for i in range(1, 51)])
    tipo_falla = st.radio("Tipo de Falla:", ["Falla Térmica", "Desalineación (Vibración)", "Cortocircuito"])
    
    if st.button("💥 INYECTAR FALLA", type="primary", use_container_width=True):
        try:
            payload = {"id_maquina": maquina_victima, "tipo_falla": tipo_falla}
            res = requests.post(API_CAOS, json=payload)
            if res.status_code == 200:
                st.sidebar.success(f"¡Anomalía enviada a {maquina_victima}!")
            else:
                st.sidebar.error("Error al inyectar falla.")
        except Exception as e:
            st.sidebar.error("Error de conexión con el Backend API.")

# ==========================================
# 🎨 ESTILOS CSS (Tu diseño original restaurado al 100%)
# ==========================================
st.markdown("""
    <style>
        .stApp { background-color: #0F172A; color: #F8FAFC; }
        
        /* Layout superior de métricas */
        .top-metrics { display: flex; justify-content: space-between; margin-bottom: 2rem; border-bottom: 1px solid #334155; padding-bottom: 1rem; }
        .metric-item { display: flex; flex-direction: column; }
        .metric-label { font-size: 0.9rem; color: #cbd5e1; display: flex; align-items: center; gap: 0.5rem; }
        .metric-val { font-size: 2.2rem; font-weight: bold; }
        
        /* Grilla de máquinas */
        .cnc-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 15px; }
        
        /* Tarjetas base */
        .cnc-card {
            background-color: #1e293b;
            border: 1px solid #334155;
            border-radius: 8px;
            padding: 15px 10px;
            text-align: center;
            display: flex;
            flex-direction: column;
            justify-content: center;
            min-height: 130px;
            transition: all 0.3s ease;
        }
        
        /* Colores dinámicos para los estados */
        .card-normal { background-color: #1e293b; }
        .card-riesgo { background-color: #92400e !important; border: 1px solid #b45309 !important; }
        .card-critico { 
            background-color: #991b1b !important; 
            border: 1px solid #ef4444 !important; 
            box-shadow: 0 0 15px rgba(239, 68, 68, 0.4);
        }
        .card-offline { opacity: 0.3; border-style: dashed; }
        
        /* Textos de las tarjetas */
        .mac-title { font-size: 1rem; font-weight: bold; margin: 0 0 10px 0; color: #f1f5f9; }
        .mac-status { font-size: 0.85rem; font-weight: bold; margin: 0 0 10px 0; }
        .mac-data { font-size: 0.75rem; color: #cbd5e1; margin: 2px 0; }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 🖥️ ESTRUCTURA HTML ESTÁTICA
# ==========================================
st.title("🏭 Planta de Producción CNC - Vista en Vivo")
st.markdown("Visualización en tiempo real del estado de las 50 máquinas.")

# Construimos el HTML base
html_content = f"""
<div class="top-metrics">
    <div class="metric-item">
        <div class="metric-label">Total Máquinas</div>
        <div class="metric-val" id="val-total">-- / 50</div>
    </div>
    <div class="metric-item">
        <div class="metric-label"><span style="color:#22c55e">●</span> Normal</div>
        <div class="metric-val" id="val-normal">--</div>
    </div>
    <div class="metric-item">
        <div class="metric-label"><span style="color:#eab308">●</span> En Riesgo</div>
        <div class="metric-val" id="val-riesgo">--</div>
    </div>
    <div class="metric-item">
        <div class="metric-label"><span style="color:#ef4444">●</span> Críticas</div>
        <div class="metric-val" id="val-critico">--</div>
    </div>
</div>
<div class="cnc-grid">
{"".join([f'''
    <div id="card-maq-cnc-{i:02d}" class="cnc-card card-offline">
        <div class="mac-title">maq-cnc-{i:02d}</div>
        <div id="status-maq-cnc-{i:02d}" class="mac-status" style="color:#64748b;">⚪ OFFLINE</div>
        <div class="mac-data">Δ Temp: <span id="dtemp-maq-cnc-{i:02d}">--</span>°C</div>
        <div class="mac-data">Falla: <span id="ffallas-maq-cnc-{i:02d}">--</span></div>
    </div>
''' for i in range(1, 51)])}
</div>
"""

# Quitamos los saltos de línea para evitar que Streamlit rompa el HTML estructurado
html_seguro = html_content.replace('\n', '')
st.markdown(html_seguro, unsafe_allow_html=True)

# ==========================================
# ⚙️ MOTOR JAVASCRIPT SILENCIOSO (Frecuencia corregida)
# ==========================================
js_updater = """
<script>
const API_URL = "http://localhost:8000/api/dashboard-tiempo-real";

async function updateDashboard() {
    try {
        const response = await fetch(API_URL);
        const res = await response.json();
        if (!res.success || !res.data) return;
        
        const maquinas = {};
        res.data.forEach(reg => {
            if(!maquinas[reg.id_maquina]) {
                maquinas[reg.id_maquina] = reg;
            }
        });
        
        let normal = 0, riesgo = 0, critico = 0, total = 0;
        const doc = window.parent.document; 
        
        for (let i = 1; i <= 50; i++) {
            const id = `maq-cnc-${String(i).padStart(2, '0')}`;
            const card = doc.getElementById(`card-${id}`);
            const statusTxt = doc.getElementById(`status-${id}`);
            const dtempTxt = doc.getElementById(`dtemp-${id}`);
            const fallaTxt = doc.getElementById(`ffallas-${id}`);
            
            if (!card) continue;
            
            if (maquinas[id]) {
                total++;
                const info = maquinas[id];
                const estado = (info.estado_maquina || 'NORMAL').trim().toUpperCase();
                
                card.className = "cnc-card";
                
                if (estado.includes('NORMAL')) {
                    card.classList.add('card-normal');
                    statusTxt.innerHTML = "🟢 NORMAL";
                    statusTxt.style.color = "#22c55e";
                    normal++;
                } else if (estado.includes('RIESGO')) {
                    card.classList.add('card-riesgo');
                    statusTxt.innerHTML = "⚠️ RIESGO: REVISAR";
                    statusTxt.style.color = "#fcd34d";
                    riesgo++;
                } else {
                    card.classList.add('card-critico');
                    statusTxt.innerHTML = "🚨 CRITICO: PARADA";
                    statusTxt.style.color = "#fca5a5";
                    critico++;
                }
                
                dtempTxt.innerText = info.delta_temp !== null ? parseFloat(info.delta_temp).toFixed(2) : "0.00";
                if(fallaTxt) fallaTxt.innerText = info.probabilidad_falla_pct || "0.0%";
                
            } else {
                card.className = "cnc-card card-offline";
                statusTxt.innerHTML = "⚪ OFFLINE";
                statusTxt.style.color = "#64748b";
                if(dtempTxt) dtempTxt.innerText = "--";
                if(fallaTxt) fallaTxt.innerText = "--";
            }
        }
        
        const topTotal = doc.getElementById('val-total');
        const topNormal = doc.getElementById('val-normal');
        const topRiesgo = doc.getElementById('val-riesgo');
        const topCritico = doc.getElementById('val-critico');
        
        if(topTotal) topTotal.innerText = `${total} / 50`;
        if(topNormal) topNormal.innerText = normal;
        if(topRiesgo) topRiesgo.innerText = riesgo;
        if(topCritico) topCritico.innerText = critico;
        
    } catch (error) {
        console.error("Esperando comunicación con Backend API...", error);
    }
}

// Ejecución inicial inmediata
updateDashboard();

// ⏱️ REFRECO REDUCIDO: Ahora consulta cada 2000ms (2 segundos)
setInterval(updateDashboard, 2000);
</script>
"""
st.components.v1.html(js_updater, height=0, width=0)