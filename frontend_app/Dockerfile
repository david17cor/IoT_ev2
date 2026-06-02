FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias de Streamlit
RUN pip install --no-cache-dir streamlit pandas requests

# Copiar el código del frontend
COPY app_frontend.py /app/

# Exponer el puerto de Streamlit
EXPOSE 8501

# Comando para iniciar la aplicación
CMD ["streamlit", "run", "app_frontend.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]