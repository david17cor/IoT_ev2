FROM python:3.11-slim

WORKDIR /app

# 1. Copiar el archivo de requerimientos primero
COPY requirements.txt /app/

# 2. Instalar las dependencias desde el archivo
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copiar el código del frontend
COPY . /app/

# Exponer el puerto de Streamlit
EXPOSE 8501

# Comando para iniciar la aplicación
CMD ["streamlit", "run", "app_frontend.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]