FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn psycopg2-binary python-dotenv pandas

# Copiar el código de la API
COPY api.py /app/

# Exponer el puerto
EXPOSE 8000

# Comando para iniciar la API y escuchar en todas las interfaces de red
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]