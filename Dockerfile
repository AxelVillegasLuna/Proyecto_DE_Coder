FROM python:3.8

LABEL author="Axel Villegas Luna"
WORKDIR /app

# Instala las dependencias definidas en requirements.txt
RUN pip install --no-cache-dir -r requirements.txt