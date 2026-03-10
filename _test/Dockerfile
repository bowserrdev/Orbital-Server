# ==================================
# FASE 1: Il "Costruttore" con Rust
# ==================================
FROM rust:latest as builder

WORKDIR /usr/src/app

# Installa il tool che crea i "Wheel" Python
RUN pip install maturin

# Crea una cartella per il codice Rust
WORKDIR /usr/src/app/orbital_core
COPY orbital_core .

# Compila il codice Rust e genera il file .whl nella cartella "wheels"
# Questo comando produrrà orbital_core-0.1.0-cp312-cp312-linux_aarch64.whl
RUN maturin build --release -o wheels


# ==================================
# FASE 2: L'Immagine Finale Python
# ==================================
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Non serve più apt-get install, non compiliamo nulla
# Ma potremmo aver bisogno di curl per i controlli di salute
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copia i requisiti Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# --- LA MAGIA ---
# Copia SOLO il file compilato dalla FASE 1 (il .whl)
COPY --from=builder /usr/src/app/orbital_core/wheels/ .

# Installa la libreria Rust come se fosse un pacchetto Python normale
RUN pip install *.whl

# Copia il codice Python
COPY . .

CMD ["python", "main.py"]
