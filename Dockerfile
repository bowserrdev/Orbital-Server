FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Utente non-root dedicato — nessuna shell, nessuna home directory
RUN useradd -r -s /bin/false orbital

# I file dell'app devono essere leggibili dall'utente orbital
RUN chown -R orbital:orbital /app

USER orbital

CMD ["supervisord", "-c", "supervisord.conf"]