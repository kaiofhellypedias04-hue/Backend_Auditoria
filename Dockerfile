FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    APP_ENV=production \
    PORT=10000

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    gnupg \
    unzip \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npx playwright install --with-deps chromium

COPY . .

RUN mkdir -p /app/runtime/saida /app/runtime/temp /app/runtime/certs

CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT:-10000}"]
