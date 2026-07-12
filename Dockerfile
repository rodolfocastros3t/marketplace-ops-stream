# Build do dashboard React (mesmo do localhost:5173)
FROM node:20-bookworm-slim AS frontend
WORKDIR /fe
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# API + dashboard estático na mesma URL
FROM python:3.12-slim
WORKDIR /app

COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

COPY shared /app/shared
COPY backend /app/backend
COPY --from=frontend /fe/dist /app/backend/static

ENV PYTHONPATH=/app
ENV PIPELINE_MODE=embedded
ENV PYTHONUNBUFFERED=1

WORKDIR /app/backend
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]
