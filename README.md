# Ethos

Participatory, intent-driven news platform.

**Stack:** FastAPI · PostgreSQL · Redis · Qdrant · React 19 · Vite

## Prerequisites

- [uv](https://docs.astral.sh/uv/) — Python deps
- Node.js v18+
- PostgreSQL, Redis, Qdrant running locally or via Docker

## Start

**Backend**
```bash
uv sync
cp backend/.env.example backend/.env   # fill in credentials
uv run uvicorn backend.main:app --reload --port 8000
```

**Frontend**
```bash
cd frontend
npm install
npm run dev   # → http://localhost:5173
```

## Add a dependency

```bash
# Python
uv add <package>          # commits pyproject.toml + uv.lock

# JS
cd frontend && npm install <package>
```

## Monitoring (Prometheus + Grafana)

1. Start backend locally (`uv run uvicorn backend.main:app --reload --port 8000`).
2. Start monitoring stack:
   ```bash
   docker compose up -d prometheus grafana
   ```
3. Open:
   - Prometheus: `http://localhost:9090`
   - Grafana: `http://localhost:3000` (default login `admin` / `admin`)
4. In Grafana, add Prometheus datasource URL: `http://prometheus:9090`.
