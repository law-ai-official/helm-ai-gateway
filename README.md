# helm-ai-gateway

Helm chart for the AI gateway stack — Kong, NewAPI, LibreChat, Postgres, MongoDB in one release.

## What it deploys

- **Kong Gateway** (DB-less) — public API entry point, routes `/v1/*` to NewAPI and `/chat/*` to LibreChat
- **NewAPI** — LLM API server (exposed publicly via Kong)
- **LibreChat** — chat UI (exposed publicly via Kong)
- **Postgres** — independent database for NewAPI + Kong chat logs (logical DBs: `newapi`, `chat_log`)
- **MongoDB** — independent database for LibreChat (its native store)
- **Log collector** — tiny Node.js service that receives Kong's http-log POSTs and writes chat history to the `chat_log` database

## Install

```bash
helm install ai-gateway ./helm-ai-gateway -n ai-gateway --create-namespace
```

## Upgrade

```bash
helm upgrade ai-gateway ./helm-ai-gateway -n ai-gateway
```

## Uninstall

```bash
helm uninstall ai-gateway -n ai-gateway
```

## Values

See `values.yaml` for the full list. Key sections:

- `global` — namespace, storageClass, labels
- `postgres` — Postgres StatefulSet (creates `newapi` + `chat_log` DBs)
- `mongodb` — MongoDB StatefulSet (LibreChat's native store)
- `newapi` — NewAPI Deployment + embedded Redis
- `librechat` — LibreChat Deployment
- `kong` — Kong Deployment (DB-less, declarative config)
- `logCollector` — Log collector Deployment

## Architecture

```
                    internet
                       │
                       ▼
            ┌────────────────────┐
            │   Kong Gateway     │  ← public entry (NodePort 30080/30443)
            │   (DB-less)        │
            └──┬─────────────┬───┘
               │ /v1/*       │ /chat/*
               ▼             ▼
        ┌───────────┐   ┌───────────┐
        │  NewAPI   │   │ LibreChat │
        │           │   │           │
        └─────┬─────┘   └─────┬─────┘
              │               │
              ▼               ▼
        ┌───────────┐   ┌───────────┐
        │ Postgres  │   │  MongoDB  │
        │ (newapi + │   │(librechat)│
        │  chat_log)│   │           │
        └───────────┘   └───────────┘

   Kong logs every request+response body → chat_log DB (via log-collector)
```

## Development

For local dev, disable NodePort and use `kubectl port-forward`:

```bash
kubectl -n ai-gateway port-forward svc/kong 8080:8000
curl http://localhost:8080/v1/models
```
