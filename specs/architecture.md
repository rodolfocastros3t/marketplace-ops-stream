# Arquitetura de streaming — Vitrine

## Pipeline oficial

```
Produtor (Python)
    → Kafka topic: vitrine.raw.events          (Redpanda)
        → Spark Streaming (micro-batch 10s)
            → vitrine.processed.events
            → vitrine.alerts
            → vitrine.kpis
                → API Starlette (aiokafka consumer)
                    → WebSocket /ws
                        → Dashboard React + Tailwind
```

## Tópicos Kafka

| Tópico | Produtor | Consumidor | Conteúdo |
|--------|----------|------------|----------|
| `vitrine.raw.events` | producer | Spark | eventos brutos |
| `vitrine.processed.events` | Spark | API | pedidos/hubs/lives |
| `vitrine.alerts` | Spark | API | alertas com `destino` roteado |
| `vitrine.kpis` | Spark | API | agregações por janela |

## Engines Spark

| Engine | Arquivo | Quando usar |
|--------|---------|-------------|
| `microbatch` (default Compose) | `spark/processor.py` | Demo rápida, mesma lógica de roteamento/KPI |
| `pyspark` | `spark/streaming_job.py` | Spark Structured Streaming oficial (`Dockerfile.pyspark`) |

## Subir

```bash
chmod +x scripts/up-pipeline.sh
./scripts/up-pipeline.sh
# ou: docker compose up --build
```

Frontend: `cd frontend && npm run dev` (proxy → API :8000)

## Modo sem Docker

API sozinha cai em `PIPELINE_MODE=embedded` (simulador interno) se `KAFKA_BOOTSTRAP_SERVERS` estiver vazio.
