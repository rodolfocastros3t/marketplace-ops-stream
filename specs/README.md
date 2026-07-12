# Spec Based Development — Ops Stream

Contratos oficiais do sistema. **Nenhuma feature deve ser implementada sem contrato correspondente aqui.**

## Ordem de verdade

1. `vision.md` — produto, posicionamento BR e diferenciais vs Amazon
2. `domain/entities.yaml` — entidades e invariantes de negócio
3. `openapi.yaml` — API REST (comandos e consultas)
4. `asyncapi.yaml` — stream WebSocket (eventos em tempo real)
5. `events/*.schema.json` — schemas JSON dos eventos do pipeline

## Pipeline (Kafka + Spark)

```
Produtor → Kafka → Spark Streaming → API Starlette → WebSocket → Dashboard React
```

Intervalo do produtor: **1.5s**. Micro-batch Spark: **10s**.

## Stack contratada

| Camada | Tecnologia |
|--------|------------|
| Mensageria | Kafka (Redpanda) |
| Stream | Spark Structured Streaming / micro-batch |
| Backend | Python, Starlette, Uvicorn, aiokafka |
| Frontend | React 18, Chart.js, Tailwind CSS |
| Deploy | GitHub → Render / Docker Compose |

## Conceitos de engenharia

- Stream processing
- Arquitetura produtor–consumidor
- Análise de dados em tempo real
- Roteamento automático por regras de negócio
