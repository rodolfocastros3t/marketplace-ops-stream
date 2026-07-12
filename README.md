# Ops Stream

**Marketplace brasileiro em tempo real** — protótipo Spec Based pensado para competir com a Amazon no Brasil, com operação ao vivo, PIX First, live commerce e inteligência de stream (Kafka + Spark).

```
Produtor → Kafka (Redpanda) → Spark Streaming → API / WebSocket → Dashboard Ops
```

---

## Visão do produto

A Amazon Brasil é forte em catálogo e logística. O **Ops Stream** ataca as falhas que mais doem no consumidor e no vendedor brasileiro:

| Dor na Amazon BR | Resposta Ops Stream |
|------------------|---------------------|
| Checkout lento / cartão no centro da UX | **PIX First** + confirmação no stream |
| Pouca compra social | **Live commerce** nativo |
| Frete opaco (Norte/Nordeste) | Hubs por UF, SLA e saturação ao vivo |
| Operação “caixa-preta” | Kanban de pedidos + alertas roteados |
| MEI / economia local invisível | Cobertura regional e campanhas por praça |

**Posicionamento:** Amazon-like (catálogo + fulfillment) + Shopee-like (live) + cultura BR (PIX, CEP, UF, boleto, LGPD).

> Este repositório é um **protótipo de hackathon / PoC** — demonstra a tese, os contratos e o pipeline de streaming. Não é o e-commerce completo (checkout real, NF-e, app etc.).

Documentação de produto: [`specs/vision.md`](./specs/vision.md) · estratégia: [`specs/business-strategy.md`](./specs/business-strategy.md)

---

## O que o dashboard mostra

Painel **Ops Stream** (React + Tailwind) consumindo WebSocket:

- **KPIs** — GMV, pedidos/min, ticket médio, % PIX, % live, alertas críticos  
- **Kanban** — ciclo de vida do pedido sem reload  
- **Mix de pagamento** — PIX vs outros  
- **Carga dos hubs** — ocupação por UF  
- **Alertas roteados** — fraude, logística, marketing, operações  
- **Mapa de hubs + Live** — clique no hub → carga, fila, SLA, risco e ação sugerida  
- **Ajuda (?)** em cada área, explicando o papel no sistema  

---

## Stack tecnológica

| Camada | Tecnologia | Papel |
|--------|------------|--------|
| Contratos (Spec Based) | OpenAPI, AsyncAPI, JSON Schema, YAML de domínio | Fonte da verdade antes do código |
| Mensageria | **Kafka** via Redpanda | Buffer desacoplado de eventos |
| Processamento | **Spark Streaming** (micro-batch + job PySpark) | Roteamento de alertas + KPIs |
| API | **Python**, Starlette, Uvicorn, aiokafka | Consome stream processado e expõe REST/WS |
| Tempo real | WebSocket `/ws` | Empurra eventos ao dashboard |
| Frontend | **React 18**, Chart.js, **Tailwind CSS v4**, Vite | UX operacional BR |
| Infra local | Docker Compose | Redpanda + Spark + producer + API |
| Deploy alvo | Render (free) | Mesmo React do local, via `backend/static` |

### Arquitetura

```
┌─────────────┐     raw      ┌──────────────┐   processed   ┌─────────────┐
│  Produtor   │ ──────────►  │ Kafka topics │ ────────────► │  API WS     │
│  (Python)   │              │  + Spark     │   alerts/kpis │  Starlette  │
└─────────────┘              └──────────────┘               └──────┬──────┘
                                                                   │
                                                                   ▼
                                                            ┌─────────────┐
                                                            │  Dashboard  │
                                                            │ React + TW  │
                                                            └─────────────┘
```

Detalhes: [`specs/architecture.md`](./specs/architecture.md)

### Tópicos Kafka

| Tópico | Função |
|--------|--------|
| `vitrine.raw.events` | Eventos brutos do produtor |
| `vitrine.processed.events` | Pedidos / hubs / lives enriquecidos |
| `vitrine.alerts` | Alertas com destino roteado |
| `vitrine.kpis` | Agregações em janela |

---

## Spec Based Development

Nada é implementado sem contrato em [`specs/`](./specs/):

1. [`vision.md`](./specs/vision.md) — produto e diferenciais BR  
2. [`domain/entities.yaml`](./specs/domain/entities.yaml) — entidades e regras  
3. [`openapi.yaml`](./specs/openapi.yaml) — API REST  
4. [`asyncapi.yaml`](./specs/asyncapi.yaml) — canal WebSocket  
5. [`events/*.schema.json`](./specs/events/) — schemas dos eventos  

---

## Como rodar

### Pré-requisitos

- Python 3.12+ (3.14 ok no modo embutido)  
- Node.js 20+  
- Docker Desktop (para o pipeline Kafka + Spark)  

### Opção A — Demo rápida (sem Docker)

API com simulador interno + dashboard:

```bash
# Terminal 1 — API
cd backend
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app:app --reload --port 8000

# Terminal 2 — Frontend
cd frontend
npm install
npm run dev
```

Abra **http://localhost:5173**

### Opção B — Pipeline completo (Kafka + Spark)

```bash
chmod +x scripts/up-pipeline.sh
./scripts/up-pipeline.sh

cd frontend && npm install && npm run dev
```

Sobe: `redpanda` · `spark` · `producer` · `api` (`:8000`)

Healthcheck: http://localhost:8000/health

### PySpark oficial (opcional)

```bash
docker build -f spark/Dockerfile.pyspark -t opsstream-spark-pyspark .
```

Por padrão o Compose usa micro-batch Spark-compatible (`spark/processor.py`) para subir rápido.

---

## Estrutura do repositório

```
marketplace-streaming/
├── specs/              # Contratos Spec Based
├── shared/             # Domínio compartilhado (Python)
├── producer/           # Produtor → Kafka
├── spark/              # Processamento de stream
├── backend/            # API Starlette + WebSocket
├── frontend/           # Dashboard React + Tailwind
├── scripts/            # up-pipeline.sh
├── docker-compose.yml
└── README.md
```

---

## Endpoints úteis

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/health` | Status, modo pipeline, clientes WS |
| GET | `/api/v1/kpis` | Snapshot dos KPIs |
| GET | `/api/v1/pedidos` | Lista para o Kanban |
| GET | `/api/v1/alertas` | Alertas ativos |
| GET | `/api/v1/hubs` | Hubs logísticos |
| WS | `/ws` | Stream em tempo real |
| POST | `/api/v1/simulator/start\|stop` | Só no modo embutido |

---

## Diferenciais competitivos (roadmap da tese)

1. Live commerce com compra em 1 clique (PIX)  
2. Frete transparente por CEP / hubs regionais  
3. Economia local e priorização MEI  
4. Kanban operacional do pedido (confiança)  
5. Campanhas BR (São João, Carnaval, Black Friday)  
6. WhatsApp commerce (evolução)  
7. LGPD by design (evolução)  
8. Cashback PIX (evolução)  

---

## Deploy (Render)

A URL do serviço serve o **mesmo dashboard React** do `localhost:5173` (build em `backend/static`).

Após alterar o frontend:

```bash
./scripts/build-dashboard.sh
git add backend/static && git commit && git push
```

Blueprint: [`render.yaml`](./render.yaml) (plano free, `PIPELINE_MODE=embedded`).

---

## Licença e contexto

Projeto acadêmico / hackathon (**Unifametro**).  
Marca do produto: **Ops Stream**.

---

## Contribuindo

1. Respeite os contratos em `specs/`  
2. Não commite `.env`, `node_modules`, `.venv` ou checkpoints Spark  
3. Prefira PRs pequenos alinhados aos contratos  

```bash
git clone <url-do-seu-repositorio>
cd marketplace-streaming
# siga "Como rodar" acima
```
