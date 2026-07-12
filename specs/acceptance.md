# Critérios de aceite — MVP Hackathon

## Pipeline

- [x] Produtor → Kafka (`vitrine.raw.events`)
- [x] Spark Streaming (micro-batch 10s + job PySpark oficial)
- [x] Tópicos processados / alerts / kpis
- [x] API consome Kafka e transmite WebSocket
- [x] Dashboard React + Tailwind consome o stream

## Contratos

- [x] OpenAPI cobre health, pedidos, kpis, alertas, hubs, simulator
- [x] AsyncAPI cobre eventos do canal `/ws`
- [x] Domínio BR: PIX, CEP, UF, live commerce
- [x] Arquitetura Kafka + Spark documentada

## Dashboard

- [x] KPIs: GMV, pedidos/min, ticket, % PIX, % live, alertas críticos
- [x] Kanban por status de pedido
- [x] Lista de alertas com destino roteado
- [x] Mapa de hubs + card de live

## Fora do escopo MVP

- Checkout real / gateway PIX
- Autenticação de usuários
- Persistência em banco
- App mobile
