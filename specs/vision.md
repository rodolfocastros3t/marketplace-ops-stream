# Visão de Produto — Ops Stream

Marketplace brasileiro com **streaming em tempo real**, posicionado como concorrente direto da Amazon Brasil, agregando cultura local, meios de pagamento nacionais e live commerce.

## Problema

A Amazon Brasil é forte em catálogo e logística, mas fraca em:

- Experiência social de compra (live commerce — domínio da Shopee)
- Integração nativa com PIX e parcelamento culturalmente esperado
- Proximidade com vendedores locais / MEI / economia de bairro
- Transparência operacional em tempo real (estoque, frete, pedidos)
- Atendimento via WhatsApp e linguagem brasileira

## Proposta

**Ops Stream** = Amazon-like (catálogo + fulfillment) + Shopee-like (live) + Méliuz-like (cashback) + cultura BR (PIX, boleto, NF-e, CEP, LGPD).

O nome comunica a proposta: **Ops** (operação do marketplace) + **Stream** (dados e compras em tempo real).

## Personas

| Persona | Necessidade |
|---------|-------------|
| Comprador urbano (SP/RJ/BH) | Entrega rápida, PIX, parcelamento, live de ofertas |
| Comprador interior / Norte-Nordeste | Frete justo por CEP, boleto, vendedores regionais |
| Vendedor MEI | Onboarding simples, NF-e, dashboard ao vivo de vendas |
| Operador logístico | Kanban de pedidos, alertas críticos, mapa de hubs |
| Gestor marketplace | KPIs ao vivo, roteamento de incidentes, campanhas sazonais |

## Diferenciais competitivos (atração de usuários Amazon)

1. **Live Commerce nativo** — vitrines ao vivo com compra em 1 clique (PIX)
2. **PIX First** — checkout em segundos; cashback imediato no PIX
3. **Frete transparente por CEP** — cotação e SLA ao vivo no stream
4. **Economia local** — filtro "perto de mim" / vendedores da mesma UF
5. **Kanban operacional público do pedido** — status em tempo real (estilo rastreio social)
6. **Campanhas BR** — Carnaval, Dia das Mães, Black Friday BR, Natal, Volta às Aulas
7. **WhatsApp Commerce** — status e suporte no canal preferido do brasileiro
8. **LGPD by design** — consentimento, portabilidade e exclusão como feature, não compliance escondido
9. **Parcelamento cultural** — até 12x sem juros em categorias estratégicas
10. **Alertas de oportunidade** — queda de preço, estoque crítico, flash sale regional

## Escopo do MVP (Hackathon / Spec Based)

Foco no **pipeline de streaming operacional**, não no e-commerce completo:

- Simulador produz eventos de marketplace a cada 1.5s
- Backend Starlette recebe, valida, aplica regras de roteamento e transmite
- Dashboard React consome WebSocket: KPIs, Kanban por status, alertas críticos, mapa de hubs

Fora do MVP (contratados para evolução): checkout real, gateway PIX, app mobile, WMS.

## Métricas de sucesso do MVP

- Latência evento→UI < 500ms (p95)
- Dashboard atualiza sem reload
- Regras de roteamento direcionam eventos críticos (fraude, atraso, estoque zero, live hot)
- KPIs refletem GMV, pedidos/min, ticket médio, % PIX, % live
