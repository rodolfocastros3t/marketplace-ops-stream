# Shared domain helpers for producer + Spark (Spec Based)

from __future__ import annotations

import random
import uuid
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Sao_Paulo")
INTERVAL_SECONDS = 1.5

TOPICS = {
    "raw": "vitrine.raw.events",
    "processed": "vitrine.processed.events",
    "alerts": "vitrine.alerts",
    "kpis": "vitrine.kpis",
}

CIDADES = {
    "SP": ("São Paulo", -23.55, -46.63),
    "RJ": ("Rio de Janeiro", -22.91, -43.17),
    "MG": ("Belo Horizonte", -19.92, -43.94),
    "CE": ("Fortaleza", -3.72, -38.54),
    "BA": ("Salvador", -12.97, -38.50),
    "PE": ("Recife", -8.05, -34.88),
    "RS": ("Porto Alegre", -30.03, -51.23),
    "PR": ("Curitiba", -25.43, -49.27),
    "AM": ("Manaus", -3.12, -60.02),
    "DF": ("Brasília", -15.78, -47.93),
}

PRODUTOS = [
    ("SKU-CAF", "Café especial cerrado 500g", 4290),
    ("SKU-FONE", "Fone TWS cancelamento de ruído", 18990),
    ("SKU-PANEL", "Panela de pressão 4,5L", 15990),
    ("SKU-CAMI", "Camiseta algodão premium", 7990),
    ("SKU-LIVRO", "Livro — Dados em Tempo Real", 6990),
    ("SKU-KIT", "Kit churrasco inox", 24990),
    ("SKU-CREME", "Creme hidratante açaí", 3590),
    ("SKU-GAME", "Controle sem fio", 32990),
]

LIVE_TITULOS = [
    "Ofertas de São João — Casa & Cozinha",
    "Tech Week PIX — Eletrônicos",
    "Moda praia Nordeste ao vivo",
    "Black Friday Antecipada — SP",
    "Feira do MEI — Artesanato local",
]

PAGAMENTOS = ["PIX", "PIX", "PIX", "CARTAO_CREDITO", "BOLETO", "CARTAO_DEBITO"]
ORIGENS = ["CATALOGO", "CATALOGO", "LIVE", "FLASH_SALE", "WHATSAPP"]

TRANSITIONS: dict[str, set[str]] = {
    "CRIADO": {"PAGO", "CANCELADO"},
    "PAGO": {"SEPARACAO", "CANCELADO"},
    "SEPARACAO": {"ENVIADO", "CANCELADO"},
    "ENVIADO": {"EM_TRANSITO"},
    "EM_TRANSITO": {"ENTREGUE", "DEVOLUCAO"},
    "ENTREGUE": {"DEVOLUCAO"},
    "CANCELADO": set(),
    "DEVOLUCAO": set(),
}


def now_iso() -> str:
    return datetime.now(TZ).isoformat()


def envelope(event: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"event": event, "ts": now_iso(), "payload": payload}


def route_alerta(tipo: str, severidade: str) -> str:
    if tipo in ("FRAUDE_SUSPEITA", "PIX_FALHOU") and severidade == "CRITICO":
        return "FRAUDE"
    if tipo in ("ATRASO_ENTREGA", "HUB_SATURADO"):
        return "LOGISTICA"
    if tipo in ("LIVE_HOT", "FLASH_SALE", "CAMPANHA_REGIONAL"):
        return "MARKETING"
    if tipo == "ESTOQUE_CRITICO":
        return "OPERACOES"
    return "OPERACOES"


def make_pedido_event() -> dict[str, Any]:
    sku, titulo, preco = random.choice(PRODUTOS)
    uf = random.choice(list(CIDADES.keys()))
    qty = random.randint(1, 3)
    frete_valor = random.choice([0, 990, 1290, 1990])
    total = preco * qty + frete_valor
    pedido = {
        "id": str(uuid.uuid4()),
        "comprador_id": f"cli-{random.randint(1000, 9999)}",
        "itens": [
            {
                "sku": sku,
                "titulo": titulo,
                "quantidade": qty,
                "preco_centavos": preco,
            }
        ],
        "status": "CRIADO",
        "pagamento_metodo": random.choice(PAGAMENTOS),
        "frete": {
            "cep_destino": f"{random.randint(10000, 99999)}-{random.randint(100, 999)}",
            "uf_destino": uf,
            "valor_centavos": frete_valor,
            "prazo_dias": random.randint(1, 8),
            "transportadora": "Ops Stream Express",
        },
        "total_centavos": total,
        "origem": random.choice(ORIGENS),
        "criado_em": now_iso(),
        "atualizado_em": now_iso(),
    }
    return envelope("pedido.created", pedido)


def make_status_event(pedido_id: str, anterior: str, novo: str, origem: str, uf: str) -> dict[str, Any]:
    return envelope(
        "pedido.status_changed",
        {
            "id": pedido_id,
            "status_anterior": anterior,
            "status": novo,
            "origem": origem,
            "uf_destino": uf,
        },
    )


def make_hub_event(hub_id: str | None = None) -> dict[str, Any]:
    uf = random.choice(list(CIDADES.keys()))
    cidade, lat, lng = CIDADES[uf]
    capacidade = random.randint(80, 150)
    carga = random.randint(20, capacidade)
    return envelope(
        "hub.updated",
        {
            "id": hub_id or f"hub-{uf.lower()}",
            "nome": f"Hub {cidade}",
            "uf": uf,
            "cidade": cidade,
            "lat": lat,
            "lng": lng,
            "carga_atual": carga,
            "capacidade": capacidade,
            "saturado": carga >= int(capacidade * 0.85),
        },
    )


def make_alerta_raw() -> dict[str, Any]:
    tipos = [
        ("CRITICO", "FRAUDE_SUSPEITA", "Pedido com padrão atípico de cartão"),
        ("CRITICO", "PIX_FALHOU", "Falha na confirmação PIX após 60s"),
        ("ATENCAO", "ATRASO_ENTREGA", "Pedido ultrapassou SLA de frete"),
        ("ATENCAO", "ESTOQUE_CRITICO", "SKU com menos de 5 unidades"),
        ("INFO", "FLASH_SALE", "Flash sale regional iniciada"),
        ("INFO", "CAMPANHA_REGIONAL", "Campanha São João no Nordeste"),
        ("ATENCAO", "LIVE_HOT", "Live ultrapassou 5 mil espectadores"),
        ("ATENCAO", "HUB_SATURADO", "Hub regional acima de 85% da capacidade"),
    ]
    sev, tipo, msg = random.choice(tipos)
    return envelope(
        "alerta.raw",
        {
            "id": str(uuid.uuid4()),
            "severidade": sev,
            "tipo": tipo,
            "mensagem": msg,
            "criado_em": now_iso(),
            "contexto": {},
        },
    )


def make_live_event(state: dict[str, Any] | None = None) -> dict[str, Any]:
    if state is None or state.get("status") != "AO_VIVO":
        uf = random.choice(list(CIDADES.keys()))
        state = {
            "id": str(uuid.uuid4()),
            "titulo": random.choice(LIVE_TITULOS),
            "status": "AO_VIVO",
            "espectadores": random.randint(200, 2000),
            "vendedor_uf": uf,
            "pedidos_na_live": 0,
            "gmv_live_centavos": 0,
        }
    else:
        state = dict(state)
        state["espectadores"] = max(50, state["espectadores"] + random.randint(-120, 350))
        if random.random() < 0.3:
            state["pedidos_na_live"] += 1
            state["gmv_live_centavos"] += random.randint(5000, 40000)
    return envelope("live.updated", state)


def enrich_alerta(raw_payload: dict[str, Any]) -> dict[str, Any]:
    destino = route_alerta(raw_payload["tipo"], raw_payload["severidade"])
    return envelope(
        "alerta.routed",
        {
            **raw_payload,
            "destino": destino,
        },
    )
