"""Vitrine API — consome Kafka processado e transmite via WebSocket."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, ValidationError
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Mount, Route, WebSocketRoute
from starlette.staticfiles import StaticFiles
from pathlib import Path
from starlette.websockets import WebSocket, WebSocketDisconnect

TZ = ZoneInfo("America/Sao_Paulo")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "kafka" if BOOTSTRAP else "embedded")

TOPICS = {
    "processed": os.getenv("KAFKA_PROCESSED_TOPIC", "vitrine.processed.events"),
    "alerts": os.getenv("KAFKA_ALERTS_TOPIC", "vitrine.alerts"),
    "kpis": os.getenv("KAFKA_KPIS_TOPIC", "vitrine.kpis"),
}

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


class ItemPedido(BaseModel):
    sku: str
    titulo: str
    quantidade: int = Field(ge=1)
    preco_centavos: int = Field(ge=1)


class Frete(BaseModel):
    cep_destino: str
    uf_destino: str
    valor_centavos: int = Field(ge=0)
    prazo_dias: int = Field(ge=0)
    transportadora: str = "Ops Stream Express"


class PedidoCreate(BaseModel):
    comprador_id: str
    itens: list[ItemPedido]
    pagamento_metodo: str
    frete: Frete
    origem: str = "CATALOGO"


class PedidoStatusPatch(BaseModel):
    status: str


class AlertaCreate(BaseModel):
    severidade: str
    tipo: str
    mensagem: str
    contexto: dict[str, Any] = Field(default_factory=dict)


class Store:
    def __init__(self) -> None:
        self.pedidos: dict[str, dict[str, Any]] = {}
        self.alertas: list[dict[str, Any]] = []
        self.hubs: dict[str, dict[str, Any]] = {}
        self.clients: set[WebSocket] = set()
        self.kpis: dict[str, Any] = {
            "gmv_centavos": 0,
            "pedidos_por_minuto": 0.0,
            "ticket_medio_centavos": 0,
            "percentual_pix": 0.0,
            "percentual_live": 0.0,
            "alertas_criticos_abertos": 0,
            "hubs_saturados": 0,
            "espectadores_live_total": 0,
            "atualizado_em": now_iso(),
        }
        self.current_live: dict[str, Any] | None = None
        self.pipeline_mode = PIPELINE_MODE
        self.kafka_running = False
        # embedded fallback
        self.simulator_task: asyncio.Task | None = None
        self.simulator_running = False


store = Store()


async def broadcast(message: dict[str, Any]) -> None:
    dead: list[WebSocket] = []
    for ws in list(store.clients):
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        store.clients.discard(ws)


async def apply_stream_message(msg: dict[str, Any]) -> None:
    event = msg.get("event")
    payload = msg.get("payload", {})

    if event == "pedido.created":
        store.pedidos[payload["id"]] = payload
        await broadcast(msg)
    elif event == "pedido.status_changed":
        prev = store.pedidos.get(payload["id"])
        if prev:
            prev["status"] = payload["status"]
            prev["atualizado_em"] = now_iso()
        await broadcast(msg)
    elif event == "alerta.routed":
        store.alertas.insert(0, payload)
        store.alertas = store.alertas[:100]
        await broadcast(msg)
    elif event == "hub.updated":
        store.hubs[payload["id"]] = payload
        await broadcast(msg)
    elif event == "live.updated":
        store.current_live = payload
        await broadcast(msg)
    elif event == "kpis.updated":
        store.kpis = {**payload, "atualizado_em": payload.get("atualizado_em", now_iso())}
        await broadcast(msg)


async def kafka_consumer_loop() -> None:
    from aiokafka import AIOKafkaConsumer

    topics = [TOPICS["processed"], TOPICS["alerts"], TOPICS["kpis"]]
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=BOOTSTRAP,
        group_id="vitrine-api",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    while True:
        try:
            await consumer.start()
            store.kafka_running = True
            print(f"[api] Kafka consumer ativo → {BOOTSTRAP} topics={topics}")
            async for record in consumer:
                await apply_stream_message(record.value)
        except Exception as exc:
            store.kafka_running = False
            print(f"[api] Kafka reconnect em 3s: {exc}")
            try:
                await consumer.stop()
            except Exception:
                pass
            await asyncio.sleep(3)
            consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=BOOTSTRAP,
                group_id="vitrine-api",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )


# --- Embedded fallback (sem Kafka) ---

async def embedded_simulator() -> None:
    import random
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    from shared.vitrine_domain import (
        INTERVAL_SECONDS,
        enrich_alerta,
        make_alerta_raw,
        make_hub_event,
        make_live_event,
        make_pedido_event,
        make_status_event,
        TRANSITIONS as T,
    )

    live = None
    while store.simulator_running:
        roll = random.random()
        if roll < 0.55:
            msg = make_pedido_event()
            await apply_stream_message(msg)
            if store.pedidos and random.random() < 0.7:
                pid = random.choice(list(store.pedidos.keys()))
                cur = store.pedidos[pid]
                opts = list(T.get(cur["status"], set()))
                if opts:
                    novo = random.choice(opts)
                    st = make_status_event(
                        pid, cur["status"], novo, cur["origem"], cur["frete"]["uf_destino"]
                    )
                    await apply_stream_message(st)
        elif roll < 0.75:
            await apply_stream_message(make_hub_event())
        elif roll < 0.88:
            await apply_stream_message(enrich_alerta(make_alerta_raw()["payload"]))
        else:
            live_msg = make_live_event(live)
            live = live_msg["payload"]
            await apply_stream_message(live_msg)

        # local KPIs
        pedidos = list(store.pedidos.values())
        total = max(len(pedidos), 1)
        gmv = sum(p.get("total_centavos", 0) for p in pedidos)
        pix = sum(1 for p in pedidos if p.get("pagamento_metodo") == "PIX")
        live_n = sum(1 for p in pedidos if p.get("origem") == "LIVE")
        await apply_stream_message(
            envelope(
                "kpis.updated",
                {
                    "gmv_centavos": gmv,
                    "pedidos_por_minuto": float(min(len(pedidos), 40)),
                    "ticket_medio_centavos": int(gmv / total),
                    "percentual_pix": round(pix / total * 100, 1),
                    "percentual_live": round(live_n / total * 100, 1),
                    "alertas_criticos_abertos": sum(
                        1 for a in store.alertas if a.get("severidade") == "CRITICO"
                    ),
                    "hubs_saturados": sum(1 for h in store.hubs.values() if h.get("saturado")),
                    "espectadores_live_total": (live or {}).get("espectadores", 0),
                    "atualizado_em": now_iso(),
                },
            )
        )
        await asyncio.sleep(INTERVAL_SECONDS)


async def start_embedded() -> None:
    if store.simulator_running:
        return
    store.simulator_running = True
    store.simulator_task = asyncio.create_task(embedded_simulator())


async def stop_embedded() -> None:
    store.simulator_running = False
    if store.simulator_task:
        store.simulator_task.cancel()
        try:
            await store.simulator_task
        except asyncio.CancelledError:
            pass
        store.simulator_task = None


# --- HTTP ---


PUBLIC_DIR = Path(__file__).resolve().parent / "public"
STATIC_DIR = Path(__file__).resolve().parent / "static"  # React build (dist)


def _dashboard_index() -> Path | None:
    for candidate in (STATIC_DIR / "index.html", PUBLIC_DIR / "index.html"):
        if candidate.exists():
            return candidate
    return None


async def root(_: Request) -> FileResponse | JSONResponse:
    index = _dashboard_index()
    if index is not None:
        return FileResponse(index, media_type="text/html; charset=utf-8")
    return JSONResponse(
        {
            "service": "Ops Stream API",
            "status": "online",
            "docs": {
                "health": "/health",
                "kpis": "/api/v1/kpis",
                "websocket": "/ws",
            },
        }
    )


async def api_info(_: Request) -> JSONResponse:
    return JSONResponse(
        {
            "service": "Ops Stream API",
            "status": "online",
            "docs": {
                "health": "/health",
                "kpis": "/api/v1/kpis",
                "pedidos": "/api/v1/pedidos",
                "alertas": "/api/v1/alertas",
                "hubs": "/api/v1/hubs",
                "websocket": "/ws",
                "dashboard": "/",
            },
            "pipeline_mode": store.pipeline_mode,
        }
    )


async def health(_: Request) -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "pipeline_mode": store.pipeline_mode,
            "websocket_clients": len(store.clients),
            "kafka_running": store.kafka_running,
            "simulator_running": store.simulator_running,
            "kafka_bootstrap": BOOTSTRAP or None,
        }
    )


async def list_pedidos(request: Request) -> JSONResponse:
    status = request.query_params.get("status")
    limit = min(int(request.query_params.get("limit", 50)), 200)
    items = list(store.pedidos.values())
    if status:
        items = [p for p in items if p["status"] == status]
    items = sorted(items, key=lambda p: p.get("criado_em", ""), reverse=True)[:limit]
    return JSONResponse({"items": items, "total": len(items)})


async def create_pedido(request: Request) -> JSONResponse:
    import uuid

    try:
        body = PedidoCreate.model_validate(await request.json())
    except ValidationError as exc:
        return JSONResponse({"detail": exc.errors()}, status_code=422)
    total = sum(i.preco_centavos * i.quantidade for i in body.itens) + body.frete.valor_centavos
    pedido = {
        "id": str(uuid.uuid4()),
        "comprador_id": body.comprador_id,
        "itens": [i.model_dump() for i in body.itens],
        "status": "CRIADO",
        "pagamento_metodo": body.pagamento_metodo,
        "frete": body.frete.model_dump(),
        "total_centavos": total,
        "origem": body.origem,
        "criado_em": now_iso(),
        "atualizado_em": now_iso(),
    }
    await apply_stream_message(envelope("pedido.created", pedido))
    return JSONResponse(pedido, status_code=201)


async def patch_pedido_status(request: Request) -> JSONResponse:
    pedido_id = request.path_params["pedido_id"]
    pedido = store.pedidos.get(pedido_id)
    if not pedido:
        return JSONResponse({"detail": "Pedido não encontrado"}, status_code=404)
    try:
        body = PedidoStatusPatch.model_validate(await request.json())
    except ValidationError as exc:
        return JSONResponse({"detail": exc.errors()}, status_code=422)
    allowed = TRANSITIONS.get(pedido["status"], set())
    if body.status not in allowed:
        return JSONResponse(
            {"detail": f"Transição inválida: {pedido['status']} → {body.status}"},
            status_code=409,
        )
    anterior = pedido["status"]
    await apply_stream_message(
        envelope(
            "pedido.status_changed",
            {
                "id": pedido_id,
                "status_anterior": anterior,
                "status": body.status,
                "origem": pedido["origem"],
                "uf_destino": pedido["frete"]["uf_destino"],
            },
        )
    )
    return JSONResponse(store.pedidos[pedido_id])


async def get_kpis(_: Request) -> JSONResponse:
    return JSONResponse(store.kpis)


async def list_alertas(request: Request) -> JSONResponse:
    sev = request.query_params.get("severidade")
    dest = request.query_params.get("destino")
    items = store.alertas
    if sev:
        items = [a for a in items if a["severidade"] == sev]
    if dest:
        items = [a for a in items if a["destino"] == dest]
    return JSONResponse({"items": items})


async def create_alerta(request: Request) -> JSONResponse:
    import sys
    import uuid
    from pathlib import Path

    try:
        body = AlertaCreate.model_validate(await request.json())
    except ValidationError as exc:
        return JSONResponse({"detail": exc.errors()}, status_code=422)

    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from shared.vitrine_domain import enrich_alerta

    raw = {
        "id": str(uuid.uuid4()),
        "severidade": body.severidade,
        "tipo": body.tipo,
        "mensagem": body.mensagem,
        "criado_em": now_iso(),
        "contexto": body.contexto,
    }
    msg = enrich_alerta(raw)
    await apply_stream_message(msg)
    return JSONResponse(msg["payload"], status_code=201)


async def list_hubs(_: Request) -> JSONResponse:
    return JSONResponse({"items": list(store.hubs.values())})


async def simulator_start(_: Request) -> JSONResponse:
    if store.pipeline_mode == "kafka":
        return JSONResponse(
            {
                "running": False,
                "detail": "Modo Kafka: use o serviço producer do Compose",
                "pipeline_mode": "kafka",
            }
        )
    await start_embedded()
    return JSONResponse({"running": True, "interval_seconds": 1.5, "pipeline_mode": "embedded"})


async def simulator_stop(_: Request) -> JSONResponse:
    if store.pipeline_mode == "kafka":
        return JSONResponse({"running": False, "pipeline_mode": "kafka"})
    await stop_embedded()
    return JSONResponse({"running": False, "pipeline_mode": "embedded"})


async def ws_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    store.clients.add(websocket)
    await websocket.send_json(envelope("kpis.updated", store.kpis))
    for hub in store.hubs.values():
        await websocket.send_json(envelope("hub.updated", hub))
    if store.current_live:
        await websocket.send_json(envelope("live.updated", store.current_live))
    for alerta in reversed(store.alertas[:20]):
        await websocket.send_json(envelope("alerta.routed", alerta))
    for pedido in list(store.pedidos.values())[-30:]:
        await websocket.send_json(envelope("pedido.created", pedido))
    try:
        while True:
            data = await websocket.receive_json()
            if data.get("event") == "client.ping":
                await websocket.send_json(envelope("server.pong", {}))
            elif data.get("event") == "client.subscribe":
                await websocket.send_json(
                    envelope("server.subscribed", {"channels": data.get("channels", [])})
                )
    except WebSocketDisconnect:
        pass
    finally:
        store.clients.discard(websocket)


async def on_startup() -> None:
    if store.pipeline_mode == "kafka" and BOOTSTRAP:
        asyncio.create_task(kafka_consumer_loop())
    else:
        store.pipeline_mode = "embedded"
        await start_embedded()


routes = [
    Route("/", root),
    Route("/api", api_info),
    Route("/health", health),
    Route("/api/v1/pedidos", list_pedidos, methods=["GET"]),
    Route("/api/v1/pedidos", create_pedido, methods=["POST"]),
    Route("/api/v1/pedidos/{pedido_id}/status", patch_pedido_status, methods=["PATCH"]),
    Route("/api/v1/kpis", get_kpis),
    Route("/api/v1/alertas", list_alertas, methods=["GET"]),
    Route("/api/v1/alertas", create_alerta, methods=["POST"]),
    Route("/api/v1/hubs", list_hubs),
    Route("/api/v1/simulator/start", simulator_start, methods=["POST"]),
    Route("/api/v1/simulator/stop", simulator_stop, methods=["POST"]),
    WebSocketRoute("/ws", ws_endpoint),
]

# Prefer React build (Docker/production); fallback HTML em public/
_react_assets = STATIC_DIR / "assets"
if _react_assets.exists():
    routes.append(Mount("/assets", app=StaticFiles(directory=str(_react_assets)), name="react-assets"))
elif PUBLIC_DIR.exists():
    routes.append(Mount("/public", app=StaticFiles(directory=str(PUBLIC_DIR)), name="public-assets"))


middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
]

app = Starlette(routes=routes, middleware=middleware, on_startup=[on_startup])
