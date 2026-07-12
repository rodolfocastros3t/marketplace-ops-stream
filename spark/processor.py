"""
Processador de streaming (micro-batch 10s) — mesma lógica do job Spark.

Usado no Docker Compose como serviço `spark` para subir rápido.
O job oficial PySpark está em `streaming_job.py` (ENGINE=pyspark).
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.vitrine_domain import TOPICS, route_alerta  # noqa: E402

TZ = ZoneInfo("America/Sao_Paulo")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BATCH_SECONDS = float(os.getenv("SPARK_BATCH_SECONDS", "10"))


def now_iso() -> str:
    return datetime.now(TZ).isoformat()


def wait_producer() -> KafkaProducer:
    last = None
    for _ in range(40):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP.split(","),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda v: (v or "").encode("utf-8"),
            )
        except NoBrokersAvailable as exc:
            last = exc
            print(f"[spark-microbatch] aguardando Kafka {BOOTSTRAP}...")
            time.sleep(2)
    raise RuntimeError(last)


def wait_consumer() -> KafkaConsumer:
    last = None
    for _ in range(40):
        try:
            return KafkaConsumer(
                TOPICS["raw"],
                bootstrap_servers=BOOTSTRAP.split(","),
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="vitrine-spark-microbatch",
                consumer_timeout_ms=int(BATCH_SECONDS * 1000),
            )
        except NoBrokersAvailable as exc:
            last = exc
            print(f"[spark-microbatch] aguardando Kafka {BOOTSTRAP}...")
            time.sleep(2)
    raise RuntimeError(last)


def main() -> None:
    producer = wait_producer()
    print(
        f"[spark-microbatch] ativo | kafka={BOOTSTRAP} | batch={BATCH_SECONDS}s | "
        f"raw→processed/alerts/kpis"
    )

    state = {
        "gmv_centavos": 0,
        "pedidos_total": 0,
        "pedidos_pix": 0,
        "pedidos_live": 0,
        "alertas_criticos": 0,
        "hubs_saturados": set(),
        "espectadores_live_total": 0,
    }
    recent_pedidos: deque[float] = deque()

    while True:
        consumer = wait_consumer()
        batch = []
        try:
            for record in consumer:
                batch.append(record.value)
        except Exception:
            pass
        finally:
            consumer.close()

        pedidos_batch = 0
        now = time.time()

        for msg in batch:
            event = msg.get("event")
            payload = msg.get("payload", {})

            if event == "alerta.raw":
                destino = route_alerta(payload.get("tipo", ""), payload.get("severidade", ""))
                routed = {
                    "event": "alerta.routed",
                    "ts": now_iso(),
                    "payload": {**payload, "destino": destino},
                }
                producer.send(TOPICS["alerts"], key="alerta", value=routed)
                if payload.get("severidade") == "CRITICO":
                    state["alertas_criticos"] += 1
                continue

            if event == "pedido.created":
                pedidos_batch += 1
                recent_pedidos.append(now)
                state["pedidos_total"] += 1
                state["gmv_centavos"] += int(payload.get("total_centavos", 0))
                if payload.get("pagamento_metodo") == "PIX":
                    state["pedidos_pix"] += 1
                if payload.get("origem") == "LIVE":
                    state["pedidos_live"] += 1
                producer.send(TOPICS["processed"], key="pedido", value=msg)

            elif event in ("pedido.status_changed", "hub.updated", "live.updated"):
                if event == "hub.updated":
                    hub_id = payload.get("id")
                    if payload.get("saturado"):
                        state["hubs_saturados"].add(hub_id)
                    else:
                        state["hubs_saturados"].discard(hub_id)
                if event == "live.updated":
                    state["espectadores_live_total"] = int(payload.get("espectadores", 0))
                    if state["espectadores_live_total"] > 5000:
                        producer.send(
                            TOPICS["alerts"],
                            key="alerta",
                            value={
                                "event": "alerta.routed",
                                "ts": now_iso(),
                                "payload": {
                                    "id": payload.get("id"),
                                    "severidade": "ATENCAO",
                                    "tipo": "LIVE_HOT",
                                    "mensagem": f"Live '{payload.get('titulo')}' em alta",
                                    "destino": "MARKETING",
                                    "criado_em": now_iso(),
                                    "contexto": {"live_id": payload.get("id")},
                                },
                            },
                        )
                producer.send(TOPICS["processed"], key=event, value=msg)

        while recent_pedidos and recent_pedidos[0] < now - 60:
            recent_pedidos.popleft()

        total = max(state["pedidos_total"], 1)
        kpis = {
            "event": "kpis.updated",
            "ts": now_iso(),
            "payload": {
                "gmv_centavos": state["gmv_centavos"],
                "pedidos_por_minuto": float(len(recent_pedidos)),
                "ticket_medio_centavos": int(state["gmv_centavos"] / total),
                "percentual_pix": round(state["pedidos_pix"] / total * 100, 1),
                "percentual_live": round(state["pedidos_live"] / total * 100, 1),
                "alertas_criticos_abertos": state["alertas_criticos"],
                "hubs_saturados": len(state["hubs_saturados"]),
                "espectadores_live_total": state["espectadores_live_total"],
                "atualizado_em": now_iso(),
            },
        }
        producer.send(TOPICS["kpis"], key="kpis", value=kpis)
        producer.flush()
        print(f"[spark-microbatch] batch={len(batch)} pedidos={pedidos_batch} gmv={state['gmv_centavos']}")


if __name__ == "__main__":
    main()
