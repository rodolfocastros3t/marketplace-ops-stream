"""Produtor → Kafka topic vitrine.raw.events"""

from __future__ import annotations

import json
import os
import random
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.vitrine_domain import (  # noqa: E402
    INTERVAL_SECONDS,
    TOPICS,
    TRANSITIONS,
    make_alerta_raw,
    make_hub_event,
    make_live_event,
    make_pedido_event,
    make_status_event,
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_RAW_TOPIC", TOPICS["raw"])


def build_producer() -> KafkaProducer:
    last_err: Exception | None = None
    for _ in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP.split(","),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda v: (v or "").encode("utf-8"),
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable as exc:
            last_err = exc
            print(f"Aguardando Kafka em {BOOTSTRAP}...")
            time.sleep(2)
    raise RuntimeError(f"Kafka indisponível: {last_err}")


def main() -> None:
    producer = build_producer()
    print(f"Produtor ativo → {BOOTSTRAP} topic={TOPIC} interval={INTERVAL_SECONDS}s")
    pedidos: dict[str, dict] = {}
    live_state = None

    while True:
        roll = random.random()
        if roll < 0.55:
            msg = make_pedido_event()
            pedidos[msg["payload"]["id"]] = msg["payload"]
            producer.send(TOPIC, key="pedido", value=msg)
            if pedidos and random.random() < 0.7:
                pid = random.choice(list(pedidos.keys()))
                current = pedidos[pid]
                options = list(TRANSITIONS.get(current["status"], set()))
                if options:
                    novo = random.choice(options)
                    anterior = current["status"]
                    current["status"] = novo
                    status_msg = make_status_event(
                        pid, anterior, novo, current["origem"], current["frete"]["uf_destino"]
                    )
                    producer.send(TOPIC, key="pedido_status", value=status_msg)
        elif roll < 0.75:
            msg = make_hub_event()
            producer.send(TOPIC, key="hub", value=msg)
            if msg["payload"]["saturado"] and random.random() < 0.4:
                raw = make_alerta_raw()
                raw["payload"]["tipo"] = "HUB_SATURADO"
                raw["payload"]["severidade"] = "ATENCAO"
                raw["payload"]["mensagem"] = f"{msg['payload']['nome']} saturado"
                # raw goes to spark for routing; also ok to send raw only
                producer.send(TOPIC, key="alerta", value=raw)
        elif roll < 0.88:
            producer.send(TOPIC, key="alerta", value=make_alerta_raw())
        else:
            live_msg = make_live_event(live_state)
            live_state = live_msg["payload"]
            producer.send(TOPIC, key="live", value=live_msg)

        producer.flush()
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
