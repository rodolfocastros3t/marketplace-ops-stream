"""
Spark Structured Streaming — Vitrine

Lê:  vitrine.raw.events
Escreve: vitrine.processed.events | vitrine.alerts | vitrine.kpis

Trigger: processingTime = 10 seconds (micro-batches Spark)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.vitrine_domain import TOPICS, route_alerta  # noqa: E402

TZ = ZoneInfo("America/Sao_Paulo")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/vitrine-spark-checkpoint")


def now_iso() -> str:
    return datetime.now(TZ).isoformat()


def main() -> None:
    spark = (
        SparkSession.builder.appName("vitrine-marketplace-streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPICS["raw"])
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.selectExpr("CAST(value AS STRING) as json")

    kpi_state = {
        "gmv_centavos": 0,
        "pedidos_total": 0,
        "pedidos_pix": 0,
        "pedidos_live": 0,
        "alertas_criticos": 0,
        "hubs_saturados": 0,
        "espectadores_live_total": 0,
    }

    def process_batch(batch_df, epoch_id: int) -> None:
        rows = [json.loads(r.json) for r in batch_df.collect()]
        if not rows:
            return

        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda v: (v or "").encode("utf-8"),
        )

        pedidos_na_janela = 0

        for msg in rows:
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
                    kpi_state["alertas_criticos"] += 1
                continue

            if event == "pedido.created":
                pedidos_na_janela += 1
                kpi_state["pedidos_total"] += 1
                kpi_state["gmv_centavos"] += int(payload.get("total_centavos", 0))
                if payload.get("pagamento_metodo") == "PIX":
                    kpi_state["pedidos_pix"] += 1
                if payload.get("origem") == "LIVE":
                    kpi_state["pedidos_live"] += 1
                producer.send(TOPICS["processed"], key="pedido", value=msg)

            elif event == "pedido.status_changed":
                producer.send(TOPICS["processed"], key="pedido_status", value=msg)

            elif event == "hub.updated":
                if payload.get("saturado"):
                    kpi_state["hubs_saturados"] += 1
                producer.send(TOPICS["processed"], key="hub", value=msg)

            elif event == "live.updated":
                kpi_state["espectadores_live_total"] = int(payload.get("espectadores", 0))
                producer.send(TOPICS["processed"], key="live", value=msg)
                if int(payload.get("espectadores", 0)) > 5000:
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
            else:
                producer.send(TOPICS["processed"], key="other", value=msg)

        total = max(kpi_state["pedidos_total"], 1)
        producer.send(
            TOPICS["kpis"],
            key="kpis",
            value={
                "event": "kpis.updated",
                "ts": now_iso(),
                "payload": {
                    "gmv_centavos": kpi_state["gmv_centavos"],
                    "pedidos_por_minuto": float(pedidos_na_janela) * 6.0,
                    "ticket_medio_centavos": int(kpi_state["gmv_centavos"] / total),
                    "percentual_pix": round(kpi_state["pedidos_pix"] / total * 100, 1),
                    "percentual_live": round(kpi_state["pedidos_live"] / total * 100, 1),
                    "alertas_criticos_abertos": kpi_state["alertas_criticos"],
                    "hubs_saturados": kpi_state["hubs_saturados"],
                    "espectadores_live_total": kpi_state["espectadores_live_total"],
                    "atualizado_em": now_iso(),
                    "spark_epoch": epoch_id,
                },
            },
        )
        producer.flush()
        producer.close()
        print(f"[spark] epoch={epoch_id} events={len(rows)} gmv={kpi_state['gmv_centavos']}")

    query = (
        parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"Spark Structured Streaming ativo | kafka={BOOTSTRAP} | raw={TOPICS['raw']}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
