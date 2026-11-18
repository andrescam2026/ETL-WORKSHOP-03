import argparse
import json
import os
import sqlite3
import time
from collections import Counter
from pathlib import Path

import certifi
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

load_dotenv()

TOPIC = "etl.records"
DB_PATH = Path("data/happiness_raw.db")
IDLE_SECONDS = 20       # sin mensajes nuevos durante este tiempo -> se asume que se leyó todo
MAX_WAIT_FIRST = 60     # tiempo máximo esperando el primer mensaje


def build_consumer(group_id):
    return Consumer({
        "bootstrap.servers": "kafka-classroom-2026.westus.cloudapp.azure.com:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "classroom-students",
        "sasl.password": os.environ["KAFKA_PASSWORD"],
        "ssl.ca.location": certifi.where(),
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # se confirma al final, después de guardar
    })


def consume(consumer, idle_seconds=IDLE_SECONDS, max_wait_first=MAX_WAIT_FIRST):
    """Consume el tópico y acumula los registros EN MEMORIA (lista de diccionarios).

    Cada elemento: {"record_id", "year", "source_file", "partition", "offset",
                    "kafka_ts", "data"}  donde "data" son las columnas originales del CSV.
    Un record_id repetido se ignora (no duplica el registro).
    """
    records, seen = [], set()
    stats = {"received": 0, "duplicates": 0, "invalid": 0, "errors": 0}
    last = time.time()

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            limit = idle_seconds if stats["received"] else max_wait_first
            if time.time() - last > limit:
                break
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            stats["errors"] += 1
            print("Error de Kafka:", msg.error())
            continue

        last = time.time()
        stats["received"] += 1

        # 1. Decodificar y validar la estructura esperada
        try:
            value = json.loads(msg.value().decode("utf-8"))
            meta, data = value["metadata"], value["data"]
            if not isinstance(meta, dict) or not isinstance(data, dict):
                raise ValueError("'metadata' y 'data' deben ser objetos")
        except Exception as e:
            stats["invalid"] += 1
            print(f"Mensaje inválido (offset {msg.offset()}): {e!r}")
            continue

        # 2. Acumular en memoria (sin duplicados)
        record_id = meta.get("record_id") or f"{msg.partition()}-{msg.offset()}"
        if record_id in seen:
            stats["duplicates"] += 1
            continue
        seen.add(record_id)
        records.append({
            "record_id": record_id,
            "year": meta.get("year"),
            "source_file": meta.get("source_file"),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "kafka_ts": msg.timestamp()[1],
            "data": data,
        })

        if len(records) == 1:
            print("Primer mensaje:", json.dumps(value, indent=2)[:800])

    return records, stats


def verify(records, stats):
    """Verifica el estado construido en memoria."""
    print("\n=== Verificación del estado en memoria ===")
    print(f"Mensajes recibidos        : {stats['received']}")
    print(f"Registros únicos (estado) : {len(records)}")
    print(f"Duplicados ignorados      : {stats['duplicates']}")
    print(f"Mensajes inválidos        : {stats['invalid']}")
    por_anio = Counter(r["year"] for r in records)
    print("Registros por año         :", dict(sorted(por_anio.items(), key=lambda x: str(x[0]))))
    ok = stats["received"] == len(records) + stats["duplicates"] + stats["invalid"]
    print("Consistencia (recibidos = únicos + duplicados + inválidos):",
          "OK" if ok else "REVISAR")
    return ok


def save_sqlite(records, db_path=DB_PATH):
    """Guarda el estado en SQLite. Cada ejecución reemplaza la tabla por completo."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        with conn:  # una sola transacción: o se guarda todo o nada
            conn.execute("DROP TABLE IF EXISTS raw_records")
            conn.execute("""
                CREATE TABLE raw_records (
                    record_id   TEXT PRIMARY KEY,
                    year        INTEGER,
                    source_file TEXT,
                    kafka_partition INTEGER,
                    kafka_offset    INTEGER,
                    kafka_ts    INTEGER,
                    payload     TEXT NOT NULL      -- columnas originales del CSV, en JSON
                )
            """)
            conn.executemany(
                "INSERT INTO raw_records VALUES (?, ?, ?, ?, ?, ?, ?)",
                [(r["record_id"], r["year"], r["source_file"], r["partition"],
                  r["offset"], r["kafka_ts"], json.dumps(r["data"])) for r in records])
        return conn.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0]
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fresh", action="store_true",
                    help="usa un group.id nuevo para releer el tópico desde el inicio")
    args = ap.parse_args()

    group_id = os.environ["GROUP_ID"]
    if args.fresh:
        group_id = f"{group_id}-{int(time.time())}"

    consumer = build_consumer(group_id)
    consumer.subscribe([TOPIC])
    print(f"Consumiendo '{TOPIC}' con group.id='{group_id}'...")

    try:
        records, stats = consume(consumer)
        verify(records, stats)

        if not records:
            print("\nNo llegaron mensajes: la base de datos no se modificó. "
                  "Usa --fresh (o cambia GROUP_ID) para releer el tópico.")
            return

        en_bd = save_sqlite(records)
        print(f"\nGuardado en {DB_PATH}: {en_bd} filas "
              f"({'coincide' if en_bd == len(records) else 'NO coincide'} con el estado en memoria)")

        try:
            consumer.commit(asynchronous=False)
            print("Offsets confirmados en Kafka.")
        except KafkaException as e:
            print("Aviso: no se pudieron confirmar los offsets:", e)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()