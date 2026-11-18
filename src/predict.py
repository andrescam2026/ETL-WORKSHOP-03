"""Kafka consumer -> predicción con el modelo -> carga en la base de datos.

    python predict.py             # consume el tópico de Kafka (requiere .env)
    python predict.py --fresh     # igual, con un group.id nuevo para releer desde el inicio
    python predict.py --from-db   # sin Kafka: usa los registros ya guardados en raw_records
"""
import argparse
import json
import os
import sqlite3
import time
from pathlib import Path

import joblib
import pandas as pd

from transform import integrate

DB_PATH = Path("data/happiness_raw.db")
MODEL_PATH = Path("models/happiness_model.joblib")


def load_from_db(db_path=DB_PATH):
    """Reconstruye la lista de registros a partir de la tabla raw_records."""
    conn = sqlite3.connect(db_path)
    try:
        filas = conn.execute("SELECT record_id, year, payload FROM raw_records").fetchall()
    finally:
        conn.close()
    return [{"record_id": r, "year": y, "data": json.loads(p)} for r, y, p in filas]


def predict(records, model_path=MODEL_PATH):
    """Aplica la misma limpieza del notebook y predice el score de cada registro."""
    bundle = joblib.load(model_path)
    df = integrate(records)
    df["predicted_score"] = bundle["pipeline"].predict(df[bundle["features"]])
    df["abs_error"] = (df["score"] - df["predicted_score"]).abs()
    df["split"] = df["country"].isin(bundle["test_countries"]).map({True: "test", False: "train"})
    return df.rename(columns={"score": "actual_score"})


def save_predictions(df, db_path=DB_PATH):
    """Guarda las predicciones. Cada ejecución reemplaza la tabla por completo."""
    columnas = ["record_id", "country", "year", "actual_score", "predicted_score",
                "abs_error", "split"]
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            conn.execute("DROP TABLE IF EXISTS predictions")
            conn.execute("""
                CREATE TABLE predictions (
                    record_id       TEXT PRIMARY KEY,
                    country         TEXT,
                    year            INTEGER,
                    actual_score    REAL,
                    predicted_score REAL,
                    abs_error       REAL,
                    split           TEXT   -- 'test': país no visto en el entrenamiento
                )
            """)
            conn.executemany(
                "INSERT INTO predictions VALUES (?, ?, ?, ?, ?, ?, ?)",
                df[columnas].itertuples(index=False, name=None))
        return conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fresh", action="store_true",
                    help="usa un group.id nuevo para releer el tópico desde el inicio")
    ap.add_argument("--from-db", action="store_true",
                    help="no usa Kafka: lee los registros de la tabla raw_records")
    args = ap.parse_args()

    # Falla antes de consumir o tocar la base si todavía no se entrenó el modelo
    if not MODEL_PATH.exists():
        raise SystemExit(f"No existe {MODEL_PATH}. Ejecuta primero el notebook "
                         "notebooks/analysis.ipynb (Run All) para entrenar y guardar el modelo.")

    consumer = None
    if args.from_db:
        records = load_from_db()
        print(f"Registros leídos de {DB_PATH}: {len(records)}")
    else:
        from consumer import TOPIC, build_consumer, consume, save_sqlite, verify
        group_id = os.environ["GROUP_ID"]
        if args.fresh:
            group_id = f"{group_id}-{int(time.time())}"
        consumer = build_consumer(group_id)
        consumer.subscribe([TOPIC])
        print(f"Consumiendo '{TOPIC}' con group.id='{group_id}'...")

    try:
        if consumer is not None:
            records, stats = consume(consumer)
            verify(records, stats)
            if not records:
                print("No llegaron mensajes. Usa --fresh (o cambia GROUP_ID) para releer el tópico.")
                return
            print(f"Guardado crudo en {DB_PATH}: {save_sqlite(records)} filas")

        resultado = predict(records)
        n = save_predictions(resultado)
        print(f"Predicciones guardadas en la tabla 'predictions': {n} filas")
        print("\nError absoluto medio por conjunto:")
        print(resultado.groupby("split")["abs_error"].mean().round(3).to_string())

        if consumer is not None:
            consumer.commit(asynchronous=False)
            print("Offsets confirmados en Kafka.")
    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    main()
