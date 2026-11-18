# ETL Workshop 3 — Kafka + Machine Learning (World Happiness Report)

Pipeline que consume desde Apache Kafka los registros del World Happiness Report (2015-2019),
los integra, entrena un modelo de regresión para predecir el *Happiness Score* y carga
las predicciones en una base de datos.

## Flujo

```
Kafka (etl.records) ──► src/consumer.py ──► SQLite: raw_records ──► notebook (EDA + modelo) ──► models/happiness_model.joblib
                                                                                                   │
Kafka (etl.records) ──► src/predict.py (consume + limpieza + predicción) ◄─────────────────────────────┘
                                          └──► SQLite: predictions
```

| Archivo | Qué hace |
|---|---|
| `src/consumer.py` | Se conecta al broker, consume `etl.records`, acumula los registros en memoria (sin duplicados), verifica el estado y lo guarda en `raw_records`. |
| `notebooks/analysis.ipynb` | EDA, integración de los cinco años, selección de features, entrenamiento, evaluación y guardado del modelo. |
| `src/transform.py` | Limpieza e integración compartida (nombres de columnas y países). El notebook comprueba que da el mismo resultado que su propio código. |
| `src/predict.py` | Consume de Kafka, aplica `transform.py`, predice con el modelo y guarda la tabla `predictions`. |

## Cómo ejecutarlo

Todos los comandos se ejecutan desde la raíz del repositorio.

1. **Instalar dependencias** (Python 3.10+):
   ```bash
   python -m venv .venv
   source .venv/bin/activate        # en Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. **Configurar credenciales:** copiar `.env.example` a `.env` y completar `KAFKA_PASSWORD`
   (la entrega el profesor) y `GROUP_ID` (nombre completo en minúsculas con guiones).
3. **Extraer desde Kafka:** `python src/consumer.py` (con `--fresh` para releer el tópico desde el inicio).
   Crea `data/happiness_raw.db`.
4. **Notebook:** abrir `notebooks/analysis.ipynb` y ejecutar todas las celdas. Genera `models/happiness_model.joblib`.
5. **Predicciones:** `python src/predict.py` (o `python src/predict.py --from-db` para usar los datos ya guardados, sin Kafka).

Resultado: la base `data/happiness_raw.db` contiene `raw_records` (datos originales en JSON, con metadatos de Kafka)
y `predictions` (país, año, score real, score predicho, error y si el país fue de entrenamiento o de prueba).

## Decisiones principales

**Estructura de los mensajes.** Cada mensaje trae `metadata` (`record_id`, `year`, `source_file`) y `data` (las columnas
originales del CSV). El consumer valida esa estructura, descarta los inválidos y los `record_id` repetidos, y
comprueba que *recibidos = únicos + duplicados + inválidos*. Los offsets se confirman solo después de guardar en la base.

**Integración de los años.** Los cinco años se apilan en formato largo (una fila por país y año). Los CSV usan nombres
de columna distintos, que se unifican con una tabla de equivalencias (`src/transform.py`). Se conservan
`country, year, score, gdp, social_support, life_expectancy, freedom, corruption, generosity`.
Se descartan las columnas que no existen en todos los años o que son derivadas del score (ranking, error estándar,
intervalos, `Dystopia Residual`, `Region`).

**Calidad de datos.** Seis países cambian de nombre entre ediciones (por ejemplo `Macedonia` / `North Macedonia`);
se unifican (de 170 a 164 países). Hay un único nulo (`corruption`, UAE 2018), que se imputa con la mediana
**dentro del pipeline**. Los outliers (principalmente en `corruption`) son plausibles y se conservan.
Hay cambios de nivel entre años en algunas variables (p. ej. `social_support` en 2016-2017), posiblemente metodológicos.

**Modelo.** Variables predictoras: `gdp, social_support, life_expectancy, freedom, corruption, generosity`.
`year` no se usa para que el modelo sirva con ediciones futuras. El split es 80/20 **agrupado por país**
(`GroupShuffleSplit`) para evitar que el mismo país esté en entrenamiento y en prueba. Imputación y escalado van
en un `Pipeline` ajustado solo con el entrenamiento.

## Resultados (conjunto de prueba, países no vistos)

| Modelo | MAE | MSE | RMSE | R² | R² ajustado |
|---|---|---|---|---|---|
| **Linear Regression** (elegido) | 0.450 | 0.348 | 0.590 | 0.774 | 0.765 |
| Random Forest | 0.489 | 0.397 | 0.630 | 0.742 | 0.732 |

La regresión lineal supera al Random Forest, que además sobreajusta (R² de 0.96 en entrenamiento). `gdp` es la
variable más influyente. El análisis completo, los gráficos y las mejoras propuestas están al final del notebook.

## Limitaciones

- Un mismo país aparece hasta cinco veces; por eso el split agrupado, pero el R² varía entre folds (±0.10).
- Se asume que `Family` (2015-2016) y `Social support` (2017-2019) son la misma variable.
- Se pierde `Region`, que solo existe en 2015-2016.
- El modelo guardado se entrena con el 80 % de los datos para que las métricas correspondan a él; en `predictions`
  la columna `split` indica qué filas son de prueba (las de entrenamiento dan un error optimista).
- `predict.py` procesa el tópico en lote (termina tras unos segundos sin mensajes nuevos), no como servicio continuo.
