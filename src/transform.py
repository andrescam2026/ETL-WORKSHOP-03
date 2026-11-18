import pandas as pd

TARGET = "score"
FEATURES = ["gdp", "social_support", "life_expectancy",
            "freedom", "corruption", "generosity"]

EQUIVALENCIAS = {
    "country": ["Country", "Country or region"],
    "score": ["Happiness Score", "Happiness.Score", "Score"],
    "gdp": ["Economy (GDP per Capita)", "Economy..GDP.per.Capita.", "GDP per capita"],
    "social_support": ["Family", "Social support"],
    "life_expectancy": ["Health (Life Expectancy)", "Health..Life.Expectancy.",
                        "Healthy life expectancy"],
    "freedom": ["Freedom", "Freedom to make life choices"],
    "corruption": ["Trust (Government Corruption)", "Trust..Government.Corruption.",
                   "Perceptions of corruption"],
    "generosity": ["Generosity"],
}
RENOMBRAR = {viejo: nuevo for nuevo, viejos in EQUIVALENCIAS.items() for viejo in viejos}

CORRECCION_PAISES = {
    "Somaliland region": "Somaliland Region",
    "Hong Kong S.A.R., China": "Hong Kong",
    "Taiwan Province of China": "Taiwan",
    "Trinidad & Tobago": "Trinidad and Tobago",
    "North Cyprus": "Northern Cyprus",
    "Macedonia": "North Macedonia",
}


def integrate(records):
    """records: lista de dicts con 'record_id', 'year' y 'data' (columnas originales).

    Devuelve un DataFrame con: record_id, country, year, score y las 6 features.
    """
    filas = []
    for year, grupo in pd.DataFrame(records).groupby("year"):
        df = pd.DataFrame(list(grupo["data"]), index=grupo["record_id"].values)
        df = df.rename(columns=RENOMBRAR)[list(EQUIVALENCIAS)]  # KeyError si falta alguna
        df.insert(1, "year", year)
        filas.append(df.rename_axis("record_id").reset_index())
    datos = pd.concat(filas, ignore_index=True)
    datos["country"] = datos["country"].str.strip().replace(CORRECCION_PAISES)
    return datos
