"""Règles de qualité (Great Expectations) appliquées à l'entrée de la couche Silver.

Une seule définition (CHECKS) sert à deux choses :
  1. run_ge()     : exécute les expectations Great Expectations et produit un rapport ;
  2. spark_valid(): condition Spark équivalente, pour mettre en quarantaine les lignes invalides.
"""
import os

from pyspark.sql import functions as F

from schema import SENSORS

# Bornes larges (0.2x .. 1.8x le nominal). À recalibrer sur vos données réelles.
LO, HI = 0.2, 1.8

TEL_CHECKS = (
    [("not_null", "engine_id"), ("not_null", "event_ts"), ("not_null", "cycle"), ("between", "cycle", 1, 10000)]
    + [("not_null", s) for s in SENSORS]
    + [("between", s, nom * LO, nom * HI) for s, (nom, _) in SENSORS.items()]
)

ENG_CHECKS = [
    ("not_null", "engine_id"),
    ("in_set", "status", ["RUNNING", "FAILED", "MAINTENANCE"]),
]


def spark_valid(checks):
    cond = F.lit(True)
    for c in checks:
        kind, col = c[0], F.col(c[1])
        if kind == "not_null":
            cond = cond & col.isNotNull()
        elif kind == "between":
            cond = cond & col.between(c[2], c[3])
        elif kind == "in_set":
            cond = cond & col.isin(list(c[2]))
    return F.coalesce(cond, F.lit(False))


def run_ge(df, checks):
    """Retourne une liste de résultats [{expectation, success, unexpected_pct}]."""
    if os.getenv("GE_ENABLED", "1") != "1":
        return []
    from great_expectations.dataset import SparkDFDataset  # API legacy, présente en 0.18.x

    ds = SparkDFDataset(df)
    out = []
    for c in checks:
        kind, col = c[0], c[1]
        if kind == "not_null":
            r = ds.expect_column_values_to_not_be_null(col)
        elif kind == "between":
            r = ds.expect_column_values_to_be_between(col, c[2], c[3])
        else:
            r = ds.expect_column_values_to_be_in_set(col, list(c[2]))
        out.append({
            "expectation": f"{kind}:{col}",
            "success": bool(r.success),
            "unexpected_pct": float(r.result.get("unexpected_percent") or 0.0),
        })
    return out