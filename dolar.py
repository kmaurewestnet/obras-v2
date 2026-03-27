#!/usr/bin/python3
"""
dolar.py — Inserta cotización del dólar oficial del día anterior en records.
"""

import os
import logging
import requests
from datetime import date, timedelta
from dotenv import load_dotenv
from db import get_records_conn

load_dotenv()
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

yesterday = date.today() - timedelta(days=1)
fecha_str = str(yesterday).replace("-", "/")
url = f"https://api.argentinadatos.com/v1/cotizaciones/dolares/oficial/{fecha_str}"

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
except Exception as exc:
    log.error("dolar: error al obtener cotización: %s", exc)
    exit(1)

fecha_d = data["fecha"]
compra = data["compra"]
venta = data["venta"]

conn = get_records_conn()
with conn.cursor() as cur:
    if DRY_RUN:
        log.info("[DRY_RUN] dolar: insertaría fecha=%s compra=%s venta=%s", fecha_d, compra, venta)
    else:
        try:
            cur.execute(
                """
                INSERT INTO dolar_oficial (fecha, compra, venta)
                VALUES (%(fecha)s, %(compra)s, %(venta)s);
                """,
                {"fecha": fecha_d, "compra": compra, "venta": venta},
            )
            log.info("dolar: cotización del %s insertada (compra=%s, venta=%s).", fecha_d, compra, venta)
        except Exception as exc:
            log.error("dolar: error INSERT: %s", exc)
