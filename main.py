#!/usr/bin/python3
"""
main.py — Punto de entrada del job mensual de obras.

Corre el día 28 de cada mes via cron.
Fuente de obras: tablas Odoo DB (odoo_tasks / odoo_task_properties).
"""

import os
import logging
import sys
from dotenv import load_dotenv
from db import close_all
from utils import (
    cardid_list,
    cardid_rec_list,
    cardid_details,
    card_descriptions,
    get_naps,
    get_vnos,
    get_fechas,
    get_fechas_bw,
    add_card_ids,
    add_card_fin_obra,
    get_naps_ocupacion,
    get_naps_obras,
    get_ocupacion,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
if DRY_RUN:
    log.warning("═══ MODO DRY_RUN ACTIVO: no se escribirá en la DB ═══")

try:
    # ── 1. Sincronizar obras desde Odoo DB → records ──────────────────────────
    log.info("Obteniendo lista de obras desde Odoo DB...")
    odoo_ids = cardid_list()
    rec_ids = cardid_rec_list()
    log.info("Odoo: %d obras | Records: %d registros", len(odoo_ids), len(rec_ids))

    ids_nuevos = [i for i in odoo_ids if i not in set(rec_ids)]
    ids_actualizar = odoo_ids  # actualizar descripción de todas

    if ids_nuevos:
        log.info("Insertando %d obras nuevas en cards...", len(ids_nuevos))
        cardid_details(ids_nuevos)

    log.info("Actualizando descripciones de %d obras...", len(ids_actualizar))
    card_descriptions(ids_actualizar)

    # ── 2. NAPs ───────────────────────────────────────────────────────────────
    log.info("Cargando datos de NAPs desde Soldef...")
    get_naps()

    log.info("Obteniendo VNOs desde Napear...")
    get_vnos()

    log.info("Obteniendo fechas de alta Westnet...")
    get_fechas()

    log.info("Obteniendo fechas de alta Bigway...")
    get_fechas_bw()

    log.info("Asociando card_id a naps_obras...")
    add_card_ids()

    # ── 3. Fin de obra (deshabilitado temporalmente) ──────────────────────────
    add_card_fin_obra()

    # ── 4. Ocupación ─────────────────────────────────────────────────────────
    log.info("Actualizando ocupación de NAPs...")
    get_naps_ocupacion()

    log.info("Cargando NAPs de obras en nap_ocupacion_obras...")
    get_naps_obras()

    log.info("Actualizando ocupación en nap_ocupacion_obras...")
    get_ocupacion()

    log.info("═══ Job finalizado correctamente. ═══")

except Exception as exc:
    log.exception("Error inesperado en main: %s", exc)
    sys.exit(1)

finally:
    close_all()
    log.info("Conexiones cerradas.")
