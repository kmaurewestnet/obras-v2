#!/usr/bin/python3
"""
utils.py — Funciones de negocio del proyecto obras.

Fuente de obras: tablas odoo_tasks / odoo_task_properties (DB datosodoo).
Todas las conexiones provienen del módulo db.py (sin N+1 connections).
"""

import os
import logging
import requests
from typing import List
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from db import (
    get_records_conn,
    get_soldef_conn,
    get_mesa_conn,
    get_gestion_conn,
    get_gestionbw_conn,
    get_nap_conn,
    get_odoo_conn,
)

load_dotenv()
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 1 — Obras desde Odoo DB
# ══════════════════════════════════════════════════════════════════════════════

def cardid_list():
    """Retorna lista de IDs de tareas de Odoo (antes leía de Planka)."""
    conn = get_odoo_conn()
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT id::text FROM odoo_tasks;")
        except Exception as exc:
            log.error("cardid_list — error SQL: %s", exc)
            return []
        rows = cur.fetchall()
    return [row[0] for row in rows] if rows else []


def cardid_rec_list():
    """Retorna lista de card_id ya registrados en records (sin cambios)."""
    conn = get_records_conn()
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT DISTINCT card_id::text FROM card_description;")
        except Exception as exc:
            log.error("cardid_rec_list — error SQL: %s", exc)
            return []
        rows = cur.fetchall()
    return [row[0] for row in rows] if rows else []


def cardid_details(ids: list):
    """
    Inserta en tabla cards los registros nuevos.
    ids: lista de task_id (str) que no están en records todavía.
    """
    if not ids:
        return 1

    conn_odoo = get_odoo_conn()
    with conn_odoo.cursor() as cur:
        try:
            cur.execute(
                """
                SELECT id::text, name
                FROM odoo_tasks
                WHERE id = ANY(%s);
                """,
                (ids,),
            )
        except Exception as exc:
            log.error("cardid_details — error SQL Odoo: %s", exc)
            return 1
        rows = cur.fetchall()

    if not rows:
        return 1

    # Batch insert a records
    values = [(row[0], row[0], "Odoo", row[1]) for row in rows]
    conn_rec = get_records_conn()
    with conn_rec.cursor() as cur:
        try:
            if DRY_RUN:
                log.info("[DRY_RUN] cardid_details: insertaría %d filas en cards", len(values))
            else:
                execute_values(
                    cur,
                    """
                    INSERT INTO cards (list_id, card_id, list_name, card_name)
                    VALUES %s
                    ON CONFLICT (card_id) DO NOTHING;
                    """,
                    values,
                )
        except Exception as exc:
            log.error("cardid_details — error INSERT records: %s", exc)
            return 1
    return 0


def card_descriptions(ids: list):
    """
    Upsert de descripción de obras desde odoo_tasks + odoo_task_properties.
    ids: lista de task_id (str) a procesar.
    """
    if not ids:
        return 1

    conn_odoo = get_odoo_conn()
    with conn_odoo.cursor() as cur:
        try:
            cur.execute(
                """
                SELECT
                    t.id::text,
                    t.name,
                    MAX(CASE WHEN p.label = 'Tipo de obra'
                        THEN COALESCE(p.selection_label, p.value_text) END)   AS tipo_obra,
                    MAX(CASE WHEN p.label = 'Coordenadas'
                        THEN p.value_text END)                                 AS geo,
                    NULL::text                                                  AS cantidad,
                    MAX(CASE WHEN p.label = 'Permisos OK'
                        THEN p.value_boolean::text END)                        AS posteo,
                    MAX(CASE WHEN p.label = 'Fecha solicitud cotizaciòn'
                        THEN p.value_date::text END)                           AS fecha_presupuesto,
                    MAX(CASE WHEN p.label = 'Cotizaciòn OBRA USD:'
                        THEN p.value_number::text END)                         AS valor_presupuesto,
                    MAX(CASE WHEN p.label = 'Nomenclatura NAPs'
                        THEN p.value_text END)                                 AS naps,
                    NULL::text                                                  AS solicitado_por
                FROM odoo_tasks t
                JOIN odoo_task_properties p ON p.task_id = t.id
                WHERE t.id = ANY(%s) AND p.type != 'separator'
                GROUP BY t.id, t.name;
                """,
                (ids,),
            )
        except Exception as exc:
            log.error("card_descriptions — error SQL Odoo: %s", exc)
            return 1
        rows = cur.fetchall()

    if not rows:
        return 1

    # Batch upsert a records
    values = [
        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
        for row in rows
    ]
    conn_rec = get_records_conn()
    with conn_rec.cursor() as cur:
        try:
            if DRY_RUN:
                log.info("[DRY_RUN] card_descriptions: upsertaría %d filas", len(values))
            else:
                execute_values(
                    cur,
                    """
                    INSERT INTO card_description
                        (card_id, nombre_obra, tipo_obra, geo, cantidad, posteo,
                         fecha_presupuesto, valor_presupuesto, naps, solicitado_por)
                    VALUES %s
                    ON CONFLICT (card_id) DO UPDATE SET
                        nombre_obra       = EXCLUDED.nombre_obra,
                        tipo_obra         = EXCLUDED.tipo_obra,
                        geo               = EXCLUDED.geo,
                        cantidad          = EXCLUDED.cantidad,
                        posteo            = EXCLUDED.posteo,
                        fecha_presupuesto = EXCLUDED.fecha_presupuesto,
                        valor_presupuesto = EXCLUDED.valor_presupuesto,
                        naps              = EXCLUDED.naps,
                        solicitado_por    = EXCLUDED.solicitado_por;
                    """,
                    values,
                )
        except Exception as exc:
            log.error("card_descriptions — error UPSERT records: %s", exc)
            return 1
    return 0


def add_card_fin_obra():
    """
    DESHABILITADA temporalmente: la DB datosodoo no contiene etapas de Odoo,
    por lo que no se puede determinar el fin de obra.
    """
    log.warning("add_card_fin_obra: deshabilitada (sin etapas en DB Odoo local).")


# ══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 2 — NAPs desde Soldef  (helper compartido, elimina la query duplicada)
# ══════════════════════════════════════════════════════════════════════════════

_SOLDEF_NAP_QUERY = """
SELECT
    d.etiqueta                                                             AS nap,
    dn.cant_bocas                                                          AS bocas,
    COALESCE(o.ocupacion, 0)                                               AS ocupacion,
    db.id                                                                  AS id,
    CASE WHEN donu.codigo_cliente !~ '^[0-9]+$' THEN NULL
         ELSE donu.codigo_cliente END                                       AS cliente,
    dp.etiqueta                                                            AS precinto
FROM dispositivos d
LEFT JOIN dispositivos_naps     dn   ON d.id = dn.dispositivo_id
LEFT JOIN dispositivos_bocas    db   ON dn.id = db.naps_id
LEFT JOIN dispositivos_precintos dp  ON db.precinto_id = dp.id
LEFT JOIN dispositivos_onuses   donu ON db.onu_id      = donu.id
LEFT JOIN (
    SELECT
        d2.etiqueta,
        COUNT(CASE WHEN UPPER(e.nombre) IN ('RESERVADO','OCUPADO') THEN 1 END) AS ocupacion
    FROM dispositivos d2
    INNER JOIN dispositivos_naps    n ON d2.id = n.dispositivo_id
    INNER JOIN dispositivos_bocas   b ON n.id  = b.naps_id
    INNER JOIN dispositivos_estados e ON b.estado_id = e.id
    WHERE d2.id = n.dispositivo_id
      AND d2.etiqueta = ANY(%s)
    GROUP BY d2.etiqueta, n.cant_bocas
) o ON d.etiqueta = o.etiqueta
WHERE d.tipo_id = 3
  AND d.etiqueta = ANY(%s);
"""


def _query_soldef_naps(naps_list: list) -> list:
    """Consulta Soldef para una lista de etiquetas NAP. Retorna lista de dicts."""
    if not naps_list:
        return []
    conn = get_soldef_conn()
    with conn.cursor() as cur:
        try:
            cur.execute(_SOLDEF_NAP_QUERY, (naps_list, naps_list))
        except Exception as exc:
            log.error("_query_soldef_naps — error SQL: %s", exc)
            return []
        rows = cur.fetchall()
    return [
        {
            "nap": r[0], "bocas": r[1], "ocupacion": r[2],
            "id": r[3], "cliente": r[4], "precinto": r[5],
        }
        for r in rows
    ]


# ══════════════════════════════════════════════════════════════════════════════
# SECCIÓN 3 — Funciones principales
# ══════════════════════════════════════════════════════════════════════════════

def _get_all_naps_from_records() -> List[str]:
    """Lee todas las NAPs de card_description y retorna lista de etiquetas."""
    conn = get_records_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT naps FROM card_description;")
        rows = cur.fetchall()

    result = []
    for row in rows:
        raw = (row[0] or "").replace(" ", "")
        for nap in raw.split(","):
            if nap:
                result.append(nap)
    return result


def get_naps():
    """Carga NAPs desde Soldef y hace upsert en naps_obras."""
    all_naps = _get_all_naps_from_records()
    if not all_naps:
        log.info("get_naps: no hay NAPs en card_description.")
        return

    detalles = _query_soldef_naps(all_naps)
    if not detalles:
        return

    values = [
        (d["nap"], d["bocas"], d["ocupacion"], d["precinto"], d["cliente"], d["id"])
        for d in detalles
    ]
    conn = get_records_conn()
    with conn.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] get_naps: upsertaría %d filas en naps_obras", len(values))
        else:
            execute_values(
                cur,
                """
                INSERT INTO naps_obras AS n (nap, bocas, ocupacion, precinto, codigo_cliente, id)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    ocupacion       = EXCLUDED.ocupacion,
                    nap             = EXCLUDED.nap,
                    precinto        = EXCLUDED.precinto,
                    codigo_cliente  = EXCLUDED.codigo_cliente;
                """,
                values,
            )


def get_naps_ocupacion():
    """Actualiza ocupacion y bocas de naps_obras desde Soldef."""
    all_naps = _get_all_naps_from_records()
    if not all_naps:
        return

    detalles = _query_soldef_naps(all_naps)
    if not detalles:
        return

    values = [(d["ocupacion"], d["bocas"], d["nap"]) for d in detalles]
    conn = get_records_conn()
    with conn.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] get_naps_ocupacion: actualizaría %d filas", len(values))
        else:
            cur.executemany(
                "UPDATE naps_obras SET ocupacion = %s, bocas = %s WHERE nap = %s;",
                values,
            )


def get_vnos():
    """Actualiza empresa, fecha_alta y fecha_baja en naps_obras desde Napear."""
    conn_rec = get_records_conn()
    with conn_rec.cursor() as cur:
        cur.execute(
            r"SELECT String_agg(id::text, ',') FROM naps_obras WHERE id ~ '^\d+$';"
        )
        row = cur.fetchone()
    if not row or not row[0]:
        return

    ids_list = [int(x) for x in row[0].split(",")]

    conn_nap = get_nap_conn()
    with conn_nap.cursor() as cursor:
        cursor.execute(
            """
            SELECT rr.external_connector_id AS id, rr.fecha_desde, rb.fecha_baja, emp.nombre
            FROM napear.registros reg
            LEFT JOIN registro_reservas rr  ON rr.registro_id  = reg.id
            LEFT JOIN registro_bajas    rb  ON rr.registro_id  = rb.registro_id
            LEFT JOIN empresas          emp ON reg.empresa_id   = emp.id
            LEFT JOIN estados_configs   ec  ON reg.estado_id    = ec.id
            WHERE (rr.external_connector_id IN ({placeholders})
               OR reg.item_id              IN ({placeholders}))
              AND ec.nombre = 'INSTALADA'
            """.format(placeholders=",".join(["%s"] * len(ids_list))),
            ids_list + ids_list,
        )
        rows = cursor.fetchall()

    if not rows:
        return

    values = [(r[3], r[1], r[2], str(r[0])) for r in rows]
    conn_rec = get_records_conn()
    with conn_rec.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] get_vnos: actualizaría %d filas", len(values))
        else:
            cur.executemany(
                """
                UPDATE naps_obras
                SET empresa = %s, fecha_alta = %s, fecha_baja = %s
                WHERE id = %s;
                """,
                values,
            )


def get_fechas():
    """Actualiza fecha_alta en naps_obras desde Gestion Westnet (no-BWA)."""
    conn_rec = get_records_conn()
    with conn_rec.cursor() as cur:
        cur.execute(
            r"""
            SELECT String_agg(codigo_cliente::text, ',')
            FROM naps_obras
            WHERE codigo_cliente ~ '^\d+$'
              AND precinto !~* 'BWA'
              AND fecha_alta IS NULL;
            """
        )
        row = cur.fetchone()
    if not row or not row[0]:
        return

    clientes = row[0]
    conn_g = get_gestion_conn()
    with conn_g.cursor() as cursor:
        cursor.execute(
            """
            SELECT cus.code, CAST(MAX(b.date) AS date) AS fecha_alta
            FROM customer cus
            LEFT JOIN customer_log b ON cus.customer_id = b.customer_id
            WHERE cus.code IN ({placeholders})
              AND b.action REGEXP 'Nodo'
              AND b.new_value REGEXP 'FTTH'
            GROUP BY cus.code
            ORDER BY cus.code ASC;
            """.format(placeholders=clientes),
        )
        rows = cursor.fetchall()

    if not rows:
        return

    values = [(r[1], str(r[0])) for r in rows]
    conn_rec = get_records_conn()
    with conn_rec.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] get_fechas: actualizaría %d filas", len(values))
        else:
            cur.executemany(
                "UPDATE naps_obras SET fecha_alta = %s WHERE codigo_cliente = %s;",
                values,
            )


def get_fechas_bw():
    """Actualiza fecha_alta en naps_obras desde Gestion Bigway (BWA)."""
    conn_rec = get_records_conn()
    with conn_rec.cursor() as cur:
        cur.execute(
            r"""
            SELECT String_agg(codigo_cliente::text, ',')
            FROM naps_obras
            WHERE codigo_cliente ~ '^\d+$'
              AND precinto ~* 'BWA'
              AND fecha_alta IS NULL;
            """
        )
        row = cur.fetchone()
    if not row or not row[0]:
        return

    clientes = row[0]
    conn_g = get_gestionbw_conn()
    with conn_g.cursor() as cursor:
        cursor.execute(
            """
            SELECT cus.code, CAST(MAX(b.date) AS date) AS fecha_alta
            FROM customer cus
            LEFT JOIN customer_log b ON cus.customer_id = b.customer_id
            WHERE cus.code IN ({placeholders})
              AND b.action REGEXP 'Nodo'
              AND b.new_value REGEXP 'FTTH'
            GROUP BY cus.code
            ORDER BY cus.code ASC;
            """.format(placeholders=clientes),
        )
        rows = cursor.fetchall()

    if not rows:
        return

    values = [(r[1], str(r[0])) for r in rows]
    conn_rec = get_records_conn()
    with conn_rec.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] get_fechas_bw: actualizaría %d filas", len(values))
        else:
            cur.executemany(
                "UPDATE naps_obras SET fecha_alta = %s WHERE codigo_cliente = %s;",
                values,
            )


def add_card_ids():
    """Asocia card_id a naps_obras donde todavía es NULL."""
    conn = get_records_conn()
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                WITH q1 AS (
                    SELECT cd.card_id,
                           REPLACE(UNNEST(STRING_TO_ARRAY(cd.naps, ',')), ' ', '') AS naps
                    FROM card_description cd
                )
                SELECT DISTINCT n.nap, a.card_id
                FROM naps_obras n
                INNER JOIN q1 a ON n.nap = a.naps
                WHERE n.card_id IS NULL
                ORDER BY n.nap;
                """
            )
        except Exception as exc:
            log.error("add_card_ids — error SQL: %s", exc)
            return
        rows = cur.fetchall()

    if not rows:
        return

    values = [(row[1], row[0]) for row in rows]
    with conn.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] add_card_ids: actualizaría %d filas", len(values))
        else:
            cur.executemany(
                "UPDATE naps_obras SET card_id = %s WHERE nap = %s;",
                values,
            )


def get_naps_obras():
    """Inserta en nap_ocupacion_obras las NAPs únicas de card_description."""
    conn = get_records_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH q1 AS (
                SELECT cd.card_id::text,
                       REPLACE(UNNEST(STRING_TO_ARRAY(cd.naps, ',')), ' ', '') AS naps
                FROM card_description cd
            )
            SELECT card_id, REPLACE(REPLACE(naps, '.', ''), ' ', '') AS naps
            FROM q1
            WHERE naps != '';
            """
        )
        rows = cur.fetchall()

    if not rows:
        return

    values = [(row[0], row[1]) for row in rows]
    with conn.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] get_naps_obras: insertaría %d filas", len(values))
        else:
            execute_values(
                cur,
                """
                INSERT INTO nap_ocupacion_obras (card_id, nap)
                VALUES %s
                ON CONFLICT (nap) DO NOTHING;
                """,
                values,
            )


def get_ocupacion():
    """Actualiza ocupacion y bocas en nap_ocupacion_obras desde Soldef."""
    conn = get_records_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT nap FROM nap_ocupacion_obras;")
        rows = cur.fetchall()

    naps_list = [r[0] for r in rows if r[0]]
    if not naps_list:
        return

    detalles = _query_soldef_naps(naps_list)
    if not detalles:
        return

    values = [(d["ocupacion"], d["bocas"], d["nap"]) for d in detalles]
    with conn.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] get_ocupacion: actualizaría %d filas", len(values))
        else:
            cur.executemany(
                "UPDATE nap_ocupacion_obras SET ocupacion = %s, bocas = %s WHERE nap = %s;",
                values,
            )
