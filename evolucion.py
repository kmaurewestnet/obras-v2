#!/usr/bin/python3
"""
evolucion.py — Evolución de contratos Westnet (no-BWA).

Lee clientes de naps_obras y registra su evolución contractual en records.
"""

import os
import logging
from datetime import date
from dotenv import load_dotenv
from db import get_records_conn, get_gestion_conn, get_mesa_conn

load_dotenv()
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

today = str(date.today())

# ── 1. Obtener clientes Westnet (no-BWA) ──────────────────────────────────────
conn_rec = get_records_conn()
with conn_rec.cursor() as cur:
    cur.execute(
        r"""
        SELECT String_agg(codigo_cliente::text, ', ' ORDER BY codigo_cliente)
        FROM naps_obras
        WHERE codigo_cliente ~ '^\d+$'
          AND precinto !~* 'BWA';
        """
    )
    row = cur.fetchone()

value = row[0] if row else None
if not value:
    log.info("evolucion: no hay clientes Westnet para procesar.")
    exit(0)

# ── 2. Insertar evolución contractual desde Gestion ───────────────────────────
conn_g = get_gestion_conn()
with conn_g.cursor() as cursor:
    cursor.execute(
        f"""
        SELECT cus.code, cont.from_date, cab.total_payed, cab.total_credit,
               cont.status,
               CASE WHEN cont.status REGEXP 'low' THEN dt.to_date ELSE cont.to_date END,
               cat.name
        FROM customer cus
        LEFT JOIN contract                                            cont ON cus.customer_id = cont.customer_id
        LEFT JOIN (SELECT contract_id, MAX(to_date) AS to_date
                   FROM contract_detail GROUP BY contract_id)        dt   ON cont.contract_id = dt.contract_id
        LEFT JOIN customer_account_balance                           cab  ON cus.customer_id = cab.customer_id
        LEFT JOIN (SELECT customer_id, customer_category_id,
                          MAX(date_updated) AS date_updated
                   FROM customer_category_has_customer
                   GROUP BY customer_id, customer_category_id)       cc   ON cus.customer_id = cc.customer_id
        LEFT JOIN customer_category                                  cat  ON cc.customer_category_id = cat.customer_category_id
        WHERE cus.code IN ({value});
        """
    )
    rows = cursor.fetchall()

if rows:
    values = [
        (today, r[0], r[1], r[2], r[3], r[4], r[5], r[6])
        for r in rows
    ]
    with conn_rec.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] evolucion: insertaría %d filas en evolucion_obras", len(values))
        else:
            cur.executemany(
                """
                INSERT INTO evolucion_obras
                    (fecha, codigo_cliente, inicio_contrato, monto_abonado,
                     monto_facturado, cont_estado, fin_contrato, cus_categoria)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """,
                values,
            )
    log.info("evolucion: %d registros de contrato insertados.", len(values))

# ── 3. Actualizar cambio de tecnología desde Mesa Westnet ─────────────────────
conn_mesa = get_mesa_conn()
with conn_mesa.cursor() as cur:
    cur.execute(
        f"""
        SELECT DISTINCT ON (t.codigo_cliente) t.codigo_cliente::text, t.fecha_cierre
        FROM ticket t
        INNER JOIN categoria c ON t.categoria_id = c.id
        WHERE t.categoria_id IN (137)
          AND t.estado = 'cerrado (resuelto)'
          AND t.codigo_cliente IN ({value})
        GROUP BY t.codigo_cliente, t.fecha_cierre
        ORDER BY t.codigo_cliente, t.fecha_cierre DESC;
        """
    )
    lista = cur.fetchall()

if lista:
    values_mesa = [("true", r[1], r[0]) for r in lista]
    with conn_rec.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] evolucion mesa: actualizaría %d filas", len(values_mesa))
        else:
            cur.executemany(
                """
                UPDATE evolucion_obras
                SET cambio_tecnologia = %s, fecha_cambio_tec = %s
                WHERE codigo_cliente = %s;
                """,
                values_mesa,
            )

# ── 4. Actualizar montos pagado/facturado por cliente desde fecha_inicio ──────
with conn_rec.cursor() as cur:
    cur.execute(
        r"""
        SELECT DISTINCT a.codigo_cliente,
            CASE
                WHEN fecha_alta IS NULL                    THEN inicio_contrato
                WHEN fecha_alta > inicio_contrato          THEN fecha_alta
                WHEN inicio_contrato > fecha_alta          THEN inicio_contrato
                WHEN fecha_alta = inicio_contrato          THEN inicio_contrato
                WHEN inicio_contrato IS NULL               THEN fecha_alta
                ELSE NULL
            END AS fecha_inicio
        FROM naps_obras a
        LEFT JOIN evolucion_obras b ON a.codigo_cliente = b.codigo_cliente
        WHERE a.codigo_cliente != '';
        """
    )
    clientes_fecha = cur.fetchall()

conn_g = get_gestion_conn()
updates = []
for cli, fecha in clientes_fecha:
    if fecha is None:
        continue
    with conn_g.cursor() as cursor:
        cursor.execute(
            """
            SELECT cus.code, p.pagado,
                   SUM(CASE WHEN b.bill_type_id IN (4,8,11,15,17) THEN b.total * -1 ELSE b.total END) AS facturado
            FROM customer cus
            INNER JOIN bill b ON cus.customer_id = b.customer_id
            INNER JOIN (
                SELECT cus2.code AS cliente,
                       COALESCE(SUM(p2.amount), 0) AS pagado
                FROM customer cus2
                INNER JOIN payment p2 ON cus2.customer_id = p2.customer_id
                WHERE cus2.code = %s AND p2.date >= %s
                GROUP BY cus2.code
            ) p ON cus.code = p.cliente
            WHERE cus.code = %s AND b.date >= %s
            GROUP BY cus.code;
            """,
            (cli, fecha, cli, fecha),
        )
        rows = cursor.fetchall()
    for r in rows:
        updates.append((r[1], r[2], str(r[0]), today))

if updates:
    with conn_rec.cursor() as cur:
        if DRY_RUN:
            log.info("[DRY_RUN] evolucion montos: actualizaría %d filas", len(updates))
        else:
            cur.executemany(
                """
                UPDATE evolucion_obras
                SET monto_abonado = %s, monto_facturado = %s
                WHERE codigo_cliente = %s AND fecha = %s;
                """,
                updates,
            )
    log.info("evolucion: %d montos actualizados.", len(updates))
