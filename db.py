#!/usr/bin/python3
"""
db.py — Módulo de conexiones centralizadas.

Expone una conexión singleton por cada base de datos.
Usar get_*_conn() para obtener la conexión. Las conexiones
se crean la primera vez que se solicitan y se reusan en adelante.

Ejemplo de uso con cursor:
    conn = get_records_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT ...")
        rows = cur.fetchall()
"""

import os
import psycopg2
import mysql.connector
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# ── Singletons internos ────────────────────────────────────────────────────────
_records_conn = None
_soldef_conn = None
_mesa_conn = None
_mesa_bw_conn = None
_gestion_conn = None
_gestionbw_conn = None
_nap_conn = None
_odoo_conn = None


# ── Records (PostgreSQL) ───────────────────────────────────────────────────────
def get_records_conn():
    global _records_conn
    if _records_conn is None or _records_conn.closed:
        _records_conn = psycopg2.connect(
            host=os.getenv("DB_RECORDS_HOST"),
            port=os.getenv("DB_RECORDS_PORT", "5432"),
            user=os.getenv("DB_RECORDS_USER"),
            password=os.getenv("DB_RECORDS_PASS"),
            database=os.getenv("DB_RECORDS_NAME"),
        )
        _records_conn.autocommit = True
    return _records_conn


# ── Soldef (PostgreSQL) ────────────────────────────────────────────────────────
def get_soldef_conn():
    global _soldef_conn
    if _soldef_conn is None or _soldef_conn.closed:
        _soldef_conn = psycopg2.connect(
            host=os.getenv("DB_SOLDEF_HOST"),
            port=os.getenv("DB_SOLDEF_PORT", "5432"),
            user=os.getenv("DB_SOLDEF_USER"),
            password=os.getenv("DB_SOLDEF_PASS"),
            database=os.getenv("DB_SOLDEF_NAME"),
        )
    return _soldef_conn


# ── Mesa / Incidencias Westnet (PostgreSQL) ────────────────────────────────────
def get_mesa_conn():
    global _mesa_conn
    if _mesa_conn is None or _mesa_conn.closed:
        _mesa_conn = psycopg2.connect(
            host=os.getenv("DB_MESA_HOST"),
            port=os.getenv("DB_MESA_PORT", "5432"),
            user=os.getenv("DB_MESA_USER"),
            password=os.getenv("DB_MESA_PASS"),
            database=os.getenv("DB_MESA_NAME"),
        )
    return _mesa_conn


# ── Mesa / Incidencias Bigway (PostgreSQL) ─────────────────────────────────────
def get_mesa_bw_conn():
    global _mesa_bw_conn
    if _mesa_bw_conn is None or _mesa_bw_conn.closed:
        _mesa_bw_conn = psycopg2.connect(
            host=os.getenv("DB_MESA_BW_HOST"),
            port=os.getenv("DB_MESA_BW_PORT", "5432"),
            user=os.getenv("DB_MESA_BW_USER"),
            password=os.getenv("DB_MESA_BW_PASS"),
            database=os.getenv("DB_MESA_BW_NAME"),
        )
    return _mesa_bw_conn


# ── Gestion Westnet (MySQL) ────────────────────────────────────────────────────
def get_gestion_conn():
    global _gestion_conn
    if _gestion_conn is None or not _gestion_conn.is_connected():
        _gestion_conn = mysql.connector.connect(
            host=os.getenv("DB_GESTION_HOST"),
            user=os.getenv("DB_GESTION_USER"),
            password=os.getenv("DB_GESTION_PASS"),
            database=os.getenv("DB_GESTION_NAME"),
        )
    return _gestion_conn


# ── Gestion Bigway (MySQL) ─────────────────────────────────────────────────────
def get_gestionbw_conn():
    global _gestionbw_conn
    if _gestionbw_conn is None or not _gestionbw_conn.is_connected():
        _gestionbw_conn = mysql.connector.connect(
            host=os.getenv("DB_GESTION_BW_HOST"),
            user=os.getenv("DB_GESTION_BW_USER"),
            password=os.getenv("DB_GESTION_BW_PASS"),
            database=os.getenv("DB_GESTION_BW_NAME"),
        )
    return _gestionbw_conn


# ── Napear (MySQL) ─────────────────────────────────────────────────────────────
def get_nap_conn():
    global _nap_conn
    if _nap_conn is None or not _nap_conn.is_connected():
        _nap_conn = mysql.connector.connect(
            host=os.getenv("DB_NAP_HOST"),
            user=os.getenv("DB_NAP_USER"),
            password=os.getenv("DB_NAP_PASS"),
            database=os.getenv("DB_NAP_NAME"),
        )
    return _nap_conn


# ── Odoo DB local (MySQL) ──────────────────────────────────────────────────────
def get_odoo_conn():
    global _odoo_conn
    if _odoo_conn is None or not _odoo_conn.is_connected():
        _odoo_conn = mysql.connector.connect(
            host=os.getenv("DB_ODOO_HOST"),
            port=int(os.getenv("DB_ODOO_PORT", "3306")),
            user=os.getenv("DB_ODOO_USER"),
            password=os.getenv("DB_ODOO_PASS"),
            database=os.getenv("DB_ODOO_NAME"),
        )
    return _odoo_conn


# ── Cerrar todas las conexiones ────────────────────────────────────────────────
def close_all():
    """Cierra todas las conexiones abiertas. Llamar al final de main.py."""
    for conn in [_records_conn, _soldef_conn, _mesa_conn, _mesa_bw_conn]:
        try:
            if conn and not conn.closed:
                conn.close()
        except Exception:
            pass
    for conn in [_gestion_conn, _gestionbw_conn, _nap_conn, _odoo_conn]:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass
