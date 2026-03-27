"""
odoo_api.py — Cliente XML-RPC para Odoo

Permite consultar el modelo de proyectos (project.task)
para verificar el "stage" (etapa) de cada tarea y filtrar
solo aquellas que se encuentren finalizadas ("OBRAS FINALIZADAS").
"""

import os
import logging
import xmlrpc.client
from typing import List

log = logging.getLogger(__name__)

# Configuraciones Odoo API
ODOO_URL = os.getenv("ODOO_API_URL")
ODOO_DB = os.getenv("ODOO_API_DB")
ODOO_USER = os.getenv("ODOO_API_USER")
ODOO_PASS = os.getenv("ODOO_API_PASS")


def get_obras_finalizadas_ids(lista_ids: List[str]) -> List[str]:
    """
    Se conecta a la API de Odoo mediante XML-RPC para verificar el stage de un lote de IDs numéricos.
    Retorna únicamente la lista de IDs que se encuentren en la etapa 'OBRAS FINALIZADAS' y sean parent_id=False.
    """
    if not lista_ids or not ODOO_URL or not ODOO_DB or not ODOO_USER or not ODOO_PASS:
        if not ODOO_URL:
            log.warning("get_obras_finalizadas_ids: Faltan credenciales de Odoo API en .env")
        return []

    try:
        # 1. Autenticación para obtener el UID
        common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
        uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASS, {})

        if not uid:
            log.error("Autenticación Odoo API fallida. Verifica usuario y contraseña.")
            return []

        # 2. Consultar modelo project.task (Search & Read)
        models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
        
        # Odoo API usa ints para IDs pero la lista podría venir de strings ('123')
        numeric_ids = [int(i) for i in lista_ids if str(i).isdigit()]
        
        domain = [
            ('id', 'in', numeric_ids),
            ('parent_id', '=', False)
        ]
        
        fields = ['name', 'stage_id']

        tasks = models.execute_kw(
            ODOO_DB, uid, ODOO_PASS,
            'project.task', 'search_read',
            [domain],
            {'fields': fields}
        )
        
        obras_filtradas = []
        for t in tasks:
            # stage_id suele retornar una tupla/lista: [ID, "Nombre"]
            stage = t.get('stage_id')
            if isinstance(stage, (list, tuple)) and len(stage) >= 2:
                stage_name = str(stage[1]).upper().strip()
                if stage_name == 'OBRAS FINALIZADAS':
                    obras_filtradas.append(str(t['id']))

        return obras_filtradas

    except Exception as e:
        log.error("Error conectando con la API de Odoo: %s", e)
        return []
