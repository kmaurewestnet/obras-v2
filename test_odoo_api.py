import os
import xmlrpc.client
from dotenv import load_dotenv

# Cargar las credenciales desde .env
load_dotenv()

ODOO_URL = os.getenv("ODOO_API_URL")
ODOO_DB = os.getenv("ODOO_API_DB")
ODOO_USER = os.getenv("ODOO_API_USER")
ODOO_PASS = os.getenv("ODOO_API_PASS")

print("=========================================")
print("  TEST DE CONEXIÓN: ODOO API XML-RPC")
print("=========================================")
print(f"-> URL:  {ODOO_URL}")
print(f"-> DB:   {ODOO_DB}")
print(f"-> USER: {ODOO_USER}")
print("=========================================\n")

if not all([ODOO_URL, ODOO_DB, ODOO_USER, ODOO_PASS]):
    print("❌ ERROR: Faltan verificar algunas de las variables ODOO_API_* en tu archivo .env")
    exit(1)

try:
    print("1. Probando Autenticación y credenciales...")
    common = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/common')
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASS, {})
    
    if uid:
        print(f"✅ ¡Autenticación EXITOSA! (UID Odoo: {uid})")
    else:
        print("❌ Falló la autenticación. El usuario, contraseña o base de datos son incorrectos.")
        exit(1)

    print("\n2. Probando Permisos de Búsqueda (project.task)...")
    models = xmlrpc.client.ServerProxy(f'{ODOO_URL}/xmlrpc/2/object')
    
    # Buscamos 3 tareas al azar que sean padres para confirmar permisos
    tareas_prueba = models.execute_kw(
        ODOO_DB, uid, ODOO_PASS,
        'project.task', 'search_read',
        [[['parent_id', '=', False]]],
        {'fields': ['name', 'stage_id'], 'limit': 3}
    )
    
    print(f"✅ ¡Búsqueda EXITOSA! El sistema pudo leer proyectos.")
    print("   Ejemplos recuperados de Odoo:")
    for t in tareas_prueba:
        stage = t.get('stage_id', 'Sin Etapa')
        print(f"   - {t.get('name')} (Etapa: {stage})")

    print("\n🚀 ¡TODO FUNCIONA CORRECTAMENTE! Ya podés usar main.py sin problemas de API.")

except Exception as e:
    print("\n❌ Ocurrió un error (posiblemente de conexión o url):")
    print(e)
