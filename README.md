# obras

Job mensual de seguimiento de infraestructura de fibra óptica.  
Se ejecuta el día 28 de cada mes via cron en el servidor Debian.

## Descripción

El script recopila datos de obras en curso desde **Odoo**,
cruza información de NAPs desde **Soldef**, clientes desde **Gestion** y **Napear**,
y registra todo en la base de datos `records`.


### Flujo general

El nuevo esquema arquitectónico (v2) funciona como un embudo que procesa **únicamente obras finalizadas**:

```
Odoo DB + Odoo API (XML-RPC)
    Obtención de IDs locales + Filtro de Etapa "OBRAS FINALIZADAS"
             │
             ▼
    records.cards / card_description (solo obras 100% terminadas)
             │
             ├── Soldef → naps_obras (NAPs, ocupación, clientes)
             ├── Napear → naps_obras (VNOs, fechas)
             ├── Gestion Westnet → naps_obras (fechas de alta)
             └── Gestion Bigway  → naps_obras (fechas de alta BWA)
```

Además, `evolucion.py` y `evolucion_bw.py` registran la evolución contractual
mensual de cada cliente, y `dolar.py` guarda la cotización oficial del dólar.

---

## Archivos

| Archivo | Descripción |
|---|---|
| `main.py` | Punto de entrada principal |
| `utils.py` | Funciones de negocio (obras, NAPs, ocupación) |
| `odoo_api.py` | Cliente XML-RPC para validación de etapas en Odoo |
| `test_odoo_api.py` | Script de prueba de conexión a la API de Odoo |
| `evolucion.py` | Evolución contractual clientes Westnet |
| `evolucion_bw.py` | Evolución contractual clientes Bigway |
| `dolar.py` | Cotización dólar oficial del día |
| `db.py` | Módulo de conexiones DB centralizadas |
| `.env.example` | Plantilla de variables de entorno |
| `requirements.txt` | Dependencias Python |

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone <url-del-repo>
cd obras
```

### 2. Crear entorno virtual

```bash
python3 -m venv venv
source venv/bin/activate   # Linux / Mac
# venv\Scripts\activate    # Windows
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar credenciales

```bash
cp .env.example .env
nano .env  
```

---

## Uso

### Ejecución normal

```bash
python3 main.py
```

### Modo DRY_RUN 

```bash
# Opción 1: variable de entorno inline
DRY_RUN=true python3 main.py

# Opción 2: editar .env
DRY_RUN=true
```

En modo DRY_RUN todos los cambios que se harían se logean con el prefijo `[DRY_RUN]` pero **no se ejecuta ningún INSERT ni UPDATE**.


## Notas importantes

- **Filtro de Obras Finalizadas**: Gracias a la integración con la API de Odoo (`odoo_api.py`), el script consulta en tiempo real cuáles obras están en el stage "OBRAS FINALIZADAS".

---

## Dependencias

- Python 3.9+
- psycopg2-binary
- mysql-connector-python
- python-dotenv
- requests
