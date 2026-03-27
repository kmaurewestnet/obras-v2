# obras

Job mensual de seguimiento de infraestructura de fibra óptica.  
Se ejecuta el día 28 de cada mes via cron en el servidor Debian.

## Descripción

El script recopila datos de obras en curso desde **Odoo** (via DB local `datosodoo`),
cruza información de NAPs desde **Soldef**, clientes desde **Gestion** y **Napear**,
y registra todo en la base de datos `records`.

### Flujo general

```
Odoo DB (datosodoo)
    └── odoo_tasks / odoo_task_properties
            │
            ▼
    records.cards / card_description
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
| `evolucion.py` | Evolución contractual clientes Westnet |
| `evolucion_bw.py` | Evolución contractual clientes Bigway |
| `dolar.py` | Cotización dólar oficial del día |
| `db.py` | Módulo de conexiones DB centralizadas |
| `.env` | Credenciales (**no se sube al repo**) |
| `.env.example` | Plantilla de variables de entorno |
| `requirements.txt` | Dependencias Python |

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone <url-del-repo>
cd obras
```

### 2. Crear entorno virtual (recomendado)

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
nano .env   # Completar con valores reales
```

---

## Uso

### Ejecución normal

```bash
python3 main.py
```

### Modo DRY_RUN (sin escribir en DB)

```bash
# Opción 1: variable de entorno inline
DRY_RUN=true python3 main.py

# Opción 2: editar .env
DRY_RUN=true
```

En modo DRY_RUN todos los cambios que se harían se logean con el prefijo `[DRY_RUN]` pero **no se ejecuta ningún INSERT ni UPDATE**.

### Cron (ejecución mensual, día 28)

```cron
0 6 28 * * cd /ruta/al/proyecto && python3 main.py >> /var/log/obras.log 2>&1
```

---

## Notas importantes

- `add_card_fin_obra()` está **deshabilitada temporalmente**: la DB local de Odoo  
  (`datosodoo`) no contiene las etapas de las tareas, por lo que no es posible  
  determinar automáticamente el fin de obra. Se habilitará cuando las etapas  
  estén disponibles.

- Los scripts **deben ejecutarse desde el servidor Debian** donde hay acceso  
  a las redes internas (`172.16.x.x`, `172.27.x.x`, etc.).

---

## Dependencias

- Python 3.9+
- psycopg2-binary
- mysql-connector-python
- python-dotenv
- requests
