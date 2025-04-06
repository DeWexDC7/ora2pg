import json
import os
import logging
import psycopg2
import oracledb
import csv
import time

CONFIG_JSON = "configuracion/conexion.json"
CSV_DIR = "csv_output"
ESTADO_JSON = "migracion_vistas.json"
LOCK_FILE = "migracion_vistas.lock"

VISTAS_OBJETIVO = [
    'ACTIVIDADES_CALLCENTER', 'SOLICITUDES_CALLCENTER', 'AVANCE_MANT_RHELEC', 'AVANCE_MANT_RHELEC_DET',
    'REPORTE_NO_CONFORMIDAD', 'DESCARGO1', 'DESCARGO2', 'DESCARGO3', 'DESCARGO4', 'PENALIDAD'
]

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

with open(CONFIG_JSON) as f:
    config = json.load(f)

oracle = config["oracle"]
postgres = config["postgresql"]
schema = config["schema"].lower()
oracle_dsn = f"{oracle['host']}:{oracle['port']}/{oracle['sid']}"

if os.path.exists(ESTADO_JSON):
    with open(ESTADO_JSON) as f:
        estado_migracion = json.load(f)
else:
    estado_migracion = {}

os.makedirs(CSV_DIR, exist_ok=True)

oracle_conn = oracledb.connect(
    user=oracle['user'],
    password=oracle['password'],
    dsn=oracle_dsn
)
ocursor = oracle_conn.cursor()

pg_conn = psycopg2.connect(
    host=postgres["host"],
    port=postgres["port"],
    dbname=postgres["database"],
    user=postgres["user"],
    password=postgres["password"]
)
pg_cursor = pg_conn.cursor()

pg_cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
pg_conn.commit()

# Función para actualizar el estado de migración con manejo de concurrencia
def actualizar_estado_migracion(vista, filas):
    # Intentar adquirir bloqueo
    max_attempts = 5
    attempts = 0
    while os.path.exists(LOCK_FILE) and attempts < max_attempts:
        time.sleep(1)
        attempts += 1
    
    if attempts >= max_attempts:
        logging.warning(f"⚠️ No se pudo adquirir bloqueo para actualizar estado de {vista}")
        return
        
    # Crear archivo de bloqueo
    with open(LOCK_FILE, 'w') as lock:
        lock.write(str(os.getpid()))
    
    try:
        # Leer estado actual
        if os.path.exists(ESTADO_JSON):
            with open(ESTADO_JSON) as f:
                estado_actual = json.load(f)
        else:
            estado_actual = {}
        
        # Actualizar estado
        estado_actual[vista] = filas
        
        # Guardar estado actualizado
        with open(ESTADO_JSON, "w") as f:
            json.dump(estado_actual, f, indent=4)
    finally:
        # Liberar bloqueo
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)

for vista in VISTAS_OBJETIVO:
    vista_pg = vista.lower()
    ocursor.execute(f"SELECT COUNT(*) FROM {schema.upper()}.{vista}")
    total_filas = ocursor.fetchone()[0]

    if estado_migracion.get(vista) == total_filas:
        logging.info(f"⏩ {vista} ya migrada con {total_filas} filas. Saltando.")
        continue

    ocursor.execute(f"""
        SELECT column_name FROM all_tab_columns
        WHERE owner = '{schema.upper()}' AND table_name = '{vista}'
        ORDER BY column_id
    """)
    columnas = [col[0].lower() for col in ocursor.fetchall()]

    csv_path = f"{CSV_DIR}/{vista_pg}.csv"
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columnas)
        ocursor.execute(f"SELECT * FROM {schema.upper()}.{vista}")
        for row in ocursor:
            writer.writerow(row)

    pg_cursor.execute(f"DROP TABLE IF EXISTS {schema}.{vista_pg} CASCADE")
    ocursor.execute(f"""
        SELECT column_name, data_type FROM all_tab_columns
        WHERE owner = '{schema.upper()}' AND table_name = '{vista}'
        ORDER BY column_id
    """)
    col_defs = []
    for col, tipo in ocursor.fetchall():
        tipo_pg = 'VARCHAR' if 'CHAR' in tipo or 'CLOB' in tipo else 'NUMERIC' if 'NUMBER' in tipo else 'TEXT'
        col_defs.append(f"{col.lower()} {tipo_pg}")
    pg_cursor.execute(f"CREATE TABLE {schema}.{vista_pg} ({', '.join(col_defs)});")

    with open(csv_path, 'r') as f:
        pg_cursor.copy_expert(f"COPY {schema}.{vista_pg} FROM STDIN WITH CSV HEADER", f)
    pg_conn.commit()
    os.remove(csv_path)
    actualizar_estado_migracion(vista, total_filas)
    logging.info(f"✅ {vista_pg} migrada con {total_filas} filas.")

pg_cursor.close()
pg_conn.close()
ocursor.close()
oracle_conn.close()
logging.info("🏁 Migración finalizada.")
