import json
import os
import logging
import psycopg2
import oracledb

CONFIG_JSON = "configuracion/conexion.json"
LOG_FILE = "logs/migracion_estructura.log"

VISTAS_OBJETIVO = [
    'ACTIVIDADES_CALLCENTER', 'SOLICITUDES_CALLCENTER', 'AVANCE_MANT_RHELEC', 'AVANCE_MANT_RHELEC_DET',
    'REPORTE_NO_CONFORMIDAD', 'DESCARGO1', 'DESCARGO2', 'DESCARGO3', 'DESCARGO4',
    'PENALIDAD', 'PLANIFICACION_MP', 'PLANIFICACION_MP_INCLUYE_CANCELADOS', 'INFORMES_MANTENIMIENTO',
    'MANT_PREVENTIVOS', 'RHELEC_PLANIFICACION_INFORMES_ELIMINADOS',
    'FAC_MP_MANO_OBRA_DETALLE', 'FAC_MP_MANO_OBRA', 'UT', 'ELEMENTOS', 'SISTEMAS'
]

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

with open(CONFIG_JSON) as f:
    config = json.load(f)

oracle = config["oracle"]
postgres = config["postgresql"]
schema = config["schema"].lower()
oracle_dsn = f"{oracle['host']}:{oracle['port']}/{oracle['sid']}"

TIPO_MAPEO = {
    "NUMBER": "NUMERIC",
    "VARCHAR2": "TEXT",
    "CHAR": "TEXT",
    "CLOB": "TEXT",
    "DATE": "TIMESTAMP",
    "TIMESTAMP": "TIMESTAMP"
}

def crear_tablas_vacias():
    try:
        conn_oracle = oracledb.connect(user=oracle["user"], password=oracle["password"], dsn=oracle_dsn)
        conn_pg = psycopg2.connect(**postgres)
        cur_pg = conn_pg.cursor()

        cur_pg.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        conn_pg.commit()

        for vista in VISTAS_OBJETIVO:
            try:
                cursor_ora = conn_oracle.cursor()
                cursor_ora.execute(f"SELECT * FROM {schema.upper()}.{vista} WHERE ROWNUM = 1")
                desc = cursor_ora.description

                columnas = []
                for col in desc:
                    nombre = col[0].lower()
                    tipo = str(col[1]).split('.')[-1]  # Extrae el nombre del tipo
                    tipo_pg = TIPO_MAPEO.get(tipo, 'TEXT')
                    columnas.append(f"{nombre} {tipo_pg}")

                nombre_tabla = f"{schema}.tabla_{vista.lower()}"
                cur_pg.execute(f"DROP TABLE IF EXISTS {nombre_tabla} CASCADE")
                cur_pg.execute(f"CREATE TABLE {nombre_tabla} ({', '.join(columnas)});")
                conn_pg.commit()
                logging.info(f"✅ Tabla {nombre_tabla} creada correctamente.")

            except Exception as e:
                logging.error(f"❌ Error creando tabla para vista {vista}: {e}")

        cur_pg.close()
        conn_pg.close()
        conn_oracle.close()

    except Exception as e:
        logging.error(f"❌ Error general en migración: {e}")

if __name__ == "__main__":
    logging.info("🚀 Iniciando migración de estructuras de vistas a tablas...")
    crear_tablas_vacias()
    logging.info("🏁 Proceso finalizado.")
