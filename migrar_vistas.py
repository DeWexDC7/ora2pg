import json
import os
import logging
import psycopg2
import oracledb

CONFIG_JSON = "configuracion/conexion.json"
BANDERA_FILE = "bandera_migracion_exitosa.txt"

VISTAS_OBJETIVO = [
    'ACTIVIDADES_CALLCENTER', 'SOLICITUDES_CALLCENTER', 'AVANCE_MANT_RHELEC', 'AVANCE_MANT_RHELEC_DET',
    'REPORTE_NO_CONFORMIDAD', 'DESCARGO1', 'DESCARGO2', 'DESCARGO3', 'DESCARGO4',
    'PENALIDAD', 'PLANIFICACION_MP', 'PLANIFICACION_MP_INCLUYE_CANCELADOS', 'INFORMES_MANTENIMIENTO',
    'MANT_PREVENTIVOS', 'RHELEC_PLANIFICACION_INFORMES_ELIMINADOS',
    'FAC_MP_MANO_OBRA_DETALLE', 'FAC_MP_MANO_OBRA', 'UT', 'ELEMENTOS', 'SISTEMAS'
]

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# Cargar configuración
with open(CONFIG_JSON) as f:
    config = json.load(f)

oracle = config["oracle"]
postgres = config["postgresql"]
schema = config["schema"].lower()
oracle_dsn = f"{oracle['host']}:{oracle['port']}/{oracle['sid']}"

vistas_migradas = True  # bandera global

def crear_esquema_postgres():
    try:
        conn_pg = psycopg2.connect(**postgres)
        cursor_pg = conn_pg.cursor()
        cursor_pg.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        conn_pg.commit()
        cursor_pg.close()
        conn_pg.close()
        logging.info(f"📁 Esquema '{schema}' verificado/creado en PostgreSQL.")
    except Exception as e:
        logging.error(f"❌ Error al crear el esquema '{schema}' en PostgreSQL: {e}")
        raise

def vistas_existentes_postgres():
    try:
        conn = psycopg2.connect(**postgres)
        cur = conn.cursor()
        cur.execute(f"""
            SELECT table_name FROM information_schema.views
            WHERE table_schema = %s;
        """, (schema,))
        existentes = [row[0].upper() for row in cur.fetchall()]
        cur.close()
        conn.close()
        return existentes
    except Exception as e:
        logging.error("❌ Error al consultar vistas existentes en PostgreSQL:")
        logging.error(e)
        return []

def obtener_vistas_oracle():
    try:
        conn = oracledb.connect(user=oracle['user'], password=oracle['password'], dsn=oracle_dsn)
        cursor = conn.cursor()
        placeholders = ', '.join([':' + str(i+2) for i in range(len(VISTAS_OBJETIVO))])
        sql = f"""
            SELECT view_name, text FROM all_views
            WHERE owner = :1 AND view_name IN ({placeholders})
        """
        params = [schema.upper()] + VISTAS_OBJETIVO
        cursor.execute(sql, params)
        vistas = cursor.fetchall()
        cursor.close()
        conn.close()
        return dict((name, text) for name, text in vistas)
    except Exception as e:
        logging.error(f"❌ Error al obtener vistas desde Oracle: {e}")
        return {}

def limpiar_definicion(texto):
    return '\n'.join(line for line in texto.splitlines() if '@' not in line)

def migrar_vistas():
    global vistas_migradas
    existentes_pg = vistas_existentes_postgres()
    vistas_oracle = obtener_vistas_oracle()

    try:
        conn_pg = psycopg2.connect(**postgres)
        cursor_pg = conn_pg.cursor()

        for vista in VISTAS_OBJETIVO:
            if vista in existentes_pg:
                logging.info(f"✅ Vista {vista} ya existe en PostgreSQL. Saltando.")
                continue

            if vista not in vistas_oracle:
                vistas_migradas = False
                logging.error(f"❌ Vista {vista} no encontrada en Oracle.")
                continue

            try:
                definicion = limpiar_definicion(vistas_oracle[vista])
                sql_create = f"CREATE OR REPLACE VIEW {schema}.{vista.lower()} AS {definicion}"
                cursor_pg.execute(sql_create)
                conn_pg.commit()
                logging.info(f"✅ Vista {vista} creada correctamente.")
            except Exception as e:
                vistas_migradas = False
                logging.error(f"❌ Error al crear vista {vista}: {e}")

        cursor_pg.close()
        conn_pg.close()

    except Exception as e:
        vistas_migradas = False
        logging.error("❌ Error general en la migración de vistas:")
        logging.error(e)

    with open(BANDERA_FILE, "w") as f:
        f.write("true" if vistas_migradas else "false")

    if vistas_migradas:
        logging.info("🎉 Todas las vistas fueron creadas o ya existían.")
    else:
        logging.warning("⚠️ Algunas vistas no fueron creadas. Revisar errores arriba.")

def main():
    crear_esquema_postgres()
    migrar_vistas()

if __name__ == "__main__":
    main()
