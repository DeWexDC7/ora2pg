import os
import logging
from multiprocessing import Process

BANDERA_FILE = "bandera_migracion_exitosa.txt"

def run_script(script_name):
    log_file = f"{script_name.replace('.py', '')}.log"
    logging.info(f"🚀 Ejecutando {script_name} ... (log: {log_file})")

    # Ejecutar el script y redirigir stdout/stderr al archivo log
    exit_code = os.system(f"python3 {script_name} > {log_file} 2>&1")

    if exit_code != 0:
        logging.error(f"❌ Error al ejecutar {script_name}. Revisa el log: {log_file}")
    else:
        logging.info(f"✅ {script_name} finalizado correctamente.")

def main():
    if not os.path.exists(BANDERA_FILE):
        logging.warning("⚠️ No existe el archivo de bandera. Abortando.")
        return

    with open(BANDERA_FILE) as f:
        contenido = f.read().strip().lower()
        if contenido != "true":
            logging.warning("⚠️ Bandera de migración no está en 'true'. Abortando.")
            return

    scripts = ["migrar_oracle1.py", "migrar_oracle2.py", "migrar_oracle3.py"]
    procesos = []

    for script in scripts:
        p = Process(target=run_script, args=(script,))
        p.start()
        procesos.append(p)

    for p in procesos:
        p.join()

    logging.info("🏁 Todos los scripts han finalizado.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    main()
