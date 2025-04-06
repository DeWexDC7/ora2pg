import os

import sys

import subprocess



def crear_entorno_virtual(nombre_entorno):

    # Crear entorno virtual

    print(f"Creando entorno virtual '{nombre_entorno}'...")

    subprocess.run([sys.executable, "-m", "venv", nombre_entorno])

    print(f"Entorno virtual '{nombre_entorno}' creado.")



def mostrar_instrucciones_activacion(nombre_entorno):

    # Mostrar instrucciones para activar el entorno virtual

    print("\n✅ Entorno virtual creado correctamente.")

    print("Para activar el entorno virtual, ejecute:")

    

    if sys.platform == "win32":

        print(f"    {nombre_entorno}\\Scripts\\activate")

    else:

        print(f"    source {nombre_entorno}/bin/activate")



if __name__ == "__main__":

    if len(sys.argv) < 2:

        print("Uso: python crear_proyecto.py nombre_del_entorno")

    else:

        nombre_entorno = sys.argv[1]

        crear_entorno_virtual(nombre_entorno)

        mostrar_instrucciones_activacion(nombre_entorno)

        print("\nPuede comenzar a instalar paquetes usando pip una vez activado el entorno.")

