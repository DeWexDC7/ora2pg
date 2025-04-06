FROM debian:bullseye

ENV DEBIAN_FRONTEND=noninteractive

# 1. Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
  python3 python3-pip python3-venv python3-dev \
  build-essential libaio1 libdbi-perl libdbd-pg-perl \
  perl cpanminus git unzip curl wget libpq-dev postgresql-client \
  && rm -rf /var/lib/apt/lists/*

# 2. Copiar Oracle Instant Client
COPY instantclient /opt/oracle/instantclient
ENV ORACLE_HOME=/opt/oracle/instantclient
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient
ENV PATH=$PATH:/opt/oracle/instantclient

# 3. Instalar DBD::Oracle para ora2pg
RUN cpanm -n DBD::Oracle

# 4. Instalar ora2pg
RUN git clone https://github.com/darold/ora2pg.git && \
    cd ora2pg && perl Makefile.PL && make && make install

# 5. Crear directorio de trabajo
WORKDIR /app

# 6. Copiar archivos del proyecto
COPY . .

# 7. Instalar dependencias de Python
RUN pip3 install --no-cache-dir -r requirements.txt

# 8. Comando por defecto: primero migrar vistas, luego launcher
CMD python3 migrar_vistas.py && python3 launcher.py
