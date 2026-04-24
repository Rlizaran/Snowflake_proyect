"""
Descarga los archivos JC (Jersey City) de Citi Bike desde el bucket publico de
S3 (https://s3.amazonaws.com/tripdata/) y los sube al stage interno de Snowflake
`BRONZE.CITIBIKE_LANDING_STAGE` usando el comando PUT del
Snowflake Python Connector.

Flujo general:
    1. Nombres esperados: JC-YYYYMM-citibike-tripdata.csv.zip
       desde 2024-01 hasta el mes actual.
    2. Descarga archivo por HTTPS a una carpeta temporal local.
    3. Comprime a .gz y lo sube con PUT al stage de Snowflake
"""

import argparse
import gzip
import os
import shutil
import sys
import tempfile
from datetime import date
from pathlib import Path

import requests
import snowflake.connector
from dotenv import load_dotenv


# URL base del bucket público de Citi Bike
S3_BASE_URL = "https://s3.amazonaws.com/tripdata"

# Nombre del stage destino
DEFAULT_STAGE = "BRONZE.CITIBIKE_LANDING_STAGE"


def build_month_list(start_yyyymm: str, end_yyyymm: str) -> list[str]:
    """
    Construye la lista de YYYYMM entre start y end.

    :param start_yyyymm: mes inicial en formato 'YYYYMM', ej. '202401'
    :param end_yyyymm:   mes final en formato 'YYYYMM',   ej. '202604'
    :return: lista ordenada de strings 'YYYYMM'
    """
    y1, m1 = int(start_yyyymm[:4]), int(start_yyyymm[4:])
    y2, m2 = int(end_yyyymm[:4]), int(end_yyyymm[4:])
    out = []
    y, m = y1, m1
    while (y, m) <= (y2, m2):
        out.append(f"{y:04d}{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def download_jc_zip(yyyymm: str, dest_dir: Path) -> Path | None:
    """
    Descarga JC-YYYYMM-citibike-tripdata.csv.zip desde el bucket de Citi Bike.

    :param yyyymm:   mes en formato 'YYYYMM'
    :param dest_dir: carpeta temporal donde guardar el archivo
    :return: Path al zip descargado, o None si el archivo no existe (404)
    """
    filename = f"JC-{yyyymm}-citibike-tripdata.csv.zip"
    url = f"{S3_BASE_URL}/{filename}"
    dest = dest_dir / filename

    print(f"  -> Descargando {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        if r.status_code == 404:
            print(f"     (no existe aún, se omite)")
            return None
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return dest


def unzip_and_gzip(zip_path: Path, work_dir: Path) -> Path:
    """
    Descomprime el .zip de Citi Bike (contiene un .csv) y recomprime el CSV como
    .csv.gz, que es el formato esperado por el COPY INTO del Bronze.

    :param zip_path: ruta al .zip descargado
    :param work_dir: carpeta temporal de trabajo
    :return: Path al archivo .csv.gz final
    """
    import zipfile

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Buscar el CSV principal dentro del zip (ignora __MACOSX, etc.)
        csv_members = [
            n for n in zf.namelist()
            if n.lower().endswith(".csv") and not n.startswith("__MACOSX")
        ]
        if not csv_members:
            raise RuntimeError(f"No se encontró un .csv dentro de {zip_path.name}")
        csv_name = csv_members[0]
        zf.extract(csv_name, work_dir)
        extracted = work_dir / csv_name

    # Recomprimir como .gz usando el mismo nombre base
    final_name = Path(csv_name).name + ".gz"
    gz_path = work_dir / final_name
    with open(extracted, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst)

    # Limpiar el CSV sin comprimir para ahorrar espacio
    try:
        extracted.unlink()
    except OSError:
        pass

    return gz_path


def put_to_stage(conn, local_path: Path, stage: str) -> None:
    """
    Sube un archivo local al stage interno de Snowflake con PUT.
    AUTO_COMPRESS=FALSE porque el archivo ya viene comprimido en .gz.

    :param conn:       conexión activa de snowflake.connector
    :param local_path: archivo .csv.gz a subir
    :param stage:      nombre completo del stage destino
    """
    # Snowflake PUT requiere forward slashes y el prefijo file://
    local_uri = f"file://{local_path.as_posix()}"
    sql = (
        f"PUT {local_uri} @{stage} "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE SOURCE_COMPRESSION=GZIP"
    )
    print(f"  -> PUT {local_path.name} -> @{stage}")
    cur = conn.cursor()
    try:
        cur.execute(sql)
        for row in cur.fetchall():
            # cada fila: (source, target, source_size, target_size, status, ...)
            print(f"     status={row[6] if len(row) > 6 else row[-1]}")
    finally:
        cur.close()


def connect_snowflake():
    """
    Crea una conexión a Snowflake usando variables de entorno.
    """
    return snowflake.connector.connect(
        account   = os.environ["SF_ACCOUNT"],
        user      = os.environ["SF_USER"],
        password  = os.environ["SF_PASSWORD"],
        role      = os.environ.get("SF_ROLE", "CURSO_DATA_ENGINEERING"),
        warehouse = os.environ.get("SF_WAREHOUSE", "WH_CURSO_DATA_ENGINEERING"),
        database  = os.environ.get("SF_DATABASE", "RODRIGO_PROYECTO_BRZ"),
        schema    = os.environ.get("SF_SCHEMA", "BRONZE"),
    )


def parse_args():
    """Lee argumentos CLI para acotar el rango de meses a procesar."""
    p = argparse.ArgumentParser(description="Sube archivos JC de Citi Bike al stage.")
    p.add_argument("--from", dest="since", default="202401",
                   help="Mes inicial YYYYMM (default 202401)")
    p.add_argument("--to", dest="until", default=None,
                   help="Mes final YYYYMM (default: mes actual)")
    p.add_argument("--only", dest="only", default=None,
                   help="Procesar un solo mes YYYYMM (ignora --from/--to)")
    p.add_argument("--stage", default=os.environ.get("SF_STAGE", DEFAULT_STAGE),
                   help=f"Stage destino (default: {DEFAULT_STAGE})")
    return p.parse_args()


def main():
    """Punto de entrada: orquesta descarga + recompresión + PUT al stage."""
    load_dotenv()  # carga .env si existe
    args = parse_args()

    if args.only:
        months = [args.only]
    else:
        today = date.today()
        until = args.until or f"{today.year:04d}{today.month:02d}"
        months = build_month_list(args.since, until)

    print(f"Meses a procesar ({len(months)}): {months[0]} -> {months[-1]}")
    print(f"Stage destino: {args.stage}")

    conn = connect_snowflake()
    try:
        with tempfile.TemporaryDirectory(prefix="jc_citibike_") as tmp:
            tmp_path = Path(tmp)
            for yyyymm in months:
                print(f"\n[{yyyymm}]")
                zip_file = download_jc_zip(yyyymm, tmp_path)
                if zip_file is None:
                    continue
                try:
                    gz_file = unzip_and_gzip(zip_file, tmp_path)
                    put_to_stage(conn, gz_file, args.stage)
                finally:
                    # Limpiar archivos del mes para no llenar el disco
                    for f in tmp_path.glob(f"*{yyyymm}*"):
                        try:
                            f.unlink()
                        except OSError:
                            pass
        print("\nOK - todos los archivos JC disponibles fueron subidos al stage.")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
        sys.exit(130)
