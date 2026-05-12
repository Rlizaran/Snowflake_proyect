# Descarga archivos JC de Citi Bike, descomprime el zip y sube el CSV al stage de Snowflake.

import os
import zipfile
from datetime import date
from pathlib import Path
import tempfile

import requests
import snowflake.connector
from dotenv import load_dotenv

S3_BASE = "https://s3.amazonaws.com/tripdata"


# Genera lista de YYYYMM entre dos meses (inclusive).
def months(start, end):
    y1, m1 = int(start[:4]), int(start[4:])
    y2, m2 = int(end[:4]), int(end[4:])
    out = []
    while (y1, m1) <= (y2, m2):
        out.append(f"{y1:04d}{m1:02d}")
        m1 += 1
        if m1 == 13:
            m1, y1 = 1, y1 + 1
    return out


# Lista los YYYYMM que ya estan en el stage para no volver a subirlos.
def existing_months(conn, stage):
    sql = f"LS @{stage}"
    rows = conn.cursor().execute(sql).fetchall()
    found = set()
    for r in rows:
        name = r[0]
        # Patron esperado: .../JC-YYYYMM-citibike-tripdata.csv.gz
        idx = name.find("JC-")
        if idx == -1:
            continue
        yyyymm = name[idx + 3:idx + 9]
        if yyyymm.isdigit():
            found.add(yyyymm)
    return found

# Descarga el zip del mes probando las dos variantes de nombre que usa Citi Bike.
def download(yyyymm, folder, prefix=""):
    candidates = [
        f"{prefix}{yyyymm}-citibike-tripdata.csv.zip",
        f"{prefix}{yyyymm}-citibike-tripdata.zip",
    ]
    for name in candidates:
        r = requests.get(f"{S3_BASE}/{name}", stream=True, timeout=120)
        if r.status_code == 404:
            r.close()
            continue
        r.raise_for_status()
        path = folder / name
        with open(path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                f.write(chunk)
        return path
    return None


# Descomprime el zip y devuelve la ruta del CSV extraído sin modificarlo.
def unzip_csv(zip_path, folder):
    with zipfile.ZipFile(zip_path) as zf:
        csv = next(n for n in zf.namelist()
                   if n.lower().endswith(".csv") and not n.startswith("__MACOSX"))
        zf.extract(csv, folder)
    return folder / csv


# Sube el CSV al stage con PUT.
def put(conn, csv_path, stage):
    sql = f"PUT file://{csv_path.as_posix()} @{stage} OVERWRITE=TRUE"
    conn.cursor().execute(sql).fetchall()


def main():
    load_dotenv()
    while True:
        mode = input("Ingrese el entorno de destino (DEV, PRO): ").strip().upper()
        if mode in ["DEV", "PRO"]:
            break
        print("Opción inválida. Por favor, ingrese DEV o PROD.")
    db = os.environ.get(f"{mode}_SF_DATABASE")
    stage_jc = os.environ.get(f"{mode}_SF_STAGE")
    stage_ny = os.environ.get(f"{mode}_SF_STAGE_NY")
    print(f"Database: {db}")
    print(f"Stage: {stage}")
    today = date.today()
    current_month = f"{today.year:04d}{today.month:02d}"

    conn = snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        role=os.environ.get("SF_ROLE"),
        warehouse=os.environ.get("SF_WAREHOUSE"),
        database=db,
        schema=os.environ.get("SF_SCHEMA"),
    )

try:
        # --- PROCESAMIENTO JERSEY CITY (JC) ---
        print(f"\n--- Procesando JC -> Stage: {stage_jc} ---")
        meses_jc = months("202401", current_month)
        cargados_jc = existing_months(conn, stage_jc, "JC-")
        pendientes_jc = [m for m in meses_jc if m not in cargados_jc]
        
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            for m in pendientes_jc:
                z = download_file(m, folder, "JC-")
                if z:
                    csvs = unzip_all_csvs(z, folder)
                    for csv in csvs:
                        # Usamos el stage de JC
                        put(conn, csv, stage_jc)
                        print(f"  [JC] subido a {stage_jc}: {csv.name}")

        # --- PROCESAMIENTO NEW YORK (NY) ---
        print(f"\n--- Procesando NY -> Stage: {stage_ny} ---")
        meses_ny = months("202604", current_month)
        # En NY el prefijo suele ser el año, pasamos cadena vacía
        cargados_ny = existing_months(conn, stage_ny, "") 
        pendientes_ny = [m for m in meses_ny if m not in cargados_ny]

        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            for m in pendientes_ny:
                z = download_file(m, folder, "") 
                if z:
                    csvs = unzip_all_csvs(z, folder)
                    for csv in csvs:
                        # Usamos el stage de NY
                        put(conn, csv, stage_ny)
                        print(f"  [NY] subido a {stage_ny}: {csv.name}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()


# Descarga archivos JC de Citi Bike y sube al stage de Snowflake solo los meses que aun no estan cargados.








