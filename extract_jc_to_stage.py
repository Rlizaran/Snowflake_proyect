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
def download(yyyymm, folder):
    candidates = [
        f"JC-{yyyymm}-citibike-tripdata.csv.zip",
        f"JC-{yyyymm}-citibike-tripdata.zip",
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
    stage = os.environ.get("SF_STAGE", "DB_CITYBIKE_BRONZE.CITYBIKE.CITYBIKE_LANDING_STAGE")
    today = date.today()
    meses = months("202401", f"{today.year:04d}{today.month:02d}")

    conn = snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        role=os.environ.get("SF_ROLE"),
        warehouse=os.environ.get("SF_WAREHOUSE"),
        database=os.environ.get("SF_DATABASE"),
        schema=os.environ.get("SF_SCHEMA"),
    )

    try:
        # Comparar contra lo que ya esta en el stage para subir solo lo nuevo.
        ya_cargados = existing_months(conn, stage)
        pendientes = [m for m in meses if m not in ya_cargados]
        print(f"En stage: {len(ya_cargados)} | Pendientes: {len(pendientes)}")

        if not pendientes:
            print("Stage al dia, nada que subir.")
            return

        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            for m in pendientes:
                print(f"[{m}]")
                z = download(m, folder)
                if not z:
                    print("  no disponible")
                    continue
                csv = unzip_csv(z, folder)
                put(conn, csv, stage)
                print(f"  subido: {csv.name}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()


# Descarga archivos JC de Citi Bike y sube al stage de Snowflake solo los meses que aun no estan cargados.








