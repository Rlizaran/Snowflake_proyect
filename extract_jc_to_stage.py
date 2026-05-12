# Descarga archivos JC + NY (202604+) de Citi Bike, descomprime los zips y sube los CSV a Snowflake stages.

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


# Devuelve los YYYYMM ya cargados en el stage. Si hay prefijo busca '{prefix}YYYYMM'; si no, los 6 primeros chars del basename.
def existing_months(conn, stage, prefix=""):
    rows = conn.cursor().execute(f"LS @{stage}").fetchall()
    found = set()
    for r in rows:
        name = r[0]
        if prefix:
            idx = name.find(prefix)
            if idx == -1:
                continue
            yyyymm = name[idx + len(prefix):idx + len(prefix) + 6]
        else:
            base = name.split("/")[-1]
            yyyymm = base[:6]
        if yyyymm.isdigit():
            found.add(yyyymm)
    return found


# Descarga el zip del mes probando las dos variantes de nombre que usa Citi Bike (.csv.zip y .zip).
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


# Descomprime y devuelve TODOS los CSVs del zip (algunos meses vienen en partes).
def unzip_csvs(zip_path, folder):
    extracted = []
    with zipfile.ZipFile(zip_path) as zf:
        for n in zf.namelist():
            if n.lower().endswith(".csv") and not n.startswith("__MACOSX"):
                zf.extract(n, folder)
                extracted.append(folder / n)
    return extracted


# Sube un CSV al stage (OVERWRITE=TRUE para reemplazar si ya existe).
def put(conn, csv_path, stage):
    conn.cursor().execute(f"PUT file://{csv_path.as_posix()} @{stage} OVERWRITE=TRUE").fetchall()


# Procesa un origen completo: lista meses pendientes, descarga, descomprime y sube.
def process(conn, label, stage, start_month, end_month, prefix):
    print(f"\n--- {label} -> stage {stage} ---")
    cargados   = existing_months(conn, stage, prefix)
    pendientes = [m for m in months(start_month, end_month) if m not in cargados]
    print(f"  pendientes: {len(pendientes)}")
    if not pendientes:
        return
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        for m in pendientes:
            z = download(m, folder, prefix)
            if not z:
                print(f"  [{label}] {m}: no encontrado en S3, salto")
                continue
            for csv in unzip_csvs(z, folder):
                put(conn, csv, stage)
                print(f"  [{label}] subido: {csv.name}")


def main():
    load_dotenv()
    while True:
        mode = input("Entorno destino (DEV, PRO): ").strip().upper()
        if mode in ("DEV", "PRO"):
            break
        print("Opcion invalida. DEV o PRO.")

    db        = os.environ.get(f"{mode}_SF_DATABASE")
    stage_jc  = os.environ.get(f"{mode}_SF_STAGE")
    stage_ny  = os.environ.get(f"{mode}_SF_STAGE_NY")
    today     = date.today()
    current   = f"{today.year:04d}{today.month:02d}"
    print(f"Database: {db}")
    print(f"Stages:   JC -> {stage_jc} | NY -> {stage_ny}")

    conn = snowflake.connector.connect(
        account   = os.environ["SF_ACCOUNT"],
        user      = os.environ["SF_USER"],
        password  = os.environ["SF_PASSWORD"],
        role      = os.environ.get("SF_ROLE"),
        warehouse = os.environ.get("SF_WAREHOUSE"),
        database  = db,
        schema    = os.environ.get("SF_SCHEMA"),
    )
    try:
        process(conn, "JC", stage_jc, "202401", current, "JC-")
        process(conn, "NY", stage_ny, "202604", current, "")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
