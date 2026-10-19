"""Etapa 1 — Ingesta batch hacia la zona RAW del Data Lake.

Descarga idempotente, con reintentos exponenciales y escritura atómica (.part → nombre final),
de modo que una caída de red nunca deja un archivo corrupto en el lake.
Uso:  python src/01_ingesta.py
"""
import time
import urllib.request

from common import BASE_URL, MONTHS, RAW


def download(url, dest, retries=3):
    if dest.exists() and dest.stat().st_size > 0:
        return "ya existe"
    for i in range(retries):
        try:
            tmp = dest.with_suffix(dest.suffix + ".part")
            urllib.request.urlretrieve(url, tmp)
            tmp.rename(dest)
            return "descargado"
        except Exception as e:  # noqa: BLE001
            print(f"  intento {i + 1} falló: {e}")
            time.sleep(2 ** i)
    raise RuntimeError(f"No se pudo descargar {url}")


if __name__ == "__main__":
    for m in MONTHS:
        f = RAW / f"yellow_tripdata_{m}.csv.gz"
        print(f.name, download(f"{BASE_URL}/yellow/{f.name}", f))
    print("taxi_zone_lookup.csv", download(f"{BASE_URL}/misc/taxi_zone_lookup.csv", RAW / "taxi_zone_lookup.csv"))
    total = sum(p.stat().st_size for p in RAW.glob("*.csv.gz"))
    print(f"RAW: {total / 1e6:.1f} MB")
