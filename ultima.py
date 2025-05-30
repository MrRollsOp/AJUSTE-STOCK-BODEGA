#!/usr/bin/env python3
"""
• Agrupa stock deseado por (art_id, vence)   → desired
• Consulta stock actual total en BD          → actual
• diff = deseado - actual  (puede ser ±)
• Genera INSERTs con diff
• UPDATE a cero pares ausentes
"""

import csv, sys
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

# ───────── Config ─────────
DB = {"host": "10.4.199.86", "port": 5432,
      "dbname": "mihis_end", "user": "postgres",
      "password": "soporte010203"}



# ───────── Config Dev ─────────
#DB = {"host": "10.4.199.39", "port": 5432,
#      "dbname": "mihis_dev", "user": "postgres",
#      "password": "soporte010203"}


BOD_ID  = 35   # ID DE LA BODEGA A AJUSTAR
LOG_ID  = 45635
CSV_FILE   = Path("bodega_farmacia.csv")  # CSV DE STOCK A INSERTAR
DELIM      = "|"
SQL_OUTPUT = Path("insert_stock_bodega.sql")

# ───────── Helpers ─────────
def get_art_id(cur, codigo: str):
    cur.execute("SELECT art_id FROM articulo WHERE art_codigo = %s", (codigo,))
    row = cur.fetchone()
    return row["art_id"] if row else None

def get_stock_actual_bulk(cur, keys):
    """keys = [(art_id, date_or_None), ...]  →  {(art_id, date): total}"""
    if not keys:
        return {}

    cur.execute("CREATE TEMP TABLE tmp_pairs(art_id int, vence date)")
    execute_values(cur,
        "INSERT INTO tmp_pairs(art_id, vence) VALUES %s ON CONFLICT DO NOTHING",
        list(set(keys)), template="(%s, %s)"
    )

    cur.execute(f"""
        SELECT p.art_id, p.vence,
               COALESCE(SUM(s.stock_cant), 0) AS total
        FROM  (SELECT DISTINCT art_id, vence FROM tmp_pairs) p
        LEFT JOIN stock s
               ON s.stock_bod_id = {BOD_ID}
              AND s.stock_art_id = p.art_id
              AND ((p.vence IS NULL AND s.stock_vence IS NULL)
                   OR p.vence = s.stock_vence)
        GROUP BY p.art_id, p.vence
    """)
    res = {(r["art_id"], r["vence"]): r["total"] for r in cur.fetchall()}
    cur.execute("DROP TABLE tmp_pairs")
    return res

# ───────── Main ─────────
def main():
    try:
        conn = psycopg2.connect(**DB)
    except Exception as e:
        sys.exit(f"💥 Conexión falló → {e}")

    # abre CSV
    try:
        f_in = open(CSV_FILE, newline="", encoding="utf-8")
        f_in.read(1); f_in.seek(0)
    except UnicodeDecodeError:
        f_in = open(CSV_FILE, newline="", encoding="latin-1")

    desired = {}                     # {(art_id, date|None): cant}
    with conn.cursor(cursor_factory=RealDictCursor) as cur, f_in:
        rdr = csv.reader(f_in, delimiter=DELIM)
        next(rdr, None)

        for ln, row in enumerate(rdr, 2):
            if len(row) < 4:
                print(f"⚠️ Línea {ln}: incompleta → {row}", file=sys.stderr)
                continue

            codigo, _lote, fv_txt, cant_txt = (c.strip() for c in row)
            try:
                cant = int(cant_txt)
            except ValueError:
                print(f"⚠️ Línea {ln}: STOCK '{cant_txt}' inválido", file=sys.stderr)
                continue

            art_id = get_art_id(cur, codigo)
            if art_id is None:
                print(f"❌ Línea {ln}: código '{codigo}' no existe", file=sys.stderr)
                continue

            # ★★ usa objeto date, no string
            vence_val = None
            if fv_txt:
                try:
                    vence_val = datetime.strptime(fv_txt, "%d-%m-%Y").date()
                except ValueError:
                    print(f"⚠️ Línea {ln}: F/V '{fv_txt}' inválida → NULL", file=sys.stderr)

            key = (art_id, vence_val)
            desired[key] = desired.get(key, 0) + cant

    # consulta stock actual
    with conn, conn.cursor(cursor_factory=RealDictCursor) as cur, \
         open(SQL_OUTPUT, "w", encoding="utf-8") as out:

        actual = get_stock_actual_bulk(cur, list(desired.keys()))

        # INSERTS
        for (art_id, vence), deseado in desired.items():
            actual_tot = actual.get((art_id, vence), 0)
            diff = deseado - actual_tot
            if diff == 0:
                continue

            vence_sql = "NULL"
            if vence is not None:
                vence_sql = f"'{vence.isoformat()}'::date"   # ★★

            out.write(
                f"INSERT INTO stock (stock_art_id, stock_bod_id, stock_cant, "
                f" stock_subtotal, stock_log_id, stock_vence) "
                f"VALUES ({art_id}, {BOD_ID}, {diff}, {diff}, {LOG_ID}, {vence_sql});\n"
            )

        # UPDATE a cero los pares ausentes
        out.write("\n-- —— ZEROS PARA PARES AUSENTES ——\n")
        out.write("WITH csv_data(art_id, vence) AS (\n  VALUES\n")
        pairs = list(desired.keys())
        for i, (aid, v) in enumerate(pairs):
            v_sql = "NULL::date" if v is None else f"'{v.isoformat()}'::date"  # ★★
            out.write(f"    ({aid}, {v_sql}){',' if i < len(pairs)-1 else ''}\n")
        out.write(")\n")
        out.write(f"""UPDATE stock AS s
SET    stock_cant     = 0,
       stock_subtotal = 0,
       stock_log_id   = {LOG_ID}
WHERE  s.stock_bod_id = {BOD_ID}
  AND NOT EXISTS (
        SELECT 1 FROM csv_data d
        WHERE d.art_id = s.stock_art_id
          AND ((d.vence IS NULL AND s.stock_vence IS NULL)
               OR d.vence = s.stock_vence)
);\n""")

    print(f"✅ Script generado: {SQL_OUTPUT}")

if __name__ == "__main__":
    main()
