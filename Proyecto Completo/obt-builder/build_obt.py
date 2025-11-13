import argparse
import os
import sys
import textwrap
from typing import Iterable, List, Tuple, Set
import time
from datetime import datetime

import psycopg2


def env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def get_conn():
    """Return a Postgres connection using ONLY environment variables.

    Se eliminan los valores por defecto para evitar depender de credenciales hardcodeadas.
    Si falta alguna variable requerida se lanza un error explícito.
    """
    host = env("PG_HOST")
    port_raw = env("PG_PORT")
    db = env("PG_DB")
    user = env("PG_USER")
    pw = env("PG_PASSWORD")
    # Validaciones mínimas
    try:
        port = int(port_raw)
    except ValueError as e:
        raise RuntimeError(f"PG_PORT debe ser entero, valor recibido: {port_raw}") from e
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)


def ensure_schemas_and_table(conn, schema_raw: str, schema_analytics: str):
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema_raw};
    CREATE SCHEMA IF NOT EXISTS {schema_analytics};

    CREATE TABLE IF NOT EXISTS {schema_analytics}.obt_trips (
        pickup_datetime           TIMESTAMP,
        dropoff_datetime          TIMESTAMP,
        pickup_hour               INTEGER,
        pickup_dow                INTEGER,
        month                     INTEGER,
        year                      INTEGER,

        pu_location_id            INTEGER,
        pu_zone                   TEXT,
        pu_borough                TEXT,
        do_location_id            INTEGER,
        do_zone                   TEXT,
        do_borough                TEXT,

        service_type              TEXT,
        vendor_id                 INTEGER,
        vendor_name               TEXT,
        rate_code_id              INTEGER,
        rate_code_desc            TEXT,
        payment_type              INTEGER,
        payment_type_desc         TEXT,
        trip_type                 INTEGER,

        passenger_count           INTEGER,
        trip_distance             DOUBLE PRECISION,
        fare_amount               DOUBLE PRECISION,
        extra                     DOUBLE PRECISION,
        mta_tax                   DOUBLE PRECISION,
        tip_amount                DOUBLE PRECISION,
        tolls_amount              DOUBLE PRECISION,
        improvement_surcharge     DOUBLE PRECISION,
        congestion_surcharge      DOUBLE PRECISION,
        airport_fee               DOUBLE PRECISION,
        total_amount              DOUBLE PRECISION,
        store_and_fwd_flag        TEXT,

        trip_duration_min         DOUBLE PRECISION,
        avg_speed_mph             DOUBLE PRECISION,
        tip_pct                   DOUBLE PRECISION,

        run_id                    TEXT,
        source_year               INTEGER,
        source_month              INTEGER,
        ingested_at_utc           TIMESTAMP DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS idx_obt_service_ym ON {schema_analytics}.obt_trips(service_type, year, month);
    CREATE INDEX IF NOT EXISTS idx_obt_pickup ON {schema_analytics}.obt_trips(pickup_datetime);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def parse_months(months: str | None) -> List[int]:
    if not months:
        return list(range(1, 13))
    out: List[int] = []
    for part in months.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            m = int(part)
            if 1 <= m <= 12:
                out.append(m)
        except ValueError:
            pass
    if not out:
        return list(range(1, 12 + 1))
    return sorted(set(out))


def get_existing_columns(conn, schema: str, table: str) -> Set[str]:
    q = textwrap.dedent(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """
    )
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return {r[0] for r in cur.fetchall()}


def _quote_ident(name: str) -> str:
    # Quote identifier if it contains uppercase or non-lowercase ascii chars
    if not name.islower():
        return '"' + name.replace('"', '""') + '"'
    return name


def col_expr(cols: Set[str], candidates: List[str], cast: str | None = None) -> str:
    for cand in candidates:
        if cand in cols:
            return _quote_ident(cand)
    # Fallback: NULL or typed NULL
    if cast:
        return f"NULL::{cast}"
    return "NULL"


def has_raw_partition(conn, schema_raw: str, table: str, year: int, month: int) -> bool:
    q = textwrap.dedent(
        f"""
        SELECT 1
        FROM {schema_raw}.{table}
        WHERE source_year = %s AND source_month = %s
        LIMIT 1;
        """
    )
    with conn.cursor() as cur:
        cur.execute(q, (year, month))
        return cur.fetchone() is not None


def obt_partition_exists(conn, schema_analytics: str, service: str, year: int, month: int) -> bool:
    q = textwrap.dedent(
        f"""
        SELECT 1
        FROM {schema_analytics}.obt_trips
        WHERE service_type = %s AND source_year = %s AND source_month = %s
        LIMIT 1;
        """
    )
    with conn.cursor() as cur:
        cur.execute(q, (service, year, month))
        return cur.fetchone() is not None


def delete_obt_partition(conn, schema_analytics: str, service: str, year: int, month: int) -> int:
    q = textwrap.dedent(
        f"DELETE FROM {schema_analytics}.obt_trips WHERE service_type=%s AND source_year=%s AND source_month=%s;"
    )
    with conn.cursor() as cur:
        cur.execute(q, (service, year, month))
        deleted = cur.rowcount
    conn.commit()
    return deleted


def insert_yellow(conn, schema_raw: str, schema_analytics: str, year: int, month: int, run_id: str) -> int:
    cols = get_existing_columns(conn, schema_raw, 'yellow_taxi_trip')
    pu_loc = col_expr(cols, ["pulocationid", "PULocationID"], cast="int")
    do_loc = col_expr(cols, ["dolocationid", "DOLocationID"], cast="int")
    vendor = col_expr(cols, ["vendorid", "VendorID"], cast="int")
    ratecd = col_expr(cols, ["ratecodeid", "RatecodeID"], cast="int")
    paytyp = col_expr(cols, ["payment_type", "Payment_type", "paymentType", "PaymentType"], cast="int")
    imp_s  = col_expr(cols, ["improvement_surcharge"], cast="double precision")
    cong_s = col_expr(cols, ["congestion_surcharge"], cast="double precision")
    air_f  = col_expr(cols, ["airport_fee"], cast="double precision")
    storef = col_expr(cols, ["store_and_fwd_flag", "store_and_fwd_flag"], cast="text")

    sql = f"""
        INSERT INTO {schema_analytics}.obt_trips (
            pickup_datetime, dropoff_datetime, pickup_hour, pickup_dow, month, year,
            pu_location_id, pu_zone, pu_borough, do_location_id, do_zone, do_borough,
            service_type, vendor_id, vendor_name, rate_code_id, rate_code_desc,
            payment_type, payment_type_desc, trip_type,
            passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount,
            tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee,
            total_amount, store_and_fwd_flag, trip_duration_min, avg_speed_mph, tip_pct,
            run_id, source_year, source_month, ingested_at_utc
        )
        SELECT
            y.tpep_pickup_datetime               AS pickup_datetime,
            y.tpep_dropoff_datetime              AS dropoff_datetime,
            EXTRACT(HOUR FROM y.tpep_pickup_datetime)::int AS pickup_hour,
            EXTRACT(DOW  FROM y.tpep_pickup_datetime)::int AS pickup_dow,
            y.source_month::int                  AS month,
            y.source_year::int                   AS year,
            {pu_loc}                             AS pu_location_id,
            pu.zone                              AS pu_zone,
            pu.borough                           AS pu_borough,
            {do_loc}                             AS do_location_id,
            dz.zone                              AS do_zone,
            dz.borough                           AS do_borough,
            'yellow'                             AS service_type,
            {vendor}                             AS vendor_id,
            CASE {vendor}
                WHEN 1 THEN 'Creative Mobile Technologies'
                WHEN 2 THEN 'VeriFone'
                ELSE 'Other'
            END                                   AS vendor_name,
            {ratecd}                              AS rate_code_id,
            CASE {ratecd}
                WHEN 1 THEN 'Standard rate'
                WHEN 2 THEN 'JFK'
                WHEN 3 THEN 'Newark'
                WHEN 4 THEN 'Nassau or Westchester'
                WHEN 5 THEN 'Negotiated fare'
                WHEN 6 THEN 'Group ride'
                ELSE 'Other'
            END                                   AS rate_code_desc,
            {paytyp}                              AS payment_type,
            CASE {paytyp}
                WHEN 1 THEN 'Credit card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided trip'
                ELSE 'Other'
            END                                   AS payment_type_desc,
            NULL::int                             AS trip_type,

            y.passenger_count,
            y.trip_distance,
            y.fare_amount,
            y.extra,
            y.mta_tax,
            y.tip_amount,
            y.tolls_amount,
            {imp_s}                               AS improvement_surcharge,
            COALESCE({cong_s}, 0)::double precision AS congestion_surcharge,
            COALESCE({air_f}, 0)::double precision   AS airport_fee,
            y.total_amount,
            {storef}                              AS store_and_fwd_flag,

            GREATEST(EXTRACT(EPOCH FROM (y.tpep_dropoff_datetime - y.tpep_pickup_datetime))/60.0, 0) AS trip_duration_min,
            CASE WHEN (EXTRACT(EPOCH FROM (y.tpep_dropoff_datetime - y.tpep_pickup_datetime)) > 0)
                 THEN y.trip_distance / NULLIF(EXTRACT(EPOCH FROM (y.tpep_dropoff_datetime - y.tpep_pickup_datetime))/3600.0, 0)
                 ELSE NULL END AS avg_speed_mph,
            CASE WHEN y.total_amount IS NOT NULL AND y.total_amount <> 0 THEN y.tip_amount / NULLIF(y.total_amount,0) ELSE NULL END AS tip_pct,

            %s AS run_id,
            y.source_year,
            y.source_month,
            now() AS ingested_at_utc
        FROM {schema_raw}.yellow_taxi_trip y
        LEFT JOIN {schema_raw}.taxi_zone_lookup pu ON pu.location_id = {pu_loc}
        LEFT JOIN {schema_raw}.taxi_zone_lookup dz ON dz.location_id = {do_loc}
        WHERE y.source_year = %s AND y.source_month = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, year, month))
        inserted = cur.rowcount
    conn.commit()
    return inserted


def insert_green(conn, schema_raw: str, schema_analytics: str, year: int, month: int, run_id: str) -> int:
    cols = get_existing_columns(conn, schema_raw, 'green_taxi_trip')
    pu_loc = col_expr(cols, ["pulocationid", "PULocationID"], cast="int")
    do_loc = col_expr(cols, ["dolocationid", "DOLocationID"], cast="int")
    vendor = col_expr(cols, ["vendorid", "VendorID"], cast="int")
    ratecd = col_expr(cols, ["ratecodeid", "RatecodeID"], cast="int")
    paytyp = col_expr(cols, ["payment_type", "Payment_type", "paymentType", "PaymentType"], cast="int")
    imp_s  = col_expr(cols, ["improvement_surcharge"], cast="double precision")
    cong_s = col_expr(cols, ["congestion_surcharge"], cast="double precision")
    air_f  = col_expr(cols, ["airport_fee"], cast="double precision")
    storef = col_expr(cols, ["store_and_fwd_flag", "store_and_fwd_flag"], cast="text")
    trip_t = col_expr(cols, ["trip_type"], cast="int")

    sql = f"""
        INSERT INTO {schema_analytics}.obt_trips (
            pickup_datetime, dropoff_datetime, pickup_hour, pickup_dow, month, year,
            pu_location_id, pu_zone, pu_borough, do_location_id, do_zone, do_borough,
            service_type, vendor_id, vendor_name, rate_code_id, rate_code_desc,
            payment_type, payment_type_desc, trip_type,
            passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount,
            tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee,
            total_amount, store_and_fwd_flag, trip_duration_min, avg_speed_mph, tip_pct,
            run_id, source_year, source_month, ingested_at_utc
        )
        SELECT
            g.lpep_pickup_datetime                 AS pickup_datetime,
            g.lpep_dropoff_datetime                AS dropoff_datetime,
            EXTRACT(HOUR FROM g.lpep_pickup_datetime)::int AS pickup_hour,
            EXTRACT(DOW  FROM g.lpep_pickup_datetime)::int AS pickup_dow,
            g.source_month::int                    AS month,
            g.source_year::int                     AS year,
            {pu_loc}                               AS pu_location_id,
            pu.zone                                AS pu_zone,
            pu.borough                             AS pu_borough,
            {do_loc}                               AS do_location_id,
            dz.zone                                AS do_zone,
            dz.borough                             AS do_borough,
            'green'                                AS service_type,
            {vendor}                               AS vendor_id,
            CASE {vendor}
                WHEN 1 THEN 'Creative Mobile Technologies'
                WHEN 2 THEN 'VeriFone'
                ELSE 'Other'
            END                                     AS vendor_name,
            {ratecd}                                AS rate_code_id,
            CASE {ratecd}
                WHEN 1 THEN 'Standard rate'
                WHEN 2 THEN 'JFK'
                WHEN 3 THEN 'Newark'
                WHEN 4 THEN 'Nassau or Westchester'
                WHEN 5 THEN 'Negotiated fare'
                WHEN 6 THEN 'Group ride'
                ELSE 'Other'
            END                                     AS rate_code_desc,
            {paytyp}                                AS payment_type,
            CASE {paytyp}
                WHEN 1 THEN 'Credit card'
                WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No charge'
                WHEN 4 THEN 'Dispute'
                WHEN 5 THEN 'Unknown'
                WHEN 6 THEN 'Voided trip'
                ELSE 'Other'
            END                                     AS payment_type_desc,
            {trip_t}                                AS trip_type,

            g.passenger_count,
            g.trip_distance,
            g.fare_amount,
            g.extra,
            g.mta_tax,
            g.tip_amount,
            g.tolls_amount,
            {imp_s}                                 AS improvement_surcharge,
            COALESCE({cong_s}, 0)::double precision AS congestion_surcharge,
            COALESCE({air_f}, 0)::double precision   AS airport_fee,
            g.total_amount,
            {storef}                                AS store_and_fwd_flag,

            GREATEST(EXTRACT(EPOCH FROM (g.lpep_dropoff_datetime - g.lpep_pickup_datetime))/60.0, 0) AS trip_duration_min,
            CASE WHEN (EXTRACT(EPOCH FROM (g.lpep_dropoff_datetime - g.lpep_pickup_datetime)) > 0)
                 THEN g.trip_distance / NULLIF(EXTRACT(EPOCH FROM (g.lpep_dropoff_datetime - g.lpep_pickup_datetime))/3600.0, 0)
                 ELSE NULL END AS avg_speed_mph,
            CASE WHEN g.total_amount IS NOT NULL AND g.total_amount <> 0 THEN g.tip_amount / NULLIF(g.total_amount,0) ELSE NULL END AS tip_pct,

            %s AS run_id,
            g.source_year,
            g.source_month,
            now() AS ingested_at_utc
        FROM {schema_raw}.green_taxi_trip g
        LEFT JOIN {schema_raw}.taxi_zone_lookup pu ON pu.location_id = {pu_loc}
        LEFT JOIN {schema_raw}.taxi_zone_lookup dz ON dz.location_id = {do_loc}
        WHERE g.source_year = %s AND g.source_month = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, year, month))
        inserted = cur.rowcount
    conn.commit()
    return inserted


def run_full(conn, schema_raw: str, schema_analytics: str, years: Iterable[int], months: List[int], services: List[str], run_id: str, overwrite: bool):
    total_inserted = 0
    t0_full = time.perf_counter()
    for year in years:
        for month in months:
            for service in services:
                table = f"{service}_taxi_trip"
                if not has_raw_partition(conn, schema_raw, table, year, month):
                    print(f"[{service} {year}-{month:02d}] RAW no disponible, se omite.", flush=True)
                    continue

                if overwrite:
                    deleted = delete_obt_partition(conn, schema_analytics, service, year, month)
                    if deleted:
                        print(f"[{service} {year}-{month:02d}] Overwrite: borradas {deleted} filas previas en OBT.", flush=True)
                else:
                    if obt_partition_exists(conn, schema_analytics, service, year, month):
                        print(f"[{service} {year}-{month:02d}] Ya existe en OBT. Usa --overwrite true para reescribir.", flush=True)
                        continue

                print(f"[{service} {year}-{month:02d}] INSERT iniciando...", flush=True)
                t0_part = time.perf_counter()
                if service == "yellow":
                    inserted = insert_yellow(conn, schema_raw, schema_analytics, year, month, run_id)
                else:
                    inserted = insert_green(conn, schema_raw, schema_analytics, year, month, run_id)
                t1_part = time.perf_counter()
                elapsed_part = t1_part - t0_part
                print(f"[{service} {year}-{month:02d}] Insertadas {inserted} filas en OBT en {elapsed_part:.2f} s.", flush=True)
                total_inserted += max(0, inserted or 0)
    t1_full = time.perf_counter()
    elapsed_full = t1_full - t0_full
    print(f"FULL: total insertadas {total_inserted} filas en {elapsed_full:.2f} s.", flush=True)


def run_by_partition(conn, schema_raw: str, schema_analytics: str, years: Iterable[int], months: List[int], services: List[str], run_id: str, overwrite: bool):
    # For this implementation, by-partition behaves like full but constrained by args
    run_full(conn, schema_raw, schema_analytics, years, months, services, run_id, overwrite)


def main():
    parser = argparse.ArgumentParser(description="Build or update analytics.obt_trips from raw.* (NYC TLC).")
    # Convenience shortcut to satisfy one-liner: `docker compose run obt-builder --full-rebuild`
    parser.add_argument('--full-rebuild', action='store_true', help='Shortcut for full rebuild 2015–2025 (yellow,green). Implies --overwrite true.')
    # Diagnóstico seguro de variables de entorno (sin exponer credenciales). Útil para comprobar que Compose las inyecta.
    parser.add_argument('--diagnose-env', action='store_true', help='Verifica presencia de variables PG_* y esquemas; no imprime valores sensibles.')

    # Standard arguments (not required here; we'll validate post-parse to allow --full-rebuild without specifying all)
    parser.add_argument('--mode', choices=['full', 'by-partition'])
    parser.add_argument('--year-start', type=int)
    parser.add_argument('--year-end', type=int)
    parser.add_argument('--months', type=str, default=None, help='Comma-separated months, e.g., 1,2,3 (default: 1-12)')
    parser.add_argument('--services', type=str, help='Comma-separated: yellow,green')
    parser.add_argument('--run-id', type=str)
    parser.add_argument('--overwrite', choices=['true', 'false'], default='false')

    args = parser.parse_args()

    # Ruta rápida: solo diagnóstico de variables, sin requerir otros argumentos ni conexión.
    if args.diagnose_env:
        required = ["PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD", "PG_SCHEMA_RAW", "PG_SCHEMA_ANALYTICS"]
        missing: List[str] = []
        print("[env-diagnose] Comprobando variables requeridas:", flush=True)
        for name in required:
            val = os.getenv(name)
            if val is None or val == "":
                missing.append(name)
                status = "MISSING"
                shown = ""  # nada
            else:
                status = "OK"
                # No imprimimos la contraseña completa; solo longitud para validar.
                if name == "PG_PASSWORD":
                    shown = f"len={len(val)}"
                else:
                    shown = "set"
            print(f"  - {name}: {status} {shown}")
        if missing:
            print(f"[env-diagnose] FALTAN variables: {', '.join(missing)}", file=sys.stderr)
            sys.exit(3)
        print("[env-diagnose] Todas las variables requeridas están presentes. ✔", flush=True)
        sys.exit(0)

    # Resolve shortcut or validate required args
    if args.full_rebuild:
        # Defaults for a full end-to-end build as per spec 2015–2025, both services
        mode = 'full'
        year_start = 2015
        year_end = 2025
        services_raw = 'yellow,green'
        run_id = args.run_id or f"AUTO_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        # Full rebuild should overwrite existing partitions by default
        overwrite = True
        months = parse_months(None)  # 1..12
    else:
        # Validate presence of standard args
        missing: List[str] = []
        if not args.mode:
            missing.append('--mode')
        if args.year_start is None:
            missing.append('--year-start')
        if args.year_end is None:
            missing.append('--year-end')
        if not args.services:
            missing.append('--services')
        if not args.run_id:
            missing.append('--run-id')
        if missing:
            print("ERROR: missing required arguments: " + ", ".join(missing), file=sys.stderr)
            parser.print_help(sys.stderr)
            sys.exit(2)

        mode = args.mode
        year_start = args.year_start
        year_end = args.year_end
        services_raw = args.services
        run_id = args.run_id
        overwrite = args.overwrite.lower() == 'true'
        months = parse_months(args.months)

    services = [s.strip().lower() for s in services_raw.split(',') if s.strip()]
    services = [s for s in services if s in ("yellow", "green")]
    if not services:
        print("ERROR: --services must include at least one of: yellow,green", file=sys.stderr)
        sys.exit(2)

    years = list(range(year_start, year_end + 1))

    # Requiere que los esquemas estén definidos explícitamente (sin defaults) para mayor transparencia.
    schema_raw = env('PG_SCHEMA_RAW')
    schema_analytics = env('PG_SCHEMA_ANALYTICS')

    print(f"OBT Builder starting: mode={mode} services={services} years={years} months={months} overwrite={overwrite} run_id={run_id}", flush=True)
    try:
        conn = get_conn()
    except Exception as e:
        print("ERROR: No se pudo conectar a Postgres:", e, file=sys.stderr)
        sys.exit(1)

    try:
        ensure_schemas_and_table(conn, schema_raw, schema_analytics)
        if mode == 'full':
            run_full(conn, schema_raw, schema_analytics, years, months, services, run_id, overwrite)
        else:
            run_by_partition(conn, schema_raw, schema_analytics, years, months, services, run_id, overwrite)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
