"""
==========================================================
  ETL Pipeline — Riwi Analytics Assessment
  Author: Dieguito Coder
  Description: Extracts cleaned data from CSV, transforms
  it into a Star Schema, and loads it into PostgreSQL.
==========================================================
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys
import logging

# ── LOGGING CONFIG ──────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ── CONSTANTS ───────────────────────────────────────────

INPUT_CSV = "Datos_limpio.csv"

REQUIRED_COLUMNS = [
    "fecha", "producto", "tipo_producto",
    "tipo_cliente", "segmento_cliente",
    "tipo_venta", "ciudad", "pais", "total"
]


# ── DATABASE CONNECTION ─────────────────────────────────

def get_engine():
    """
    Creates a SQLAlchemy engine from the DATABASE_URL
    environment variable. Validates connection on creation.

    Returns:
        SQLAlchemy Engine instance.
    """
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        logger.error("DATABASE_URL not found in .env file")
        sys.exit(1)

    try:
        engine = create_engine(database_url)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection established successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)


# ── DATA LOADING ────────────────────────────────────────

def load_csv(filepath: str) -> pd.DataFrame:
    """
    Loads the cleaned CSV file and performs initial validations.

    Args:
        filepath: Path to the cleaned CSV file.

    Returns:
        Validated DataFrame ready for ETL.
    """
    if not os.path.exists(filepath):
        logger.error(f"Input file not found: {filepath}")
        sys.exit(1)

    df = pd.read_csv(filepath)
    logger.info(f"CSV loaded: {len(df)} rows detected")
    logger.info(f"Columns: {list(df.columns)}")

    # Normalize and validate
    df.columns = df.columns.str.lower()
    df["fecha"] = pd.to_datetime(df["fecha"])

    # Check required columns exist
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        logger.error(f"Missing required columns: {missing}")
        sys.exit(1)

    # Drop rows with nulls in required columns
    initial = len(df)
    df = df.dropna(subset=REQUIRED_COLUMNS)
    dropped = initial - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} rows with null required fields")

    logger.info(f"Data ready for ETL: {len(df)} rows")
    return df


# ── DIMENSION LOADERS ───────────────────────────────────

def load_dim_region(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    Loads unique region data into dim_region and maps
    region IDs back to the DataFrame.

    Args:
        conn: Active database connection.
        df: DataFrame with 'ciudad' and 'pais' columns.

    Returns:
        DataFrame with 'id_region' column added.
    """
    regiones = df[["ciudad", "pais"]].drop_duplicates()

    conn.execute(text("""
        INSERT INTO dim_region (ciudad, pais)
        VALUES (:ciudad, :pais)
        ON CONFLICT (ciudad, pais) DO NOTHING
    """), regiones.to_dict(orient="records"))

    region_map = pd.read_sql(
        "SELECT id_region, ciudad, pais FROM dim_region", conn
    )
    df = df.merge(region_map, on=["ciudad", "pais"], how="left")

    logger.info(f"dim_region: {len(regiones)} unique regions loaded")
    return df


def load_dim_cliente(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    Loads unique customer data into dim_cliente and maps
    client IDs back to the DataFrame.

    Args:
        conn: Active database connection.
        df: DataFrame with customer fields and id_region.

    Returns:
        DataFrame with 'id_cliente' column added.
    """
    clientes = df[[
        "tipo_cliente", "segmento_cliente",
        "tipo_venta", "id_region"
    ]].drop_duplicates()

    conn.execute(text("""
        INSERT INTO dim_cliente (
            tipo_cliente, segmento_cliente,
            tipo_venta, id_region
        )
        VALUES (
            :tipo_cliente, :segmento_cliente,
            :tipo_venta, :id_region
        )
        ON CONFLICT (
            tipo_cliente, segmento_cliente,
            tipo_venta, id_region
        ) DO NOTHING
    """), clientes.to_dict(orient="records"))

    cliente_map = pd.read_sql("""
        SELECT id_cliente, tipo_cliente, segmento_cliente,
               tipo_venta, id_region
        FROM dim_cliente
    """, conn)

    df = df.merge(
        cliente_map,
        on=["tipo_cliente", "segmento_cliente", "tipo_venta", "id_region"],
        how="left"
    )

    logger.info(f"dim_cliente: {len(clientes)} unique clients loaded")
    return df


def load_dim_producto(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    Loads unique product data into dim_producto and maps
    product IDs back to the DataFrame.

    Args:
        conn: Active database connection.
        df: DataFrame with 'producto' and 'tipo_producto'.

    Returns:
        DataFrame with 'id_producto' column added.
    """
    productos = df[["producto", "tipo_producto"]].drop_duplicates()

    conn.execute(text("""
        INSERT INTO dim_producto (producto, tipo_producto)
        VALUES (:producto, :tipo_producto)
        ON CONFLICT (producto, tipo_producto) DO NOTHING
    """), productos.to_dict(orient="records"))

    producto_map = pd.read_sql("""
        SELECT id_producto, producto, tipo_producto FROM dim_producto
    """, conn)

    df = df.merge(
        producto_map,
        on=["producto", "tipo_producto"],
        how="left"
    )

    logger.info(f"dim_producto: {len(productos)} unique products loaded")
    return df


def load_dim_fecha(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    Loads unique date data into dim_fecha and maps
    date IDs back to the DataFrame.

    Args:
        conn: Active database connection.
        df: DataFrame with date columns.

    Returns:
        DataFrame with 'id_fecha' column added.
    """
    fechas = df[["fecha", "year", "month", "year_month"]].drop_duplicates()

    conn.execute(text("""
        INSERT INTO dim_fecha (fecha, year, month, year_month)
        VALUES (:fecha, :year, :month, :year_month)
        ON CONFLICT (fecha) DO NOTHING
    """), fechas.to_dict(orient="records"))

    fecha_map = pd.read_sql(
        "SELECT id_fecha, fecha FROM dim_fecha", conn
    )
    fecha_map["fecha"] = pd.to_datetime(fecha_map["fecha"])
    df = df.merge(fecha_map, on="fecha", how="left")

    logger.info(f"dim_fecha: {len(fechas)} unique dates loaded")
    return df


# ── FACT TABLE LOADER ───────────────────────────────────

def load_fact_ventas(conn, df: pd.DataFrame) -> int:
    """
    Loads transactional data into fact_ventas.
    Truncates existing data first to prevent duplicates
    on re-runs.

    Args:
        conn: Active database connection.
        df: DataFrame with all dimension IDs mapped.

    Returns:
        Number of records inserted.
    """
    fact = df[[
        "id_fecha", "id_producto", "id_cliente",
        "cantidad", "precio_unitario",
        "descuento", "costo_envio", "total"
    ]].dropna()

    # Prevent duplicates on re-execution
    conn.execute(text("TRUNCATE TABLE fact_ventas RESTART IDENTITY"))
    logger.info("fact_ventas truncated (clean re-load)")

    conn.execute(text("""
        INSERT INTO fact_ventas (
            id_fecha, id_producto, id_cliente,
            cantidad, precio_unitario,
            descuento, costo_envio, total
        )
        VALUES (
            :id_fecha, :id_producto, :id_cliente,
            :cantidad, :precio_unitario,
            :descuento, :costo_envio, :total
        )
    """), fact.to_dict(orient="records"))

    logger.info(f"fact_ventas: {len(fact)} records inserted")
    return len(fact)


# ── MAIN ETL PIPELINE ───────────────────────────────────

def run_etl():
    """
    Executes the complete ETL pipeline:
    1. Connect to PostgreSQL
    2. Load cleaned CSV
    3. Load dimensions (region → cliente → producto → fecha)
    4. Load fact table (ventas)
    5. Print summary
    """
    logger.info("=" * 50)
    logger.info("STARTING ETL PIPELINE")
    logger.info("=" * 50)

    try:
        engine = get_engine()
        df = load_csv(INPUT_CSV)

        with engine.begin() as conn:
            # Load dimensions in dependency order
            df = load_dim_region(conn, df)
            df = load_dim_cliente(conn, df)
            df = load_dim_producto(conn, df)
            df = load_dim_fecha(conn, df)

            # Load fact table
            records = load_fact_ventas(conn, df)

        # Summary
        print("\n" + "=" * 50)
        print("  ETL PIPELINE — COMPLETED SUCCESSFULLY ✓")
        print("=" * 50)
        print(f"  Records loaded:  {records:,}")
        print(f"  Source file:     {INPUT_CSV}")
        print(f"  Target DB:       PostgreSQL (Star Schema)")
        print("=" * 50 + "\n")

        logger.info("ETL COMPLETED SUCCESSFULLY ✓")

    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_etl()
