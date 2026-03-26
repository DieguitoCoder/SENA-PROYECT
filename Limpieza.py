"""
==========================================================
  Data Cleaning Pipeline — Riwi Analytics Assessment
  Author: Dieguito Coder
  Description: Automated data cleaning and transformation
  pipeline for LATAM retail sales data.
==========================================================
"""

import pandas as pd
import re
import logging
import os
import sys

# ── LOGGING CONFIG ──────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ── CONSTANTS ───────────────────────────────────────────

INPUT_CSV = "Datos.csv"
OUTPUT_CSV = "Datos_limpio.csv"

TEXT_COLUMNS = [
    'producto', 'tipo_producto', 'ciudad',
    'pais', 'tipo_venta', 'tipo_cliente'
]

NUMERIC_COLUMNS = [
    'cantidad', 'precio_unitario',
    'descuento', 'costo_envio', 'total'
]

CRITICAL_COLUMNS = ['fecha', 'producto', 'cantidad', 'precio_unitario']

FINAL_COLUMN_ORDER = [
    'fecha', 'year', 'month', 'year_month',
    'producto', 'tipo_producto',
    'cantidad', 'precio_unitario',
    'descuento', 'costo_envio', 'total',
    'tipo_venta', 'tipo_cliente', 'segmento_cliente',
    'ciudad', 'pais'
]

NULL_DEFAULTS = {
    'descuento': 0,
    'costo_envio': 0,
    'tipo_venta': 'Desconocido',
    'tipo_cliente': 'Desconocido',
    'ciudad': 'Desconocida'
}

SEGMENT_MAP = {
    'Corporativo': 'Enterprise',
    'Gobierno': 'Enterprise',
    'Mayorista': 'Wholesale'
}


# ── HELPER FUNCTIONS ────────────────────────────────────

def clean_text(value: str) -> str:
    """
    Removes special characters and normalizes whitespace
    from a text value. Returns NaN values unchanged.

    Args:
        value: Input string or NaN value.

    Returns:
        Cleaned string or original NaN.
    """
    if pd.isna(value):
        return value
    if isinstance(value, str):
        value = re.sub(r'[@#*$%^&(){}[\]<>~`]', '', value)
        value = re.sub(r'\s+', ' ', value)
        return value.strip()
    return value


def segment_customer(customer_type: str) -> str:
    """
    Classifies customers into business segments based
    on their type.

    Args:
        customer_type: Original customer type label.

    Returns:
        Segment label: 'Enterprise', 'Wholesale', or 'Retail'.
    """
    return SEGMENT_MAP.get(customer_type, 'Retail')


# ── PIPELINE STAGES ─────────────────────────────────────

def load_raw_data(filepath: str) -> pd.DataFrame:
    """Stage 1: Load CSV raw data and validate file existence."""
    if not os.path.exists(filepath):
        logger.error(f"Input file not found: {filepath}")
        sys.exit(1)

    df = pd.read_csv(filepath)
    logger.info(f"Raw data loaded: {len(df)} rows, {len(df.columns)} columns")
    return df


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 2: Normalize column names to lowercase with underscores."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    logger.info(f"Columns normalized: {list(df.columns)}")
    return df


def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 3: Remove special characters from text columns."""
    cleaned_count = 0
    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
            cleaned_count += 1
    logger.info(f"Text cleaning applied to {cleaned_count} columns")
    return df


def standardize_capitalization(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 4: Standardize text casing to Title Case."""
    df['producto'] = df['producto'].str.title()
    df['tipo_producto'] = df['tipo_producto'].str.replace("_", " ").str.title()
    df['ciudad'] = df['ciudad'].str.title()
    df['pais'] = df['pais'].str.title()
    df['tipo_venta'] = df['tipo_venta'].str.replace("_", " ").str.title()
    df['tipo_cliente'] = df['tipo_cliente'].str.title()
    logger.info("Capitalization standardized to Title Case")
    return df


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 5: Convert columns to appropriate data types."""
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
    logger.info("Data types converted (numeric + datetime)")
    return df


def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 6: Apply business rules — filter invalid records."""
    initial_count = len(df)

    # Sales must be positive
    df = df[df['total'] > 0]
    removed_negative = initial_count - len(df)

    # Remove rows missing critical fields
    df = df.dropna(subset=CRITICAL_COLUMNS)
    removed_nulls = initial_count - removed_negative - len(df)

    logger.info(
        f"Business rules applied: "
        f"removed {removed_negative} negative sales, "
        f"{removed_nulls} rows with null critical fields"
    )
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 7: Remove duplicate rows."""
    initial_count = len(df)
    df = df.drop_duplicates()
    removed = initial_count - len(df)
    logger.info(f"Duplicates removed: {removed} rows")
    return df


def create_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 8: Create time-based and segmentation columns."""
    df['year'] = df['fecha'].dt.year
    df['month'] = df['fecha'].dt.month
    df['year_month'] = df['fecha'].dt.to_period('M').astype(str)
    df['segmento_cliente'] = df['tipo_cliente'].apply(segment_customer)

    logger.info(
        f"Derived columns created: year, month, year_month, segmento_cliente | "
        f"Segments: {df['segmento_cliente'].value_counts().to_dict()}"
    )
    return df


def handle_null_values(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 9: Fill remaining null values with defaults."""
    for col, default_val in NULL_DEFAULTS.items():
        if col in df.columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                df[col] = df[col].fillna(default_val)
                logger.info(f"Filled {null_count} nulls in '{col}' with '{default_val}'")

    remaining_nulls = df.isnull().sum().sum()
    logger.info(f"Remaining null values in dataset: {remaining_nulls}")
    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 10: Reorder columns for final output."""
    available_cols = [col for col in FINAL_COLUMN_ORDER if col in df.columns]
    df = df[available_cols]
    logger.info(f"Final column order: {len(available_cols)} columns")
    return df


def export_clean_data(df: pd.DataFrame, filepath: str) -> None:
    """Stage 11: Export the cleaned DataFrame to CSV."""
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    logger.info(f"Clean data exported to: {filepath}")


def print_summary(df: pd.DataFrame) -> None:
    """Stage 12: Print a comprehensive cleaning summary."""
    print("\n" + "=" * 60)
    print("  DATA CLEANING SUMMARY — Dieguito Coder")
    print("=" * 60)
    print(f"  Final rows:        {len(df):,}")
    print(f"  Final columns:     {len(df.columns)}")
    print(f"  Memory usage:      {df.memory_usage(deep=True).sum() / 1024:.1f} KB")
    print(f"  Date range:        {df['fecha'].min()} → {df['fecha'].max()}")
    print(f"  Total null values: {df.isnull().sum().sum()}")
    print("-" * 60)
    print("  Null counts by column:")
    for col, count in df.isnull().sum().items():
        status = "✓" if count == 0 else f"⚠ {count}"
        print(f"    {col:<25} {status}")
    print("-" * 60)
    print("  Data types:")
    for col, dtype in df.dtypes.items():
        print(f"    {col:<25} {dtype}")
    print("=" * 60)
    print(f"  ✓ Output file: {OUTPUT_CSV}")
    print("=" * 60 + "\n")


# ── MAIN PIPELINE ───────────────────────────────────────

def run_pipeline():
    """
    Executes the full data cleaning pipeline in sequence:
    Load → Normalize → Clean → Standardize → Convert →
    Filter → Deduplicate → Derive → Fill Nulls → Reorder → Export
    """
    logger.info("=" * 50)
    logger.info("STARTING DATA CLEANING PIPELINE")
    logger.info("=" * 50)

    try:
        df = load_raw_data(INPUT_CSV)
        df = normalize_column_names(df)
        df = clean_text_columns(df)
        df = standardize_capitalization(df)
        df = convert_data_types(df)
        df = apply_business_rules(df)
        df = remove_duplicates(df)
        df = create_derived_columns(df)
        df = handle_null_values(df)
        df = reorder_columns(df)
        export_clean_data(df, OUTPUT_CSV)
        print_summary(df)

        logger.info("PIPELINE COMPLETED SUCCESSFULLY ✓")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        logger.error("The input CSV file is empty")
        sys.exit(1)
    except KeyError as e:
        logger.error(f"Missing expected column: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error in pipeline: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()
