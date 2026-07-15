from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta, timezone
import re

import pandas as pd
from sqlalchemy import inspect, text


# =========================================================
# CONFIGURATION
# =========================================================

POSTGRES_CONN_ID = "postgres_dwh"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

BRONZE_LOG_TABLE = "etl_file_log"
SILVER_LOG_TABLE = "etl_table_log"


# =========================================================
# TABLE RULES
# =========================================================
#
# business_key:
#   Dipakai untuk menghapus duplicate record.
#
# datetime_columns:
#   Dikonversi menjadi timestamp/date PostgreSQL.
#
# numeric_columns:
#   Dibersihkan dari format seperti 80605,0 lalu dikonversi
#   menjadi angka.
#
# required_columns:
#   Jika kosong/invalid, baris dibuang dari Silver.
#
# uppercase_columns:
#   ID dibuat konsisten dalam huruf kapital.
#
# lowercase_columns:
#   Email dibuat konsisten dalam huruf kecil.
#
# titlecase_columns:
#   Teks nama/lokasi dirapikan.
#
# allowed_values:
#   Nilai kategori distandardisasi dan yang tidak valid
#   dibuat NULL.

TABLE_CONFIG = {
    "categories": {
        "business_key": ["category_id"],
        "datetime_columns": [],
        "numeric_columns": [],
        "required_columns": ["category_id", "category_name"],
        "uppercase_columns": ["category_id"],
        "lowercase_columns": [],
        "titlecase_columns": ["category_name"],
        "allowed_values": {},
    },
    "customers": {
        "business_key": ["customer_id"],
        "datetime_columns": ["join_date"],
        "numeric_columns": [],
        "required_columns": [
            "customer_id",
            "name",
            "email",
            "join_date",
        ],
        "uppercase_columns": ["customer_id"],
        "lowercase_columns": ["email"],
        "titlecase_columns": ["name", "city"],
        "allowed_values": {
            "segment": [
                "regular",
                "silver",
                "gold",
                "platinum",
            ],
        },
    },
    "stores": {
        "business_key": ["store_id"],
        "datetime_columns": ["opened_date"],
        "numeric_columns": [],
        "required_columns": [
            "store_id",
            "store_name",
            "city",
            "opened_date",
        ],
        "uppercase_columns": ["store_id"],
        "lowercase_columns": [],
        "titlecase_columns": [
            "store_name",
            "city",
        ],
        "allowed_values": {},
    },
    "products": {
        "business_key": ["product_id"],
        "datetime_columns": [],
        "numeric_columns": ["unit_price"],
        "required_columns": [
            "product_id",
            "product_name",
            "unit_price",
        ],
        "uppercase_columns": [
            "product_id",
            "category_id",
        ],
        "lowercase_columns": [],
        "titlecase_columns": ["product_name"],
        "allowed_values": {},
    },
    "promotions": {
        "business_key": ["promo_id"],
        "datetime_columns": [
            "start_date",
            "end_date",
        ],
        "numeric_columns": ["discount_pct"],
        "required_columns": [
            "promo_id",
            "promo_code",
            "discount_pct",
            "start_date",
            "end_date",
        ],
        "uppercase_columns": [
            "promo_id",
            "promo_code",
        ],
        "lowercase_columns": [],
        "titlecase_columns": [],
        "allowed_values": {},
    },
    "orders": {
        "business_key": ["order_id"],
        "datetime_columns": ["order_date"],
        "numeric_columns": [],
        "required_columns": [
            "order_id",
            "customer_id",
            "store_id",
            "order_date",
        ],
        "uppercase_columns": [
            "order_id",
            "customer_id",
            "store_id",
        ],
        "lowercase_columns": [],
        "titlecase_columns": [],
        "allowed_values": {
            "payment_method": [
                "e-wallet",
                "credit_card",
                "bank_transfer",
                "qris",
            ],
            "status": [
                "pending",
                "completed",
                "cancelled",
            ],
        },
    },
    "order_items": {
        "business_key": ["order_item_id"],
        "datetime_columns": [],
        "numeric_columns": [
            "quantity",
            "unit_price",
        ],
        "required_columns": [
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
        ],
        "uppercase_columns": [
            "order_item_id",
            "order_id",
            "product_id",
        ],
        "lowercase_columns": [],
        "titlecase_columns": [],
        "allowed_values": {},
    },
    "payments": {
        "business_key": ["payment_id"],
        "datetime_columns": ["paid_at"],
        "numeric_columns": ["amount"],
        "required_columns": [
            "payment_id",
            "order_id",
            "method",
            "amount",
            "status",
        ],
        "uppercase_columns": [
            "payment_id",
            "order_id",
        ],
        "lowercase_columns": [],
        "titlecase_columns": [],
        "allowed_values": {
            "method": [
                "e-wallet",
                "credit_card",
                "bank_transfer",
                "qris",
            ],
            "status": [
                "pending",
                "success",
                "failed",
            ],
        },
    },
    "shipments": {
        "business_key": ["shipment_id"],
        "datetime_columns": [
            "shipped_date",
            "delivered_date",
        ],
        "numeric_columns": [],
        "required_columns": [
            "shipment_id",
            "order_id",
            "shipped_date",
            "status",
        ],
        "uppercase_columns": [
            "shipment_id",
            "order_id",
        ],
        "lowercase_columns": [],
        "titlecase_columns": ["courier"],
        "allowed_values": {
            "status": [
                "pending",
                "in_transit",
                "delivered",
                "failed",
            ],
        },
    },
    "product_reviews": {
        "business_key": ["review_id"],
        "datetime_columns": ["review_date"],
        "numeric_columns": ["rating"],
        "required_columns": [
            "review_id",
            "customer_id",
            "product_id",
            "rating",
            "review_date",
        ],
        "uppercase_columns": [
            "review_id",
            "customer_id",
            "product_id",
        ],
        "lowercase_columns": [],
        "titlecase_columns": [],
        "allowed_values": {},
    },
    "order_promotions": {
        "business_key": [
            "order_id",
            "promo_id",
        ],
        "datetime_columns": [],
        "numeric_columns": [],
        "required_columns": [
            "order_id",
            "promo_id",
        ],
        "uppercase_columns": [
            "order_id",
            "promo_id",
        ],
        "lowercase_columns": [],
        "titlecase_columns": [],
        "allowed_values": {},
    },
}


BRONZE_METADATA_COLUMNS = {
    "source_file",
    "source_path",
    "ingestion_time",
}


# =========================================================
# HELPERS
# =========================================================

def validate_identifier(identifier: str) -> str:
    """
    Validasi nama schema, tabel, atau kolom SQL.
    """

    pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"

    if not re.match(pattern, identifier):
        raise ValueError(
            f"SQL identifier tidak valid: {identifier}"
        )

    return identifier


def normalize_column_name(column_name: str) -> str:
    """
    Normalisasi nama kolom menjadi snake_case.
    """

    normalized = re.sub(
        r"[^A-Za-z0-9_]+",
        "_",
        str(column_name).strip(),
    )

    normalized = re.sub(
        r"_+",
        "_",
        normalized,
    ).strip("_").lower()

    if not normalized:
        raise ValueError(
            f"Nama kolom tidak valid: {column_name}"
        )

    if normalized[0].isdigit():
        normalized = f"col_{normalized}"

    return normalized


def clean_numeric(series: pd.Series) -> pd.Series:
    """
    Membersihkan angka:
    - 80605,0  -> 80605.0
    - spasi    -> dihapus
    - karakter selain angka, minus, dan desimal dibuang
    """

    cleaned = (
        series.astype("string")
        .str.strip()
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(
            r"[^0-9.\-]",
            "",
            regex=True,
        )
    )

    return pd.to_numeric(
        cleaned,
        errors="coerce",
    )


def clean_datetime(series: pd.Series) -> pd.Series:
    """
    Mendukung format:
    - 2026-07-10
    - 2026-07-10 07:05:50
    - 10/07/2026 07:15:42
    """

    return pd.to_datetime(
        series,
        errors="coerce",
        dayfirst=True,
        format="mixed",
    )


def clean_phone(series: pd.Series) -> pd.Series:
    """
    Normalisasi telepon Indonesia.

    Contoh:
    +62 (751) 124-5123 -> +627511245123
    0828050109         -> +62828050109

    Nomor yang tidak dapat dibaca menjadi NULL.
    """

    digits = (
        series.astype("string")
        .str.replace(
            r"\D",
            "",
            regex=True,
        )
    )

    def normalize_phone(value):
        if pd.isna(value):
            return pd.NA

        value = str(value)

        if not value:
            return pd.NA

        if value.startswith("62"):
            return f"+{value}"

        if value.startswith("0"):
            return f"+62{value[1:]}"

        return f"+62{value}"

    return digits.map(normalize_phone)


def clean_email(series: pd.Series) -> pd.Series:
    """
    Email dijadikan lowercase dan email invalid menjadi NULL.
    """

    cleaned = (
        series.astype("string")
        .str.strip()
        .str.lower()
    )

    valid_mask = cleaned.str.match(
        r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$",
        na=False,
    )

    return cleaned.where(
        valid_mask,
        pd.NA,
    )


def update_silver_log(
    dag_run_id: str,
    table_name: str,
    status: str,
    extracted_rows: int = 0,
    loaded_rows: int = 0,
    rejected_rows: int = 0,
    error_message: str | None = None,
) -> None:
    """
    Update log proses Silver.
    """

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    hook.run(
        f"""
        INSERT INTO {SILVER_SCHEMA}.{SILVER_LOG_TABLE}
        (
            dag_run_id,
            table_name,
            status,
            extracted_rows,
            loaded_rows,
            rejected_rows,
            started_at,
            finished_at,
            error_message,
            updated_at
        )
        VALUES
        (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            CURRENT_TIMESTAMP,
            CASE
                WHEN %s IN ('SUCCESS', 'FAILED')
                THEN CURRENT_TIMESTAMP
                ELSE NULL
            END,
            %s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (dag_run_id, table_name)
        DO UPDATE SET
            status = EXCLUDED.status,
            extracted_rows = EXCLUDED.extracted_rows,
            loaded_rows = EXCLUDED.loaded_rows,
            rejected_rows = EXCLUDED.rejected_rows,
            finished_at = CASE
                WHEN EXCLUDED.status IN ('SUCCESS', 'FAILED')
                THEN CURRENT_TIMESTAMP
                ELSE {SILVER_SCHEMA}.{SILVER_LOG_TABLE}.finished_at
            END,
            error_message = EXCLUDED.error_message,
            updated_at = CURRENT_TIMESTAMP
        """,
        parameters=(
            dag_run_id,
            table_name,
            status,
            extracted_rows,
            loaded_rows,
            rejected_rows,
            status,
            error_message,
        ),
    )


# =========================================================
# TRANSFORMATION
# =========================================================

def transform_dataframe(
    df: pd.DataFrame,
    table_name: str,
) -> tuple[pd.DataFrame, int]:
    """
    Cleansing dan transformasi Bronze menuju Silver.

    Return:
        clean_df, rejected_rows
    """

    if table_name not in TABLE_CONFIG:
        raise ValueError(
            f"Konfigurasi tabel belum tersedia: {table_name}"
        )

    config = TABLE_CONFIG[table_name]

    transformed = df.copy()

    # -----------------------------------------------------
    # 1. NORMALISASI NAMA KOLOM
    # -----------------------------------------------------

    transformed.columns = [
        normalize_column_name(column)
        for column in transformed.columns
    ]

    if transformed.columns.duplicated().any():
        duplicate_columns = transformed.columns[
            transformed.columns.duplicated()
        ].tolist()

        raise ValueError(
            "Kolom duplikat setelah normalisasi: "
            f"{duplicate_columns}"
        )

    # -----------------------------------------------------
    # 2. TRIM STRING DAN EMPTY STRING MENJADI NULL
    # -----------------------------------------------------

    object_columns = transformed.select_dtypes(
        include=["object", "string"]
    ).columns

    for column in object_columns:
        transformed[column] = (
            transformed[column]
            .astype("string")
            .str.strip()
            .replace(
                {
                    "": pd.NA,
                    "nan": pd.NA,
                    "NaN": pd.NA,
                    "None": pd.NA,
                    "NULL": pd.NA,
                    "null": pd.NA,
                }
            )
        )

    # -----------------------------------------------------
    # 3. STANDARDISASI UPPER/LOWER/TITLE CASE
    # -----------------------------------------------------

    for column in config["uppercase_columns"]:
        if column in transformed.columns:
            transformed[column] = (
                transformed[column]
                .astype("string")
                .str.upper()
            )

    for column in config["lowercase_columns"]:
        if column in transformed.columns:
            transformed[column] = (
                transformed[column]
                .astype("string")
                .str.lower()
            )

    for column in config["titlecase_columns"]:
        if column in transformed.columns:
            transformed[column] = (
                transformed[column]
                .astype("string")
                .str.replace(
                    r"\s+",
                    " ",
                    regex=True,
                )
                .str.title()
            )

    # -----------------------------------------------------
    # 4. CLEANING KHUSUS CUSTOMER
    # -----------------------------------------------------

    if table_name == "customers":
        transformed["email"] = clean_email(
            transformed["email"]
        )

        transformed["phone"] = clean_phone(
            transformed["phone"]
        )

        transformed["name"] = (
            transformed["name"]
            .astype("string")
            .str.replace(
                r"\s+",
                " ",
                regex=True,
            )
            .str.strip()
        )

    # -----------------------------------------------------
    # 5. KONVERSI NUMERIC
    # -----------------------------------------------------

    for column in config["numeric_columns"]:
        if column not in transformed.columns:
            raise ValueError(
                f"Kolom numeric tidak ditemukan pada "
                f"{table_name}: {column}"
            )

        transformed[column] = clean_numeric(
            transformed[column]
        )

    # -----------------------------------------------------
    # 6. KONVERSI DATETIME
    # -----------------------------------------------------

    for column in config["datetime_columns"]:
        if column not in transformed.columns:
            raise ValueError(
                f"Kolom tanggal tidak ditemukan pada "
                f"{table_name}: {column}"
            )

        transformed[column] = clean_datetime(
            transformed[column]
        )

    if "ingestion_time" in transformed.columns:
        transformed["ingestion_time"] = clean_datetime(
            transformed["ingestion_time"]
        )

    # -----------------------------------------------------
    # 7. STANDARDISASI NILAI KATEGORI
    # -----------------------------------------------------

    for column, allowed_values in config[
        "allowed_values"
    ].items():

        if column not in transformed.columns:
            continue

        transformed[column] = (
            transformed[column]
            .astype("string")
            .str.strip()
            .str.lower()
        )

        transformed[column] = transformed[
            column
        ].where(
            transformed[column].isin(
                allowed_values
            ),
            pd.NA,
        )

    # -----------------------------------------------------
    # 8. BUSINESS RULES
    # -----------------------------------------------------

    if table_name == "products":
        transformed.loc[
            transformed["unit_price"] < 0,
            "unit_price",
        ] = pd.NA

    elif table_name == "promotions":
        transformed.loc[
            ~transformed["discount_pct"].between(
                0,
                100,
                inclusive="both",
            ),
            "discount_pct",
        ] = pd.NA

        invalid_period = (
            transformed["end_date"]
            < transformed["start_date"]
        )

        transformed.loc[
            invalid_period,
            "end_date",
        ] = pd.NaT

    elif table_name == "order_items":
        transformed.loc[
            transformed["quantity"] <= 0,
            "quantity",
        ] = pd.NA

        transformed.loc[
            transformed["unit_price"] < 0,
            "unit_price",
        ] = pd.NA

    elif table_name == "payments":
        transformed.loc[
            transformed["amount"] < 0,
            "amount",
        ] = pd.NA

        # paid_at hanya masuk akal ketika pembayaran sukses.
        transformed.loc[
            transformed["status"] != "success",
            "paid_at",
        ] = pd.NaT

    elif table_name == "shipments":
        # delivered_date hanya disimpan jika status delivered.
        transformed.loc[
            transformed["status"] != "delivered",
            "delivered_date",
        ] = pd.NaT

        invalid_delivery = (
            transformed["delivered_date"].notna()
            & transformed["shipped_date"].notna()
            & (
                transformed["delivered_date"]
                < transformed["shipped_date"]
            )
        )

        transformed.loc[
            invalid_delivery,
            "delivered_date",
        ] = pd.NaT

    elif table_name == "product_reviews":
        transformed.loc[
            ~transformed["rating"].between(
                1,
                5,
                inclusive="both",
            ),
            "rating",
        ] = pd.NA

    # -----------------------------------------------------
    # 9. DROP BARIS DENGAN REQUIRED FIELD INVALID
    # -----------------------------------------------------

    required_columns = config[
        "required_columns"
    ]

    missing_required_columns = [
        column
        for column in required_columns
        if column not in transformed.columns
    ]

    if missing_required_columns:
        raise ValueError(
            f"Required column tidak ditemukan pada "
            f"{table_name}: {missing_required_columns}"
        )

    before_required_filter = len(
        transformed
    )

    transformed = transformed.dropna(
        subset=required_columns
    )

    rejected_rows = (
        before_required_filter
        - len(transformed)
    )

    # -----------------------------------------------------
    # 10. DEDUPLIKASI BUSINESS KEY
    # -----------------------------------------------------

    business_key = config[
        "business_key"
    ]

    if "ingestion_time" in transformed.columns:
        transformed = transformed.sort_values(
            by="ingestion_time",
            na_position="first",
        )

    before_dedup = len(
        transformed
    )

    transformed = transformed.drop_duplicates(
        subset=business_key,
        keep="last",
    )

    duplicate_rows_removed = (
        before_dedup
        - len(transformed)
    )

    rejected_rows += duplicate_rows_removed

    # Satu-satunya kolom tambahan di tabel Silver.
    transformed["silver_updated_at"] = (
        datetime.now(timezone.utc)
    )

    transformed = transformed.reset_index(
        drop=True
    )

    return transformed, rejected_rows


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="final_project_silver",

    start_date=datetime(2026, 7, 1),

    # Dipicu otomatis oleh final_project_bronze.
    schedule=None,

    catchup=False,

    max_active_runs=1,

    default_args={
        "owner": "mugi",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },

    tags=[
        "final_project",
        "silver",
        "etl",
        "cleansing",
        "dynamic_mapping",
    ],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    # =====================================================
    # INIT SILVER
    # =====================================================

    @task(task_id="init_silver")
    def init_silver():
        """
        Membuat schema dan log Silver.
        """

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        hook.run(
            f"""
            CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};

            CREATE TABLE IF NOT EXISTS
                {SILVER_SCHEMA}.{SILVER_LOG_TABLE}
            (
                id BIGSERIAL PRIMARY KEY,

                dag_run_id TEXT NOT NULL,
                table_name TEXT NOT NULL,

                status TEXT NOT NULL,

                extracted_rows BIGINT
                    NOT NULL DEFAULT 0,

                loaded_rows BIGINT
                    NOT NULL DEFAULT 0,

                rejected_rows BIGINT
                    NOT NULL DEFAULT 0,

                started_at TIMESTAMP
                    DEFAULT CURRENT_TIMESTAMP,

                finished_at TIMESTAMP,

                error_message TEXT,

                created_at TIMESTAMP
                    DEFAULT CURRENT_TIMESTAMP,

                updated_at TIMESTAMP
                    DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT uq_silver_table_run
                    UNIQUE (dag_run_id, table_name)
            );

            ALTER TABLE
                {SILVER_SCHEMA}.{SILVER_LOG_TABLE}
            ADD COLUMN IF NOT EXISTS
                rejected_rows BIGINT
                NOT NULL DEFAULT 0;
            """
        )

    # =====================================================
    # DISCOVER BRONZE TABLES
    # =====================================================

    @task(task_id="discover_bronze_tables")
    def discover_bronze_tables() -> list[dict]:
        """
        Hanya memilih tabel Bronze yang sudah mempunyai
        konfigurasi cleansing di TABLE_CONFIG.
        """

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        records = hook.get_records(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
              AND table_name <> %s
            ORDER BY table_name
            """,
            parameters=(
                BRONZE_SCHEMA,
                BRONZE_LOG_TABLE,
            ),
        )

        bronze_tables = {
            row[0]
            for row in records
        }

        configured_tables = []

        for table_name in TABLE_CONFIG:
            if table_name in bronze_tables:
                configured_tables.append(
                    {
                        "table_name":
                            validate_identifier(
                                table_name
                            )
                    }
                )
            else:
                print(
                    f"Tabel belum ada di Bronze, dilewati: "
                    f"{BRONZE_SCHEMA}.{table_name}"
                )

        print(
            "Jumlah tabel yang akan diproses: "
            f"{len(configured_tables)}"
        )

        return configured_tables

    # =====================================================
    # ETL BRONZE TO SILVER
    # =====================================================

    @task(
        task_id="etl_to_silver",
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def etl_to_silver(
        table_info: dict,
    ) -> dict:
        """
        Extract dari Bronze, cleansing, transform,
        lalu load ke Silver.

        Strategi load:
        full refresh per tabel.

        DELETE dan INSERT dilakukan dalam satu transaksi,
        sehingga apabila INSERT gagal, data Silver lama
        tidak hilang.
        """

        context = get_current_context()
        dag_run_id = context["run_id"]

        table_name = validate_identifier(
            table_info["table_name"]
        )

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        engine = hook.get_sqlalchemy_engine()

        extracted_rows = 0
        loaded_rows = 0
        rejected_rows = 0

        try:
            update_silver_log(
                dag_run_id=dag_run_id,
                table_name=table_name,
                status="PROCESSING",
            )

            # -------------------------------------------------
            # EXTRACT
            # -------------------------------------------------

            with engine.connect() as conn:
                df_bronze = pd.read_sql_query(
                    text(
                        f"""
                        SELECT *
                        FROM {BRONZE_SCHEMA}.{table_name}
                        """
                    ),
                    conn,
                )

            extracted_rows = len(
                df_bronze
            )

            print(
                f"Extract {BRONZE_SCHEMA}.{table_name}: "
                f"{extracted_rows} rows"
            )

            # -------------------------------------------------
            # TRANSFORM + CLEANSING
            # -------------------------------------------------

            (
                df_silver,
                rejected_rows,
            ) = transform_dataframe(
                df=df_bronze,
                table_name=table_name,
            )

            loaded_rows = len(
                df_silver
            )

            print(
                f"Cleansing {table_name}: "
                f"loaded={loaded_rows}, "
                f"rejected={rejected_rows}"
            )

            # -------------------------------------------------
            # LOAD
            # -------------------------------------------------

            with engine.begin() as conn:
                inspector = inspect(
                    conn
                )

                target_exists = inspector.has_table(
                    table_name=table_name,
                    schema=SILVER_SCHEMA,
                )

                if target_exists:
                    conn.execute(
                        text(
                            f"""
                            DELETE FROM
                                {SILVER_SCHEMA}.{table_name}
                            """
                        )
                    )

                df_silver.to_sql(
                    name=table_name,
                    con=conn,
                    schema=SILVER_SCHEMA,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000,
                )

                conn.execute(
                    text(
                        f"""
                        INSERT INTO
                            {SILVER_SCHEMA}.{SILVER_LOG_TABLE}
                        (
                            dag_run_id,
                            table_name,
                            status,
                            extracted_rows,
                            loaded_rows,
                            rejected_rows,
                            started_at,
                            finished_at,
                            error_message,
                            updated_at
                        )
                        VALUES
                        (
                            :dag_run_id,
                            :table_name,
                            'SUCCESS',
                            :extracted_rows,
                            :loaded_rows,
                            :rejected_rows,
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP,
                            NULL,
                            CURRENT_TIMESTAMP
                        )
                        ON CONFLICT
                            (dag_run_id, table_name)
                        DO UPDATE SET
                            status = 'SUCCESS',
                            extracted_rows =
                                EXCLUDED.extracted_rows,
                            loaded_rows =
                                EXCLUDED.loaded_rows,
                            rejected_rows =
                                EXCLUDED.rejected_rows,
                            finished_at =
                                CURRENT_TIMESTAMP,
                            error_message = NULL,
                            updated_at =
                                CURRENT_TIMESTAMP
                        """
                    ),
                    {
                        "dag_run_id":
                            dag_run_id,
                        "table_name":
                            table_name,
                        "extracted_rows":
                            extracted_rows,
                        "loaded_rows":
                            loaded_rows,
                        "rejected_rows":
                            rejected_rows,
                    },
                )

            print(
                f"Load berhasil: "
                f"{SILVER_SCHEMA}.{table_name}"
            )

            return {
                "table_name":
                    table_name,
                "status":
                    "SUCCESS",
                "extracted_rows":
                    extracted_rows,
                "loaded_rows":
                    loaded_rows,
                "rejected_rows":
                    rejected_rows,
            }

        except Exception as error:
            update_silver_log(
                dag_run_id=dag_run_id,
                table_name=table_name,
                status="FAILED",
                extracted_rows=extracted_rows,
                loaded_rows=loaded_rows,
                rejected_rows=rejected_rows,
                error_message=(
                    f"{type(error).__name__}: "
                    f"{str(error)}"
                )[:2000],
            )

            raise

    # =====================================================
    # END
    # =====================================================

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed",
    )

    # =====================================================
    # TRIGGER GOLD
    # =====================================================

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="final_project_gold",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
    )

    # =====================================================
    # DEPENDENCIES
    # =====================================================

    initialized = init_silver()

    bronze_tables = discover_bronze_tables()

    silver_results = etl_to_silver.expand(
        table_info=bronze_tables
    )

    (
        start
        >> initialized
        >> bronze_tables
        >> silver_results
        >> end
        >> trigger_gold
    )