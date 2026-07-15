from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import re


# =========================================================
# CONFIGURATION
# =========================================================

UNIQUE_ID = "mugi"

RAW_DIR = "/opt/airflow/data/raw"
PROCESSED_DIR = "/opt/airflow/data/processed"

GENERATOR_PATH = (
    "/opt/airflow/dataset_generator/"
    "retail_csv_generator.py"
)

POSTGRES_CONN_ID = "postgres_dwh"

BRONZE_SCHEMA = "bronze"
LOG_TABLE = "etl_file_log"


# =========================================================
# HELPER FUNCTIONS
# =========================================================

def validate_identifier(identifier: str) -> str:
    """
    Validasi nama schema atau tabel.

    Hanya mengizinkan:
    - huruf
    - angka
    - underscore

    Nama harus dimulai dengan huruf atau underscore.
    """

    pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"

    if not re.match(pattern, identifier):
        raise ValueError(
            f"SQL identifier tidak valid: {identifier}"
        )

    return identifier


def update_file_status(
    log_id: int,
    status: str,
    error_message: str | None = None,
) -> None:
    """
    Memperbarui status file pada tabel log.

    Digunakan ketika transform atau load gagal.
    """

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    hook.run(
        f"""
        UPDATE {BRONZE_SCHEMA}.{LOG_TABLE}
        SET
            status = %s,
            error_message = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        parameters=(
            status,
            error_message,
            log_id,
        ),
    )


# =========================================================
# DAG DEFINITION
# =========================================================

with DAG(
    dag_id="final_project_bronze",

    start_date=datetime(2026, 7, 1),

    # DAG utama dijadwalkan setiap 2 menit.
    # Setelah sukses, DAG ini memicu Silver.
    schedule="*/2 * * * *",

    catchup=False,

    # Hanya satu DAG Run aktif dalam satu waktu.
    max_active_runs=1,

    default_args={
        "owner": "mugi",

        # Percobaan awal + 2 retry = maksimal 3 percobaan.
        "retries": 2,

        # Jeda sebelum mencoba ulang task.
        "retry_delay": timedelta(minutes=1),
    },

    tags=[
        "final_project",
        "bronze",
        "incremental",
        "dynamic_mapping",
    ],
) as dag:

    # =====================================================
    # START
    # =====================================================

    start = EmptyOperator(
        task_id="start"
    )

    # =====================================================
    # INITIALIZE LOG TABLE
    # =====================================================

    @task(task_id="init_log_table")
    def init_log_table():
        """
        Membuat schema Bronze dan tabel log.

        ALTER TABLE menjaga kompatibilitas jika tabel log
        sebelumnya sudah pernah dibuat.
        """

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        hook.run(
            f"""
            CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA};

            CREATE TABLE IF NOT EXISTS
                {BRONZE_SCHEMA}.{LOG_TABLE}
            (
                id SERIAL PRIMARY KEY,

                table_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,

                status TEXT NOT NULL,

                dag_run_id TEXT,

                started_at TIMESTAMP,
                loaded_at TIMESTAMP,

                error_message TEXT,

                created_at TIMESTAMP
                    DEFAULT CURRENT_TIMESTAMP,

                updated_at TIMESTAMP
                    DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT uq_etl_file_log_path
                    UNIQUE (file_path)
            );

            ALTER TABLE
                {BRONZE_SCHEMA}.{LOG_TABLE}
            ADD COLUMN IF NOT EXISTS
                dag_run_id TEXT;

            ALTER TABLE
                {BRONZE_SCHEMA}.{LOG_TABLE}
            ADD COLUMN IF NOT EXISTS
                started_at TIMESTAMP;

            ALTER TABLE
                {BRONZE_SCHEMA}.{LOG_TABLE}
            ADD COLUMN IF NOT EXISTS
                loaded_at TIMESTAMP;

            ALTER TABLE
                {BRONZE_SCHEMA}.{LOG_TABLE}
            ADD COLUMN IF NOT EXISTS
                error_message TEXT;

            ALTER TABLE
                {BRONZE_SCHEMA}.{LOG_TABLE}
            ADD COLUMN IF NOT EXISTS
                created_at TIMESTAMP
                DEFAULT CURRENT_TIMESTAMP;

            ALTER TABLE
                {BRONZE_SCHEMA}.{LOG_TABLE}
            ADD COLUMN IF NOT EXISTS
                updated_at TIMESTAMP
                DEFAULT CURRENT_TIMESTAMP;
            """
        )

    # =====================================================
    # EXTRACT
    # =====================================================

    extract = BashOperator(
        task_id="extract",

        bash_command=f"""
        set -e

        python "{GENERATOR_PATH}" \
            --unique-id "{UNIQUE_ID}" \
            --output-dir "{RAW_DIR}" \
            --once
        """,
    )

    # =====================================================
    # SCAN AND CLAIM FILES
    # =====================================================

    @task(task_id="scan_files")
    def scan_files():
        """
        Mencari dan mengklaim file CSV.

        Logika:
        - file baru        → PROCESSING
        - file SUCCESS     → dilewati
        - file PROCESSING  → dilewati
        - file FAILED      → PROCESSING dan dicoba lagi
        """

        context = get_current_context()
        dag_run_id = context["run_id"]

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        postgres_conn = hook.get_conn()
        cursor = postgres_conn.cursor()

        base_path = Path(RAW_DIR) / UNIQUE_ID

        if not base_path.exists():
            print(
                f"Folder raw belum tersedia: {base_path}"
            )
            return []

        csv_files = sorted(
            base_path.glob("*/*.csv")
        )

        print(
            f"Jumlah CSV ditemukan: {len(csv_files)}"
        )

        files_to_process = []

        try:
            for csv_file in csv_files:

                file_path = str(
                    csv_file.resolve()
                )

                file_name = csv_file.name

                table_name = validate_identifier(
                    csv_file.parent.name
                )

                cursor.execute(
                    f"""
                    INSERT INTO
                        {BRONZE_SCHEMA}.{LOG_TABLE}
                    (
                        table_name,
                        file_path,
                        file_name,
                        status,
                        dag_run_id,
                        started_at,
                        loaded_at,
                        error_message,
                        created_at,
                        updated_at
                    )
                    VALUES
                    (
                        %s,
                        %s,
                        %s,
                        'PROCESSING',
                        %s,
                        CURRENT_TIMESTAMP,
                        NULL,
                        NULL,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP
                    )

                    ON CONFLICT (file_path)
                    DO UPDATE SET
                        table_name =
                            EXCLUDED.table_name,

                        file_name =
                            EXCLUDED.file_name,

                        status =
                            'PROCESSING',

                        dag_run_id =
                            EXCLUDED.dag_run_id,

                        started_at =
                            CURRENT_TIMESTAMP,

                        loaded_at =
                            NULL,

                        error_message =
                            NULL,

                        updated_at =
                            CURRENT_TIMESTAMP

                    WHERE
                        {BRONZE_SCHEMA}.{LOG_TABLE}
                        .status = 'FAILED'

                    RETURNING id
                    """,
                    (
                        table_name,
                        file_path,
                        file_name,
                        dag_run_id,
                    ),
                )

                claimed_row = cursor.fetchone()

                if claimed_row is None:
                    cursor.execute(
                        f"""
                        SELECT status
                        FROM {BRONZE_SCHEMA}.{LOG_TABLE}
                        WHERE file_path = %s
                        """,
                        (file_path,),
                    )

                    existing_row = cursor.fetchone()

                    if existing_row:
                        print(
                            f"File dilewati: {file_name} | "
                            f"status={existing_row[0]}"
                        )

                    continue

                log_id = claimed_row[0]

                files_to_process.append(
                    {
                        "log_id": log_id,
                        "table_name": table_name,
                        "file_path": file_path,
                        "file_name": file_name,
                    }
                )

                print(
                    f"File diklaim: {file_name} | "
                    f"log_id={log_id}"
                )

            postgres_conn.commit()

        except Exception:
            postgres_conn.rollback()
            raise

        finally:
            cursor.close()
            postgres_conn.close()

        print(
            "Jumlah file yang akan diproses: "
            f"{len(files_to_process)}"
        )

        return files_to_process

    # =====================================================
    # TRANSFORM
    # =====================================================

    @task(
        task_id="transform",
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def transform(file_info: dict):
        """
        Membaca CSV mentah, menambahkan metadata,
        lalu menyimpan file hasil transformasi.
        """

        log_id = file_info["log_id"]
        file_path = file_info["file_path"]
        file_name = file_info["file_name"]

        table_name = validate_identifier(
            file_info["table_name"]
        )

        try:
            # Jika task ini sedang di-retry oleh Airflow,
            # status dikembalikan menjadi PROCESSING.
            update_file_status(
                log_id=log_id,
                status="PROCESSING",
                error_message=None,
            )

            raw_file = Path(file_path)

            if not raw_file.exists():
                raise FileNotFoundError(
                    f"Raw file tidak ditemukan: {file_path}"
                )

            df = pd.read_csv(file_path)

            # Metadata audit.
            df["source_file"] = file_name
            df["source_path"] = file_path

            df["ingestion_time"] = (
                datetime.now(timezone.utc)
                .strftime("%Y-%m-%d %H:%M:%S")
            )

            processed_path = (
                Path(PROCESSED_DIR)
                / table_name
                / f"processed_{file_name}"
            )

            processed_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            df.to_csv(
                processed_path,
                index=False,
            )

            print(
                f"Transform berhasil: {file_name}"
            )

            print(
                f"Jumlah baris: {len(df)}"
            )

            return {
                "log_id": log_id,
                "table_name": table_name,
                "file_path": file_path,
                "file_name": file_name,
                "processed_path": str(
                    processed_path
                ),
            }

        except Exception as error:
            update_file_status(
                log_id=log_id,
                status="FAILED",
                error_message=(
                    f"Transform error: {str(error)}"
                )[:2000],
            )

            # Error dilempar kembali agar Airflow menjalankan retry.
            raise

    # =====================================================
    # LOAD BRONZE
    # =====================================================

    @task(
        task_id="load_bronze",
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def load_bronze(data: dict):
        """
        Memasukkan hasil transformasi ke PostgreSQL Bronze.

        Flow:
        1. Lock baris log.
        2. Jika SUCCESS, load dilewati.
        3. Status diubah menjadi PROCESSING.
        4. Hapus data dari source_path yang sama.
        5. Insert data.
        6. Update status menjadi SUCCESS.

        DELETE, INSERT, dan update SUCCESS dilakukan
        dalam satu transaksi.
        """

        from sqlalchemy import inspect, text

        log_id = data["log_id"]
        file_path = data["file_path"]
        file_name = data["file_name"]
        processed_path = data["processed_path"]

        table_name = validate_identifier(
            data["table_name"]
        )

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        engine = hook.get_sqlalchemy_engine()

        try:
            processed_file = Path(
                processed_path
            )

            if not processed_file.exists():
                raise FileNotFoundError(
                    "Processed file tidak ditemukan: "
                    f"{processed_path}"
                )

            df = pd.read_csv(processed_path)

            required_metadata_columns = {
                "source_file",
                "source_path",
                "ingestion_time",
            }

            missing_columns = (
                required_metadata_columns
                - set(df.columns)
            )

            if missing_columns:
                raise ValueError(
                    "Kolom metadata tidak lengkap: "
                    f"{sorted(missing_columns)}"
                )

            with engine.begin() as conn:

                log_row = conn.execute(
                    text(
                        f"""
                        SELECT status
                        FROM {BRONZE_SCHEMA}.{LOG_TABLE}
                        WHERE id = :log_id
                        FOR UPDATE
                        """
                    ),
                    {
                        "log_id": log_id,
                    },
                ).fetchone()

                if log_row is None:
                    raise ValueError(
                        f"Log ID tidak ditemukan: {log_id}"
                    )

                current_status = log_row[0]

                # Melindungi dari clear atau rerun task
                # setelah file sudah berhasil dimuat.
                if current_status == "SUCCESS":
                    print(
                        f"File {file_name} sudah SUCCESS. "
                        "Load dilewati."
                    )

                    return {
                        "status": "SKIPPED",
                        "file_name": file_name,
                    }

                # FAILED diperbolehkan karena mungkin ini
                # adalah retry task Airflow.
                if current_status not in {
                    "PROCESSING",
                    "FAILED",
                }:
                    raise ValueError(
                        f"Status file tidak valid: "
                        f"{current_status}"
                    )

                conn.execute(
                    text(
                        f"""
                        UPDATE {BRONZE_SCHEMA}.{LOG_TABLE}
                        SET
                            status = 'PROCESSING',
                            error_message = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :log_id
                        """
                    ),
                    {
                        "log_id": log_id,
                    },
                )

                inspector = inspect(conn)

                table_exists = inspector.has_table(
                    table_name=table_name,
                    schema=BRONZE_SCHEMA,
                )

                # Idempotency berdasarkan source_path.
                if table_exists:
                    delete_result = conn.execute(
                        text(
                            f"""
                            DELETE FROM
                                {BRONZE_SCHEMA}.{table_name}
                            WHERE source_path = :source_path
                            """
                        ),
                        {
                            "source_path": file_path,
                        },
                    )

                    print(
                        "Jumlah data lama dihapus: "
                        f"{delete_result.rowcount}"
                    )

                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=BRONZE_SCHEMA,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000,
                )

                conn.execute(
                    text(
                        f"""
                        UPDATE {BRONZE_SCHEMA}.{LOG_TABLE}
                        SET
                            status = 'SUCCESS',
                            loaded_at = CURRENT_TIMESTAMP,
                            error_message = NULL,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :log_id
                        """
                    ),
                    {
                        "log_id": log_id,
                    },
                )

            print(
                f"Load berhasil: {file_name}"
            )

            print(
                "Tabel tujuan: "
                f"{BRONZE_SCHEMA}.{table_name}"
            )

            print(
                f"Jumlah baris dimuat: {len(df)}"
            )

            return {
                "status": "SUCCESS",
                "table_name": table_name,
                "file_name": file_name,
                "rows_loaded": len(df),
            }

        except Exception as error:
            # Transaksi DELETE + INSERT otomatis rollback.
            update_file_status(
                log_id=log_id,
                status="FAILED",
                error_message=(
                    f"Load error: {str(error)}"
                )[:2000],
            )

            # Airflow akan menjalankan retry task.
            raise

    # =====================================================
    # END
    # =====================================================

    end = EmptyOperator(
        task_id="end",

        # Tetap bisa selesai ketika tidak ada file baru
        # dan mapped task berstatus skipped.
        trigger_rule="none_failed",
    )

    # =====================================================
    # TRIGGER SILVER
    # =====================================================

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="final_project_silver",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        reset_dag_run=False,
    )

    # =====================================================
    # TASK DEPENDENCIES
    # =====================================================

    init = init_log_table()

    scanned_files = scan_files()

    transformed_data = transform.expand(
        file_info=scanned_files
    )

    loaded_data = load_bronze.expand(
        data=transformed_data
    )

    (
        start
        >> init
        >> extract
        >> scanned_files
        >> transformed_data
        >> loaded_data
        >> end
        >> trigger_silver
    )